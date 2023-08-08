use crate::{ff::Serializable, sync::Arc};
use dashmap::DashMap;
use futures::Stream;
use std::{
    marker::PhantomData,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
};
use typenum::Unsigned;

use crate::{
    helpers::{ChannelId, Error, Message, Role, TotalRecords},
    protocol::RecordId,
    telemetry::{
        labels::{ROLE, STEP},
        metrics::{BYTES_SENT, RECORDS_SENT},
    },
};

/// Sending end of the gateway channel.
pub struct SendingEnd<M: Message> {
    sender_role: Role,
    channel_id: ChannelId,
    inner: Arc<GatewaySender>,
    _phantom: PhantomData<M>,
}

/// Sending channels, indexed by (role, step).
#[derive(Default)]
pub(super) struct GatewaySenders {
    inner: DashMap<ChannelId, Arc<GatewaySender>>,
}

#[cfg(feature = "idle-tracking")]
use crate::helpers::buffers::IdleTrackOrderingSender;
#[cfg(feature = "idle-tracking")]
type OrderingSenderType = IdleTrackOrderingSender;

#[cfg(not(feature = "idle-tracking"))]
use crate::helpers::buffers::OrderingSender;

#[cfg(not(feature = "idle-tracking"))]
type OrderingSenderType = OrderingSender;

#[cfg(feature = "idle-tracking")]
use crate::helpers::buffers::LoggingRanges;

pub(super) struct GatewaySender {
    channel_id: ChannelId,
    ordering_tx: OrderingSenderType,
    total_records: TotalRecords,
}

pub(super) struct GatewaySendStream {
    inner: Arc<GatewaySender>,
}

impl GatewaySender {
    fn new(channel_id: ChannelId, tx: OrderingSenderType, total_records: TotalRecords) -> Self {
        Self {
            channel_id,
            ordering_tx: tx,
            total_records,
        }
    }

    pub async fn send<M: Message>(&self, record_id: RecordId, msg: M) -> Result<(), Error> {
        debug_assert!(
            self.total_records.is_specified(),
            "total_records cannot be unspecified when sending"
        );
        if let TotalRecords::Specified(count) = self.total_records {
            if usize::from(record_id) >= count.get() {
                return Err(Error::TooManyRecords {
                    record_id,
                    channel_id: self.channel_id.clone(),
                    total_records: self.total_records,
                });
            }
        }

        // TODO: make OrderingSender::send fallible
        // TODO: test channel close
        let i = usize::from(record_id);
        self.ordering_tx.send(i, msg).await;
        if self.total_records.is_last(record_id) {
            self.ordering_tx.close(i + 1).await;
        }
        Ok(())
    }

    #[cfg(feature = "idle-tracking")]
    fn check_idle_and_reset(&self) -> bool {
        self.ordering_tx.check_idle_and_reset()
    }

    #[cfg(feature = "idle-tracking")]
    fn get_missing_messages(&self) -> Vec<LoggingRanges> {
        self.ordering_tx.get_missing_messages()
    }
}

impl<M: Message> SendingEnd<M> {
    pub(super) fn new(sender: Arc<GatewaySender>, role: Role, channel_id: &ChannelId) -> Self {
        Self {
            sender_role: role,
            channel_id: channel_id.clone(),
            inner: sender,
            _phantom: PhantomData,
        }
    }

    /// Sends the given message to the recipient. This method will block if there is no enough
    /// capacity to hold the message and will return only after message has been confirmed
    /// for sending.
    ///
    /// ## Errors
    /// If send operation fails or `record_id` exceeds the channel limit set by [`set_total_records`]
    /// call.
    ///
    /// [`set_total_records`]: crate::protocol::context::Context::set_total_records
    pub async fn send(&self, record_id: RecordId, msg: M) -> Result<(), Error> {
        let r = self.inner.send(record_id, msg).await;
        metrics::increment_counter!(RECORDS_SENT,
            STEP => self.channel_id.gate.as_ref().to_string(),
            ROLE => self.sender_role.as_static_str()
        );
        metrics::counter!(BYTES_SENT, M::Size::U64,
            STEP => self.channel_id.gate.as_ref().to_string(),
            ROLE => self.sender_role.as_static_str()
        );

        r
    }
}

impl GatewaySenders {
    /// Returns or creates a new communication channel. In case if channel is newly created,
    /// returns the receiving end of it as well. It must be send over to the receiver in order for
    /// messages to get through.
    pub(crate) fn get_or_create<M: Message>(
        &self,
        channel_id: &ChannelId,
        capacity: NonZeroUsize,
        total_records: TotalRecords, // TODO track children for indeterminate senders
    ) -> (Arc<GatewaySender>, Option<GatewaySendStream>) {
        assert!(
            total_records.is_specified(),
            "unspecified total records for {channel_id:?}"
        );
        let senders = &self.inner;
        if let Some(sender) = senders.get(channel_id) {
            (Arc::clone(&sender), None)
        } else {
            const SPARE: Option<NonZeroUsize> = NonZeroUsize::new(64);
            // a little trick - if number of records is indeterminate, set the capacity to 1.
            // Any send will wake the stream reader then, effectively disabling buffering.
            // This mode is clearly inefficient, so avoid using this mode.
            let write_size = if total_records.is_indeterminate() {
                NonZeroUsize::new(1).unwrap()
            } else {
                // capacity is defined in terms of number of elements, while sender wants bytes
                // so perform the conversion here
                capacity
                    .checked_mul(
                        NonZeroUsize::new(M::Size::USIZE)
                            .expect("Message size should be greater than 0"),
                    )
                    .expect("capacity should not overflow")
            };

            let sender = Arc::new(GatewaySender::new(
                channel_id.clone(),
                OrderingSenderType::new(
                    write_size,
                    SPARE.unwrap(),
                    <M as Serializable>::Size::to_usize(),
                ),
                total_records,
            ));
            if senders
                .insert(channel_id.clone(), Arc::clone(&sender))
                .is_some()
            {
                tracing::error!("Channel {channel_id:?} was already created");
                panic!("TODO - make sender creation contention less dangerous");
            }
            let stream = GatewaySendStream {
                inner: Arc::clone(&sender),
            };
            (sender, Some(stream))
        }
    }

    #[cfg(feature = "idle-tracking")]
    pub fn check_idle_and_reset(&self) -> bool {
        let mut rst = true;
        for entry in self.inner.iter() {
            rst &= entry.value().check_idle_and_reset();
        }
        rst
    }

    #[cfg(feature = "idle-tracking")]
    pub fn get_all_missing_messages(
        &self,
    ) -> std::collections::HashMap<ChannelId, Vec<LoggingRanges>> {
        self.inner
            .iter()
            .filter_map(|entry| {
                let mising_messages = entry.value().get_missing_messages();
                if mising_messages.is_empty() {
                    None
                } else {
                    Some((entry.key().clone(), mising_messages))
                }
            })
            .collect()
    }
}

impl Stream for GatewaySendStream {
    type Item = Vec<u8>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::get_mut(self).inner.ordering_tx.take_next(cx)
    }
}
