use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    marker::PhantomData,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
};

use dashmap::{mapref::entry::Entry, DashMap};
use delegate::delegate;
use futures::Stream;
use typenum::Unsigned;

use crate::{
    helpers::{
        buffers::OrderingSender,
        gateway::{
            observable::{ObserveState, Observed},
            to_ranges,
        },
        ChannelId, Error, Message, Role, TotalRecords,
    },
    protocol::RecordId,
    sync::Arc,
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

impl<M: Message> Observed<SendingEnd<M>> {
    delegate! {
        to { self.inc_sn(); self.inner() } {
            #[inline]
            pub async fn send(&self, record_id: RecordId, msg: M) -> Result<(), Error>;
        }
    }
}

/// Sending channels, indexed by (role, step).
#[derive(Default, Clone)]
pub(super) struct GatewaySenders {
    inner: DashMap<ChannelId, Arc<GatewaySender>>,
}

pub struct WaitingTasks(HashMap<ChannelId, Vec<String>>);

impl Debug for WaitingTasks {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for (channel, records) in &self.0 {
            write!(
                f,
                "\n\"{:?}\", to={:?}. Waiting to send records {:?}.",
                channel.gate, channel.role, records
            )?;
        }

        Ok(())
    }
}

impl ObserveState for GatewaySenders {
    type State = WaitingTasks;

    fn get_state(&self) -> Option<Self::State> {
        let mut state = HashMap::new();
        for entry in &self.inner {
            let channel = entry.key();
            let sender = entry.value();
            if let Some(sender_state) = sender.get_state() {
                state.insert(channel.clone(), sender_state);
            }
        }

        Some(WaitingTasks(state))
    }
}

pub(super) struct GatewaySender {
    channel_id: ChannelId,
    ordering_tx: OrderingSender,
    total_records: TotalRecords,
}

pub(super) struct GatewaySendStream {
    inner: Arc<GatewaySender>,
}

impl ObserveState for GatewaySender {
    type State = Vec<String>;

    fn get_state(&self) -> Option<Self::State> {
        let waiting_indices = self.ordering_tx.waiting();
        to_ranges(waiting_indices).get_state()
    }
}

impl GatewaySender {
    fn new(channel_id: ChannelId, tx: OrderingSender, total_records: TotalRecords) -> Self {
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

        // TODO: raw entry API would be nice to have here but it's not exposed yet
        match self.inner.entry(channel_id.clone()) {
            Entry::Occupied(entry) => (Arc::clone(entry.get()), None),
            Entry::Vacant(entry) => {
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
                    OrderingSender::new(write_size, SPARE.unwrap()),
                    total_records,
                ));
                entry.insert(Arc::clone(&sender));

                (
                    Arc::clone(&sender),
                    Some(GatewaySendStream { inner: sender }),
                )
            }
        }
    }
}

impl Stream for GatewaySendStream {
    type Item = Vec<u8>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::get_mut(self).inner.ordering_tx.take_next(cx)
    }
}
