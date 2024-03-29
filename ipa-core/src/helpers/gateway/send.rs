use std::{
    borrow::Borrow,
    marker::PhantomData,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
};

use dashmap::{mapref::entry::Entry, DashMap};
use futures::Stream;
use typenum::Unsigned;
use crate::helpers::Transport;

use crate::{
    helpers::{
        buffers::OrderingSender, ChannelId, Error, HelperChannelId, Message, Role, TotalRecords,
        TransportIdentity,
    },
    protocol::RecordId,
    sync::Arc,
    telemetry::{
        labels::{ROLE, STEP},
        metrics::{BYTES_SENT, RECORDS_SENT},
    },
};
use crate::helpers::gateway::TransportContainer;
use crate::helpers::routing::RouteId;
use crate::helpers::ShardChannelId;
use crate::protocol::QueryId;
use crate::secret_sharing::Sendable;
use crate::sharding::ShardIndex;

/// Sending end of the gateway channel.
pub struct SendingEnd<I: TransportIdentity, M> {
    sender_id: I,
    channel_id: ChannelId<I>,
    inner: Arc<GatewaySender<I>>,
    _phantom: PhantomData<M>,
}

/// Sending channels, indexed by (role, step).
pub(super) struct GatewaySenders<I> {
    pub(super) inner: DashMap<ChannelId<I>, Arc<GatewaySender<I>>>,
}

pub(super) struct GatewaySender<I> {
    channel_id: ChannelId<I>,
    ordering_tx: OrderingSender,
    total_records: TotalRecords,
}

pub(super) struct GatewaySendStream<I> {
    inner: Arc<GatewaySender<I>>,
}

impl <I: TransportIdentity> Default for GatewaySenders<I> {
    fn default() -> Self {
        Self {
            inner: DashMap::default()
        }
    }
}


impl <I: TransportIdentity> GatewaySender<I> {
    fn new(channel_id: ChannelId<I>, tx: OrderingSender, total_records: TotalRecords) -> Self {
        Self {
            channel_id,
            ordering_tx: tx,
            total_records,
        }
    }

    pub async fn send<M: Message, B: Borrow<M>>(
        &self,
        record_id: RecordId,
        msg: B,
    ) -> Result<(), Error<I>> {
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

    #[cfg(feature = "stall-detection")]
    pub fn waiting(&self) -> Vec<usize> {
        self.ordering_tx.waiting()
    }

    #[cfg(feature = "stall-detection")]
    pub fn total_records(&self) -> TotalRecords {
        self.total_records
    }
}

impl<I: TransportIdentity, M: Message> SendingEnd<I, M> {
    pub(super) fn new(sender: Arc<GatewaySender<I>>, id: I, channel_id: &ChannelId<I>) -> Self {
        Self {
            sender_id: id,
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
    #[tracing::instrument(level = "trace", "send", skip_all, fields(i = %record_id, total = %self.inner.total_records, to = ?self.channel_id.peer, gate = ?self.channel_id.gate.as_ref()))]
    pub async fn send<B: Borrow<M>>(&self, record_id: RecordId, msg: B) -> Result<(), Error<I>> {
        let r = self.inner.send(record_id, msg).await;
        metrics::increment_counter!(RECORDS_SENT,
            STEP => self.channel_id.gate.as_ref().to_string(),
            ROLE => self.sender_id.as_str(),
        );
        metrics::counter!(BYTES_SENT, M::Size::U64,
            STEP => self.channel_id.gate.as_ref().to_string(),
            ROLE => self.sender_id.as_str(),
        );

        r
    }
}
// impl GatewaySenders<Role> {
//     pub fn get_or_create<M: Sendable>(&self,
//                                      channel_id: HelperChannelId,
//                                      capacity: NonZeroUsize,
//                                      total_records: TotalRecords
//     ) -> (Arc<GatewaySender<Role>>, Option<GatewaySendStream<Role>>) {
//         self.make_channel::<M>(&channel_id, capacity, total_records)
//     }
// }
//
// impl GatewaySenders<ShardIndex> {
//     pub fn get_or_create<M: Sendable>(&self,
//                                      channel_id: ShardChannelId,
//                                      capacity: NonZeroUsize,
//                                      total_records: TotalRecords
//     ) -> (Arc<GatewaySender<ShardIndex>>, Option<GatewaySendStream<ShardIndex>>) {
//         self.make_channel::<M>(&channel_id, capacity, total_records)
//     }
// }


impl <I: TransportIdentity> GatewaySenders<I> {


    /// Returns or creates a new communication channel. In case if channel is newly created,
    /// returns the receiving end of it as well. It must be sent over to the receiver in order for
    /// messages to get through.
    pub fn get_or_create<M: Message>(
        &self,
        channel_id: &ChannelId<I>,
        capacity: NonZeroUsize,
        total_records: TotalRecords, // TODO track children for indeterminate senders
    ) -> (Arc<GatewaySender<I>>, Option<GatewaySendStream<I>>) {
        assert!(
            total_records.is_specified(),
            "unspecified total records for {channel_id:?}"
        );

        // TODO: raw entry API would be nice to have here but it's not exposed yet
        match self.inner.entry(channel_id.clone()) {
            Entry::Occupied(entry) => (Arc::clone(entry.get()), None),
            Entry::Vacant(entry) => {
                // Spare buffer is not required when messages have uniform size and buffer is a
                // multiple of that size.
                const SPARE: usize = 0;
                // a little trick - if number of records is indeterminate, set the capacity to one
                // message.  Any send will wake the stream reader then, effectively disabling
                // buffering.  This mode is clearly inefficient, so avoid using this mode.
                let write_size = if total_records.is_indeterminate() {
                    NonZeroUsize::new(M::Size::USIZE).unwrap()
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
                    OrderingSender::new(write_size, SPARE),
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

impl <I> Stream for GatewaySendStream<I> {
    type Item = Vec<u8>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::get_mut(self).inner.ordering_tx.take_next(cx)
    }
}
