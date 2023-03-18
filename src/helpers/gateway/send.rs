use dashmap::DashMap;
use std::{marker::PhantomData, num::NonZeroUsize};
use std::any::type_name;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use futures::Stream;
use typenum::{Or, Unsigned};
use crate::sync::Arc;

use crate::{
    helpers::{
        buffers::{ordering_mpsc, OrderingMpscReceiver, OrderingMpscSender},
        ChannelId, Error, Message, Role, TotalRecords,
    },
    protocol::RecordId,
    telemetry::{
        labels::{ROLE, STEP},
        metrics::RECORDS_SENT,
    },
};
use crate::helpers::buffers::{OrderedStream, OrderingSender};


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

pub(super) struct GatewaySender {
    channel_id: ChannelId,
    ordering_tx: OrderingSender,
    total_records: TotalRecords,
    records_sent: AtomicUsize,
}

pub(super) struct GatewaySendStream {
    inner: Arc<GatewaySender>
}

impl GatewaySender {
    fn new(
        channel_id: ChannelId,
        tx: OrderingSender,
        total_records: TotalRecords,
    ) -> Self {
        Self {
            channel_id,
            ordering_tx: tx,
            total_records,
            records_sent: AtomicUsize::new(0)
        }
    }

    pub async fn send<M: Message>(&self, record_id: RecordId, msg: M) -> Result<(), Error> {
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
        let r = Ok(self.ordering_tx
            .send(record_id.into(), msg)
            .await);

        let records_sent = self.records_sent.fetch_add(1, Ordering::AcqRel);
        if matches!(self.total_records, TotalRecords::Unspecified) {
            println!("unspecified!");
        }
        println!("{record_id:?} sent through channel {:?}, total: {records_sent}, expected: {:?}", self.channel_id, self.total_records);
        if Some(records_sent + 1) == self.total_records.count() {
            println!("closing channel");
            self.ordering_tx.close(records_sent + 1).await
        }

        r
    }
}

impl <M: Message> SendingEnd<M> {
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
    /// If send operation fails or [`record_id`] exceeds the channel limit set by [`set_total_records`]
    /// call.
    ///
    /// [`set_total_records`]: crate::protocol::context::Context::set_total_records
    pub async fn send(&self, record_id: RecordId, msg: M) -> Result<(), Error> {
        let r = self.inner.send(record_id, msg).await;
        metrics::increment_counter!(RECORDS_SENT,
            STEP => self.channel_id.step.as_ref().to_string(),
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
        total_records: TotalRecords,
    ) -> (
        Arc<GatewaySender>,
        Option<GatewaySendStream>,
    ) {
        let senders = &self.inner;
        if let Some(sender) = senders.get(channel_id) {
            (Arc::clone(&sender), None)
        } else {
            // todo: capacity is ignored
            let capacity_bytes = NonZeroUsize::new(4096).unwrap();
            let spare_bytes = NonZeroUsize::new(64).unwrap();
            // a little trick - if number of records is indeterminate, set the spare capacity
            // to be >> buffer capacity. What it means is that write carrying a single byte  will
            // enable stream to flush the data and make it available on the receiving side.
            // Effectively it disables buffering, but it does not mean that receiver will get one
            // item at a time, because polling is still greedy.
            // This mode is clearly inefficient, so it is ideal if we don't have a lot of cases
            // where we need to put the sender in this mode.
            let ordering_sender = if matches!(total_records, TotalRecords::Indeterminate) {
                OrderingSender::new(NonZeroUsize::new(1).unwrap(), capacity_bytes)
            } else {
                OrderingSender::new(capacity_bytes, spare_bytes)
            };

            // , total_records));
            let sender = Arc::new(GatewaySender::new(channel_id.clone(), ordering_sender, total_records));
            senders.insert(channel_id.clone(), Arc::clone(&sender));
            let stream = GatewaySendStream { inner: Arc::clone(&sender) };
            (sender, Some(stream))
        }
    }
}

impl Stream for GatewaySendStream {
    type Item = Vec<u8>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::get_mut(self).inner.ordering_tx.take_next(cx)
    }
}
