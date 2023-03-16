use dashmap::DashMap;
use std::{marker::PhantomData, num::NonZeroUsize};
use std::any::type_name;
use typenum::Unsigned;
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
    channel_id: ChannelId,
    my_role: Role,
    ordering_tx: Arc<OrderingSender>,
    total_records: TotalRecords,
    _phantom: PhantomData<M>,
}

/// Sending channels, indexed by (role, step).
#[derive(Default)]
pub(super) struct GatewaySenders {
    inner: DashMap<ChannelId, Arc<OrderingSender>>,
}

impl<M: Message> SendingEnd<M> {
    pub(super) fn new(
        channel_id: ChannelId,
        my_role: Role,
        tx: Arc<OrderingSender>,
        total_records: TotalRecords,
    ) -> Self {
        Self {
            channel_id,
            my_role,
            ordering_tx: tx,
            total_records,
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
        let close = if let TotalRecords::Specified(count) = self.total_records {
            match usize::from(record_id) {
                v if v >= count.get() => {
                    return Err(Error::TooManyRecords {
                        record_id,
                        channel_id: self.channel_id.clone(),
                        total_records: self.total_records,
                    });
                },
                v => v == count.get() - 1
            }
        } else {
            false
        };

        metrics::increment_counter!(RECORDS_SENT,
            STEP => self.channel_id.step.as_ref().to_string(),
            ROLE => self.my_role.as_static_str()
        );

        // TODO: make OrderingSender::send fallible
        // TODO: test channel close
        let r = Ok(self.ordering_tx
            .send(record_id.into(), msg)
            .await);

        if close {
            self.ordering_tx.close(self.total_records.try_into().unwrap()).await;
        }

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
    ) -> (
        Arc<OrderingSender>,
        Option<OrderedStream<Arc<OrderingSender>>>,
    ) {
        let senders = &self.inner;
        if let Some(sender) = senders.get(channel_id) {
            (Arc::clone(&sender), None)
        } else {
            // todo: figure out the right spare size. It seems that in case where capacity mod M::size == 0
            // we don't really need spare, but it cannot be set to 0 or any value < M::size
            let sender = Arc::new(OrderingSender::new(capacity, NonZeroUsize::new(M::Size::USIZE + 1).unwrap()));
            senders.insert(channel_id.clone(), Arc::clone(&sender));
            let stream = sender.as_rc_stream();
            (sender, Some(stream))
        }
    }
}
