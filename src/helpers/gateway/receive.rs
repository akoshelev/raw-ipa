use std::marker::PhantomData;

use dashmap::DashMap;
use futures::Stream;
use std::sync::atomic::AtomicU64;
use dashmap::mapref::entry::Entry;
use tracing::{Instrument, instrument};

use crate::{
    helpers::{buffers::UnorderedReceiver, ChannelId, Error, Message, Transport},
    protocol::RecordId,
};
use crate::helpers::Role;

static mut ATOMIC_CNT: AtomicU64 = AtomicU64::new(0);

/// Receiving end end of the gateway channel.
pub struct ReceivingEnd<T: Transport, M: Message> {
    channel_id: ChannelId,
    unordered_rx: UR<T>,
    _phantom: PhantomData<M>,
    pub my_role: Role,
}

/// Receiving channels, indexed by (role, step).
pub(super) struct GatewayReceivers<T: Transport> {
    inner: DashMap<ChannelId, UR<T>>,
}

pub(super) type UR<T> = UnorderedReceiver<
    <T as Transport>::RecordsStream,
    <<T as Transport>::RecordsStream as Stream>::Item,
>;

impl<T: Transport, M: Message> ReceivingEnd<T, M> {
    pub(super) fn new(my_role: Role,channel_id: ChannelId, rx: UR<T>) -> Self {
        Self {
            channel_id,
            my_role,
            unordered_rx: rx,
            _phantom: PhantomData,
        }
    }

    /// Receive message associated with the given record id. This method does not return until
    /// message is actually received and deserialized.
    ///
    /// ## Errors
    /// Returns an error if receiving fails
    ///
    /// ## Panics
    /// This will panic if message size does not fit into 8 bytes and it somehow got serialized
    /// and sent to this helper.
    pub async fn receive(&self, record_id: RecordId) -> Result<M, Error> {
        tracing::trace!("{:?} start {:?}/{record_id:?}", std::thread::current().id(), self.channel_id);
        let r = self.unordered_rx
            .recv::<M, _>(record_id, self.channel_id.clone())
            .await
            .map_err(|e| Error::ReceiveError {
                source: self.channel_id.role,
                step: self.channel_id.gate.to_string(),
                inner: Box::new(e),
            });
        tracing::trace!("{:?} complete {:?}/{record_id:?}", std::thread::current().id(), self.channel_id);

        r
    }
}

impl<T: Transport> Default for GatewayReceivers<T> {
    fn default() -> Self {
        Self {
            inner: DashMap::default(),
        }
    }
}

impl<T: Transport> GatewayReceivers<T> {
    pub fn get_or_create<F: FnOnce() -> UR<T>>(&self, channel_id: &ChannelId, ctr: F) -> UR<T> {
        let receivers = &self.inner;
        match receivers.entry(channel_id.clone()) {
            Entry::Occupied(entry) => {
                entry.get().clone()
            }
            Entry::Vacant(entry) => {
                let stream = ctr();
                entry.insert(stream.clone());

                stream
            }
        }
        // if let Some(recv) = receivers.get(channel_id) {
        //     recv.clone()
        // } else {
        //     let stream = ctr();
        //     if let Some(_) = receivers.insert(channel_id.clone(), stream.clone()) {
        //         panic!("duplicate channel_id {:?}", channel_id)
        //     }
        //     stream
        // }
    }
}
