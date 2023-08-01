use crate::{
    helpers::{ChannelId, Error, Message, Transport},
    protocol::RecordId,
};
use dashmap::DashMap;
use futures::Stream;
use std::marker::PhantomData;

#[cfg(feature = "idle-tracking")]
use crate::helpers::buffers::IdleTrackUnorderedReceiver;

#[cfg(not(feature = "idle-tracking"))]
use crate::helpers::buffers::UnorderedReceiver;

/// Receiving end end of the gateway channel.
pub struct ReceivingEnd<T: Transport, M: Message> {
    channel_id: ChannelId,
    unordered_rx: UR<T>,
    _phantom: PhantomData<M>,
}

/// Receiving channels, indexed by (role, step).
pub(super) struct GatewayReceivers<T: Transport> {
    inner: DashMap<ChannelId, UR<T>>,
}

#[cfg(feature = "idle-tracking")]
pub(super) type UR<T> = IdleTrackUnorderedReceiver<
    <T as Transport>::RecordsStream,
    <<T as Transport>::RecordsStream as Stream>::Item,
>;

#[cfg(not(feature = "idle-tracking"))]
pub(super) type UR<T> = UnorderedReceiver<
    <T as Transport>::RecordsStream,
    <<T as Transport>::RecordsStream as Stream>::Item,
>;

impl<T: Transport, M: Message> ReceivingEnd<T, M> {
    pub(super) fn new(channel_id: ChannelId, rx: UR<T>) -> Self {
        Self {
            channel_id,
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
        self.unordered_rx
            .recv::<M, _>(record_id)
            .await
            .map_err(|e| Error::ReceiveError {
                source: self.channel_id.role,
                step: self.channel_id.gate.to_string(),
                inner: Box::new(e),
            })
    }
}

impl<T: Transport> Default for GatewayReceivers<T> {
    fn default() -> Self {
        Self {
            inner: DashMap::default(),
        }
    }
}
#[cfg(feature = "idle-tracking")]
use crate::helpers::buffers::LoggingRanges;

impl<T: Transport> GatewayReceivers<T> {
    pub fn get_or_create<F: FnOnce() -> UR<T>>(&self, channel_id: &ChannelId, ctr: F) -> UR<T> {
        let receivers = &self.inner;
        if let Some(recv) = receivers.get(channel_id) {
            recv.clone()
        } else {
            let stream = ctr();
            receivers.insert(channel_id.clone(), stream.clone());
            stream
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
    pub fn get_waiting_messages(&self) -> std::collections::HashMap<ChannelId, LoggingRanges> {
        self.inner
            .iter()
            .filter_map(|entry| {
                let (channel_id, rec) = entry.pair();
                let message = rec.get_waiting_messages();
                if message.is_empty() {
                    None
                } else {
                    Some((channel_id.clone(), message))
                }
            })
            .collect()
    }
}
