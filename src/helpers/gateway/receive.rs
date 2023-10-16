use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    marker::PhantomData,
};

use dashmap::{mapref::entry::Entry, DashMap};
use delegate::delegate;
use futures::Stream;

use crate::{
    helpers::{
        buffers::UnorderedReceiver,
        gateway::{
            observable::{ObserveState, Observed},
            to_ranges,
        },
        ChannelId, Error, Message, Transport, TransportImpl,
    },
    protocol::RecordId,
};

/// Receiving end end of the gateway channel.
pub struct ReceivingEnd<M: Message> {
    channel_id: ChannelId,
    unordered_rx: UR,
    _phantom: PhantomData<M>,
}

impl<M: Message> Observed<ReceivingEnd<M>> {
    delegate! {
        to { self.inc_sn(); self.inner() } {
            #[inline]
            pub async fn receive(&self, record_id: RecordId) -> Result<M, Error>;
        }
    }
}

pub struct WaitingTasks(HashMap<ChannelId, Vec<String>>);

impl Debug for WaitingTasks {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for (channel, records) in &self.0 {
            write!(
                f,
                "\n\"{:?}\", from={:?}. Waiting to receive records {:?}.",
                channel.gate, channel.role, records
            )?;
        }

        Ok(())
    }
}

/// Receiving channels, indexed by (role, step).
#[derive(Default, Clone)]
pub(super) struct GatewayReceivers {
    inner: DashMap<ChannelId, UR>,
}

pub(super) type UR = UnorderedReceiver<
    <TransportImpl as Transport>::RecordsStream,
    <<TransportImpl as Transport>::RecordsStream as Stream>::Item,
>;

impl<M: Message> ReceivingEnd<M> {
    pub(super) fn new(channel_id: ChannelId, rx: UR) -> Self {
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

impl GatewayReceivers {
    pub fn get_or_create<F: FnOnce() -> UR>(&self, channel_id: &ChannelId, ctr: F) -> UR {
        // TODO: raw entry API if it becomes available to avoid cloning the key
        match self.inner.entry(channel_id.clone()) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let stream = ctr();
                entry.insert(stream.clone());

                stream
            }
        }
    }
}

impl ObserveState for GatewayReceivers {
    type State = WaitingTasks;

    fn get_state(&self) -> Option<Self::State> {
        let mut map = HashMap::default();
        for entry in &self.inner {
            let channel = entry.key();
            if let Some(waiting) = to_ranges(entry.value().waiting()).get_state() {
                map.insert(channel.clone(), waiting);
            }
        }

        Some(WaitingTasks(map))
    }
}
