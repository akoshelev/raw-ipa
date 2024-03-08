mod transport;
pub use transport::Setup;

use crate::{
    helpers::{HelperIdentity, TransportCallbacks},
    sync::{Arc, Weak},
};
// pub use crate::helpers::transport::in_memory::shard::InMemoryShardNetwork;
// pub use crate::helpers::transport::in_memory::shard::InMemoryGateBoundConnector;
pub use crate::helpers::transport::TransportIdentity;
use crate::sharding::ShardId;

pub type InMemoryTransport<O> = Weak<transport::InMemoryTransport<O>>;
/// FIXME: create a container similar to InMemoryNetwork that will initialize shards
pub type OwnedInMemoryTransport<O> = Arc<transport::InMemoryTransport<O>>;

/// Container for all active transports
#[derive(Clone)]
pub struct InMemoryNetwork {
    pub transports: [Arc<transport::InMemoryTransport<HelperIdentity>>; 3],
}

impl Default for InMemoryNetwork {
    fn default() -> Self {
        Self::new([
            TransportCallbacks::default(),
            TransportCallbacks::default(),
            TransportCallbacks::default(),
        ])
    }
}

#[allow(dead_code)]
impl InMemoryNetwork {
    #[must_use]
    pub fn new(callbacks: [TransportCallbacks<InMemoryTransport<HelperIdentity>>; 3]) -> Self {
        let [mut first, mut second, mut third]: [_; 3] =
            HelperIdentity::make_three().map(Setup::new);

        first.connect(&mut second);
        second.connect(&mut third);
        third.connect(&mut first);

        let [cb1, cb2, cb3] = callbacks;

        Self {
            transports: [first.start(cb1), second.start(cb2), third.start(cb3)],
        }
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn helper_identities(&self) -> [HelperIdentity; 3] {
        self.transports
            .iter()
            .map(|t| t.identity())
            .collect::<Vec<_>>()
            .try_into()
            .unwrap()
    }

    /// Returns the transport to communicate with the given helper.
    ///
    /// ## Panics
    /// If [`HelperIdentity`] is somehow points to a non-existent helper, which shouldn't happen.
    #[must_use]
    pub fn transport(&self, id: HelperIdentity) -> InMemoryTransport<HelperIdentity> {
        self.transports
            .iter()
            .find(|t| t.identity() == id)
            .map_or_else(|| panic!("No transport for helper {id:?}"), Arc::downgrade)
    }

    #[allow(clippy::missing_panics_doc)]
    #[must_use]
    pub fn transports(&self) -> [InMemoryTransport<HelperIdentity>; 3] {
        let transports: [InMemoryTransport<_>; 3] = self
            .transports
            .iter()
            .map(Arc::downgrade)
            .collect::<Vec<_>>()
            .try_into()
            .map_err(|_| "What is dead may never die")
            .unwrap();
        transports
    }

    /// Reset all transports to the clear state.
    pub fn reset(&self) {
        for t in &self.transports {
            t.reset();
        }
    }
}
