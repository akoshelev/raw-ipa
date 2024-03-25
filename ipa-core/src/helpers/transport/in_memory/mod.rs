mod handlers;
mod sharding;
mod transport;

use std::array;
pub use sharding::InMemoryShardNetwork;
pub use transport::Setup;

use crate::{
    helpers::{HelperIdentity},
    sync::{Arc, Weak},
};
use crate::app::QueryRequestHandler;
use crate::helpers::{PanickingHandler, RequestHandler};

pub type InMemoryTransport<I> = Weak<transport::InMemoryTransport<I>>;

/// Container for all active MPC communication channels
#[derive(Clone)]
pub struct InMemoryMpcNetwork {
    pub transports: [Arc<transport::InMemoryTransport<HelperIdentity>>; 3],
}

impl Default for InMemoryMpcNetwork {
    fn default() -> Self {
        Self::new(PanickingHandler::default(), PanickingHandler::default(), PanickingHandler::default())
    }
}

impl InMemoryMpcNetwork {
    #[must_use]
    pub fn new<
        H1: RequestHandler<Identity = HelperIdentity>,
        H2: RequestHandler<Identity = HelperIdentity>,
        H3: RequestHandler<Identity = HelperIdentity>
    >(h1: H1, h2: H2, h3: H3) -> Self {
        let [mut first, mut second, mut third]: [_; 3] =
            HelperIdentity::make_three().map(Setup::new);

        first.connect(&mut second);
        second.connect(&mut third);
        third.connect(&mut first);

        Self {
            transports: [first.start(h1), second.start(h2), third.start(h3)],
        }
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
