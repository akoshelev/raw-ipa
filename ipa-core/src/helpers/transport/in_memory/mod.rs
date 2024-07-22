pub mod config;
mod sharding;
mod transport;

pub use sharding::InMemoryShardNetwork;
pub use transport::Setup;
pub use config::{MaliciousHelper, passthrough as passthrough_peeker, StreamInterceptor};
use transport::TransportConfigBuilder;
use crate::{
    helpers::{HandlerRef, HelperIdentity},
    sync::{Arc, Weak},
};
use crate::helpers::in_memory_config::InspectContext;
use crate::helpers::transport::in_memory::config::passthrough;

pub type InMemoryTransport<I> = Weak<transport::InMemoryTransport<I>>;

/// Container for all active MPC communication channels
#[derive(Clone)]
pub struct InMemoryMpcNetwork {
    pub transports: [Arc<transport::InMemoryTransport<HelperIdentity>>; 3],
}

impl Default for InMemoryMpcNetwork {
    fn default() -> Self {
        Self::new(Self::noop_handlers())
    }
}

impl InMemoryMpcNetwork {
    pub fn noop_handlers() -> [Option<HandlerRef>; 3] {
        [None, None, None]
    }

    #[must_use]
    pub fn new(handlers: [Option<HandlerRef>; 3]) -> Self {
        Self::with_stream_peeker(handlers, &passthrough())
    }

    #[must_use]
    pub fn with_stream_peeker(handlers: [Option<HandlerRef>; 3], peeker: &Arc<dyn StreamInterceptor<Context =InspectContext>>) -> Self {
        let [mut first, mut second, mut third]: [_; 3] =
            HelperIdentity::make_three().map(|i| {
                let mut config_builder = TransportConfigBuilder::for_helper(i);
                config_builder.with_peeker(peeker);

                Setup::with_config(i, config_builder.not_sharded())
            });

        first.connect(&mut second);
        second.connect(&mut third);
        third.connect(&mut first);

        let [h1, h2, h3] = handlers;

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
        self.transports.each_ref().map(Arc::downgrade)
    }

    /// Reset all transports to the clear state.
    pub fn reset(&self) {
        for t in &self.transports {
            t.reset();
        }
    }
}
