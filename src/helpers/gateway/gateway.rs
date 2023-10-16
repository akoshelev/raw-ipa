use std::{
    fmt::{Debug, Formatter},
    num::NonZeroUsize,
    time::Duration,
};

use delegate::delegate;
#[cfg(all(feature = "shuttle", test))]
use shuttle::future as tokio;

use crate::{
    helpers::{
        gateway::{
            observable::{ObserveState, Observed},
            receive,
            receive::GatewayReceivers,
            send,
            send::GatewaySenders,
            transport::RoleResolvingTransport,
        },
        ChannelId, Message, ReceivingEnd, Role, RoleAssignment, SendingEnd, TotalRecords,
        TransportImpl,
    },
    protocol::QueryId,
    sync::{atomic::AtomicUsize, Arc, Weak},
};

/// Gateway into IPA Network infrastructure. It allows helpers send and receive messages.
pub struct Gateway {
    config: GatewayConfig,
    transport: RoleResolvingTransport,
    // todo: use different state when feature is off
    inner: Arc<State>,
}

#[derive(Default)]
pub struct State {
    senders: GatewaySenders,
    receivers: GatewayReceivers,
}
impl State {
    pub fn downgrade(self: &Arc<Self>) -> Weak<Self> {
        Arc::downgrade(self)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct GatewayConfig {
    /// The number of items that can be active at the one time.
    /// This is used to determine the size of sending and receiving buffers.
    active: NonZeroUsize,

    /// Time to wait before checking gateway progress. If no progress has been made between
    /// checks, the gateway is considered to be stalled and will create a report with outstanding
    /// send/receive requests
    pub progress_check_interval: Duration,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self::new(1024)
    }
}

impl GatewayConfig {
    /// Generate a new configuration with the given active limit.
    ///
    /// ## Panics
    /// If `active` is 0.
    #[must_use]
    pub fn new(active: usize) -> Self {
        // In-memory tests move data fast, so progress check intervals can be lower.
        // Real world scenarios currently over-report stalls because of inefficiencies inside
        // infrastructure and actual networking issues. This checks is only valuable to report
        // bugs, so keeping it large enough to avoid false positives.
        Self {
            active: NonZeroUsize::new(active).unwrap(),
            progress_check_interval: Duration::from_secs(if cfg!(test) { 5 } else { 60 }),
        }
    }

    /// The configured amount of active work.
    #[must_use]
    pub fn active_work(&self) -> NonZeroUsize {
        self.active
    }
}

impl Observed<Gateway> {
    delegate! {
        to self.inner() {
            #[inline]
            pub fn role(&self) -> Role;

            #[inline]
            pub fn config(&self) -> &GatewayConfig;
        }
    }

    pub fn new(
        query_id: QueryId,
        config: GatewayConfig,
        roles: RoleAssignment,
        transport: TransportImpl,
    ) -> Self {
        let version = Arc::new(AtomicUsize::default());
        // todo: this sucks, we shouldn't do an extra clone
        Self::wrap(&version, Gateway::new(query_id, config, roles, transport))
    }

    #[must_use]
    pub fn get_sender<M: Message>(
        &self,
        channel_id: &ChannelId,
        total_records: TotalRecords,
    ) -> SendingEnd<M> {
        Observed::wrap(
            self.get_version(),
            self.inner().get_sender(channel_id, total_records),
        )
    }

    #[must_use]
    pub fn get_receiver<M: Message>(&self, channel_id: &ChannelId) -> ReceivingEnd<M> {
        Observed::wrap(self.get_version(), self.inner().get_receiver(channel_id))
    }

    pub fn to_observed(&self) -> Observed<Weak<State>> {
        // todo: inner.inner
        Observed::wrap(self.get_version(), self.inner().inner.downgrade())
    }
}

pub struct GatewayWaitingTasks<S, R> {
    senders_state: Option<S>,
    receivers_state: Option<R>,
}

impl<S: Debug, R: Debug> Debug for GatewayWaitingTasks<S, R> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(senders_state) = &self.senders_state {
            write!(f, "\n{{{senders_state:?}\n}}")?;
        }
        if let Some(receivers_state) = &self.receivers_state {
            write!(f, "\n{{{receivers_state:?}\n}}")?;
        }

        Ok(())
    }
}

impl ObserveState for Weak<State> {
    type State = GatewayWaitingTasks<send::WaitingTasks, receive::WaitingTasks>;

    fn get_state(&self) -> Option<Self::State> {
        self.upgrade().map(|state| Self::State {
            senders_state: state.senders.get_state(),
            receivers_state: state.receivers.get_state(),
        })
    }
}

impl Gateway {
    #[must_use]
    pub fn new(
        query_id: QueryId,
        config: GatewayConfig,
        roles: RoleAssignment,
        transport: TransportImpl,
    ) -> Self {
        Self {
            config,
            transport: RoleResolvingTransport {
                query_id,
                roles,
                inner: transport,
                config,
            },
            inner: State::default().into(),
        }
    }

    #[must_use]
    pub fn role(&self) -> Role {
        self.transport.role()
    }

    #[must_use]
    pub fn config(&self) -> &GatewayConfig {
        &self.config
    }

    ///
    /// ## Panics
    /// If there is a failure connecting via HTTP
    #[must_use]
    pub fn get_sender<M: Message>(
        &self,
        channel_id: &ChannelId,
        total_records: TotalRecords,
    ) -> send::SendingEnd<M> {
        let (tx, maybe_stream) = self.inner.senders.get_or_create::<M>(
            channel_id,
            self.config.active_work(),
            total_records,
        );
        if let Some(stream) = maybe_stream {
            tokio::spawn({
                let channel_id = channel_id.clone();
                let transport = self.transport.clone();
                async move {
                    // TODO(651): In the HTTP case we probably need more robust error handling here.
                    transport
                        .send(&channel_id, stream)
                        .await
                        .expect("{channel_id:?} receiving end should be accepted by transport");
                }
            });
        }

        send::SendingEnd::new(tx, self.role(), channel_id)
    }

    #[must_use]
    pub fn get_receiver<M: Message>(&self, channel_id: &ChannelId) -> receive::ReceivingEnd<M> {
        receive::ReceivingEnd::new(
            channel_id.clone(),
            self.inner
                .receivers
                .get_or_create(channel_id, || self.transport.receive(channel_id)),
        )
    }
}
