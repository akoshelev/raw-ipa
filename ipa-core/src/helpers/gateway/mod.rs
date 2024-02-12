mod receive;
mod send;
#[cfg(feature = "stall-detection")]
pub(super) mod stall_detection;
mod transport;

use std::future::Future;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::pin::Pin;
use futures::Stream;
use futures_util::StreamExt;

pub use receive::RoleIndexedReceiver;
pub(super) use receive::ReceivingEnd;
pub(super) use send::SendingEnd;
#[cfg(all(test, feature = "shuttle"))]
use shuttle::future as tokio;
#[cfg(feature = "stall-detection")]
pub(super) use stall_detection::InstrumentedGateway;

use crate::{
    helpers::{
        gateway::{
            receive::GatewayReceivers, send::GatewaySenders, transport::RoleResolvingTransport,
        },
        ChannelId, Message, Role, RoleAssignment, TotalRecords, Transport,
    },
    protocol::QueryId,
};
use crate::error::Error;
use crate::ff::Serializable;
use crate::helpers::{HelperIdentity, RouteId};
use crate::helpers::buffers::UnorderedReceiver;
pub use crate::helpers::gateway::receive::ShardRecvStream;
use crate::helpers::transport::BufDeque;
use crate::protocol::context::{ShardConnection, ShardConnector, ShardMessage};
use crate::protocol::step::Gate;
use crate::sharding::ShardId;

/// Alias for the currently configured transport.
///
/// To avoid proliferation of type parameters, most code references this concrete type alias, rather
/// than a type parameter `T: Transport`.
#[cfg(feature = "in-memory-infra")]
pub type TransportImpl<O> = super::transport::InMemoryTransport<O>;

// #[cfg(feature = "in-memory-infra")]
// pub type ShardTransport = super::transport::InMemoryTransport<ShardId>;

#[cfg(feature = "real-world-infra")]
pub type TransportImpl = crate::sync::Arc<crate::net::HttpTransport>;

pub type TransportError<O> = <TransportImpl<O> as Transport<O>>::Error;

/// Gateway into IPA Network infrastructure. It allows helpers send and receive messages.
pub struct Gateway {
    config: GatewayConfig,
    transport: RoleResolvingTransport,
    shard_transport: TransportImpl<ShardId>,
    #[cfg(feature = "stall-detection")]
    inner: crate::sync::Arc<State>,
    #[cfg(not(feature = "stall-detection"))]
    inner: State,
    pub query_id: QueryId,
}

struct Foo;

impl <V: ShardMessage> ShardConnection<V> for Foo {
    fn send(&self, payload: &V) -> impl Future<Output=Result<(), Error>> + Send {
        async move { Ok(()) }
    }
}

use crate::sync::{Arc, Mutex};

#[derive(Default)]
pub struct State {
    senders: GatewaySenders<Role>,
    // FIXME: the plan is - we don't need unordered receivers for shard transports
    receivers: GatewayReceivers<Role, RoleIndexedReceiver>,
    shard_senders: GatewaySenders<ShardId>,
    shard_receivers: GatewayReceivers<ShardId, ShardRecvStream>,
}

#[derive(Clone, Copy, Debug)]
pub struct GatewayConfig {
    /// The number of items that can be active at the one time.
    /// This is used to determine the size of sending and receiving buffers.
    active: NonZeroUsize,

    /// Time to wait before checking gateway progress. If no progress has been made between
    /// checks, the gateway is considered to be stalled and will create a report with outstanding
    /// send/receive requests
    #[cfg(feature = "stall-detection")]
    pub progress_check_interval: std::time::Duration,
}

impl Gateway {
    #[must_use]
    pub fn new(
        query_id: QueryId,
        config: GatewayConfig,
        roles: RoleAssignment,
        transport: TransportImpl<HelperIdentity>,
        shard_transport: TransportImpl<ShardId>,
    ) -> Self {
        #[allow(clippy::useless_conversion)] // not useless in stall-detection build
        Self {
            config,
            query_id,
            transport: RoleResolvingTransport {
                roles,
                inner: transport,
            },
            inner: State::default().into(),
            shard_transport,
        }
    }

    #[must_use]
    pub fn role(&self) -> Role {
        self.transport.identity()
    }

    pub fn shard(&self) -> ShardId {
        // FIXME
        self.shard_transport.upgrade().unwrap().identity()
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
        channel_id: &ChannelId<Role>,
        total_records: TotalRecords,
    ) -> send::SendingEnd<Role, M> {
        // todo tests
        assert_ne!(self.role(), channel_id.id, "Can't send to myself");

        let (tx, maybe_stream) = self.inner.senders.get_or_create::<M>(
            channel_id,
            self.config.active_work(),
            total_records,
        );
        if let Some(stream) = maybe_stream {
            tokio::spawn({
                let channel_id = channel_id.clone();
                let transport = self.transport.clone();
                let query_id = self.query_id;
                async move {
                    // TODO(651): In the HTTP case we probably need more robust error handling here.
                    transport
                        .send(channel_id.id, (RouteId::Records, query_id, channel_id.gate), stream)
                        .await
                        .expect("{channel_id:?} receiving end should be accepted by transport");
                }
            });
        }

        send::SendingEnd::new(tx, self.role(), channel_id)
    }

    #[must_use]
    pub fn get_receiver<M: Message>(&self, channel_id: &ChannelId<Role>) -> receive::ReceivingEnd<Role, RoleIndexedReceiver, M> {
        assert_ne!(self.role(), channel_id.id, "Can't receive from myself");
        receive::ReceivingEnd::new(
            channel_id.clone(),
            self.inner
                .receivers
                .get_or_create(channel_id, || UnorderedReceiver::new(Box::pin(self.transport.receive(channel_id.id, (self.query_id, channel_id.gate.clone()))), self.config.active_work()))
        )
    }

    pub fn get_shard_receiver<M: Send + Serializable + 'static>(&self, channel_id: &ChannelId<ShardId>) -> receive::ShardReceivingEnd<M> {
        assert_ne!(self.shard(), channel_id.id, "Can't receive from myself");
        receive::ShardReceivingEnd {
            my_id: format!("{:?}/{:?}", self.role(), self.shard()),
            channel_id: channel_id.clone(),
            recv_stream: Mutex::new(self.shard_transport.receive(channel_id.id, (self.query_id, channel_id.gate.clone())).fuse()),
            buf: BufDeque::new(),
            received: 0,
            _phantom: PhantomData
        }
    }

    #[must_use]
    pub fn get_shard_sender<M: Send + Serializable + 'static>(&self, channel: &ChannelId<ShardId>, total_records: TotalRecords) -> send::SendingEnd<ShardId, M> {
        let (tx, maybe_stream) = self.inner.shard_senders.get_or_create::<M>(
            channel,
            self.config.active_work(),
            total_records,
        );
        if let Some(stream) = maybe_stream {
            tokio::spawn({
                let channel_id = channel.clone();
                let transport = self.shard_transport.clone();
                let query_id = self.query_id;
                async move {
                    // TODO(651): In the HTTP case we probably need more robust error handling here.
                    transport
                        .send(channel_id.id, (RouteId::Records, query_id, channel_id.gate), stream)
                        .await
                        .expect("{channel_id:?} receiving end should be accepted by transport");
                }
            });
        }

        send::SendingEnd::new(tx, self.shard(), channel)
    }

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
        // In-memory tests are fast, so progress check intervals can be lower.
        // Real world scenarios currently over-report stalls because of inefficiencies inside
        // infrastructure and actual networking issues. This checks is only valuable to report
        // bugs, so keeping it large enough to avoid false positives.
        Self {
            active: NonZeroUsize::new(active).unwrap(),
            #[cfg(feature = "stall-detection")]
            progress_check_interval: std::time::Duration::from_secs(if cfg!(test) {
                5
            } else {
                30
            }),
        }
    }

    /// The configured amount of active work.
    #[must_use]
    pub fn active_work(&self) -> NonZeroUsize {
        self.active
    }
}

#[cfg(all(test, unit_test))]
mod tests {
    use std::iter::{repeat, zip};

    use futures_util::future::{join, try_join, try_join_all};

    use crate::{
        ff::{Fp31, Fp32BitPrime, Gf2, U128Conversions},
        helpers::{Direction, GatewayConfig, Message, Role, SendingEnd},
        protocol::{context::Context, RecordId},
        test_fixture::{Runner, TestWorld, TestWorldConfig},
    };

    /// Verifies that [`Gateway`] send buffer capacity is adjusted to the message size.
    /// IPA protocol opens many channels to send values from different fields, while message size
    /// is set per channel, it does not have to be the same across multiple send channels.
    ///
    /// Gateway must be able to deal with it.
    #[tokio::test]
    async fn can_handle_heterogeneous_channels() {
        async fn send<V: Message + U128Conversions>(channel: &SendingEnd<Role, V>, i: usize) {
            channel
                .send(i.into(), V::truncate_from(u128::try_from(i).unwrap()))
                .await
                .unwrap();
        }

        let config = TestWorldConfig {
            gateway_config: GatewayConfig::new(2),
            ..Default::default()
        };

        let world = TestWorld::new_with(config);
        world
            .semi_honest((), |ctx, ()| async move {
                let fp2_ctx = ctx.narrow("fp2").set_total_records(100);
                let fp32_ctx = ctx.narrow("fp32").set_total_records(100);
                let role = ctx.role();

                let fp2_channel = fp2_ctx.send_channel::<Gf2>(role.peer(Direction::Right));
                let fp32_channel =
                    fp32_ctx.send_channel::<Fp32BitPrime>(role.peer(Direction::Right));

                // joins must complete, despite us not closing the send channel.
                // fp2 channel byte capacity must be set to 2 bytes, fp32 channel can store 8 bytes.
                join(send(&fp2_channel, 0), send(&fp2_channel, 1)).await;
                join(send(&fp32_channel, 0), send(&fp32_channel, 1)).await;
            })
            .await;
    }

    #[tokio::test]
    pub async fn handles_reordering() {
        let config = TestWorldConfig {
            gateway_config: GatewayConfig::new(2),
            ..TestWorldConfig::default()
        };
        let world = Box::leak(Box::new(TestWorld::new_with(config)));
        let world_ptr = world as *mut _;
        let contexts = world.contexts();
        let sender_ctx = contexts[0].narrow("reordering-test").set_total_records(2);
        let recv_ctx = contexts[1].narrow("reordering-test").set_total_records(2);

        // send record 1 first and wait for confirmation before sending record 0.
        // when gateway received record 0 it triggers flush so it must make sure record 1 is also
        // sent (same batch or different does not matter here)
        let spawned = tokio::spawn(async move {
            let channel = sender_ctx.send_channel(Role::H2);
            try_join(
                channel.send(RecordId::from(1), Fp31::truncate_from(1_u128)),
                channel.send(RecordId::from(0), Fp31::truncate_from(0_u128)),
            )
            .await
            .unwrap();
        });

        let recv_channel = recv_ctx.recv_channel::<Fp31>(Role::H1);
        let result = try_join(
            recv_channel.receive(RecordId::from(1)),
            recv_channel.receive(RecordId::from(0)),
        )
        .await
        .unwrap();

        assert_eq!(
            (Fp31::truncate_from(1u128), Fp31::truncate_from(0u128)),
            result
        );
        spawned.await.unwrap();
        let _world = unsafe { Box::from_raw(world_ptr) };
    }

    /// this test requires quite a few threads to simulate send contention and will panic if
    /// there is more than one sender channel created per step.
    #[tokio::test(flavor = "multi_thread", worker_threads = 20)]
    pub async fn send_contention() {
        let (world, world_ptr) = make_world();

        try_join_all(world.contexts().map(|ctx| {
            tokio::spawn(async move {
                const TOTAL_RECORDS: usize = 10;
                let ctx = ctx
                    .narrow("send_contention")
                    .set_total_records(TOTAL_RECORDS);

                let receive_handle = tokio::spawn({
                    let ctx = ctx.clone();
                    async move {
                        for record in 0..TOTAL_RECORDS {
                            let v = Fp31::truncate_from(u128::try_from(record).unwrap());
                            let r = ctx
                                .recv_channel::<Fp31>(ctx.role().peer(Direction::Left))
                                .receive(record.into())
                                .await
                                .unwrap();

                            assert_eq!(v, r, "Bad value for record {record}");
                        }
                    }
                });

                try_join_all(zip(0..TOTAL_RECORDS, repeat(ctx)).map(|(record, ctx)| {
                    tokio::spawn(async move {
                        let r = Fp31::truncate_from(u128::try_from(record).unwrap());
                        ctx.send_channel(ctx.role().peer(Direction::Right))
                            .send(RecordId::from(record), r)
                            .await
                            .unwrap();
                    })
                }))
                .await
                .unwrap();

                receive_handle.await.unwrap();
            })
        }))
        .await
        .unwrap();

        let _world = unsafe { Box::from_raw(world_ptr) };
    }

    /// This test should hang if receiver channel is not created atomically. It may occasionally
    /// pass, but it will not give false negatives.
    #[tokio::test(flavor = "multi_thread", worker_threads = 20)]
    pub async fn receive_contention() {
        let (world, world_ptr) = make_world();
        let contexts = world.contexts();

        try_join_all(contexts.map(|ctx| {
            tokio::spawn(async move {
                const TOTAL_RECORDS: u32 = 20;
                let ctx = ctx
                    .narrow("receive_contention")
                    .set_total_records(usize::try_from(TOTAL_RECORDS).unwrap());

                tokio::spawn({
                    let ctx = ctx.clone();
                    async move {
                        for record in 0..TOTAL_RECORDS {
                            ctx.send_channel(ctx.role().peer(Direction::Right))
                                .send(RecordId::from(record), Fp31::truncate_from(record))
                                .await
                                .unwrap();
                        }
                    }
                });

                try_join_all((0..TOTAL_RECORDS).zip(repeat(ctx)).map(|(record, ctx)| {
                    tokio::spawn(async move {
                        let r = ctx
                            .recv_channel::<Fp31>(ctx.role().peer(Direction::Left))
                            .receive(RecordId::from(record))
                            .await
                            .unwrap();
                        assert_eq!(
                            Fp31::truncate_from(record),
                            r,
                            "received bad value for record {record}"
                        );
                    })
                }))
                .await
                .unwrap();
            })
        }))
        .await
        .unwrap();

        let _world = unsafe { Box::from_raw(world_ptr) };
    }

    fn make_world() -> (&'static TestWorld, *mut TestWorld) {
        let world = Box::leak(Box::<TestWorld>::default());
        let world_ptr = world as *mut _;
        (world, world_ptr)
    }
}
