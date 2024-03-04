#[cfg(all(test, unit_test))]
mod tests {
    use std::{collections::HashMap, fmt::Debug, future::Future, num::NonZeroU32, sync::Arc};
    use std::fmt::Formatter;
    use std::iter::zip;
    use std::num::NonZeroUsize;

    use async_trait::async_trait;
    use futures::{Stream, StreamExt};
    use futures_util::future::join_all;
    use futures_util::stream::FuturesOrdered;
    use generic_array::GenericArray;
    use rand::{distributions::Standard, prelude::Distribution, rngs::StdRng};
    use rand_core::SeedableRng;
    use tracing::Instrument;
    use typenum::U1;

    use crate::{
        error::Error,
        ff::{Field, Fp32BitPrime, Serializable},
        helpers::{
            Direction, Gateway, GatewayConfig, HelperIdentity, InMemoryNetwork, RoleAssignment,
        },
        protocol::{
            context::{
                malicious::{Context as MaliciousContext, Upgraded as UpgradedMaliciousContext},
                semi_honest::Context as SemiHonestContext,
                Context, UpgradeContext, UpgradeToMalicious,
            },
            prss,
            prss::{FromPrss, FromRandom, PrssIndex, SharedRandomness},
            QueryId, RecordId,
        },
        secret_sharing::{
            replicated::malicious::{DowngradeMalicious, ExtendableField},
            IntoShares, Linear,
        },
        test_executor::run,
        test_fixture::{
            logging, make_participants, sharing::ValidateMalicious, Reconstruct, Runner, TestWorld,
        },
    };
    use crate::helpers::{ChannelId, InMemoryNetworkSetup, InMemoryTransport, Message, OwnedInMemoryTransport, ReceiveRecords, ReceivingEnd, Role, SendingEnd, TotalRecords, TransportCallbacks};
    use crate::protocol::context::prss::{InstrumentedIndexedSharedRandomness, InstrumentedSequentialSharedRandomness};
    use crate::protocol::context::{Base, reshard, ShardConnector, ShardedContext, ShardMessage};
    use crate::protocol::step::{Gate, Step, StepNarrow};
    use crate::seq_join::{assert_send, SeqJoin};
    use crate::sharding::{ShardConfiguration, ShardCount, ShardId};

    struct ShardWorldSetup {
        seed: u64,
        role_assignment: RoleAssignment,
        shard_connections: [InMemoryNetworkSetup<ShardId>; 3],
        shard: ShardId,
        shard_count: ShardCount,
    }

    struct ShardWorld {
        seed: u64,
        participants: [prss::Endpoint; 3],
        shard_connections: [OwnedInMemoryTransport<ShardId>; 3],
        gateways: [Gateway; 3],
        shard: ShardId,
        shard_count: ShardCount,
        network: InMemoryNetwork,
    }

    impl ShardWorld {
        /// Creates protocol contexts for 3 helpers
        ///
        /// # Panics
        /// if world has more or less than 3 gateways/participants
        #[must_use]
        pub fn contexts(&self) -> [ShardedSemiHonestContext<'_>; 3] {
            zip(&self.shard_connections, zip(&self.participants, &self.gateways))
                .map(|(shard_network, (participant, gateway))| {
                    // FIXME narrow for executions
                    ShardedSemiHonestContext::new(participant, gateway, self.shard, self.shard_count)
                })
                .collect::<Vec<_>>()
                .try_into()
                .unwrap()
        }

        async fn semi_honest<'a, I, A, O, H, R>(&'a self, input: I, helper_fn: H) -> [O; 3]
            where
                I: IntoShares<A> + Send + 'static,
                A: Send,
                O: Send + Debug,
                H: Fn(ShardedSemiHonestContext<'a>, A) -> R + Send + Sync,
                R: Future<Output=O> + Send,
        {
            let input_shares = input.share_with(&mut StdRng::seed_from_u64(self.seed));
            #[allow(clippy::disallowed_methods)] // It's just 3 items.
                let output = join_all(zip(self.contexts(), input_shares).map(|(ctx, shares)| {
                let role = ctx.role();
                helper_fn(ctx, shares).instrument(tracing::trace_span!("", role = ?role))
            }))
                // .instrument(span)
                .await;
            <[_; 3]>::try_from(output).unwrap()
        }
    }

    impl ShardWorldSetup {
        fn connect(&mut self, other: &mut Self) {
            for i in 0..self.shard_connections.len() {
                self.shard_connections[i].connect(&mut other.shard_connections[i]);
            }
        }

        fn finish(mut self) -> ShardWorld {
            // fixme unique seed per shard
            let participants = make_participants(&mut StdRng::seed_from_u64(self.seed));
            let network = InMemoryNetwork::default();
            let shard_connections = self.shard_connections
                .map(|s| s.start(TransportCallbacks::default()));

            let mut gateways = [None, None, None];
            for i in 0..3 {
                let transport = &network.transports[i];
                let role_assignment = self.role_assignment.clone();
                let gateway = Gateway::new(
                    QueryId,
                    // FIXME
                    GatewayConfig::default(),
                    role_assignment,
                    Arc::downgrade(transport),
                    Arc::downgrade(&shard_connections[i]),
                );
                let role = gateway.role();
                gateways[role] = Some(gateway);
            }

            ShardWorld {
                seed: self.seed,
                participants,
                shard_connections,
                gateways: gateways.map(Option::unwrap),
                shard: self.shard,
                shard_count: self.shard_count,
                network,
            }
        }

        #[must_use]
        fn new(seed: u64, role_assignment: RoleAssignment, shard: ShardId, shard_count: ShardCount) -> Self {
            Self {
                seed,
                role_assignment,
                shard,
                shard_count,
                // todo: this probably needs to be HashMap<Role, ShardNetwork>
                shard_connections: [InMemoryNetworkSetup::new(shard), InMemoryNetworkSetup::new(shard), InMemoryNetworkSetup::new(shard)],
                // shard_connections: [InMemoryShardNetwork::new(shard), InMemoryShardNetwork::new(shard), InMemoryShardNetwork::new(shard)]
            }

            // for shard_id in shard_count.iter() {
            //     let network = InMemoryNetwork::default();
            // }

            //
            // let mut gateways = [None, None, None];
            // for i in 0..3 {
            //     let transport = &network.transports[i];
            //     let role_assignment = role_assignment.clone();
            //     let gateway = Gateway::new(
            //         QueryId,
            //         // FIXME
            //         GatewayConfig::default(),
            //         role_assignment,
            //         Arc::downgrade(transport),
            //     );
            //     let role = gateway.role();
            //     gateways[role] = Some(gateway);
            // }
            // let gateways = gateways.map(Option::unwrap);
            //
            // Self {
            //     seed,
            //     participants,
            //     gateways,
            //     _network: network,
            //     shard,
            //     shard_count,
            // }
        }
    }

    struct ShardedWorld {
        seed: u64,
        shards: HashMap<ShardId, ShardWorld>,
        // gateway: [Gateway; 3]
    }


    impl ShardedWorld {
        pub fn with_shards(shard_count: ShardCount) -> Self {
            logging::setup();
            let seed = 123;
            let role_assignment = RoleAssignment::new([
                HelperIdentity::ONE,
                HelperIdentity::TWO,
                HelperIdentity::THREE,
            ]);

            let mut map: HashMap<_, ShardWorldSetup> = HashMap::new();

            for i in shard_count.iter() {
                let mut world = ShardWorldSetup::new(seed, role_assignment.clone(), i, shard_count);
                for other_world in map.values_mut() {
                    other_world.connect(&mut world);
                }
                map.insert(i, world);
            }

            Self {
                seed,
                shards: map.into_iter().map(|(shard_id, setup)| (shard_id, setup.finish())).collect(),
            }
        }
    }

    trait Mergeable {
        fn merge(&mut self, other: Self);
    }

    impl<O: Mergeable> Mergeable for [O; 3] {
        fn merge(&mut self, other: Self) {
            let [u0, u1, u2] = self;
            let [v0, v1, v2] = other;
            u0.merge(v0);
            u1.merge(v1);
            u2.merge(v2);
        }
    }

    impl<O> Mergeable for Vec<O> {
        fn merge(&mut self, other: Self) {
            self.extend(other.into_iter())
        }
    }

    impl<O: Mergeable> Mergeable for Option<O> {
        fn merge(&mut self, other: Self) {
            match other {
                None => {}
                Some(lhs) => {
                    match self.as_mut() {
                        None => {
                            *self = Some(lhs)
                        }
                        Some(rhs) => {
                            rhs.merge(lhs)
                        }
                    }
                }
            }

            // match self {
            //     None => *self = other,
            //     Some(v) => match other {
            //         None => Some(std::mem::take(v)),
            //         Some(o) => { v.merge(o); Some(v) }
            //     }
            // }
            // match (self, other) {
            //     (None, None) => None,
            //     (Some(lhs), Some(rhs)) => Some(lhs.merge(rhs)),
            //     (Some(v), None) => Some(v),
            //     (None, Some(v)) => Some(v)
            // }
        }
    }

    #[derive(Clone)]
    struct ShardedSemiHonestContext<'a> {
        inner: Base<'a>,
        shard: ShardId,
        shard_count: ShardCount,
    }

    impl Debug for ShardedSemiHonestContext<'_> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "ShardedSemiHonestContext")
        }
    }

    impl ShardConnector for ShardedSemiHonestContext<'_> {

        fn recv_from_all<V: ShardMessage>(self) -> impl Stream<Item=(ShardId, V)> + Send + Unpin {
            futures::stream::empty()
        }
    }

    impl<'a> ShardedSemiHonestContext<'a> {
        pub fn new(participant: &'a prss::Endpoint, gateway: &'a Gateway, shard: ShardId, shard_count: ShardCount) -> Self {
            let base = Base::new(participant, gateway);
            Self {
                shard,
                shard_count,
                inner: base,
            }
        }
    }

    struct Foo;

    impl<V: ShardMessage> ShardConnection<V> for &Foo {
        fn send(&self, payload: &V) -> impl Future<Output=Result<(), Error>> + Send {
            async { Ok(()) }
        }
    }

    impl ShardConnector for Foo {

        fn recv_from_all<V: crate::protocol::context::ShardMessage>(self) -> impl Stream<Item=(ShardId, V)> + Unpin {
            futures::stream::empty()
        }
    }


    impl ShardedContext for ShardedSemiHonestContext<'_> {
        fn shard_send_channel<M: ShardMessage>(&self, dest: ShardId) -> SendingEnd<ShardId, M> {
            // fixme don't expose gateway
            self.inner.gateway().get_shard_sender(&ChannelId::new(dest, self.gate().clone()), self.total_records())
        }

        fn recv_from_shards<M: ShardMessage>(&self) -> impl Stream<Item=(ShardId, Result<M, crate::error::Error>)> + Send {
            futures::stream::select_all(self.shard_count.iter().filter(|shard_id| *shard_id != self.shard).map(|shard_id| {
                self.inner.gateway().get_shard_receiver(&ChannelId::new(shard_id, self.gate().clone())).map(move |v| (shard_id, v.map_err(crate::error::Error::from)))
            }))
        }
    }

    impl SeqJoin for ShardedSemiHonestContext<'_> {
        fn active_work(&self) -> NonZeroUsize {
            self.inner.active_work()
        }
    }

    impl Context for ShardedSemiHonestContext<'_> {
        fn role(&self) -> Role {
            self.inner.role()
        }

        fn gate(&self) -> &Gate {
            self.inner.gate()
        }

        fn narrow<S: Step + ?Sized>(&self, step: &S) -> Self where Gate: StepNarrow<S> {
            Self {
                inner: self.inner.narrow(step),
                shard: self.shard,
                shard_count: self.shard_count,
            }
        }

        fn set_total_records<T: Into<TotalRecords>>(&self, total_records: T) -> Self {
            Self {
                inner: self.inner.set_total_records(total_records),
                shard: self.shard,
                shard_count: self.shard_count,
            }
        }

        fn total_records(&self) -> TotalRecords {
            self.inner.total_records()
        }

        fn prss(&self) -> InstrumentedIndexedSharedRandomness<'_> {
            self.inner.prss()
        }

        fn prss_rng(&self) -> (InstrumentedSequentialSharedRandomness, InstrumentedSequentialSharedRandomness) {
            self.inner.prss_rng()
        }

        fn send_channel<M: Message>(&self, role: Role) -> SendingEnd<Role, M> {
            self.inner.send_channel(role)
        }

        fn recv_channel<M: Message>(&self, role: Role) -> ReceivingEnd<M> {
            self.inner.recv_channel(role)
        }
    }

    impl ShardConfiguration for ShardedSemiHonestContext<'_> {
        fn my_shard(&self) -> ShardId {
            self.shard
        }

        fn shard_count(&self) -> ShardCount {
            self.shard_count
        }
    }

    #[async_trait]
    pub trait ShardedRunner {
        async fn shard_semi_honest<'a, I, A, B, O, H, R>(
            &'a self,
            input: I,
            helper_fn: H,
        ) -> [O; 3]
            where
                I: IntoIterator<Item=A> + Send,
                I::IntoIter: ExactSizeIterator + Send,
                A: IntoShares<B> + Send + 'static,
                B: Send,
                O: Mergeable + Send + Debug,
                H: Fn(ShardedSemiHonestContext<'a>, Vec<B>) -> R + Send + Sync,
                R: Future<Output=O> + Send;
    }

    #[async_trait]
    impl ShardedRunner for ShardedWorld {
        async fn shard_semi_honest<'a, I, A, B, O, H, R>(&'a self, input: I, helper_fn: H) -> [O; 3]
            where
                I: IntoIterator<Item=A> + Send,
                I::IntoIter: ExactSizeIterator + Send,
                A: IntoShares<B> + Send + 'static,
                B: Send,
                O: Mergeable + Send + Debug,
                H: Fn(ShardedSemiHonestContext<'a>, Vec<B>) -> R + Send + Sync,
                R: Future<Output=O> + Send,
        {
            let input = input.into_iter();

            let input_size = input.len();
            let shard_count = self.shards.len();
            if input_size < shard_count {
                panic!("Number of shards '{shard_count}' must be greater or equal to input size '{input_size}'")
            }

            // build shard input
            let chunk_size = input_size / shard_count;
            // FIXME: uneven shards
            let mut shard_inputs = (0..shard_count)
                .map(|_| Vec::with_capacity(chunk_size))
                .collect::<Vec<_>>();
            input.enumerate().for_each(|(i, item)| {
                let index = i / chunk_size;
                shard_inputs[index].push(item)
            });

            // run multiple MPC in parallel
            let shard_fn = |ctx, input| helper_fn(ctx, input);
            let mut futures = shard_inputs
                .into_iter()
                .zip(self.shards.values())
                .map(|(x, shard_world)| shard_world.semi_honest(x.into_iter(), shard_fn))
                .collect::<FuturesOrdered<_>>();
            let mut r = None;
            while let Some(v) = futures.next().await {
                r.merge(Some(v));
            }

            r.unwrap()
        }
    }

    trait RandomShardPicker {
        fn pick_shard(&self, record_id: RecordId, direction: Direction) -> ShardId;
    }

    impl<C: ShardedContext> RandomShardPicker for C {
        fn pick_shard(&self, record_id: RecordId, direction: Direction) -> ShardId {
            // FIXME: update PRSS trait to compute only left or right part
            let (l, r): (u128, u128) = self.prss().generate(record_id);
            ShardId::from(u32::try_from(match direction {
                Direction::Left => l,
                Direction::Right => r,
            } % u128::from(self.shard_count())).unwrap())
        }
    }

    // trait ShardConnectivity {
    //     type Connector<'a>: ShardConnector where Self: 'a;
    //     fn shard_connector(&self) -> Self::Connector<'_>;
    // }
    use crate::protocol::context::ShardConnection;
    use crate::secret_sharing::replicated::ReplicatedSecretSharing;

    async fn toy_protocol<I, F, S, C>(ctx: C, shares: I) -> Result<Vec<S>, crate::error::Error>
        where
            I: IntoIterator<Item=S> + Send,
            I::IntoIter: Send + ExactSizeIterator,
            F: SharedValue,
            Standard: Distribution<F>,
            C: ShardedContext,
            S: ReplicatedSecretSharing<F> + Serializable + Unpin,
    {
        let ctx = ctx.narrow("shuffle");

        let r = assert_send(reshard(ctx.clone(), shares, |ctx, i, v| {
            // let dest_shard = ctx.pick_shard(RecordId::from(i), Direction::Left);
            let dest_shard = ShardId::from(0);
            (dest_shard, v)
        })).await?;


        Ok(r)
    }
    use crate::ff::{Fp31, U128Conversions};
    use crate::ff::boolean_array::BA8;
    use crate::secret_sharing::SharedValue;

    #[test]
    fn trivial() {
        run(|| async move {
            let world = ShardedWorld::with_shards(3.try_into().unwrap());
            let inputs = [1_u32, 2, 3]
                .map(|x| BA8::truncate_from(x))
                .to_vec();
            let mut result: Vec<_> = world
                .shard_semi_honest(inputs.clone().into_iter(), |ctx, input| async move {
                    toy_protocol(ctx, input).await.unwrap()
                })
                .await
                .reconstruct();

            result.sort_by(|a, b| a.as_u128().cmp(&b.as_u128()));

            assert_eq!(inputs, result);
        })
    }
}
