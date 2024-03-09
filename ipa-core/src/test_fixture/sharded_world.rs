#[cfg(all(test, unit_test))]
mod tests {
    use std::{collections::HashMap, fmt::Debug, future::Future, num::NonZeroU32, sync::Arc};
    use std::fmt::Formatter;
    use std::iter::zip;
    use std::num::NonZeroUsize;

    use async_trait::async_trait;
    use futures::{Stream, StreamExt};
    use futures_util::future::{join_all, try_join};
    use futures_util::stream::FuturesOrdered;
    use generic_array::GenericArray;
    use rand::{distributions::Standard, prelude::Distribution, rngs::StdRng};
    use rand::seq::SliceRandom;
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
                helper_fn(ctx, shares).instrument(tracing::trace_span!("", role = ?role, shard = ?self.shard))
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

    struct ShardRecordId(ShardId, RecordId);

    impl ShardRecordId {
        fn new<C: ShardedContext>(ctx: &C, record_id: usize) -> Self {
            Self(ctx.my_shard(), RecordId::from(record_id))
        }
    }

    // This conversion is badly broken
    impl From<ShardRecordId> for PrssIndex {
        fn from(value: ShardRecordId) -> Self {
            let high_bits = u32::from(value.0) << 16;
            let low_bits = u32::from(value.1) & u32::from(u16::MAX);

            Self::from(high_bits | low_bits)
        }
    }

    async fn shuffle<I, F, C>(ctx: C, data: I, direction: Direction) -> Result<Vec<F>, crate::error::Error>
        where
            I: IntoIterator<Item=F> + Send,
            I::IntoIter: Send + ExactSizeIterator,
            F: SharedValue + FromRandomU128 + Unpin + U128Conversions,
            Standard: Distribution<F>,
            C: ShardedContext {
        let mut r = assert_send(reshard(ctx.clone(), data, |ctx, i, v| {
            (ctx.pick_shard(RecordId::from(i), direction), v)
        })).await?;

        let ctx = ctx.narrow("inter-shuffle");
        match direction {
            Direction::Left => r.shuffle(&mut ctx.prss_rng().0),
            Direction::Right => r.shuffle(&mut ctx.prss_rng().1),
        }

        Ok(r)
    }

    async fn toy_protocol<I, F, S, C>(ctx: C, shares: I) -> Result<Vec<S>, crate::error::Error>
        where
            I: IntoIterator<Item=S> + Send,
            I::IntoIter: Send + ExactSizeIterator,
            F: SharedValue + FromRandomU128 + Unpin + U128Conversions,
            Standard: Distribution<F>,
            C: ShardedContext,
            S: ReplicatedSecretSharing<F> + Serializable + Unpin,
    {
        let ctx = ctx.narrow("shuffle");
        let shares = shares.into_iter();
        let len = shares.len();
        let r: Vec<S> = match ctx.role() {
            Role::H1 => {
                // first pass to generate X_1 = perm_12(left ⊕ right ⊕ z_12)
                let mut x1 = Vec::with_capacity(len);
                let mask_ctx = ctx.narrow("z12");
                for (i, share) in shares.enumerate() {
                    let (_, z12): (F, F) = mask_ctx.prss().generate(RecordId::from(i));
                    x1.push(share.left() + share.right() + z12);
                }

                let r = shuffle(ctx.narrow("reshard_12"), x1, Direction::Right).await?;

                // second pass to generate X_2 = perm_31(x_1 ⊕ z_31)
                let mask_ctx = ctx.narrow("z31");
                let mut x2 = Vec::with_capacity(len);
                for (i, val) in r.into_iter().enumerate() {
                    let (z31, _): (F, F) = mask_ctx.prss().generate(RecordId::from(i));
                    x2.push(val + z31)
                }

                let x2 = shuffle(ctx.narrow("reshard_31"), x2, Direction::Left).await?;

                // sharded shuffle quirk - need to advertise the length to other shards
                // FIXME: random u128?
                ctx.narrow("|x2|").set_total_records(1).send_channel(ctx.role().peer(Direction::Right)).send(RecordId::FIRST, F::from_random_u128(u128::try_from(x2.len()).unwrap())).await?;

                // send all the values to the right
                if !x2.is_empty() {
                    let send_channel = ctx.narrow("x2")
                        .set_total_records(x2.len())
                        .send_channel(ctx.role().peer(Direction::Right));
                    for (i, val) in x2.iter().enumerate() {
                        send_channel.send(RecordId::from(i), *val).await?;
                    }
                }

                // need to know the cardinality of C before H1 can set its shares
                let sz: u32 = ctx.narrow("|c|").set_total_records(1).recv_channel::<F>(ctx.role().peer(Direction::Right))
                    .receive(RecordId::FIRST).await?.as_u128().try_into().unwrap();

                // set our shares
                (0..sz).map(|i| {
                    let (a_tilde, _): (F, F) = ctx.narrow("atilde").prss().generate(RecordId::from(i));
                    let (_, b_tilde): (F, F) = ctx.narrow("btilde").prss().generate(RecordId::from(i));

                    S::new(a_tilde, b_tilde)
                }).collect()


                // FIXME: get the C cardinality from other helpers
                // x2.into_iter().enumerate().map(|(i, _)| {
                //     let (_, a_hat): (F, F) = ctx.narrow("ahat").prss().generate(ShardRecordId::new(&ctx, i));
                //     let (_, b_hat): (F, F) = ctx.narrow("bhat").prss().generate(ShardRecordId::new(&ctx, i));
                //
                //     S::new(a_hat, b_hat)
                // }).collect()
            }
            Role::H2 => {
                // first pass to generate Y_1 = perm_12(right ⊕ z_12)
                let mut y1 = Vec::with_capacity(len);
                for (i, share) in shares.enumerate() {
                    let mask_ctx = ctx.narrow("z12");
                    let (z12, _): (F, F) = mask_ctx.prss().generate(RecordId::from(i));
                    y1.push(share.right() + z12);
                }

                // FIXME error messages when stream has been used are clunky
                let y1 = shuffle(ctx.narrow("reshard_12"), y1, Direction::Left).await?;

                // sharded shuffle quirk - need to advertise the length to other shards
                // FIXME: random u128?
                ctx.narrow("|y1|").set_total_records(1).send_channel(ctx.role().peer(Direction::Right)).send(RecordId::FIRST, F::from_random_u128(u128::try_from(y1.len()).unwrap())).await?;

                // share y1 to the right
                if y1.len() > 0 {
                    let send_channel = ctx.narrow("y1").set_total_records(y1.len()).send_channel(ctx.role().peer(Direction::Right));
                    for (i, val) in y1.iter().enumerate() {
                        send_channel.send(RecordId::from(i), *val).await?;
                    }
                }

                // need to know the cardinality of X1 before H2 can set its shares
                let x2_sz: u32 = ctx.narrow("|x2|").recv_channel::<F>(ctx.role().peer(Direction::Left))
                    .receive(RecordId::FIRST).await?.as_u128().try_into().unwrap();

                let mut x2: Vec<F> = Vec::with_capacity(x2_sz as usize);
                if x2_sz > 0 {
                    // receive x2 from H1
                    let recv_channel = ctx.narrow("x2").recv_channel(ctx.role().peer(Direction::Left));
                    for i in 0..x2_sz {
                        x2.push(recv_channel.receive(RecordId::from(i)).await?);
                    }
                }

                // generate X_3 = perm_23(x_2 ⊕ z_23)
                let mut x3 = Vec::with_capacity(x2.len());
                let mask_ctx = ctx.narrow("z23");
                for (i, val) in x2.into_iter().enumerate() {
                    let (_, z23): (F, F) = mask_ctx.prss().generate(RecordId::from(i));
                    x3.push(val + z23);
                }

                let x3 = shuffle(ctx.narrow("reshard_23"), x3, Direction::Right).await?;

                // generate c_1 = x_3 ⊕ b
                let mask_ctx = ctx.narrow("btilde");
                let b = (0..x3.len()).map(|i| {
                    let (r, _): (F, F) = mask_ctx.prss().generate(RecordId::from(i));
                    r
                }).collect::<Vec<_>>();

                let c1: Vec<F> = x3.into_iter().zip(b.iter()).map(|(x3, b)| x3 + *b).collect();

                // share c_1 to the right and receive c_2
                let mut c2: Vec<F> = Vec::with_capacity(len);
                if !c1.is_empty() {
                    let send_channel = ctx.narrow("c1").set_total_records(c1.len()).send_channel(ctx.role().peer(Direction::Right));
                    let recv_channel = ctx.narrow("c2").recv_channel(ctx.role().peer(Direction::Right));
                    for (i, val) in c1.iter().enumerate() {
                        let rid = RecordId::from(i);
                        // TODO: seq_join
                        send_channel.send(rid, *val).await?;
                    }
                    for i in 0..c1.len() {
                        let rid = RecordId::from(i);
                        let v = recv_channel.receive(rid).await?;
                        c2.push(v);
                    }
                }

                assert_eq!(c1.len(), c2.len());
                // FIXME: random u128?
                ctx.narrow("|c|").set_total_records(1).send_channel(ctx.role().peer(Direction::Left)).send(RecordId::FIRST, F::from_random_u128(u128::try_from(c1.len()).unwrap())).await?;

                // set out shares
                b.into_iter().zip(zip(c1.into_iter(), c2.into_iter())).map(|(b, (c1, c2))| S::new(b, c1 + c2)).collect()
            }
            Role::H3 => {
                // receive y1 from the left
                // need to know the cardinality of X1 before H2 can set its shares
                let y1_sz: u32 = ctx.narrow("|y1|").recv_channel::<F>(ctx.role().peer(Direction::Left))
                    .receive(RecordId::FIRST).await?.as_u128().try_into().unwrap();
                let mut y1: Vec<F> = Vec::with_capacity(y1_sz as usize);

                if y1_sz > 0 {
                    let recv_channel = ctx.narrow("y1").recv_channel(ctx.role().peer(Direction::Left));
                    for i in 0..y1_sz {
                        y1.push(recv_channel.receive(RecordId::from(i)).await?);
                    }
                }

                // generate y2 = perm_31(y_1 ⊕ z_31)
                let mut y2 = Vec::with_capacity(y1_sz as usize);
                let mask_ctx = ctx.narrow("z31");
                for (i, val) in y1.into_iter().enumerate() {
                    let (_, z31): (F, F) = mask_ctx.prss().generate(RecordId::from(i));
                    y2.push(val + z31);
                }

                let y2 = shuffle(ctx.narrow("reshard_31"), y2, Direction::Right).await?;

                // generate y3 = perm_23(y_2 ⊕ z_23)
                let mut y3 = Vec::with_capacity(len);
                let mask_ctx = ctx.narrow("z23");
                for (i, val) in y2.into_iter().enumerate() {
                    let (z23, _): (F, F) = mask_ctx.prss().generate(RecordId::from(i));
                    y3.push(val + z23);
                }

                let y3 = shuffle(ctx.narrow("reshard_23"), y3, Direction::Left).await?;

                let mask_ctx = ctx.narrow("atilde");
                let a = (0..y3.len()).map(|i| {
                    let (_, r): (F, F) = mask_ctx.prss().generate(RecordId::from(i));
                    r
                }).collect::<Vec<_>>();
                let c2: Vec<_> = y3.into_iter().zip(a.iter()).map(|(y_3, a)| y_3 + *a).collect();

                // send c2 and receive c1
                // fixme: capacity is wrong
                let mut c1: Vec<F> = Vec::with_capacity(len);
                if !c2.is_empty() {
                    let send_channel = ctx.narrow("c2").set_total_records(c2.len()).send_channel(ctx.role().peer(Direction::Left));
                    let recv_channel = ctx.narrow("c1").recv_channel(ctx.role().peer(Direction::Left));
                    for (i, val) in c2.iter().enumerate() {
                        let rid = RecordId::from(i);
                        // TODO: seq_join
                        send_channel.send(rid, *val).await?;
                    }
                    for i in 0..c2.len() {
                        let rid = RecordId::from(i);
                        let v = recv_channel.receive(rid).await?;
                        c1.push(v);
                    }
                }

                assert_eq!(c1.len(), c2.len());

                // set our shares
                zip(c1, c2).zip(a).map(|((c1, c2), a)| S::new(c1 + c2, a)).collect()
            }
        };


        Ok(r)
    }

    use crate::ff::{Fp31, U128Conversions};
    use crate::ff::boolean_array::BA8;
    use crate::protocol::prss::FromRandomU128;
    use crate::secret_sharing::SharedValue;

    #[test]
    fn trivial() {
        run(|| async move {
            let world = ShardedWorld::with_shards(3.try_into().unwrap());
            let inputs = [1_u32, 2, 3, 4, 5, 6, 7, 8, 9]
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
