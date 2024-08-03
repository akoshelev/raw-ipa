use std::{future::Future, iter::zip, marker::PhantomData, ops::Mul};

use futures_util::future::try_join;
use typenum::Const;

use crate::{
    error::Error,
    ff::{
        boolean::Boolean, curve_points::RP25519, ec_prime_field::Fp25519, Expand, Invert,
        PrimeField,
    },
    helpers::{Role, TotalRecords},
    protocol::{
        basics::{mac_validated_reveal, Reveal, SecureMul},
        context::{
            upgrade::UpgradeVectorizedFriendly, Context, UpgradableContext, UpgradedContext,
            UpgradedSemiHonestContext, Validator,
        },
        ipa_prf::{boolean_ops::step::MultiplicationStep::Add, step::PrfStep as Step},
        prss::{FromPrss, SharedRandomness},
        BasicProtocols, RecordId,
    },
    secret_sharing::{
        replicated::{malicious::ExtendableField, semi_honest::AdditiveShare},
        FieldSimd, Linear, SecretSharing, Sendable, SharedValue, StdArray, Vectorizable,
        VectorizedSecretSharing,
    },
    seq_join::assert_send,
    sharding::NotSharded,
};
use crate::protocol::context::UpgradedMaliciousContext;
use crate::protocol::context::validator::{BatchUpgradedContext, Upgradeable};
use crate::secret_sharing::replicated::malicious;
use crate::secret_sharing::replicated::malicious::ThisCodeIsAuthorizedToDowngradeFromMalicious;

/// This trait defines the requirements to the sharing types and the underlying fields
/// used to generate PRF values.
pub trait PrfSharing<C: UpgradedContext, const N: usize>:
    Upgradeable<C, Output = Self::UpgradedSharing>
    + FromPrss
{
    /// The type of field used to compute `z`
    type Field: FieldSimd<N>;
    type UpgradedSharing: BasicProtocols<C, N, ProtocolField = Self::Field>;
}

struct RevealablePrfSharing<C: UpgradedContext, Z: PrfSharing<C, N>, const N: usize>
where RP25519: Vectorizable<N>
{
    prf_share: Z::UpgradedSharing,
    curve_point_share: AdditiveShare<RP25519, N>
}

impl<'a, const N: usize> PrfSharing<UpgradedSemiHonestContext<'a, NotSharded, Fp25519>, N> for AdditiveShare<Fp25519, N>
where
    Fp25519: FieldSimd<N>,
    RP25519: Vectorizable<N>,
    AdditiveShare<Fp25519, N>: BasicProtocols<UpgradedSemiHonestContext<'a, NotSharded, Fp25519>, N, ProtocolField = Fp25519> + FromPrss,
{
    type Field = Fp25519;
    type UpgradedSharing = AdditiveShare<Fp25519, N>;
}

impl <'a, const N: usize> PrfSharing<BatchUpgradedContext<'a, Fp25519>, N> for AdditiveShare<Fp25519, N>
    where
        Fp25519: FieldSimd<N>,
        RP25519: Vectorizable<N>,
        malicious::AdditiveShare<Fp25519, N>: BasicProtocols<BatchUpgradedContext<'a, Fp25519>, N, ProtocolField = Fp25519>,
        AdditiveShare<Fp25519, N>: FromPrss,
{
    type Field = Fp25519;
    type UpgradedSharing = malicious::AdditiveShare<Fp25519, N>;
}

impl<const N: usize, C: UpgradedContext, Z: PrfSharing<C, N, Field = Fp25519>> Reveal<C>
    for RevealablePrfSharing<C, Z, N>
where
    RP25519: Vectorizable<N>,
    Fp25519: FieldSimd<N>
{
    type Output = (
        <RP25519 as Vectorizable<N>>::Array,
        <Fp25519 as Vectorizable<N>>::Array,
    );

    fn generic_reveal<'fut>(
        &'fut self,
        ctx: C,
        record_id: RecordId,
        excluded: Option<Role>,
    ) -> impl Future<Output = Result<Option<Self::Output>, Error>> + Send + 'fut
    where
        C: 'fut,
    {
        let sh_gr = &self.curve_point_share;
        let y = &self.prf_share;
        async move {
            let (gr, z) = assert_send(try_join(
                sh_gr.reveal(ctx.narrow(&Step::RevealR), record_id),
                // sh_gr.reveal(ctx.narrow(&Step::RevealR), record_id),
                y.reveal(ctx.narrow(&Step::Revealz), record_id),
            ))
            .await?;

            Ok(Some((gr, z)))
        }
    }
}

impl<const N: usize> From<AdditiveShare<Fp25519, N>> for AdditiveShare<RP25519, N>
where
    Fp25519: Vectorizable<N>,
    RP25519: Vectorizable<N>,
{
    fn from(value: AdditiveShare<Fp25519, N>) -> Self {
        value.map(|v| RP25519::from(v))
    }
}

/// TODO: this is wildly unsafe and badly broken design. Need a safer way to go from
/// malicious FP to semi-honest RP
impl<const N: usize> From<malicious::AdditiveShare<Fp25519, N>> for AdditiveShare<RP25519, N>
where
    Fp25519: FieldSimd<N>,
    RP25519: Vectorizable<N>,
{
    fn from(value: malicious::AdditiveShare<Fp25519, N>) -> Self {
        value.x().access_without_downgrade().clone().map(|v| RP25519::from(v))
    }
}

/// generates PRF key k as secret sharing over Fp25519
pub fn gen_prf_key<C, const N: usize>(ctx: C) -> AdditiveShare<Fp25519, N>
where
    C: UpgradedContext<Field = Fp25519>,
    Fp25519: Vectorizable<N>,
{
    let ctx = ctx.narrow(&Step::PRFKeyGen).set_total_records(TotalRecords::ONE);
    let v: AdditiveShare<Fp25519, 1> = ctx.prss().generate(RecordId(0));

    v.expand()

    // // TODO: recordid first will conflict with PRSS
    // ctx.upgrade_record(
    //     RecordId::FIRST,
    //     AdditiveShare::<Fp25519, { Fp25519::VECTORIZE }>::expand(&v),
    // ).await
}

// pub(super) async fn compute_prf<C, V, const N: usize>(
//     ctx: C,
//     validator: V,
//     record_id: RecordId,
//     key: &AdditiveShare<Fp25519, N>,
//     value: AdditiveShare<Fp25519, N>
// ) -> Result<[u64; N], Error>
// where
//     C: UpgradedContext<Field = Fp25519>,
//     V: Validator<C>,
//     AdditiveShare<Fp25519, N>: PrfSharing<C, N>,
// {
//     // TODO: this will blow up because the same context is used to upgrade two different things
//     eval_dy_prf(ctx, validator, record_id, key, value).await
// }

/// evaluates the Dodis-Yampolski PRF g^(1/(k+x))
/// the input x and k are secret shared over finite field Fp25519, i.e. the scalar field of curve 25519
/// PRF key k needs to be generated using `gen_prf_key`
///  x is the match key in Fp25519 format
/// outputs a u64 as specified in `protocol/prf_sharding/mod.rs`, all parties learn the output
///
/// This function takes an expanded key `key` replicated with the vectorization factor
/// that matches the `x` and the same key is used to evaluate the PRF function across
/// all records.
///
/// `r` is a mask
/// # Errors
/// Propagates errors from multiplications, reveal and scalar multiplication
/// # Panics
/// Never as of when this comment was written, but the compiler didn't know that.
pub(super) fn eval_dy_prf<C, const N: usize>(
    ctx: C,
    record_id: RecordId,
    k: &AdditiveShare<Fp25519, N>,
    x: AdditiveShare<Fp25519, N>
) -> impl Future<Output = Result<[u64; N], Error>> + Send
where
    C: UpgradedContext<Field = Fp25519>,
    AdditiveShare<Fp25519, N>: PrfSharing<C, N, Field = Fp25519>,
    Fp25519: FieldSimd<N>,
    RP25519: Vectorizable<N>,
{
    //compute x+k
    let mut y = x + k;

    async move {

        // todo: try_join
        let r: AdditiveShare<Fp25519, N> = ctx.narrow(&Step::GenRandomMask).prss().generate(record_id);
        let y = ctx.clone().upgrade_record(record_id, y).await?;

        // compute (g^left, g^right)
        let sh_gr = AdditiveShare::<RP25519, N>::from(r.clone());

        let r = ctx.narrow(&Step::UpgradeMask).upgrade_record(record_id, r).await?;

        // compute y <- r*y
        let y = y
            .multiply(&r, ctx.narrow(&Step::MultMaskWithPRFInput), record_id)
            .await?;

        //reconstruct (z,R)
        let (gr, z): (<RP25519 as Vectorizable<N>>::Array, <Fp25519 as Vectorizable<N>>::Array) = mac_validated_reveal(
            ctx,
            record_id,
            RevealablePrfSharing::<C, AdditiveShare<Fp25519, N>, N> {
                curve_point_share: sh_gr,
                prf_share: y,
            },
        )
        .await?;

        //compute R^(1/z) to u64
        Ok(zip(gr, z)
            .map(|(gr, z)| (gr * z.invert()).into())
            .collect::<Vec<_>>()
            .try_into()
            .expect("iteration over arrays"))
    }
}

#[cfg(all(test, unit_test))]
mod test {
    use std::iter;
    use futures_util::future::try_join_all;
    use rand::Rng;

    use crate::{
        error::Error,
        ff::{boolean::Boolean, curve_points::RP25519, ec_prime_field::Fp25519, Invert},
        helpers::TotalRecords,
        protocol::{
            context::{Context, UpgradableContext, UpgradedContext, Validator},
            ipa_prf::prf_eval::eval_dy_prf,
            BasicProtocols,
        },
        secret_sharing::{replicated::semi_honest::AdditiveShare, IntoShares},
        test_executor::run,
        test_fixture::{Reconstruct, Runner, TestWorld},
    };
    use crate::helpers::in_memory_config::MaliciousHelper;
    use crate::helpers::Role;
    use crate::helpers::stream::ChunkBuffer;
    use crate::protocol::context::validator::Upgradeable;
    use crate::protocol::ipa_prf::prf_eval::PrfSharing;
    use crate::protocol::ipa_prf::step::PrfStep;
    use crate::protocol::ipa_prf::step::PrfStep::RevealR;
    use crate::protocol::RecordId;
    use crate::secret_sharing::replicated::malicious;
    use crate::seq_join::SeqJoin;
    use crate::test_fixture::TestWorldConfig;

    ///defining test input struct
    #[derive(Copy, Clone)]
    struct ShuffledTestInput {
        match_key: Fp25519,
    }

    ///defining test output struct
    #[derive(Debug, PartialEq)]
    struct TestOutput {
        match_key_pseudonym: u64,
    }

    fn test_input(mk: u64) -> ShuffledTestInput {
        ShuffledTestInput {
            match_key: Fp25519::from(mk),
        }
    }

    impl IntoShares<AdditiveShare<Fp25519>> for ShuffledTestInput {
        fn share_with<R: Rng>(self, rng: &mut R) -> [AdditiveShare<Fp25519>; 3] {
            self.match_key.share_with(rng)
        }
    }

    impl Reconstruct<TestOutput> for [&u64; 3] {
        fn reconstruct(&self) -> TestOutput {
            TestOutput {
                match_key_pseudonym: if *self[0] == *self[1] && *self[0] == *self[2] {
                    *self[0]
                } else {
                    0u64
                },
            }
        }
    }

    /// generates match key pseudonyms from match keys (in Fp25519 format) and PRF key
    /// PRF key needs to be generated separately using `gen_prf_key`
    ///
    /// `gen_prf_key` is not included such that `compute_match_key_pseudonym` can be tested for correctness
    /// # Errors
    /// Propagates errors from multiplications
    async fn compute_match_key_pseudonym<C>(
        ctx: C,
        prf_key: AdditiveShare<Fp25519>,
        input_match_keys: Vec<AdditiveShare<Fp25519>>,
    ) -> Result<Vec<u64>, Error>
    where
        C: UpgradableContext,
        AdditiveShare<Fp25519>: PrfSharing<C::BatchUpgradedContext<Fp25519>, 1, Field = Fp25519>
    {
        let validator = ctx.batch_validator::<Fp25519>(TotalRecords::specified(input_match_keys.len())?);
        let ctx = validator.context();

        let futures = input_match_keys
            .into_iter()
            .zip(iter::repeat(ctx.clone()))
            .enumerate()
            .map(|(i, (x, ctx))| {
                eval_dy_prf(ctx, i.into(), &prf_key, x)
            });
        Ok(ctx.try_join(futures).await?.into_iter().flatten().collect())
    }

    fn test_case() -> (Vec<ShuffledTestInput>, Vec<TestOutput>, Fp25519) {
        let u = 3_216_412_445u64;
        let k: Fp25519 = Fp25519::from(u);
        let input = vec![
            //first two need to be identical for tests to succeed
            test_input(3),
            test_input(3),
            test_input(23_443_524_523),
            test_input(56),
            test_input(895_764_542),
            test_input(456_764_576),
            test_input(56),
            test_input(3),
            test_input(56),
            test_input(23_443_524_523),
        ];

        let output: Vec<_> = input
            .iter()
            .map(|&x| TestOutput {
                match_key_pseudonym: (RP25519::from((x.match_key + k).invert())).into(),
            })
            .collect();

        (input, output, k)
    }

    ///testing correctness of DY PRF evaluation
    /// by checking MPC generated pseudonym with pseudonym generated in the clear
    #[test]
    fn semi_honest() {
        run(|| async move {
            let world = TestWorld::default();
            let (records, expected, key) = test_case();

            let result: Vec<_> = world
                .semi_honest(
                    (records.into_iter(), key),
                    |ctx, (input_match_keys, prf_key)| async move {
                        compute_match_key_pseudonym(ctx, prf_key, input_match_keys)
                            .await
                            .unwrap()
                    },
                )
                .await
                .reconstruct();
            assert_eq!(result, expected);
            assert_eq!(result[0], result[1]);
        });
    }


    #[test]
    fn malicious() {
        run(|| async move {
            let world = TestWorld::default();
            let (records, expected, key) = test_case();

            let result = world.malicious((records.into_iter(), key), |ctx, (match_key_shares, prf_key)| async move {
                let v = ctx.batch_validator(TotalRecords::from(match_key_shares.len()));
                let upgraded_ctx = v.context();

                let prf_key = &prf_key;
                let r: Vec<_> = try_join_all(match_key_shares.into_iter().zip(iter::repeat(upgraded_ctx))
                    .enumerate()
                    .map(|(i, (share, ctx))| {
                        async move {
                            let record_id = RecordId::from(i);
                            eval_dy_prf(ctx, record_id, prf_key, share).await
                        }
                    })).await.unwrap();

                r.into_iter().flatten().collect::<Vec<_>>()
            }).await.reconstruct();
            assert_eq!(result, expected);
            assert_eq!(result[0], result[1]);
        })
    }

    #[test]
    fn malicious_attack() {
        run(|| async move {
            for attacker_role in Role::all() {
                let mut config = TestWorldConfig::default();
                config.stream_interceptor = MaliciousHelper::new(*attacker_role, config.role_assignment(), |ctx, data| {
                    if ctx.gate.as_ref().contains(PrfStep::RevealR.as_ref()) {
                        if data[0] == 0 { data[0] = 1 } else { data[0] = 0 }
                    }
                });
                let world = TestWorld::new_with(&config);
                let (records, _, key) = test_case();
                let result = world.malicious((records.into_iter(), key), |ctx, (match_key_shares, prf_key)| async move {
                    let v = ctx.batch_validator(TotalRecords::from(match_key_shares.len()));
                    let upgraded_ctx = v.context();
                    let my_role = upgraded_ctx.role();

                    let prf_key = &prf_key;
                    match try_join_all(match_key_shares.into_iter().zip(iter::repeat(upgraded_ctx))
                        .enumerate()
                        .map(|(i, (share, ctx))| {
                            async move {
                                let record_id = RecordId::from(i);
                                eval_dy_prf(ctx, record_id, prf_key, share).await
                            }
                        })).await {
                        Ok(_) if my_role == *attacker_role => {},
                        Err(Error::MaliciousSecurityCheckFailed) => {}
                        Ok(_) | Err(_) => { panic!("Malicious validation check passed when it shouldn't have") }
                    }
                }).await.reconstruct();
            }
        });
    }
}
