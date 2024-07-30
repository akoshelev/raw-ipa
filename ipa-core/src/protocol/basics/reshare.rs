use async_trait::async_trait;
use embed_doc_image::embed_doc_image;

use crate::{
    error::Error,
    ff::Field,
    helpers::{Direction, Role},
    protocol::{
        basics::mul::step::MaliciousMultiplyStep::{RandomnessForValidation, ReshareRx},
        context::{Context, SpecialAccessToUpgradedContext, UpgradedMaliciousContext},
        prss::SharedRandomness,
        RecordId,
    },
    secret_sharing::replicated::{
        malicious::{
            AdditiveShare as MaliciousReplicated, ExtendableField,
            ThisCodeIsAuthorizedToDowngradeFromMalicious,
        },
        semi_honest::AdditiveShare as Replicated,
        ReplicatedSecretSharing,
    },
};

#[embed_doc_image("reshare", "images/sort/reshare.png")]
/// Trait for reshare protocol to renew shares of a secret value for all 3 helpers.
///
/// Steps
/// ![Reshare steps][reshare]
/// 1. While calculating for a helper, we call pseudo random secret sharing (prss) to get random values which match
///    with those generated by other helpers (say `rand_left`, `rand_right`)
///    `to_helper.left` knows `rand_left` (named r1) and `to_helper.right` knows `rand_right` (named r0)
/// 2. `to_helper.left` calculates part1 = (a1 + a2) - r2 = Same as (input.left() + input.right()) - r1 from helper POV
///    `to_helper.right` calculates part2 = (a3 - r3) = Same as (input.left() - r0) from helper POV
/// 3. `to_helper.left` and `to_helper.right` exchange their calculated shares
/// 4. Everyone sets their shares
///    `to_helper.left`  = (part1 + part2, `rand_left`)  = (part1 + part2, r1)
///    `to_helper`       = (`rand_left`, `rand_right`)     = (r0, r1)
///    `to_helper.right` = (`rand_right`, part1 + part2) = (r0, part1 + part2)
#[async_trait]
pub trait Reshare<C: Context>: Sized + 'static {
    async fn reshare<'fut>(
        &self,
        ctx: C,
        record_id: RecordId,
        to_helper: Role,
    ) -> Result<Self, Error>
    where
        C: 'fut;
}

#[async_trait]
/// Reshare(i, \[x\])
/// This implements semi-honest reshare algorithm of "Efficient Secure Three-Party Sorting Protocol with an Honest Majority" at communication cost of 2R.
/// Input: Pi-1 and Pi+1 know their secret shares
/// Output: At the end of the protocol, all 3 helpers receive their shares of a new, random secret sharing of the secret value
impl<C: Context, F: Field> Reshare<C> for Replicated<F> {
    async fn reshare<'fut>(
        &self,
        ctx: C,
        record_id: RecordId,
        to_helper: Role,
    ) -> Result<Self, Error>
    where
        C: 'fut,
    {
        let r = ctx.prss().generate_fields(record_id);

        // `to_helper.left` calculates part1 = (self.0 + self.1) - r1 and sends part1 to `to_helper.right`
        // This is same as (a1 + a2) - r2 in the diagram
        if ctx.role() == to_helper.peer(Direction::Left) {
            let part1 = self.left() + self.right() - r.1;
            ctx.send_channel(to_helper.peer(Direction::Right))
                .send(record_id, part1)
                .await?;

            // Sleep until `to_helper.right` sends us their part2 value
            let part2 = ctx
                .recv_channel(to_helper.peer(Direction::Right))
                .receive(record_id)
                .await?;

            Ok(Replicated::new(part1 + part2, r.1))
        } else if ctx.role() == to_helper.peer(Direction::Right) {
            // `to_helper.right` calculates part2 = (self.left() - r0) and sends it to `to_helper.left`
            // This is same as (a3 - r3) in the diagram
            let part2 = self.left() - r.0;
            ctx.send_channel(to_helper.peer(Direction::Left))
                .send(record_id, part2)
                .await?;

            // Sleep until `to_helper.left` sends us their part1 value
            let part1: F = ctx
                .recv_channel(to_helper.peer(Direction::Left))
                .receive(record_id)
                .await?;

            Ok(Replicated::new(r.0, part1 + part2))
        } else {
            Ok(Replicated::new(r.0, r.1))
        }
    }
}

#[async_trait]
/// For malicious reshare, we run semi honest reshare protocol twice, once for x and another for rx and return the results
/// # Errors
/// If either of reshares fails
impl<'a, F: ExtendableField> Reshare<UpgradedMaliciousContext<'a, F>> for MaliciousReplicated<F> {
    async fn reshare<'fut>(
        &self,
        ctx: UpgradedMaliciousContext<'a, F>,
        record_id: RecordId,
        to_helper: Role,
    ) -> Result<Self, Error>
    where
        UpgradedMaliciousContext<'a, F>: 'fut,
    {
        use futures::future::try_join;
        let random_constant_ctx = ctx.narrow(&RandomnessForValidation);

        let (rx, x) = try_join(
            self.rx()
                .reshare(ctx.narrow(&ReshareRx).base_context(), record_id, to_helper),
            self.x()
                .access_without_downgrade()
                .reshare(ctx.base_context(), record_id, to_helper),
        )
        .await?;
        let malicious_input = MaliciousReplicated::new(x, rx);
        random_constant_ctx.accumulate_macs(record_id, &malicious_input);
        Ok(malicious_input)
    }
}

#[cfg(all(test, unit_test))]
mod tests {
    mod semi_honest {
        use crate::{
            ff::Fp32BitPrime,
            helpers::Role,
            protocol::{basics::Reshare, context::Context, prss::SharedRandomness, RecordId},
            rand::{thread_rng, Rng},
            test_fixture::{Reconstruct, Runner, TestWorld},
        };

        /// Validates that reshare protocol actually generates new additive shares using PRSS.
        #[tokio::test]
        async fn generates_unique_shares() {
            let world = TestWorld::default();

            for &target in Role::all() {
                let secret = thread_rng().gen::<Fp32BitPrime>();
                let shares = world
                    .semi_honest(secret, |ctx, share| async move {
                        let record_id = RecordId::from(0);
                        let ctx = ctx.set_total_records(1);

                        // run reshare protocol for all helpers except the one that does not know the input
                        if ctx.role() == target {
                            // test follows the reshare protocol
                            ctx.prss().generate_fields(record_id).into()
                        } else {
                            share.reshare(ctx, record_id, target).await.unwrap()
                        }
                    })
                    .await;

                let reshared_secret = shares.reconstruct();

                // if reshare cheated and just returned its input without adding randomness,
                // this test will catch it with the probability of error (1/|F|)^2.
                // Using 32 bit field is sufficient to consider error probability negligible
                assert_eq!(secret, reshared_secret);
            }
        }

        /// This test validates the correctness of the protocol, relying on `generates_unique_shares`
        /// to ensure security. It does not verify that helpers actually attempt to generate new shares
        /// so a naive implementation of reshare that just output shares `[O]` = `[I]` where `[I]` is
        /// the input will pass this test. However `generates_unique_shares` will fail this implementation.
        #[tokio::test]
        async fn correct() {
            let world = TestWorld::default();

            for &role in Role::all() {
                let secret = thread_rng().gen::<Fp32BitPrime>();
                let new_shares = world
                    .semi_honest(secret, |ctx, share| async move {
                        share
                            .reshare(ctx.set_total_records(1), RecordId::from(0), role)
                            .await
                            .unwrap()
                    })
                    .await;

                assert_eq!(secret, new_shares.reconstruct());
            }
        }
    }

    mod malicious {

        use rand::{distributions::Standard, prelude::Distribution};

        use crate::{
            error::Error,
            ff::{Field, Fp32BitPrime, Gf2, Gf32Bit},
            helpers::{in_memory_config::MaliciousHelper, Role},
            protocol::{
                basics::Reshare,
                context::{Context, UpgradableContext, UpgradedContext, Validator},
                RecordId,
            },
            rand::{thread_rng, Rng},
            secret_sharing::{replicated::malicious::ExtendableField, SharedValue},
            test_fixture::{Reconstruct, Runner, TestWorld, TestWorldConfig},
        };

        /// Relies on semi-honest protocol tests that enforce reshare to communicate and produce
        /// new shares.
        /// TODO: It would be great to have a test to validate that helpers cannot cheat. In this
        /// setting we have 1 helper that does not know the input and if another one is malicious
        /// adversary, we are only left with one honest helper that knows the input and can validate
        /// it.
        #[tokio::test]
        async fn correct() {
            let world = TestWorld::default();

            for &role in Role::all() {
                let secret = thread_rng().gen::<Fp32BitPrime>();
                let new_shares = world
                    .upgraded_malicious(secret, |ctx, share| async move {
                        share
                            .reshare(ctx.set_total_records(1), RecordId::from(0), role)
                            .await
                            .unwrap()
                    })
                    .await;

                assert_eq!(secret, new_shares.reconstruct());
            }
        }

        #[tokio::test]
        async fn fp32bit_reshare_validation_fail() {
            const PERTURBATIONS: [(Fp32BitPrime, Fp32BitPrime); 3] = [
                (Fp32BitPrime::ONE, Fp32BitPrime::ONE),
                (Fp32BitPrime::ONE, Fp32BitPrime::ZERO),
                (Fp32BitPrime::ZERO, Fp32BitPrime::ONE),
            ];
            malicious_validation_fail_helper::<Fp32BitPrime>(&PERTURBATIONS).await;
        }

        #[tokio::test]
        async fn gf2_reshare_validation_fail() {
            const PERTURBATIONS: [(Gf2, Gf32Bit); 3] = [
                (Gf2::ONE, Gf32Bit::ONE),
                (Gf2::ONE, Gf32Bit::ZERO),
                (Gf2::ZERO, Gf32Bit::ONE),
            ];
            malicious_validation_fail_helper::<Gf2>(&PERTURBATIONS).await;
        }

        async fn malicious_validation_fail_helper<F>(perturbations: &[(F, F::ExtendedField)])
        where
            F: ExtendableField,
            Standard: Distribution<F>,
        {
            const STEP: &str = "malicious-attack";

            /// Corrupts a single value `F` by running an additive attack.
            /// `binary_data` must carry the exact one value of `F` and the result
            /// will be written back to it, so the attack is run in place
            fn corrupt<F: Field>(binary_data: &mut [u8], add: F) {
                let v = F::deserialize_from_slice(binary_data) + add;
                v.serialize_to_slice(binary_data);
            }

            for (small_value, large_value) in perturbations.iter().copied() {
                for malicious_actor in [Role::H2, Role::H3] {
                    let mut config = TestWorldConfig::default();
                    config.stream_interceptor = MaliciousHelper::new(
                        malicious_actor,
                        config.role_assignment(),
                        move |ctx, data| {
                            if ctx.gate.as_ref().contains(STEP) {
                                if ctx.gate.as_ref().contains("reshare_rx") {
                                    corrupt(data, large_value);
                                } else {
                                    corrupt(data, small_value);
                                }
                            }
                        },
                    );
                    let world = TestWorld::new_with(&config);
                    let mut rng = thread_rng();
                    let a = rng.gen::<F>();
                    let to_helper = Role::H1;

                    world
                        .malicious(a, |ctx, a| async move {
                            let v = ctx.validator();
                            let m_ctx = v.context().set_total_records(1);
                            let m_a = v.context().upgrade(a).await.unwrap();

                            let m_reshared_a = m_a.reshare(m_ctx.narrow(STEP), RecordId::FIRST, to_helper).await.unwrap();
                            match v.validate(m_reshared_a).await {
                                Ok(result) => panic!("Got a result {result:?}"),
                                Err(err) => {
                                    assert!(matches!(err, Error::MaliciousSecurityCheckFailed));
                                }
                            }

                        }).await;
                }
            }
        }
    }
}
