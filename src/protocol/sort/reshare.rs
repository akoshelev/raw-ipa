use crate::ff::Field;
use crate::protocol::context::{Context, MaliciousContext};
use crate::secret_sharing::{MaliciousReplicated, SecretSharing};
use crate::{
    error::Error,
    helpers::{Direction, Role},
    protocol::{context::SemiHonestContext, sort::ReshareStep::ReshareMAC, RecordId},
    secret_sharing::Replicated,
};
use async_trait::async_trait;
use embed_doc_image::embed_doc_image;
use futures::future::try_join;

/// Trait for reshare protocol to renew shares of a secret value for all 3 helpers.
#[async_trait]
pub trait Reshare<F: Field> {
    type Share: SecretSharing<F>;

    async fn reshare(
        self,
        input: &Self::Share,
        record: RecordId,
        to_helper: Role,
    ) -> Result<Self::Share, Error>;
}

/// Reshare(i, \[x\])
// This implements semi-honest reshare algorithm of "Efficient Secure Three-Party Sorting Protocol with an Honest Majority" at communication cost of 2R.
// Input: Pi-1 and Pi+1 know their secret shares
// Output: At the end of the protocol, all 3 helpers receive their shares of a new, random secret sharing of the secret value
#[embed_doc_image("reshare", "images/sort/reshare.png")]
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
impl<F: Field> Reshare<F> for SemiHonestContext<'_, F> {
    type Share = Replicated<F>;
    async fn reshare(
        self,
        input: &Self::Share,
        record_id: RecordId,
        to_helper: Role,
    ) -> Result<Self::Share, Error> {
        let channel = self.mesh();
        let prss = self.prss();
        let (r0, r1) = prss.generate_fields(record_id);

        // `to_helper.left` calculates part1 = (input.0 + input.1) - r1 and sends part1 to `to_helper.right`
        // This is same as (a1 + a2) - r2 in the diagram
        if self.role() == to_helper.peer(Direction::Left) {
            let part1 = input.left() + input.right() - r1;
            channel
                .send(to_helper.peer(Direction::Right), record_id, part1)
                .await?;

            // Sleep until `to_helper.right` sends us their part2 value
            let part2 = channel
                .receive(to_helper.peer(Direction::Right), record_id)
                .await?;

            Ok(Replicated::new(part1 + part2, r1))
        } else if self.role() == to_helper.peer(Direction::Right) {
            // `to_helper.right` calculates part2 = (input.left() - r0) and sends it to `to_helper.left`
            // This is same as (a3 - r3) in the diagram
            let part2 = input.left() - r0;
            channel
                .send(to_helper.peer(Direction::Left), record_id, part2)
                .await?;

            // Sleep until `to_helper.left` sends us their part1 value
            let part1: F = channel
                .receive(to_helper.peer(Direction::Left), record_id)
                .await?;

            Ok(Replicated::new(r0, part1 + part2))
        } else {
            Ok(Replicated::new(r0, r1))
        }
    }
}

/// For malicious reshare, we run semi honest reshare protocol twice, once for x and another for rx and return the results
/// # Errors
/// If either of reshares fails
#[async_trait]
impl<F: Field> Reshare<F> for MaliciousContext<'_, F> {
    type Share = MaliciousReplicated<F>;
    async fn reshare(
        self,
        input: &Self::Share,
        record_id: RecordId,
        to_helper: Role,
    ) -> Result<Self::Share, Error> {
        let rx_ctx = self.narrow(&ReshareMAC);
        let (x, rx) = try_join(
            self.to_semi_honest()
                .reshare(input.x(), record_id, to_helper),
            rx_ctx
                .to_semi_honest()
                .reshare(input.rx(), record_id, to_helper),
        )
        .await?;
        Ok(MaliciousReplicated::new(x, rx))
    }
}

#[cfg(test)]
mod tests {
    mod semi_honest {
        use proptest::prelude::Rng;

        use rand::thread_rng;

        use crate::ff::Fp32BitPrime;
        use crate::protocol::context::Context;
        use crate::{
            helpers::Role,
            protocol::{sort::reshare::Reshare, QueryId, RecordId},
            test_fixture::{make_world, validate_and_reconstruct},
        };

        use crate::test_fixture::Runner;

        /// Validates that reshare protocol actually generates new shares using PRSS.
        #[tokio::test]
        async fn generates_unique_shares() {
            let world = make_world(QueryId);

            for &target in Role::all() {
                let secret = thread_rng().gen::<Fp32BitPrime>();
                let shares = world
                    .semi_honest(secret, |ctx, share| async move {
                        let record_id = RecordId::from(0);

                        // run reshare protocol for all helpers except the one that does not know the input
                        if ctx.role() == target {
                            // test follows the reshare protocol
                            ctx.prss().generate_fields(record_id).into()
                        } else {
                            ctx.reshare(&share, record_id, target).await.unwrap()
                        }
                    })
                    .await;

                let reshared_secret = validate_and_reconstruct(&shares[0], &shares[1], &shares[2]);

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
            let world = make_world(QueryId);

            for &role in Role::all() {
                let secret = thread_rng().gen::<Fp32BitPrime>();
                let new_shares = world
                    .semi_honest(secret, |ctx, share| async move {
                        ctx.reshare(&share, RecordId::from(0), role).await.unwrap()
                    })
                    .await;

                assert_eq!(
                    secret,
                    validate_and_reconstruct(&new_shares[0], &new_shares[1], &new_shares[2])
                );
            }
        }
    }

    mod malicious {
        use crate::ff::Fp32BitPrime;
        use crate::helpers::Role;
        use crate::protocol::sort::reshare::Reshare;
        use crate::protocol::{QueryId, RecordId};
        use crate::test_fixture::{
            join3, make_malicious_contexts, make_world, share_malicious,
            validate_and_reconstruct_malicious,
        };
        use rand::rngs::mock::StepRng;
        use rand::Rng;

        /// Relies on semi-honest protocol tests that enforce reshare to communicate and produce
        /// new shares.
        /// TODO: It would be great to have a test to validate that helpers cannot cheat. In this
        /// setting we have 1 helper that does not know the input and if another one is malicious
        /// adversary, we are only left with one honest helper that knows the input and can validate
        /// it.
        #[tokio::test]
        pub async fn correct() {
            let mut rand = StepRng::new(100, 1);
            let mut rng = rand::thread_rng();
            let world = make_world(QueryId);

            for &role in Role::all() {
                let r = rng.gen::<Fp32BitPrime>();
                let secret = rng.gen::<Fp32BitPrime>();

                let [ctx0, ctx1, ctx2] = make_malicious_contexts::<Fp32BitPrime>(&world);
                let shares = share_malicious(secret, r, &mut rand);
                let record_id = RecordId::from(0);

                let f = join3(
                    ctx0.ctx.reshare(&shares[0], record_id, role),
                    ctx1.ctx.reshare(&shares[1], record_id, role),
                    ctx2.ctx.reshare(&shares[2], record_id, role),
                )
                .await;

                let (new_secret, new_secret_times_r) =
                    validate_and_reconstruct_malicious(&f[0], &f[1], &f[2]);

                assert_eq!(secret, new_secret);
                assert_eq!(secret * r, new_secret_times_r);
            }
        }
    }
}
