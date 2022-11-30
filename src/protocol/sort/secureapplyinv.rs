use crate::secret_sharing::SecretSharing;
use crate::{
    error::Error,
    ff::Field,
    protocol::{context::Context, sort::ApplyInvStep::ShuffleInputs},
};
use embed_doc_image::embed_doc_image;

use super::{apply::apply_inv, shuffle::shuffle_shares};

/// This is an implementation of ApplyInv (Algorithm 4) found in the paper:
/// "An Efficient Secure Three-Party Sorting Protocol with an Honest Majority"
/// by K. Chida, K. Hamada, D. Ikarashi, R. Kikuchi, N. Kiribuchi, and B. Pinkas
/// <https://eprint.iacr.org/2019/695.pdf>
/// This is a protocol that applies the inverse of a secret-shared permutation to a vector of secret-shared values
/// Input: Each helpers know their own secret shares of input and permutation
/// Output: At the end of the protocol, all helpers receive inputs after the permutation is applied
#[embed_doc_image("secureapplyinv", "images/sort/secureapplyinv.png")]
/// This algorithm applies a permutation to the `input` vector. The permutation is secret-shared,
/// and none of the helpers should learn it through this protocol.
/// To keep the permutation secret, it (and the inputs) are first randomly securely shuffled.
/// After this shuffle, the permutation can be revealed.
/// An adversary can only obtain a shuffled permutation, which is just a random permutation.
/// Steps
/// ![Secure Apply Inv steps][secureapplyinv]
/// 1. Generate random permutations using prss
/// 2. Secret shared permutation is shuffled with random permutations
/// 3. Secret shared value is shuffled using the same random permutations
/// 4. The permutation is revealed
/// 5. All helpers call `apply` to apply the permutation locally.
pub async fn secureapplyinv<F: Field, S: SecretSharing<F>, C: Context<F, Share = S>>(
    ctx: C,
    input: Vec<S>,
    random_permutations_for_shuffle: (&[u32], &[u32]),
    shuffled_sort_permutation: &[u32],
) -> Result<Vec<S>, Error> {
    let mut shuffled_input = shuffle_shares(
        input,
        random_permutations_for_shuffle,
        ctx.narrow(&ShuffleInputs),
    )
    .await?;

    apply_inv(shuffled_sort_permutation, &mut shuffled_input);
    Ok(shuffled_input)
}

#[cfg(all(test, not(feature = "shuttle")))]
mod tests {
    mod semi_honest {
        use proptest::prelude::Rng;
        use rand::seq::SliceRandom;

        use crate::protocol::context::Context;
        use crate::protocol::sort::secureapplyinv::secureapplyinv;
        use crate::test_fixture::{Reconstruct, Runner};
        use crate::{
            ff::Fp31,
            protocol::{
                sort::{apply::apply_inv, generate_permutation::shuffle_and_reveal_permutation},
                QueryId,
            },
            test_fixture::TestWorld,
        };

        #[tokio::test]
        pub async fn semi_honest() {
            const BATCHSIZE: u32 = 25;
            let world = TestWorld::new(QueryId);
            let mut rng = rand::thread_rng();

            let mut input = Vec::with_capacity(BATCHSIZE as usize);
            input.resize_with(BATCHSIZE.try_into().unwrap(), || rng.gen::<Fp31>());

            let mut permutation: Vec<u32> = (0..BATCHSIZE).collect();
            permutation.shuffle(&mut rng);

            let mut expected_result = input.clone();

            // Applying permutation on the input in clear to get the expected result
            apply_inv(&permutation, &mut expected_result);

            let permutation_iter = permutation.into_iter().map(u128::from).map(Fp31::from);

            let result = world
                .semi_honest(
                    (input, permutation_iter),
                    |ctx, (m_shares, m_perms)| async move {
                        let perm_and_randoms = shuffle_and_reveal_permutation(
                            ctx.narrow("shuffle_reveal"),
                            BATCHSIZE,
                            m_perms,
                        )
                        .await
                        .unwrap();
                        secureapplyinv(
                            ctx,
                            m_shares,
                            (
                                perm_and_randoms.1 .0.as_slice(),
                                perm_and_randoms.1 .1.as_slice(),
                            ),
                            &perm_and_randoms.0,
                        )
                        .await
                        .unwrap()
                    },
                )
                .await;

            assert_eq!(&expected_result[..], &result.reconstruct());
        }
    }
}
