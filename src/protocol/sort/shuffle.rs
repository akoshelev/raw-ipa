use std::iter::{repeat, zip};

use embed_doc_image::embed_doc_image;
use futures::future::try_join_all;
use rand::seq::SliceRandom;

use crate::protocol::prss::SequentialSharedRandomness;
use crate::secret_sharing::SecretSharing;
use crate::{
    error::Error,
    ff::Field,
    helpers::{Direction, Role},
    protocol::{context::Context, RecordId, Substep},
};

use super::{
    apply::{apply, apply_inv},
    ShuffleStep::{self, Step1, Step2, Step3},
};

#[derive(Debug)]
enum ShuffleOrUnshuffle {
    Shuffle,
    Unshuffle,
}

impl Substep for ShuffleOrUnshuffle {}
impl AsRef<str> for ShuffleOrUnshuffle {
    fn as_ref(&self) -> &str {
        match self {
            Self::Shuffle => "shuffle",
            Self::Unshuffle => "unshuffle",
        }
    }
}

/// This implements Fisher Yates shuffle described here <https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle>
#[allow(clippy::cast_possible_truncation)]
pub fn get_two_of_three_random_permutations(
    batch_size: u32,
    mut rng: (SequentialSharedRandomness, SequentialSharedRandomness),
) -> (Vec<u32>, Vec<u32>) {
    let mut left_permutation = (0..batch_size).collect::<Vec<_>>();
    let mut right_permutation = left_permutation.clone();

    left_permutation.shuffle(&mut rng.0);
    right_permutation.shuffle(&mut rng.1);

    (left_permutation, right_permutation)
}

/// This is SHUFFLE(Algorithm 1) described in <https://eprint.iacr.org/2019/695.pdf>.
/// This protocol shuffles the given inputs across 3 helpers making them indistinguishable to the helpers

// We call shuffle with helpers involved as (H2, H3), (H3, H1) and (H1, H2). In other words, the shuffle is being called for
// H1, H2 and H3 respectively (since they do not participate in the step) and hence are the recipients of the shuffle.
fn shuffle_for_helper(which_step: ShuffleStep) -> Role {
    match which_step {
        Step1 => Role::H1,
        Step2 => Role::H2,
        Step3 => Role::H3,
    }
}

#[allow(clippy::cast_possible_truncation)]
async fn reshare_all_shares<F: Field, S: SecretSharing<F>, C: Context<F, Share = S>>(
    input: &[S],
    ctx: C,
    to_helper: Role,
) -> Result<Vec<S>, Error> {
    let reshares = zip(repeat(ctx), input)
        .enumerate()
        .map(|(index, (ctx, input))| async move {
            ctx.reshare(input, RecordId::from(index), to_helper).await
        });
    try_join_all(reshares).await
}

/// `shuffle_or_unshuffle_once` is called for the helpers
/// i)   2 helpers receive permutation pair and choose the permutation to be applied
/// ii)  2 helpers apply the permutation to their shares
/// iii) reshare to `to_helper`
#[allow(clippy::cast_possible_truncation)]
async fn shuffle_or_unshuffle_once<F: Field, S: SecretSharing<F>, C: Context<F, Share = S>>(
    mut input: Vec<S>,
    random_permutations: (&[u32], &[u32]),
    shuffle_or_unshuffle: ShuffleOrUnshuffle,
    ctx: &C,
    which_step: ShuffleStep,
) -> Result<Vec<S>, Error> {
    let to_helper = shuffle_for_helper(which_step);
    let ctx = ctx.narrow(&which_step);

    if to_helper != ctx.role() {
        let permutation_to_apply = if to_helper.peer(Direction::Left) == ctx.role() {
            random_permutations.0
        } else {
            random_permutations.1
        };

        match shuffle_or_unshuffle {
            ShuffleOrUnshuffle::Shuffle => apply_inv(permutation_to_apply, &mut input),
            ShuffleOrUnshuffle::Unshuffle => apply(permutation_to_apply, &mut input),
        }
    }
    reshare_all_shares(&input, ctx, to_helper).await
}

#[embed_doc_image("shuffle", "images/sort/shuffle.png")]
/// Shuffle calls `shuffle_or_unshuffle_once` three times with 2 helpers shuffling the shares each time.
/// Order of calling `shuffle_or_unshuffle_once` is shuffle with (H2, H3), (H3, H1) and (H1, H2).
/// Each shuffle requires communication between helpers to perform reshare.
/// Infrastructure has a pre-requisite to distinguish each communication step uniquely.
/// For this, we have three shuffle steps one per `shuffle_or_unshuffle_once` i.e. Step1, Step2 and Step3.
/// The Shuffle object receives a step function and appends a `ShuffleStep` to form a concrete step
/// ![Shuffle steps][shuffle]
pub async fn shuffle_shares<F: Field, S: SecretSharing<F>, C: Context<F, Share = S>>(
    input: Vec<S>,
    random_permutations: (&[u32], &[u32]),
    ctx: C,
) -> Result<Vec<S>, Error> {
    let input = shuffle_or_unshuffle_once(
        input,
        random_permutations,
        ShuffleOrUnshuffle::Shuffle,
        &ctx,
        Step1,
    )
    .await?;
    let input = shuffle_or_unshuffle_once(
        input,
        random_permutations,
        ShuffleOrUnshuffle::Shuffle,
        &ctx,
        Step2,
    )
    .await?;
    shuffle_or_unshuffle_once(
        input,
        random_permutations,
        ShuffleOrUnshuffle::Shuffle,
        &ctx,
        Step3,
    )
    .await
}

#[embed_doc_image("unshuffle", "images/sort/unshuffle.png")]
/// Unshuffle calls `shuffle_or_unshuffle_once` three times with 2 helpers shuffling the shares each time in the opposite order to shuffle.
/// Order of calling `shuffle_or_unshuffle_once` is shuffle with (H1, H2), (H3, H1) and (H2, H3)
/// ![Unshuffle steps][unshuffle]
pub async fn unshuffle_shares<F: Field, S: SecretSharing<F>, C: Context<F, Share = S>>(
    input: Vec<S>,
    random_permutations: (&[u32], &[u32]),
    ctx: C,
) -> Result<Vec<S>, Error> {
    let input = shuffle_or_unshuffle_once(
        input,
        random_permutations,
        ShuffleOrUnshuffle::Unshuffle,
        &ctx,
        Step3,
    )
    .await?;
    let input = shuffle_or_unshuffle_once(
        input,
        random_permutations,
        ShuffleOrUnshuffle::Unshuffle,
        &ctx,
        Step2,
    )
    .await?;
    shuffle_or_unshuffle_once(
        input,
        random_permutations,
        ShuffleOrUnshuffle::Unshuffle,
        &ctx,
        Step1,
    )
    .await
}

#[cfg(all(test, not(feature = "shuttle")))]
mod tests {
    use std::collections::HashSet;
    use std::iter::zip;

    use crate::test_fixture::{logging, ParticipantSetup};
    use crate::{
        ff::Fp31,
        protocol::{
            context::Context,
            sort::shuffle::{
                get_two_of_three_random_permutations, shuffle_shares, unshuffle_shares,
                ShuffleOrUnshuffle,
            },
            QueryId, Step,
        },
        test_fixture::{
            generate_shares, join3, narrow_contexts, permutation_valid, Reconstruct, TestWorld,
        },
    };

    #[test]
    fn random_sequence_generated() {
        const BATCH_SIZE: u32 = 10000;

        logging::setup();

        let [p1, p2, p3] = ParticipantSetup::default().into_participants();
        let step = Step::default();
        let perm1 = get_two_of_three_random_permutations(BATCH_SIZE, p1.sequential(&step));
        let perm2 = get_two_of_three_random_permutations(BATCH_SIZE, p2.sequential(&step));
        let perm3 = get_two_of_three_random_permutations(BATCH_SIZE, p3.sequential(&step));

        assert_eq!(perm1.1, perm2.0);
        assert_eq!(perm2.1, perm3.0);
        assert_eq!(perm3.1, perm1.0);

        // Due to less randomness, the below three asserts can fail. However, the chance of failure is
        // 1/18Quintillian (a billion billion since u64 is used to generate randomness)! Hopefully we should not hit that
        assert_ne!(perm1.0, perm1.1);
        assert_ne!(perm2.0, perm2.1);
        assert_ne!(perm3.0, perm3.1);

        assert!(permutation_valid(&perm1.0));
        assert!(permutation_valid(&perm2.0));
        assert!(permutation_valid(&perm3.0));
    }

    #[tokio::test]
    async fn semi_honest() {
        let world = TestWorld::<Fp31>::new(QueryId);
        let context = world.contexts();

        let batchsize = 25;
        let input: Vec<u8> = (0..batchsize).collect();
        let hashed_input: HashSet<u8> = input.clone().into_iter().collect();
        let input_len = u32::from(batchsize);

        let input_u128: Vec<u128> = input.iter().map(|x| u128::from(*x)).collect();
        let shares = generate_shares(&input_u128);

        let original = shares.clone();

        let perm1 = get_two_of_three_random_permutations(input_len, context[0].prss_rng());
        let perm2 = get_two_of_three_random_permutations(input_len, context[1].prss_rng());
        let perm3 = get_two_of_three_random_permutations(input_len, context[2].prss_rng());

        let [c0, c1, c2] = context;

        let [shares0, shares1, shares2] = shares;
        let h0_future = shuffle_shares(shares0, (perm1.0.as_slice(), perm1.1.as_slice()), c0);
        let h1_future = shuffle_shares(shares1, (perm2.0.as_slice(), perm2.1.as_slice()), c1);
        let h2_future = shuffle_shares(shares2, (perm3.0.as_slice(), perm3.1.as_slice()), c2);

        let results = join3(h0_future, h1_future, h2_future).await;
        let mut hashed_output_secret = HashSet::new();
        let mut output_secret = Vec::new();
        for (r0, (r1, r2)) in zip(results[0].iter(), zip(results[1].iter(), results[2].iter())) {
            let val = (r0, r1, r2).reconstruct();
            output_secret.push(u8::from(val));
            hashed_output_secret.insert(u8::from(val));
        }

        // Order of shares should now be different from original
        assert_ne!(results, original);
        // Secrets should be shuffled also
        assert_ne!(output_secret, input);

        // Shuffled output should have same inputs
        assert_eq!(hashed_output_secret, hashed_input);
    }

    #[tokio::test]
    async fn shuffle_unshuffle() {
        const BATCHSIZE: u32 = 5;

        let world = TestWorld::<Fp31>::new(QueryId);
        let context = world.contexts();

        let input: Vec<u128> = (0..u128::try_from(BATCHSIZE).unwrap()).collect();

        let shares = generate_shares(&input);

        let perm1 = get_two_of_three_random_permutations(BATCHSIZE, context[0].prss_rng());
        let perm2 = get_two_of_three_random_permutations(BATCHSIZE, context[1].prss_rng());
        let perm3 = get_two_of_three_random_permutations(BATCHSIZE, context[2].prss_rng());

        let shuffled: [_; 3] = {
            let [ctx0, ctx1, ctx2] = narrow_contexts(&context, &ShuffleOrUnshuffle::Shuffle);
            let [shares0, shares1, shares2] = shares;
            let h0_future = shuffle_shares(shares0, (perm1.0.as_slice(), perm1.1.as_slice()), ctx0);
            let h1_future = shuffle_shares(shares1, (perm2.0.as_slice(), perm2.1.as_slice()), ctx1);
            let h2_future = shuffle_shares(shares2, (perm3.0.as_slice(), perm3.1.as_slice()), ctx2);

            join3(h0_future, h1_future, h2_future).await
        };
        let unshuffled: [_; 3] = {
            let [ctx0, ctx1, ctx2] = narrow_contexts(&context, &ShuffleOrUnshuffle::Unshuffle);
            let [shuffled0, shuffled1, shuffled2] = shuffled;
            let h0_future =
                unshuffle_shares(shuffled0, (perm1.0.as_slice(), perm1.1.as_slice()), ctx0);
            let h1_future =
                unshuffle_shares(shuffled1, (perm2.0.as_slice(), perm2.1.as_slice()), ctx1);
            let h2_future =
                unshuffle_shares(shuffled2, (perm3.0.as_slice(), perm3.1.as_slice()), ctx2);

            // When unshuffle and shuffle are called with same step, they undo each other's effect
            join3(h0_future, h1_future, h2_future).await
        };

        assert_eq!(&input[..], &unshuffled.reconstruct());
    }
}
