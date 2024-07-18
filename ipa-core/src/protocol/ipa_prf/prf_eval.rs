use std::future::Future;
use std::iter::zip;
use futures_util::future::try_join;
use crate::{
    error::Error,
    ff::{boolean::Boolean, curve_points::RP25519, ec_prime_field::Fp25519},
    helpers::TotalRecords,
    protocol::{
        basics::{Reveal, SecureMul},
        context::Context,
        ipa_prf::step::PrfStep as Step,
        prss::{FromPrss, SharedRandomness},
        RecordId,
    },
    secret_sharing::{
        replicated::semi_honest::AdditiveShare, Sendable, SharedValue, StdArray, Vectorizable,
    },
};
use crate::ff::Expand;
use crate::protocol::BasicProtocols;
use crate::protocol::context::{UpgradableContext, UpgradedContext};
use crate::protocol::ipa_prf::boolean_ops::step::MultiplicationStep::Add;
use crate::secret_sharing::{Linear, SecretSharing};
use crate::seq_join::assert_send;

/// generates match key pseudonyms from match keys (in Fp25519 format) and PRF key
/// PRF key needs to be generated separately using `gen_prf_key`
///
/// `gen_prf_key` is not included such that `compute_match_key_pseudonym` can be tested for correctness
/// # Errors
/// Propagates errors from multiplications
pub async fn compute_match_key_pseudonym<C>(
    sh_ctx: C,
    prf_key: AdditiveShare<Fp25519>,
    input_match_keys: Vec<AdditiveShare<Fp25519>>,
) -> Result<Vec<u64>, Error>
where
    C: Context,
    AdditiveShare<Boolean, 1>: SecureMul<C>,
    AdditiveShare<Fp25519>: SecureMul<C>,
{
    todo!()
    // let ctx = sh_ctx.set_total_records(TotalRecords::specified(input_match_keys.len())?);
    // let futures = input_match_keys
    //     .into_iter()
    //     .enumerate()
    //     .map(|(i, x)| eval_dy_prf(ctx.clone(), i.into(), &prf_key, x));
    // Ok(ctx.try_join(futures).await?.into_iter().flatten().collect())
}

impl<const N: usize> From<AdditiveShare<Fp25519, N>> for AdditiveShare<RP25519, N>
where
    Fp25519: Vectorizable<N>,
    RP25519: Vectorizable<N, Array = StdArray<RP25519, N>>,
    StdArray<RP25519, N>: Sendable,
{
    fn from(value: AdditiveShare<Fp25519, N>) -> Self {
        let (left_arr, right_arr) =
            StdArray::<RP25519, N>::from_tuple_iter(value.into_unpacking_iter().map(|sh| {
                let (l, r) = sh.as_tuple();
                (RP25519::from(l), RP25519::from(r))
            }));
        Self::new_arr(left_arr, right_arr)
    }
}

/// generates PRF key k as secret sharing over Fp25519
pub fn gen_prf_key<C>(ctx: &C) -> AdditiveShare<Fp25519, {Fp25519::VECTORIZE}>
where
    C: Context,
{
    let v: AdditiveShare<Fp25519, 1> = ctx.narrow(&Step::PRFKeyGen).prss().generate(RecordId(0));
    AdditiveShare::<Fp25519, { Fp25519::VECTORIZE }>::expand(&v)
}

/// evaluates the Dodis-Yampolski PRF g^(1/(k+x))
/// the input x and k are secret shared over finite field Fp25519, i.e. the scalar field of curve 25519
/// PRF key k needs to be generated using `gen_prf_key`
///  x is the match key in Fp25519 format
/// outputs a u64 as specified in `protocol/prf_sharding/mod.rs`, all parties learn the output
///
/// This function takes an expanded key `key` replicated with the vectorization factor
/// that matches the `x` and the same key is used to evaluate the PRF function across
/// all records.
/// # Errors
/// Propagates errors from multiplications, reveal and scalar multiplication
/// # Panics
/// Never as of when this comment was written, but the compiler didn't know that.
pub fn eval_dy_prf<C, L, R>(
    ctx: C,
    record_id: RecordId,
    k: &L,
    x: L,
) -> impl Future<Output = Result<[u64; Fp25519::VECTORIZE], Error>> + Send
where
    C: UpgradedContext,
    L: Linear<Fp25519> + BasicProtocols<C, Fp25519, { Fp25519::VECTORIZE }>,
    R: SecretSharing<RP25519> + Reveal<C, Output = <RP25519 as Vectorizable<{ Fp25519::VECTORIZE }>>::Array> + From<L>,
{
    let sh_r: L = ctx.narrow(&Step::GenRandomMask).prss().generate(record_id);
    //compute x+k
    let mut y = x + k;

    async move {
        // compute y <- r*y
        y = y
            .multiply(&sh_r, ctx.narrow(&Step::MultMaskWithPRFInput), record_id)
            .await?;

        // compute (g^left, g^right)
        let sh_gr = R::from(sh_r);

        //reconstruct (z,R)
        let (gr, z) = assert_send(try_join(
            sh_gr.reveal(ctx.narrow(&Step::RevealR), record_id),
            y.reveal(ctx.narrow(&Step::Revealz), record_id)
        )).await?;

        //compute R^(1/z) to u64
        Ok(zip(gr, z)
            .map(|(gr, z)| u64::from(gr * z.invert()))
            .collect::<Vec<_>>()
            .try_into()
            .expect("iteration over arrays"))
    }
}

#[cfg(all(test, unit_test))]
mod test {
    use rand::Rng;

    use crate::{
        ff::{curve_points::RP25519, ec_prime_field::Fp25519},
        protocol::ipa_prf::prf_eval::compute_match_key_pseudonym,
        secret_sharing::{replicated::semi_honest::AdditiveShare, IntoShares},
        test_executor::run,
        test_fixture::{Reconstruct, Runner, TestWorld},
    };

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

    ///testing correctness of DY PRF evaluation
    /// by checking MPC generated pseudonym with pseudonym generated in the clear
    #[test]
    fn semi_honest() {
        run(|| async move {
            let world = TestWorld::default();

            //first two need to be identical for test to succeed
            let records: Vec<ShuffledTestInput> = vec![
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

            //PRF Key Gen
            let u = 3_216_412_445u64;
            let k: Fp25519 = Fp25519::from(u);

            let expected: Vec<TestOutput> = records
                .iter()
                .map(|&x| TestOutput {
                    match_key_pseudonym: (RP25519::from((x.match_key + k).invert())).into(),
                })
                .collect();

            let result: Vec<_> = world
                .semi_honest(
                    (records.into_iter(), k),
                    |ctx, (input_match_keys, prf_key)| async move {
                        compute_match_key_pseudonym::<_>(ctx, prf_key, input_match_keys)
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
}
