use crate::{
    error::BoxError,
    field::Field,
    helpers::mesh::{Gateway, Mesh},
    protocol::{context::ProtocolContext, IPAProtocolStep, RecordId, SortStep},
    secret_sharing::Replicated,
};

use futures::future::try_join_all;
use rhs_first_assign::rhs_first_assign;

#[derive(Debug)]
pub struct BitPermutations<'a, F> {
    input: &'a [Replicated<F>],
}

impl<'a, F: Field> BitPermutations<'a, F> {
    /// Create an object to generate bit permutations for a given bit column of query. This is GENBITPERM(Algorithm 3) from the paper
    #[allow(dead_code)]
    pub fn new(input: &'a [Replicated<F>]) -> BitPermutations<'a, F> {
        Self { input }
    }

    /// In this step, multiplication inputs are generated locally at each helper from input share, x in following steps
    /// 1. calculate 1 - x, x and concatenate them
    /// 2. calculate cumulative sum at each vector row
    /// 3. return back tuple of step 1 and step 2 output
    fn prepare_mult_inputs<M: Mesh + 'a, G: Gateway<M, IPAProtocolStep>>(
        &self,
        ctx: &ProtocolContext<'a, G, IPAProtocolStep>,
    ) -> impl Iterator<Item = (u32, (Replicated<F>, Replicated<F>))> + 'a
    where
        F: Field,
    {
        let channel = ctx
            .gateway
            .get_channel(IPAProtocolStep::Sort(SortStep::BitPermutations));

        self.input
            .iter()
            .map(move |x: &Replicated<F>| channel.share_of_one() - *x)
            .chain(self.input.iter().copied())
            .enumerate()
            .scan(Replicated::<F>::new(F::ZERO, F::ZERO), |sum, (index, n)| {
                *sum += n;
                Some((index as u32, (n, *sum)))
            })
    }

    /// multiplies the input vector pairs across helpers and returns result
    /// For this, it spawns all multiplication, wait for them to finish in parallel and then collect the results

    #[allow(clippy::cast_possible_truncation)]
    async fn secure_multiply<M: Mesh + 'a, G: Gateway<M, IPAProtocolStep>>(
        &self,
        ctx: &ProtocolContext<'a, G, IPAProtocolStep>,
        mult_input: (u32, (Replicated<F>, Replicated<F>)),
    ) -> Result<Replicated<F>, BoxError>
    where
        F: Field,
    {
        ctx.multiply(
            RecordId::from(mult_input.0),
            IPAProtocolStep::Sort(SortStep::BitPermutations),
        )
        .await
        .execute(mult_input.1 .0, mult_input.1 .1)
        .await
    }

    /// Executes sorting of a bit column on mpc helpers. Each helper receives their input shares and do following steps
    /// 1. local computation by `prepare_mult_inputs` which outputs 2 vectors [x,y]
    /// 2. multiply each row of previous output individually (i.e. x*y) across mpc helpers.
    /// 3. add ith column by i+len to obtain helper's share of sorted location, where len is same as input shares length
    #[allow(dead_code)]
    #[rhs_first_assign]
    pub async fn execute<M: Mesh, G: Gateway<M, IPAProtocolStep>>(
        &self,
        ctx: &ProtocolContext<'_, G, IPAProtocolStep>,
    ) -> Result<Vec<Replicated<F>>, BoxError>
    where
        F: Field,
    {
        let mult_input = self.prepare_mult_inputs(ctx);
        let async_multiply =
            mult_input.map(|input| async move { self.secure_multiply(ctx, input).await });
        let mut mult_output = try_join_all(async_multiply).await?;

        assert_eq!(mult_output.len(), self.input.len() * 2);
        // Generate permutation location
        let len = mult_output.len() / 2;
        for i in 0..len {
            mult_output[i] += mult_output[i + len];
        }
        mult_output.truncate(len);

        Ok(mult_output)
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::mock::StepRng;
    use tokio::try_join;

    use crate::{
        field::Fp31,
        protocol::{sort::bit_permutations::BitPermutations, IPAProtocolStep, QueryId},
        test_fixture::{make_contexts, make_world, share, validate_and_reconstruct, TestWorld},
    };

    #[tokio::test]
    pub async fn bit_permutations() {
        let world: TestWorld<IPAProtocolStep> = make_world(QueryId);
        let context = make_contexts(&world);
        let mut rand = StepRng::new(100, 1);

        // With this input, for stable sort we expect all 0's to line up before 1's. The expected sort order is same as expected_sort_output
        let input: Vec<u128> = vec![1, 0, 1, 0, 0, 1, 0];
        let expected_sort_output = [5_u128, 1, 6, 2, 3, 7, 4];

        let input_len = input.len();
        let mut shares = [
            Vec::with_capacity(input_len),
            Vec::with_capacity(input_len),
            Vec::with_capacity(input_len),
        ];
        for iter in input {
            let share = share(Fp31::from(iter), &mut rand);
            for i in 0..3 {
                shares[i].push(share[i]);
            }
        }

        let bitperms0 = BitPermutations::new(&shares[0]);
        let bitperms1 = BitPermutations::new(&shares[1]);
        let bitperms2 = BitPermutations::new(&shares[2]);
        let h0_future = bitperms0.execute(&context[0]);
        let h1_future = bitperms1.execute(&context[1]);
        let h2_future = bitperms2.execute(&context[2]);

        let result = try_join!(h0_future, h1_future, h2_future).unwrap();

        assert_eq!(result.0.len(), input_len);
        assert_eq!(result.1.len(), input_len);
        assert_eq!(result.2.len(), input_len);

        (0..result.0.len()).for_each(|i| {
            assert_eq!(
                validate_and_reconstruct((result.0[i], result.1[i], result.2[i])),
                Fp31::from(expected_sort_output[i])
            );
        });
    }
}
