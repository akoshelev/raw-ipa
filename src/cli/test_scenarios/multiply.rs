use crate::cli::test_scenarios::{CommandInput, InputType};
use std::fmt::{Debug, Display};
use crate::secret_sharing::SecretSharing;

fn boxed_shares<S: Debug + 'static>(input: [Vec<S>; 3]) -> [Vec<Box<dyn Debug>>; 3] {
    input.map(|v| v.into_iter().map(|s| Box::new(s) as Box<dyn Debug>).collect())
}

/// Performs secure multiplication on all the input shares.
///
/// ## Panics
/// Panic if input values are not valid field values. Will panic if result is do
#[must_use]
pub fn secure_multiply(input: CommandInput) -> [Vec<Box<dyn Debug>>; 3] {
    let mut rng = input.rng();
    match input.input_type {
        InputType::Fp31 => {
            let output = input.read_fp31().share(&mut rng);
            // TODO: well, we need to talk to helpers, now just return the share
            boxed_shares(output)
        }
        InputType::Fp32BitPrime => {
            let output = input.read_fp32().share(&mut rng);
            // TODO: well, we need to talk to helpers
            boxed_shares(output)
        }
        InputType::Int64 => {
            panic!("{:?} input type is not supported for multiply.", input.input_type)
        }
    }
}
