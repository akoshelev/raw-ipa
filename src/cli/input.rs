use std::io;
use std::io::{BufRead, ErrorKind};

use crate::ff::Field;
use crate::rand::Rng;

use crate::secret_sharing::IntoShares;

pub trait InputElement: Sized {
    /// Returns new element from the given string slice.
    /// ## Errors
    /// If slice is not a valid input element. For example, if string is not a valid integer,
    /// it cannot be used to create a field value or an integer.
    fn from_slice(buf: &str) -> Result<Self, io::Error>;
}

impl<F: Field> InputElement for F {
    fn from_slice(buf: &str) -> Result<Self, io::Error> {
        let int =
            str::parse::<u128>(buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(F::from(int))
    }
}

impl InputElement for u64 {
    fn from_slice(buf: &str) -> Result<Self, io::Error> {
        str::parse::<u64>(buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

pub struct TestInput<I> {
    data: Vec<I>,
}

impl<I: InputElement> TestInput<I> {
    /// Reads test data from the provided input. Expects \n to be the delimiter between records
    /// ## Errors
    /// if one or more lines in the input do not contain a valid `I`
    pub fn from_reader<R: io::Read>(input: &mut R) -> Result<Self, io::Error> {
        let mut lines = io::BufReader::new(input).lines();
        let mut data = Vec::new();
        for line in lines {
            let line = line?;
            data.push(I::from_slice(&line)?);
        }

        if data.is_empty() {
            Err(io::Error::new(
                ErrorKind::InvalidInput,
                "Provided input is empty.",
            ))
        } else {
            Ok(Self { data })
        }
    }

    /// Convert this input into a set of secret shares, one per helper.
    #[must_use]
    pub fn share<R, T>(self, r: &mut R) -> [Vec<T>; 3]
    where
        R: Rng,
        I: IntoShares<T>,
    {
        self.data.share_with(r)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ff::{Fp31, Fp32BitPrime};
    use crate::secret_sharing::{Replicated, XorReplicated};
    use crate::test_fixture::Reconstruct;
    use rand::distributions::{Distribution, Standard};
    use rand::thread_rng;

    fn mk_input<I>(data: &[I]) -> String
    where
        I: ToString,
    {
        data.iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn mk_field_input<F: Field>(data: &[F]) -> String {
        let data = data.iter().map(Field::as_u128).collect::<Vec<_>>();
        mk_input(&data)
    }

    fn in_out_replicated_test<F: Field>(input: &[F])
    where
        Standard: Distribution<F>,
    {
        let test_input =
            TestInput::<F>::from_reader(&mut mk_field_input(input).as_bytes()).unwrap();
        let output = test_input.share::<_, Replicated<_>>(&mut thread_rng());
        assert_eq!(input, output.reconstruct());
    }

    #[test]
    fn read_replicated_fp31() {
        let input = (0u128..2).map(Fp31::from).collect::<Vec<_>>();
        in_out_replicated_test(&input);
    }

    #[test]
    fn read_replicated_fp32() {
        let input = (0u128..2).map(Fp32BitPrime::from).collect::<Vec<_>>();
        in_out_replicated_test(&input);
    }

    #[test]
    fn read_xor() {
        let input = vec![0, 1, 2];
        let test_input = TestInput::<u64>::from_reader(&mut mk_input(&input).as_bytes()).unwrap();
        let output = test_input.share::<_, XorReplicated>(&mut thread_rng());

        assert_eq!(input, output.reconstruct());
    }

    #[test]
    #[should_panic(expected = "Provided input is empty.")]
    fn read_empty() {
        let input = "";
        TestInput::<u64>::from_reader(&mut input.as_bytes()).unwrap();
    }

    #[test]
    fn error_if_out_of_bounds() {
        // panic if u64 is read but u128 is in the input
        let input = vec![u128::from(u64::MAX) + 1, 1, 2];
        let test_input = TestInput::<u64>::from_reader(&mut mk_input(&input).as_bytes());
        assert!(matches!(
            test_input,
            Err(e) if e.kind() == ErrorKind::InvalidData
        ));
    }
}
