mod multiply;

use crate::cli::TestInput;
use crate::ff::{Fp31, Fp32BitPrime};
use clap::{Parser, ValueEnum};
use rand::rngs::StdRng;
use rand_core::SeedableRng;
use std::fs::File;
use std::io::{stdin, Read};
use std::path::PathBuf;

pub use multiply::secure_multiply;

#[derive(Debug, Parser)]
pub struct CommandInput {
    #[arg(
        long,
        help = "Read the input from the provided file, instead of standard input"
    )]
    input_file: Option<PathBuf>,
    #[arg(value_enum, long, default_value_t = InputType::Fp32BitPrime, help = "Convert the input into the given field before sending to helpers")]
    input_type: InputType,
    #[arg(long, help = "Optional seed for rng")]
    seed: Option<u64>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum InputType {
    Fp31,
    Fp32BitPrime,
    Int64,
}

impl CommandInput {
    pub fn read_fp31(self) -> TestInput<Fp31> {
        TestInput::<Fp31>::from_reader(&mut self.input_stream()).unwrap()
    }

    pub fn read_fp32(self) -> TestInput<Fp32BitPrime> {
        TestInput::<Fp32BitPrime>::from_reader(&mut self.input_stream()).unwrap()
    }

    pub fn rng(&self) -> StdRng {
        self.seed
            .map(|seed| StdRng::seed_from_u64(seed))
            .unwrap_or(StdRng::from_entropy())
    }

    fn input_stream(&self) -> Box<dyn Read> {
        if let Some(ref file_name) = self.input_file {
            Box::new(File::open(file_name).unwrap())
        } else {
            Box::new(stdin())
        }
    }
}
