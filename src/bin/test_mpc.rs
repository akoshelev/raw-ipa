use axum::extract::Path;
use clap::{Parser, Subcommand, ValueEnum};
use comfy_table::Table;
use rand::rngs::StdRng;
use rand_core::SeedableRng;
use raw_ipa::cli::test_scenarios::{secure_multiply, CommandInput};
use raw_ipa::cli::{InputElement, TestInput};
use raw_ipa::ff::{Field, Fp31, Fp32BitPrime};
use raw_ipa::secret_sharing::{IntoShares, Replicated, SecretSharing};
use raw_ipa::{cli::Verbosity, helpers::Role, net::MpcHelperClient};
use std::error::Error;
use std::fmt::Debug;
use std::fs::File;
use std::io;
use std::io::stdin;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[clap(
    name = "mpc-client",
    about = "CLI to execute test scenarios on IPA MPC helpers"
)]
#[command(about)]
struct Args {
    #[clap(flatten)]
    logging: Verbosity,

    #[clap(flatten)]
    input: CommandInput,

    #[command(subcommand)]
    action: TestAction,
}

#[derive(Debug, Subcommand)]
enum TestAction {
    /// Execute end-to-end multiplication.
    Multiply,
}

fn print_output<O: Debug>(input: &[Vec<O>; 3]) {
    let mut shares_table = Table::new();
    shares_table.set_header(vec!["Row", "H1", "H2", "H3"]);
    for i in 0..input[0].len() {
        shares_table.add_row(vec![
            i.to_string(),
            format!("{:?}", input[0][i]),
            format!("{:?}", input[1][i]),
            format!("{:?}", input[2][i]),
        ]);
    }

    println!("{}", shares_table);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = Args::parse();
    let _handle = args.logging.setup_logging();

    match args.action {
        TestAction::Multiply => {
            let output = secure_multiply(args.input);
            print_output(&output);
        }
    }
    Ok(())
}
