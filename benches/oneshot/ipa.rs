use clap::Parser;
use rand::{rngs::StdRng, thread_rng, Rng, SeedableRng};
use raw_ipa::{
    error::Error,
    ff::Fp32BitPrime,
    helpers::GatewayConfig,
    test_fixture::{
        generate_random_user_records_in_reverse_chronological_order, test_ipa,
        update_expected_output_for_user, IpaSecurityModel, TestWorld, TestWorldConfig,
    },
};
use std::time::Instant;

#[derive(Debug, Parser)]
pub struct BenchArgs {
    #[arg(
        long,
        short = 'i',
        help = "Input size, in records",
        default_value = "100"
    )]
    pub query_size: usize,

    #[arg(value_enum, long, short = 'm', help = "Run malicious or semi-honest IPA", default_value_t = IpaSecurityModel::Malicious)]
    pub mode: IpaSecurityModel,

    #[arg(long, help = "Capped user contributions", default_value = "1")]
    pub user_cap: u32,

    #[arg(short, long, help = "ignored")]
    pub bench: bool,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 3)]
async fn main() -> Result<(), Error> {
    const MAX_BREAKDOWN_KEY: u32 = 16;
    const MAX_TRIGGER_VALUE: u32 = 5;
    const MAX_RECORDS_PER_USER: usize = 10;
    const ATTRIBUTION_WINDOW_SECONDS: u32 = 0;
    type BenchField = Fp32BitPrime;

    let args = BenchArgs::parse();
    let query_size = args.query_size;
    let mode = args.mode;

    let mut config = TestWorldConfig::default();
    config.gateway_config =
        GatewayConfig::symmetric_buffers::<BenchField>(query_size.clamp(16, 1024));

    let random_seed = thread_rng().gen();
    println!("Using random seed: {random_seed}");
    let mut rng = StdRng::seed_from_u64(random_seed);

    let mut total_count = 0;

    let mut random_user_records = Vec::with_capacity(query_size / MAX_RECORDS_PER_USER);
    while total_count < query_size {
        let mut records_for_user = generate_random_user_records_in_reverse_chronological_order(
            &mut rng,
            MAX_RECORDS_PER_USER,
            MAX_BREAKDOWN_KEY,
            MAX_TRIGGER_VALUE,
        );
        records_for_user.truncate(query_size - total_count);

        total_count += records_for_user.len();
        random_user_records.push(records_for_user);
    }
    let mut raw_data = random_user_records.concat();
    let start = Instant::now();

    // Sort the records in chronological order
    // This is part of the IPA spec. Callers should do this before sending a batch of records in for processing.
    raw_data.sort_unstable_by(|a, b| a.timestamp.cmp(&b.timestamp));

    let per_user_cap = args.user_cap;
    let mut expected_results = vec![0_u32; MAX_BREAKDOWN_KEY.try_into().unwrap()];

    for records_for_user in &random_user_records {
        update_expected_output_for_user(
            records_for_user,
            &mut expected_results,
            per_user_cap,
            ATTRIBUTION_WINDOW_SECONDS,
        );
    }
    let world = TestWorld::new_with(config.clone());

    test_ipa::<BenchField>(
        &world,
        &raw_data,
        &expected_results,
        per_user_cap,
        MAX_BREAKDOWN_KEY,
        ATTRIBUTION_WINDOW_SECONDS,
        mode,
    )
    .await;

    let duration = start.elapsed().as_secs_f32();
    println!(
        "{mode:?} IPA benchmark for {query_size} records complete successfully after {duration}s"
    );
    Ok(())
}
