use std::{
    num::{NonZeroU32, NonZeroUsize},
    time::Instant,
};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, AtomicUsize};

use clap::Parser;
use ipa::{
    error::Error,
    ff::Fp32BitPrime,
    helpers::{query::IpaQueryConfig, GatewayConfig},
    test_fixture::{
        ipa::{ipa_in_the_clear, test_ipa, IpaSecurityModel},
        EventGenerator, EventGeneratorConfig, TestWorld, TestWorldConfig,
    },
};
use rand::{random, Rng, rngs::StdRng, SeedableRng};
use tokio::runtime::Builder;
use ipa::ff::{Gf3Bit, Gf5Bit};
use ipa::protocol::prf_sharding::{attribution_and_capping, attribution_and_capping_par};
use ipa::protocol::prf_sharding::tests::{PreShardedAndSortedOPRFTestInput, test_input};
use ipa::test_fixture::Runner;

#[cfg(all(not(target_env = "msvc"), not(feature = "dhat-heap")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// A benchmark for the full IPA protocol.
#[derive(Parser)]
#[command(about, long_about = None)]
struct Args {
    #[arg(short = 'p', long, default_value_t = false)]
    parallel: bool,
    // The number of threads to use for running IPA.
    // #[arg(short = 'j', long, default_value = "12")]
    // threads: usize,
    // /// The total number of records to process.
    // #[arg(short = 'n', long, default_value = "1000")]
    // query_size: usize,
    // /// The maximum number of records for each person.
    // #[arg(short = 'u', long, default_value = "50")]
    // records_per_user: u32,
    // /// The contribution cap for each person.
    // #[arg(short = 'c', long, default_value = "3")]
    // per_user_cap: u32,
    // /// The number of breakdown keys.
    // #[arg(short = 'b', long, default_value = "16")]
    // breakdown_keys: u32,
    // /// The maximum trigger value.
    // #[arg(short = 't', long, default_value = "5")]
    // max_trigger_value: u32,
    // /// The size of the attribution window, in seconds.
    // #[arg(
    // short = 'w',
    // long,
    // default_value = "86400",
    // help = "The size of the attribution window, in seconds. Pass 0 for an infinite window."
    // )]
    // attribution_window: u32,
    // /// The number of sequential bits of breakdown key and match key to process in parallel
    // /// while doing modulus conversion and attribution
    // #[arg(long, default_value = "3")]
    // num_multi_bits: u32,
    // /// The random seed to use.
    // #[arg(short = 's', long)]
    // random_seed: Option<u64>,
    // /// The amount of active items to concurrently track.
    // #[arg(short = 'a', long)]
    // active_work: Option<NonZeroUsize>,
    // /// Desired security model for IPA protocol
    // #[arg(short = 'm', long, value_enum, default_value_t=IpaSecurityModel::Malicious)]
    // mode: IpaSecurityModel,
    // /// Needed for benches.
    // #[arg(long, hide = true)]
    // bench: bool,
}

impl Args {
    // fn active(&self) -> usize {
    //     self.active_work
    //         .map(NonZeroUsize::get)
    //         .unwrap_or_else(|| self.query_size.clamp(16, 1024))
    // }
    }

async fn run(args: Args) -> Result<(), Error> {
    const UNIQUE_USERS: u64 = 1000;
    const SEED: u64 = 652102342824;

    let _prep_time = Instant::now();
    let config = TestWorldConfig {
        gateway_config: GatewayConfig::new(UNIQUE_USERS.try_into().unwrap()),
        ..TestWorldConfig::default()
    };

    // let seed = args.random_seed.unwrap_or_else(|| random());
    // tracing::trace!(
    //     "Using random seed: {seed} for {q} records",
    //     q = args.query_size
    // );
    let rng = StdRng::seed_from_u64(SEED);
    let world = TestWorld::new_with(config.clone());
    const EVENTS_PER_USER: Range<u64> = 10..64;

    let mut records: Vec<PreShardedAndSortedOPRFTestInput<Gf5Bit, Gf3Bit>> = Vec::default();
    let mut rng = StdRng::seed_from_u64(SEED);
    for user in 1..=UNIQUE_USERS {
        let events_for_this_user = rng.gen_range(EVENTS_PER_USER);
        for event in 1..=events_for_this_user {
            let is_trigger = rng.gen();
            let breakdown_key = rng.gen();
            let trigger_value = if is_trigger { rng.gen() } else { 0 };
            records.push(test_input(user, is_trigger, breakdown_key, trigger_value ));
        }
    }
    let num_saturating_bits: usize = 5;
    println!("Preparation complete in {:?}", _prep_time.elapsed());
    let _protocol_time = Instant::now();
    let result = world
        .semi_honest(records.into_iter(), |ctx, input_rows| async move {
            if args.parallel {
                attribution_and_capping_par::<_, Gf5Bit, Gf3Bit>(
                    ctx,
                    input_rows,
                    num_saturating_bits,
                )
                    .await
                    .unwrap()
            } else {
                attribution_and_capping::<_, Gf5Bit, Gf3Bit>(
                    ctx,
                    input_rows,
                    num_saturating_bits,
                )
                    .await
                    .unwrap()
            }
        })
        .await;
    println!(
        "query took took {t:?}",
        t = _protocol_time.elapsed()
    );



    // test_ipa::<BenchField>(
    //     &world,
    //     &raw_data,
    //     &expected_results,
    //     args.config(),
    //     args.mode,
    // )
    //     .await;
    // tracing::trace!(
    //     "{m:?}  for {q} records took {t:?}",
    //     m = args.mode,
    //     q = args.query_size,
    //     t = _protocol_time.elapsed()
    // );
    Ok(())
}

fn main() -> Result<(), Error> {
    let args = Args::parse();
    let next_core = AtomicUsize::default();
    let rt = Builder::new_multi_thread()
        // .worker_threads(args.threads)
        .enable_all()
        .on_thread_start(move || {
            let mut core_ids = core_affinity::get_core_ids().unwrap();
            let random_core = next_core.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % core_ids.len();
            core_affinity::set_for_current(core_ids[random_core]);
        })
        .build()
        .unwrap();
    let _guard = rt.enter();
    let task = rt.spawn(run(args));
    rt.block_on(task)?
}
