#[cfg(feature = "descriptive-gate")]
pub mod prf_eval;
pub mod prf_sharding;
#[cfg(feature = "descriptive-gate")]
pub mod shuffle;



#[cfg(test)]
#[cfg(all(test, any(unit_test, feature = "shuttle")))]
mod tests {
    use crate::ff::Fp32BitPrime;
    use crate::helpers::GatewayConfig;
    use crate::helpers::query::IpaQueryConfig;
    use crate::test_executor::run;
    use crate::test_fixture::{EventGenerator, EventGeneratorConfig, TestWorld, TestWorldConfig};
    use crate::test_fixture::ipa::{CappingOrder, ipa_in_the_clear, test_oprf_ipa};

    #[test]
    fn basic_test() {
        const MAX_BREAKDOWN_KEY: u32 = 255;
        const MAX_TRIGGER_VALUE: u32 = 5;
        const NUM_USERS: u32 = 8;
        const MIN_RECORDS_PER_USER: u32 = 1;
        const MAX_RECORDS_PER_USER: u32 = 8;
        const NUM_MULTI_BITS: u32 = 3;
        const PER_USER_CAP: u32 = 16;

        let raw_data = EventGenerator::with_config(
            rand::thread_rng(),
            EventGeneratorConfig::new(
                u64::from(NUM_USERS),
                MAX_TRIGGER_VALUE,
                MAX_BREAKDOWN_KEY,
                MIN_RECORDS_PER_USER,
                MAX_RECORDS_PER_USER,
            ),
        )
            .take(usize::try_from(NUM_USERS * MAX_RECORDS_PER_USER).unwrap())
            .collect::<Vec<_>>();

        let expected_results = ipa_in_the_clear(
            &raw_data,
            PER_USER_CAP,
            None,
            MAX_BREAKDOWN_KEY,
            &CappingOrder::CapOldestFirst,
        );

        run(move || {
            let raw_data = raw_data.clone();
            let expected_results = expected_results.clone();
            async move {
                let config = TestWorldConfig {
                    gateway_config: GatewayConfig::new(raw_data.len().clamp(4, 1024)),
                    ..Default::default()
                };
                let world = TestWorld::new_with(config);
                test_oprf_ipa::<Fp32BitPrime>(&world, raw_data, &expected_results, IpaQueryConfig {
                    per_user_credit_cap: PER_USER_CAP,
                    max_breakdown_key: MAX_BREAKDOWN_KEY,
                    attribution_window_seconds: None,
                    num_multi_bits: NUM_MULTI_BITS,
                    plaintext_match_keys: true,
                }).await;
            }
        })
    }
}