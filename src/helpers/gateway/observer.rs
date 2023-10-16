use std::time::Duration;

use tracing::{Instrument, Span};

use crate::{
    helpers::gateway::observable::{ObserveState, Observed},
    task::JoinHandle,
};

#[cfg(not(feature = "shuttle"))]
pub fn spawn<T: ObserveState + Send + Sync + 'static>(
    within: Span,
    check_interval: Duration,
    observed: Observed<T>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_observed = 0;
        loop {
            ::tokio::time::sleep(check_interval).await;
            let now = observed.get_sn();
            if now == last_observed {
                if let Some(state) = observed.get_state() {
                    tracing::warn!(sn = now, state = ?state, "Helper is stalled after {check_interval:?}");
                } else {
                    break
                }
            } else {
                last_observed = now;
            }
        }
    }.instrument(within))
}
