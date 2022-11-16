use std::sync::Once;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

pub fn setup() {
    static INIT: Once = Once::new();

    INIT.call_once(|| {
        // let console_layer = console_subscriber::spawn();
        tracing_subscriber::registry()
            .with(EnvFilter::from_default_env())
            .with(fmt::layer().with_ansi(false))
            // .with(console_layer)
            .init();
    });
}
