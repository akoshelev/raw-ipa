pub mod metrics {
    use axum::http::Version;
    use metrics::Unit;
    use metrics::{describe_counter, KeyName};

    pub const REQUESTS_RECEIVED: &str = "requests-received";
    pub const RECORDS_SENT: &str = "records-sent";
    pub const INDEXED_PRSS_GENERATED: &str = "iprss-generated";
    pub const SEQUENTIAL_PRSS_GENERATED: &str = "sprss-generated";

    /// Metric that records the version of HTTP protocol used for a particular request.
    #[cfg(feature = "web-app")]
    pub struct RequestProtocolVersion(Version);

    #[cfg(feature = "web-app")]
    impl From<Version> for RequestProtocolVersion {
        fn from(v: Version) -> Self {
            RequestProtocolVersion(v)
        }
    }

    #[cfg(feature = "web-app")]
    impl From<RequestProtocolVersion> for KeyName {
        fn from(v: RequestProtocolVersion) -> Self {
            const HTTP11: &str = "request.protocol.HTTP/1.1";
            const HTTP2: &str = "request.protocol.HTTP/2";
            const HTTP3: &str = "request.protocol.HTTP/3";
            const UNKNOWN: &str = "request.protocol.HTTP/UNKNOWN";

            KeyName::from_const_str(match v.0 {
                Version::HTTP_11 => HTTP11,
                Version::HTTP_2 => HTTP2,
                Version::HTTP_3 => HTTP3,
                _ => UNKNOWN,
            })
        }
    }

    /// Registers metrics used in the system with the metrics recorder.
    ///
    /// ## Panics
    /// Panic if there is no recorder installed
    pub fn register() {
        assert!(
            matches!(metrics::try_recorder(), Some(_)),
            "metrics recorder must be installed before metrics can be described"
        );

        describe_counter!(
            REQUESTS_RECEIVED,
            Unit::Count,
            "Total number of requests received by the web server"
        );

        describe_counter!(
            RequestProtocolVersion::from(Version::HTTP_11),
            Unit::Count,
            "Total number of HTTP/1.1 requests received"
        );

        describe_counter!(
            RequestProtocolVersion::from(Version::HTTP_2),
            Unit::Count,
            "Total number of HTTP/2 requests received"
        );
    }

    #[cfg(test)]
    #[must_use]
    pub fn get_counter_value<K: Into<KeyName>>(
        snapshot: metrics_util::debugging::Snapshot,
        metric_name: K,
    ) -> Option<u64> {
        use metrics_util::debugging::DebugValue;
        use metrics_util::MetricKind;

        let snapshot = snapshot.into_vec();
        let metric_name = metric_name.into();

        for (key, _unit, _, val) in snapshot {
            if key.kind() == MetricKind::Counter && key.key().name() == metric_name.as_str() {
                match val {
                    DebugValue::Counter(v) => return Some(v),
                    _ => unreachable!(),
                }
            }
        }

        None
    }
}
