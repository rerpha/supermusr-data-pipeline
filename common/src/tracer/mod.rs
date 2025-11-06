mod otel_tracer;
mod propagator;
mod tracer_engine;

pub use otel_tracer::OtelTracer;
pub use propagator::{FutureRecordTracerExt, OptionalHeaderTracerExt};
pub use tracer_engine::{TracerEngine, TracerOptions};

/// Should be called at the start of each component
/// The `conditional_` prefix used in the methods of FutureRecordTracerExt and OptionalHeaderTracerExt
/// indicate the method's first parameter is a bool, however here the first parameter is an Option<&str>
/// with the URL of the OpenTelemetry collector to be used, or None, if OpenTelemetry is not used.
#[macro_export]
macro_rules! init_tracer {
    ($options:expr) => {{
        let tracer = TracerEngine::new($options, env!("CARGO_BIN_NAME"));
        // This is called here (in the macro) rather than as part of `TracerEngine::new`
        // to ensure the warning is emitted in the correct module.
        if tracer.use_otel() {
            if let Some(e) = tracer.get_otel_setup_error() {
                warn!("{e}");
            } else if let Err(e) = tracer.set_otel_error_handler(|e| warn!("{e}")) {
                warn!("{e}");
            }
        }
        tracer
    }};
}

/// Should be called to populate the metadata fields of a given span
/// # Arguments
/// - metadata: digital_muon_streaming_types::frame_metadata::FrameMetadata
/// - span: Span
///
/// # Prerequisites
/// The span should have been created with appropriate empty fields, either by
/// ```ignore
/// fields(
///     //...
///     metadata_timestamp = tracing::field::Empty,
///     metadata_frame_number = tracing::field::Empty,
///     metadata_period_number = tracing::field::Empty,
///     metadata_veto_flags = tracing::field::Empty,
///     metadata_protons_per_pulse = tracing::field::Empty,
///     metadata_running = tracing::field::Empty,
///     //...
/// )
/// ```
/// if using the `#[instrument]` macro over a function, or with
/// ```ignore
///     "metadata_timestamp" = tracing::field::Empty,
///     "metadata_frame_number" = tracing::field::Empty,
///     "metadata_period_number" = tracing::field::Empty,
///     "metadata_veto_flags" = tracing::field::Empty,
///     "metadata_protons_per_pulse" = tracing::field::Empty,
///     "metadata_running" = tracing::field::Empty,
/// ```
/// if creating the span directly using `info_span!()` or similar.
#[macro_export]
macro_rules! record_metadata_fields_to_span {
    ($metadata:expr, $span:expr) => {
        $span.record("metadata_timestamp", $metadata.timestamp.to_rfc3339());
        $span.record("metadata_frame_number", $metadata.frame_number);
        $span.record("metadata_period_number", $metadata.period_number);
        $span.record("metadata_veto_flags", $metadata.veto_flags);
        $span.record("metadata_protons_per_pulse", $metadata.protons_per_pulse);
        $span.record("metadata_running", $metadata.running);
    };
}
