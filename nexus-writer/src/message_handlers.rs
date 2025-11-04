//! Functions which process kafka message payloads into the appropriate
//! flatbuffer objects and pushes them to a [NexusEngine] instance.
use crate::{
    EngineDependencies,
    run_engine::{NexusEngine, run_messages::SampleEnvironmentLog},
};
use digital_muon_common::{
    metrics::{
        failures::{self, FailureKind},
        messages_received::{self, MessageKind},
        names::{FAILURES, MESSAGES_RECEIVED},
    },
    record_metadata_fields_to_span,
};
use digital_muon_streaming_types::{
    FrameMetadata,
    aev2_frame_assembled_event_v2_generated::{
        frame_assembled_event_list_message_buffer_has_identifier,
        root_as_frame_assembled_event_list_message,
    },
    ecs_6s4t_run_stop_generated::{root_as_run_stop, run_stop_buffer_has_identifier},
    ecs_al00_alarm_generated::{alarm_buffer_has_identifier, root_as_alarm},
    ecs_f144_logdata_generated::{f_144_log_data_buffer_has_identifier, root_as_f_144_log_data},
    ecs_pl72_run_start_generated::{root_as_run_start, run_start_buffer_has_identifier},
    ecs_se00_data_generated::{
        root_as_se_00_sample_environment_data, se_00_sample_environment_data_buffer_has_identifier,
    },
    flatbuffers::InvalidFlatbuffer,
};
use metrics::counter;
use tracing::{instrument, warn, warn_span};

/// Processes the message payload for a message on the `frame_event_list` topic
/// # Parameters
/// - nexus_engine: the engine to push the message to.
/// - kafka_message_timestamp_ms: the timestamp in milliseconds as reported in the Kafka message header. Only used for tracing.
/// - payload: the byte-stream of the message.
pub(crate) fn process_payload_on_frame_event_list_topic(
    nexus_engine: &mut NexusEngine<EngineDependencies>,
    message_kafka_timestamp_ms: i64,
    payload: &[u8],
) {
    if frame_assembled_event_list_message_buffer_has_identifier(payload) {
        push_frame_event_list(nexus_engine, message_kafka_timestamp_ms, payload);
    } else {
        warn!("Incorrect message identifier on frame event list topic");
    }
}

/// Processes the message payload for a message on the `sample_environment` topic
/// # Parameters
/// - nexus_engine: the engine to push the message to.
/// - kafka_message_timestamp_ms: the timestamp in milliseconds as reported in the Kafka message header. Only used for tracing.
/// - payload: the byte-stream of the message.
pub(crate) fn process_payload_on_sample_env_topic(
    nexus_engine: &mut NexusEngine<EngineDependencies>,
    message_kafka_timestamp_ms: i64,
    payload: &[u8],
) {
    if f_144_log_data_buffer_has_identifier(payload) {
        push_f144_sample_environment_log(nexus_engine, message_kafka_timestamp_ms, payload);
    } else if se_00_sample_environment_data_buffer_has_identifier(payload) {
        push_se00_sample_environment_log(nexus_engine, message_kafka_timestamp_ms, payload);
    } else {
        warn!("Incorrect message identifier on sample environment topic");
    }
}

/// Processes the message payload for a message on the `run_log` topic
/// # Parameters
/// - nexus_engine: the engine to push the message to.
/// - kafka_message_timestamp_ms: the timestamp in milliseconds as reported in the Kafka message header. Only used for tracing.
/// - payload: the byte-stream of the message.
pub(crate) fn process_payload_on_runlog_topic(
    nexus_engine: &mut NexusEngine<EngineDependencies>,
    message_kafka_timestamp_ms: i64,
    payload: &[u8],
) {
    if f_144_log_data_buffer_has_identifier(payload) {
        push_run_log(nexus_engine, message_kafka_timestamp_ms, payload);
    } else {
        warn!("Incorrect message identifier on runlog topic");
    }
}

/// Processes the message payload for a message on the `alarm` topic
/// # Parameters
/// - nexus_engine: the engine to push the message to.
/// - kafka_message_timestamp_ms: the timestamp in milliseconds as reported in the Kafka message header. Only used for tracing.
/// - payload: the byte-stream of the message.
pub(crate) fn process_payload_on_alarm_topic(
    nexus_engine: &mut NexusEngine<EngineDependencies>,
    message_kafka_timestamp_ms: i64,
    payload: &[u8],
) {
    if alarm_buffer_has_identifier(payload) {
        push_alarm(nexus_engine, message_kafka_timestamp_ms, payload);
    } else {
        warn!("Incorrect message identifier on alarm topic");
    }
}

/// Processes the message payload for a message on the `control` topic
/// # Parameters
/// - nexus_engine: the engine to push the message to.
/// - kafka_message_timestamp_ms: the timestamp in milliseconds as reported in the Kafka message header. Only used for tracing.
/// - payload: the byte-stream of the message.
pub(crate) fn process_payload_on_control_topic(
    nexus_engine: &mut NexusEngine<EngineDependencies>,
    message_kafka_timestamp_ms: i64,
    payload: &[u8],
) {
    if run_start_buffer_has_identifier(payload) {
        push_run_start(nexus_engine, message_kafka_timestamp_ms, payload);
    } else if run_stop_buffer_has_identifier(payload) {
        push_run_stop(nexus_engine, message_kafka_timestamp_ms, payload);
    } else {
        warn!("Incorrect message identifier on control topic");
    }
}

/// This wrapper function is used to allow tracing of the flatbuffer decoding function.
/// This operation is generally so fast, that I'm not sure this is needed.
/// Alternatively, it may be possible to trace this using functionality in the flatbuffer crate.
#[instrument(skip_all, level = "trace", err(level = "WARN"))]
fn spanned_root_as<'a, R>(
    f: impl Fn(&'a [u8]) -> Result<R, InvalidFlatbuffer>,
    payload: &'a [u8],
) -> Result<R, InvalidFlatbuffer> {
    f(payload)
}

/// Emit the warning on an invalid flatbuffer error and increase metric
fn report_parse_message_failure(e: InvalidFlatbuffer) {
    warn!("Failed to parse message: {}", e);
    counter!(
        FAILURES,
        &[failures::get_label(FailureKind::UnableToDecodeMessage)]
    )
    .increment(1);
}

fn increment_message_received_counter(kind: MessageKind) {
    counter!(MESSAGES_RECEIVED, &[messages_received::get_label(kind)]).increment(1);
}

/// Decode, validate and process a flatbuffer `RunStart` message
/// # Parameters
/// - nexus_engine: the engine to push the message to.
/// - kafka_message_timestamp_ms: the timestamp in milliseconds as reported in the Kafka message header. Only used for tracing.
/// - payload: the byte-stream of the message.
#[tracing::instrument(skip_all, fields(kafka_message_timestamp_ms=kafka_message_timestamp_ms))]
fn push_run_start(
    nexus_engine: &mut NexusEngine<EngineDependencies>,
    kafka_message_timestamp_ms: i64,
    payload: &[u8],
) {
    increment_message_received_counter(MessageKind::RunStart);

    match spanned_root_as(root_as_run_start, payload) {
        Ok(data) => {
            if let Err(e) = nexus_engine.push_run_start(data) {
                warn!("Start command ({data:?}) failed {e}");
            }
        }
        Err(e) => report_parse_message_failure(e),
    }
}

/// Decode, validate and process a flatbuffer `FrameEventList` message
/// # Parameters
/// - nexus_engine: the engine to push the message to.
/// - kafka_message_timestamp_ms: the timestamp in milliseconds as reported in the Kafka message header. Only used for tracing.
/// - payload: the byte-stream of the message.
#[tracing::instrument(
    skip_all,
    fields(
        kafka_message_timestamp_ms=kafka_message_timestamp_ms,
        metadata_timestamp,
        metadata_frame_number,
        metadata_period_number,
        metadata_veto_flags,
        metadata_protons_per_pulse,
        metadata_running,
        frame_is_complete,
        has_run,
    )
)]
fn push_frame_event_list(
    nexus_engine: &mut NexusEngine<EngineDependencies>,
    kafka_message_timestamp_ms: i64,
    payload: &[u8],
) {
    increment_message_received_counter(MessageKind::Event);
    match spanned_root_as(root_as_frame_assembled_event_list_message, payload) {
        Ok(data) => {
            data.metadata()
                .try_into()
                .map(|metadata: FrameMetadata| {
                    record_metadata_fields_to_span!(metadata, tracing::Span::current());
                    tracing::Span::current().record("frame_is_complete", data.complete());
                })
                .ok();
            if let Err(e) = nexus_engine.push_frame_event_list(data) {
                warn!("Failed to save frame assembled event list to file: {}", e);
            }
        }
        Err(e) => report_parse_message_failure(e),
    }
}

/// Decode, validate and process a flatbuffer `RunLog` message
/// # Parameters
/// - nexus_engine: the engine to push the message to.
/// - kafka_message_timestamp_ms: the timestamp in milliseconds as reported in the Kafka message header. Only used for tracing.
/// - payload: the byte-stream of the message.
#[tracing::instrument(skip_all, fields(kafka_message_timestamp_ms=kafka_message_timestamp_ms, has_run))]
pub(crate) fn push_run_log(
    nexus_engine: &mut NexusEngine<EngineDependencies>,
    kafka_message_timestamp_ms: i64,
    payload: &[u8],
) {
    increment_message_received_counter(MessageKind::LogData);

    match spanned_root_as(root_as_f_144_log_data, payload) {
        Ok(data) => {
            if let Err(e) = nexus_engine.push_run_log(&data) {
                warn!("Run Log Data ({data:?}) failed. Error: {e}");
            }
        }
        Err(e) => report_parse_message_failure(e),
    }
}

/// Decode, validate and process flatbuffer `SampleEnvironmentLog` messages
/// # Parameters
/// - nexus_engine: the engine to push the message to.
/// - kafka_message_timestamp_ms: the timestamp in milliseconds as reported in the Kafka message header. Only used for tracing.
/// - payload: the byte-stream of the message.
#[tracing::instrument(skip_all, fields(kafka_message_timestamp_ms=kafka_message_timestamp_ms, has_run))]
fn push_f144_sample_environment_log(
    nexus_engine: &mut NexusEngine<EngineDependencies>,
    kafka_message_timestamp_ms: i64,
    payload: &[u8],
) {
    increment_message_received_counter(MessageKind::SampleEnvironmentData);
    let wrapped_result =
        spanned_root_as(root_as_f_144_log_data, payload).map(SampleEnvironmentLog::LogData);
    match wrapped_result {
        Ok(wrapped_se) => {
            if let Err(e) = nexus_engine.push_sample_environment_log(wrapped_se) {
                warn!("Sample environment error: {e}.");
            }
        }
        Err(e) => report_parse_message_failure(e),
    }
}

/// Decode, validate and process flatbuffer `SampleEnvironmentLog` messages
/// # Parameters
/// - nexus_engine: the engine to push the message to.
/// - kafka_message_timestamp_ms: the timestamp in milliseconds as reported in the Kafka message header. Only used for tracing.
/// - payload: the byte-stream of the message.
#[tracing::instrument(skip_all, fields(kafka_message_timestamp_ms=kafka_message_timestamp_ms, has_run))]
fn push_se00_sample_environment_log(
    nexus_engine: &mut NexusEngine<EngineDependencies>,
    kafka_message_timestamp_ms: i64,
    payload: &[u8],
) {
    increment_message_received_counter(MessageKind::SampleEnvironmentData);
    let wrapped_result = spanned_root_as(root_as_se_00_sample_environment_data, payload)
        .map(SampleEnvironmentLog::SampleEnvironmentData);
    match wrapped_result {
        Ok(wrapped_se) => {
            if let Err(e) = nexus_engine.push_sample_environment_log(wrapped_se) {
                warn!("Sample environment error: {e}.");
            }
        }
        Err(e) => report_parse_message_failure(e),
    }
}

/// Decode, validate and process a flatbuffer `Alarm` message
/// # Parameters
/// - nexus_engine: the engine to push the message to.
/// - kafka_message_timestamp_ms: the timestamp in milliseconds as reported in the Kafka message header. Only used for tracing.
/// - payload: the byte-stream of the message.
#[tracing::instrument(skip_all, fields(kafka_message_timestamp_ms=kafka_message_timestamp_ms, has_run))]
fn push_alarm(
    nexus_engine: &mut NexusEngine<EngineDependencies>,
    kafka_message_timestamp_ms: i64,
    payload: &[u8],
) {
    increment_message_received_counter(MessageKind::Alarm);
    match spanned_root_as(root_as_alarm, payload) {
        Ok(data) => {
            if let Err(e) = nexus_engine.push_alarm(data) {
                warn!("Alarm ({data:?}) failed {e}");
            }
        }
        Err(e) => report_parse_message_failure(e),
    }
}

/// Decode, validate and process a flatbuffer `RunStop` message
/// # Parameters
/// - nexus_engine: the engine to push the message to.
/// - kafka_message_timestamp_ms: the timestamp in milliseconds as reported in the Kafka message header. Only used for tracing.
/// - payload: the byte-stream of the message.
#[tracing::instrument(skip_all, fields(kafka_message_timestamp_ms=kafka_message_timestamp_ms, has_run))]
fn push_run_stop(
    nexus_engine: &mut NexusEngine<EngineDependencies>,
    kafka_message_timestamp_ms: i64,
    payload: &[u8],
) {
    increment_message_received_counter(MessageKind::RunStop);
    match spanned_root_as(root_as_run_stop, payload) {
        Ok(data) => {
            if let Err(e) = nexus_engine.push_run_stop(data) {
                let _guard = warn_span!(
                    "RunStop Error",
                    run_name = data.run_name(),
                    stop_time = data.stop_time(),
                )
                .entered();
                warn!("{e}");
            }
        }
        Err(e) => report_parse_message_failure(e),
    }
}
