use crate::integrated::{
    simulation_elements::event_list::{EventList, Trace},
    simulation_engine::{
        actions::{SelectionModeOptions, SourceOptions},
        cache::{CacheError, SimulationEngineCache},
    },
};
use digital_muon_common::{Channel, DigitizerId, Intensity, Time, spanned::Spanned};
use digital_muon_streaming_types::{
    FrameMetadata,
    aev2_frame_assembled_event_v2_generated::{
        FrameAssembledEventListMessage, FrameAssembledEventListMessageArgs,
        finish_frame_assembled_event_list_message_buffer,
    },
    dat2_digitizer_analog_trace_v2_generated::{
        ChannelTrace, ChannelTraceArgs, DigitizerAnalogTraceMessage,
        DigitizerAnalogTraceMessageArgs, finish_digitizer_analog_trace_message_buffer,
    },
    dev2_digitizer_event_v2_generated::{
        DigitizerEventListMessage, DigitizerEventListMessageArgs,
        finish_digitizer_event_list_message_buffer,
    },
    flatbuffers::FlatBufferBuilder,
    frame_metadata_v2_generated::{FrameMetadataV2, FrameMetadataV2Args, GpsTime},
};
use std::collections::VecDeque;
use thiserror::Error;
use tracing::info_span;

#[derive(Debug, Error)]
pub(crate) enum BuildError {
    #[error("Cache Error: {0}")]
    Cache(#[from] CacheError),
}

fn create_v2_metadata_args<'a>(
    timestamp: &'a GpsTime,
    metadata: &FrameMetadata,
) -> FrameMetadataV2Args<'a> {
    FrameMetadataV2Args {
        frame_number: metadata.frame_number,
        period_number: metadata.period_number,
        protons_per_pulse: metadata.protons_per_pulse,
        running: metadata.running,
        timestamp: Some(timestamp),
        veto_flags: metadata.veto_flags,
    }
}

pub(crate) fn build_trace_message(
    fbb: &mut FlatBufferBuilder<'_>,
    sample_rate: u64,
    cache: &mut VecDeque<Trace>,
    metadata: &FrameMetadata,
    digitizer_id: DigitizerId,
    channels: &[Channel],
    selection_mode: SelectionModeOptions,
) -> Result<(), BuildError> {
    let channels = channels
        .iter()
        .map(|&channel| {
            info_span!("channel", channel = channel).in_scope(|| {
                let trace = cache.extract_one(selection_mode)?;

                tracing::Span::current()
                    .follows_from(trace.span().get().expect("Span should be initialised"));
                let voltage = Some(fbb.create_vector::<Intensity>(trace.get_intensities()));

                cache.finish_one(selection_mode)?;
                Ok(ChannelTrace::create(
                    fbb,
                    &ChannelTraceArgs { channel, voltage },
                ))
            })
        })
        .collect::<Result<Vec<_>, BuildError>>()?;

    let timestamp = metadata.timestamp.into();
    let metadata_args = create_v2_metadata_args(&timestamp, metadata);

    let message = DigitizerAnalogTraceMessageArgs {
        digitizer_id,
        metadata: Some(FrameMetadataV2::create(fbb, &metadata_args)),
        sample_rate,
        channels: Some(fbb.create_vector(&channels)),
    };
    let message = DigitizerAnalogTraceMessage::create(fbb, &message);
    finish_digitizer_analog_trace_message_buffer(fbb, message);
    Ok(())
}

pub(crate) fn build_digitiser_event_list_message(
    fbb: &mut FlatBufferBuilder<'_>,
    cache: &mut VecDeque<EventList<'_>>,
    metadata: &FrameMetadata,
    digitizer_id: DigitizerId,
    channels: &[Channel],
    source_options: &SourceOptions,
) -> Result<(), BuildError> {
    let mut time = Vec::<Time>::new();
    let mut voltage = Vec::<Intensity>::new();
    let mut channel = Vec::<Channel>::new();

    if let SourceOptions::SelectFromCache(selection_mode) = source_options {
        let event_lists = cache.extract(*selection_mode, channels.len())?;
        channels
            .iter()
            .zip(event_lists)
            .for_each(|(c, event_list)| {
                info_span!("channel", channel = c).in_scope(|| {
                    tracing::Span::current()
                        .follows_from(event_list.span().get().expect("Span exists"));
                    event_list.pulses.iter().for_each(|p| {
                        time.push(p.time());
                        voltage.push(p.intensity());
                        channel.push(*c);
                    });
                })
            });
        cache.finish(*selection_mode, channels.len())?;
    }

    let timestamp = metadata.timestamp.into();
    let metadata_args = create_v2_metadata_args(&timestamp, metadata);

    let message = DigitizerEventListMessageArgs {
        digitizer_id,
        metadata: Some(FrameMetadataV2::create(fbb, &metadata_args)),
        time: Some(fbb.create_vector(&time)),
        voltage: Some(fbb.create_vector(&voltage)),
        channel: Some(fbb.create_vector(&channel)),
    };
    let message = DigitizerEventListMessage::create(fbb, &message);
    finish_digitizer_event_list_message_buffer(fbb, message);
    Ok(())
}

pub(crate) fn build_aggregated_event_list_message(
    fbb: &mut FlatBufferBuilder<'_>,
    cache: &mut VecDeque<EventList<'_>>,
    metadata: &FrameMetadata,
    channels: &[Channel],
    source_options: &SourceOptions,
) -> Result<(), BuildError> {
    let mut time = Vec::<Time>::new();
    let mut voltage = Vec::<Intensity>::new();
    let mut channel = Vec::<Channel>::new();

    if let SourceOptions::SelectFromCache(selection_mode) = source_options {
        let event_lists = cache.extract(*selection_mode, channels.len())?;
        channels
            .iter()
            .zip(event_lists)
            .for_each(|(c, event_list)| {
                info_span!("channel", channel = c).in_scope(|| {
                    tracing::Span::current()
                        .follows_from(event_list.span().get().expect("Span exists"));
                    event_list.pulses.iter().for_each(|p| {
                        time.push(p.time());
                        voltage.push(p.intensity());
                        channel.push(*c);
                    });
                })
            });
        cache.finish(*selection_mode, channels.len())?;
    }

    let timestamp = metadata.timestamp.into();
    let metadata_args = create_v2_metadata_args(&timestamp, metadata);

    let message = FrameAssembledEventListMessageArgs {
        metadata: Some(FrameMetadataV2::create(fbb, &metadata_args)),
        time: Some(fbb.create_vector(&time)),
        voltage: Some(fbb.create_vector(&voltage)),
        channel: Some(fbb.create_vector(channels)),
        complete: true,
        digitizers_present: None,
    };
    let message = FrameAssembledEventListMessage::create(fbb, &message);
    finish_frame_assembled_event_list_message_buffer(fbb, message);
    Ok(())
}
