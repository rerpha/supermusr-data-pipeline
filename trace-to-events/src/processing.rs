use crate::{channels::find_channel_events, parameters::DetectorSettings, pulse_detection::Real};
use digital_muon_common::{
    Channel, EventData,
    spanned::{SpanWrapper, Spanned},
};
use digital_muon_streaming_types::{
    dat2_digitizer_analog_trace_v2_generated::DigitizerAnalogTraceMessage,
    dev2_digitizer_event_v2_generated::{
        DigitizerEventListMessage, DigitizerEventListMessageArgs,
        finish_digitizer_event_list_message_buffer,
    },
    flatbuffers::FlatBufferBuilder,
    frame_metadata_v2_generated::{FrameMetadataV2, FrameMetadataV2Args},
};
use metrics::counter;
use rayon::prelude::*;
use tracing::debug;

#[tracing::instrument(skip_all, fields(num_total_pulses = tracing::field::Empty))]
pub(crate) fn process<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    trace: &'a DigitizerAnalogTraceMessage,
    detector_settings: &DetectorSettings,
) {
    debug!(
        "Dig ID: {}, Metadata: {:?}",
        trace.digitizer_id(),
        trace.metadata()
    );

    let sample_time_in_ns: Real = 1_000_000_000.0 / trace.sample_rate() as Real;

    let vec: Vec<(Channel, _)> = trace
        .channels()
        .unwrap()
        .iter()
        .map(SpanWrapper::<_>::new_with_current)
        .collect::<Vec<_>>()
        .par_iter()
        .map(|spanned_channel_trace| {
            let channel_span = spanned_channel_trace
                .span()
                .get()
                .expect("Channel has span");

            channel_span.in_scope(|| {
                let channel = spanned_channel_trace.channel();
                let events = find_channel_events(
                    spanned_channel_trace,
                    sample_time_in_ns,
                    detector_settings,
                );
                (channel, events)
            })
        })
        .collect();

    let mut events = EventData::default();
    for (channel, (time, voltage)) in vec {
        let num_events = voltage.len();
        counter!(
            crate::EVENTS_FOUND_METRIC,
            &[
                ("digitizer_id", format!("{}", trace.digitizer_id())),
                ("channel", format!("{channel}"))
            ]
        )
        .increment(num_events as u64);

        events.channel.extend_from_slice(&vec![channel; time.len()]);
        events.time.extend_from_slice(&time);
        events.voltage.extend_from_slice(&voltage);
    }

    let metadata = FrameMetadataV2Args {
        frame_number: trace.metadata().frame_number(),
        period_number: trace.metadata().period_number(),
        running: trace.metadata().running(),
        protons_per_pulse: trace.metadata().protons_per_pulse(),
        timestamp: trace.metadata().timestamp(),
        veto_flags: trace.metadata().veto_flags(),
    };
    let metadata = FrameMetadataV2::create(fbb, &metadata);

    let time = Some(fbb.create_vector(&events.time));
    let voltage = Some(fbb.create_vector(&events.voltage));
    let channel = Some(fbb.create_vector(&events.channel));

    let message = DigitizerEventListMessageArgs {
        digitizer_id: trace.digitizer_id(),
        metadata: Some(metadata),
        time,
        voltage,
        channel,
    };
    let message = DigitizerEventListMessage::create(fbb, &message);
    finish_digitizer_event_list_message_buffer(fbb, message);

    tracing::Span::current().record("num_total_pulses", events.channel.len());
}

#[cfg(test)]
mod tests {
    use crate::{
        Mode, Polarity,
        parameters::{AdvancedMuonDetectorParameters, FixedThresholdDiscriminatorParameters},
    };

    use super::*;
    use chrono::Utc;
    use digital_muon_common::Intensity;
    use digital_muon_streaming_types::{
        dat2_digitizer_analog_trace_v2_generated::{
            ChannelTrace, ChannelTraceArgs, DigitizerAnalogTraceMessage,
            DigitizerAnalogTraceMessageArgs, finish_digitizer_analog_trace_message_buffer,
            root_as_digitizer_analog_trace_message,
        },
        dev2_digitizer_event_v2_generated::{
            digitizer_event_list_message_buffer_has_identifier,
            root_as_digitizer_event_list_message,
        },
        frame_metadata_v2_generated::{FrameMetadataV2, FrameMetadataV2Args, GpsTime},
    };

    fn create_message(
        fbb: &mut FlatBufferBuilder<'_>,
        channel_intensities: &[&[Intensity]],
        time: &GpsTime,
    ) {
        let metadata = FrameMetadataV2Args {
            frame_number: 0,
            period_number: 0,
            protons_per_pulse: 0,
            running: true,
            timestamp: Some(time),
            veto_flags: 0,
        };
        let metadata = FrameMetadataV2::create(fbb, &metadata);

        let channel_vectors: Vec<_> = channel_intensities
            .iter()
            .map(|intensities| Some(fbb.create_vector::<u16>(intensities)))
            .collect();
        let channel_traces: Vec<_> = channel_vectors
            .iter()
            .enumerate()
            .map(|(i, intensities)| {
                ChannelTrace::create(
                    fbb,
                    &ChannelTraceArgs {
                        channel: i as Channel,
                        voltage: *intensities,
                    },
                )
            })
            .collect();

        let message = DigitizerAnalogTraceMessageArgs {
            digitizer_id: 0,
            metadata: Some(metadata),
            sample_rate: 1_000_000_000,
            channels: Some(fbb.create_vector(&channel_traces)),
        };
        let message = DigitizerAnalogTraceMessage::create(fbb, &message);
        finish_digitizer_analog_trace_message_buffer(fbb, message);
    }

    #[test]
    fn fixed_threshold_discriminator_positive_zero_baseline() {
        let mut fbb = FlatBufferBuilder::new();

        let time: GpsTime = Utc::now().into();
        let channels: Vec<&[Intensity]> =
            vec![[0, 1, 2, 1, 0, 1, 2, 1, 8, 0, 2, 8, 3, 1, 2].as_slice()];
        create_message(&mut fbb, &channels, &time);
        let message = fbb.finished_data().to_vec();
        let message = root_as_digitizer_analog_trace_message(&message).unwrap();

        let test_parameters = FixedThresholdDiscriminatorParameters {
            threshold: 5.0,
            duration: 1,
            cool_off: 0,
        };
        let mut fbb = FlatBufferBuilder::new();
        process(
            &mut fbb,
            &message,
            &DetectorSettings {
                mode: &Mode::FixedThresholdDiscriminator(test_parameters),
                polarity: &Polarity::Positive,
                baseline: Intensity::default(),
            },
        );

        assert!(digitizer_event_list_message_buffer_has_identifier(
            fbb.finished_data()
        ));
        let event_message = root_as_digitizer_event_list_message(fbb.finished_data()).unwrap();

        assert_eq!(
            vec![0, 0],
            event_message.channel().unwrap().iter().collect::<Vec<_>>()
        );

        assert_eq!(
            vec![8, 11],
            event_message.time().unwrap().iter().collect::<Vec<_>>()
        );

        assert_eq!(
            vec![8, 8],
            event_message.voltage().unwrap().iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn fixed_threshold_discriminator_positive_zero_baseline_two_channel() {
        let mut fbb = FlatBufferBuilder::new();

        let time: GpsTime = Utc::now().into();
        let channels: Vec<&[Intensity]> = vec![
            [0, 1, 2, 1, 0, 1, 2, 1, 9, 0, 2, 8, 3, 1, 2].as_slice(),
            [0, 1, 2, 1, 0, 1, 2, 1, 8, 0, 2, 9, 3, 1, 2].as_slice(),
        ];
        create_message(&mut fbb, &channels, &time);
        let message = fbb.finished_data().to_vec();
        let message = root_as_digitizer_analog_trace_message(&message).unwrap();

        let test_parameters = FixedThresholdDiscriminatorParameters {
            threshold: 5.0,
            duration: 1,
            cool_off: 0,
        };
        let mut fbb = FlatBufferBuilder::new();
        process(
            &mut fbb,
            &message,
            &DetectorSettings {
                mode: &Mode::FixedThresholdDiscriminator(test_parameters),
                polarity: &Polarity::Positive,
                baseline: Intensity::default(),
            },
        );

        assert!(digitizer_event_list_message_buffer_has_identifier(
            fbb.finished_data()
        ));
        let event_message = root_as_digitizer_event_list_message(fbb.finished_data()).unwrap();

        assert_eq!(
            vec![0, 0, 1, 1],
            event_message.channel().unwrap().iter().collect::<Vec<_>>()
        );

        assert_eq!(
            vec![8, 11, 8, 11],
            event_message.time().unwrap().iter().collect::<Vec<_>>()
        );

        assert_eq!(
            vec![9, 8, 8, 9],
            event_message.voltage().unwrap().iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn advanced_positive_zero_baseline() {
        let mut fbb = FlatBufferBuilder::new();

        let time: GpsTime = Utc::now().into();
        let channel0: Vec<u16> = vec![0, 1, 2, 1, 0, 1, 2, 1, 8, 0, 2, 8, 3, 1, 2];
        create_message(&mut fbb, &[channel0.as_slice()], &time);
        let message = fbb.finished_data().to_vec();
        let message = root_as_digitizer_analog_trace_message(&message).unwrap();

        let mut fbb = FlatBufferBuilder::new();

        let test_parameters = AdvancedMuonDetectorParameters {
            muon_onset: 0.5,
            muon_fall: -0.01,
            muon_termination: 0.001,
            duration: 0.0,
            smoothing_window_size: Some(2),
            ..Default::default()
        };
        process(
            &mut fbb,
            &message,
            &DetectorSettings {
                mode: &Mode::AdvancedMuonDetector(test_parameters),
                polarity: &Polarity::Positive,
                baseline: Intensity::default(),
            },
        );

        assert!(digitizer_event_list_message_buffer_has_identifier(
            fbb.finished_data()
        ));
        let event_message = root_as_digitizer_event_list_message(fbb.finished_data()).unwrap();

        assert_eq!(
            vec![0, 0],
            event_message.channel().unwrap().iter().collect::<Vec<_>>()
        );

        assert_eq!(
            vec![1, 7],
            event_message.time().unwrap().iter().collect::<Vec<_>>()
        );

        assert_eq!(
            vec![1, 4],
            event_message.voltage().unwrap().iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn fixed_threshold_discriminator_positive_nonzero_baseline() {
        let mut fbb = FlatBufferBuilder::new();

        let time: GpsTime = Utc::now().into();
        let channel0: Vec<u16> = vec![3, 4, 5, 4, 3, 4, 5, 4, 11, 3, 5, 11, 6, 4, 5];
        create_message(&mut fbb, &[channel0.as_slice()], &time);
        let message = fbb.finished_data().to_vec();
        let message = root_as_digitizer_analog_trace_message(&message).unwrap();

        let test_parameters = FixedThresholdDiscriminatorParameters {
            threshold: 5.0,
            duration: 1,
            cool_off: 0,
        };
        let mut fbb = FlatBufferBuilder::new();
        process(
            &mut fbb,
            &message,
            &DetectorSettings {
                mode: &Mode::FixedThresholdDiscriminator(test_parameters),
                polarity: &Polarity::Positive,
                baseline: 3,
            },
        );

        assert!(digitizer_event_list_message_buffer_has_identifier(
            fbb.finished_data()
        ));
        let event_message = root_as_digitizer_event_list_message(fbb.finished_data()).unwrap();

        assert_eq!(
            vec![0, 0],
            event_message.channel().unwrap().iter().collect::<Vec<_>>()
        );

        assert_eq!(
            vec![8, 11],
            event_message.time().unwrap().iter().collect::<Vec<_>>()
        );

        assert_eq!(
            vec![8, 8],
            event_message.voltage().unwrap().iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn advanced_positive_nonzero_baseline() {
        let mut fbb = FlatBufferBuilder::new();

        let time: GpsTime = Utc::now().into();
        let channel0: Vec<u16> = vec![3, 4, 5, 4, 3, 4, 5, 4, 11, 3, 5, 11, 6, 4, 5];
        create_message(&mut fbb, &[channel0.as_slice()], &time);
        let message = fbb.finished_data().to_vec();
        let message = root_as_digitizer_analog_trace_message(&message).unwrap();

        let mut fbb = FlatBufferBuilder::new();

        let test_parameters = AdvancedMuonDetectorParameters {
            muon_onset: 0.5,
            muon_fall: -0.01,
            muon_termination: 0.001,
            duration: 0.0,
            smoothing_window_size: Some(2),
            ..Default::default()
        };
        process(
            &mut fbb,
            &message,
            &DetectorSettings {
                mode: &Mode::AdvancedMuonDetector(test_parameters),
                polarity: &Polarity::Positive,
                baseline: 3,
            },
        );

        assert!(digitizer_event_list_message_buffer_has_identifier(
            fbb.finished_data()
        ));
        let event_message = root_as_digitizer_event_list_message(fbb.finished_data()).unwrap();

        assert_eq!(
            vec![0, 0],
            event_message.channel().unwrap().iter().collect::<Vec<_>>()
        );

        assert_eq!(
            vec![1, 7],
            event_message.time().unwrap().iter().collect::<Vec<_>>()
        );

        assert_eq!(
            vec![1, 4],
            event_message.voltage().unwrap().iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn fixed_threshold_discriminator_negative_nonzero_baseline() {
        let mut fbb = FlatBufferBuilder::new();

        let time: GpsTime = Utc::now().into();
        let channel0: Vec<u16> = vec![10, 9, 8, 9, 10, 9, 8, 9, 2, 10, 8, 2, 7, 9, 8];
        create_message(&mut fbb, &[channel0.as_slice()], &time);
        let message = fbb.finished_data().to_vec();
        let message = root_as_digitizer_analog_trace_message(&message).unwrap();

        let test_parameters = FixedThresholdDiscriminatorParameters {
            threshold: 5.0,
            duration: 1,
            cool_off: 0,
        };
        let mut fbb = FlatBufferBuilder::new();
        process(
            &mut fbb,
            &message,
            &DetectorSettings {
                mode: &Mode::FixedThresholdDiscriminator(test_parameters),
                polarity: &Polarity::Negative,
                baseline: 10,
            },
        );

        assert!(digitizer_event_list_message_buffer_has_identifier(
            fbb.finished_data()
        ));
        let event_message = root_as_digitizer_event_list_message(fbb.finished_data()).unwrap();

        assert_eq!(
            vec![0, 0],
            event_message.channel().unwrap().iter().collect::<Vec<_>>()
        );

        assert_eq!(
            vec![8, 11],
            event_message.time().unwrap().iter().collect::<Vec<_>>()
        );

        assert_eq!(
            vec![8, 8],
            event_message.voltage().unwrap().iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn advanced_negative_nonzero_baseline() {
        let mut fbb = FlatBufferBuilder::new();

        let time: GpsTime = Utc::now().into();
        let channel0: Vec<u16> = vec![10, 9, 8, 9, 10, 9, 8, 9, 2, 10, 8, 2, 7, 9, 8];
        create_message(&mut fbb, &[channel0.as_slice()], &time);
        let message = fbb.finished_data().to_vec();
        let message = root_as_digitizer_analog_trace_message(&message).unwrap();

        let mut fbb = FlatBufferBuilder::new();

        let test_parameters = AdvancedMuonDetectorParameters {
            muon_onset: 0.5,
            muon_fall: -0.01,
            muon_termination: 0.001,
            duration: 0.0,
            smoothing_window_size: Some(2),
            ..Default::default()
        };
        process(
            &mut fbb,
            &message,
            &DetectorSettings {
                mode: &Mode::AdvancedMuonDetector(test_parameters),
                polarity: &Polarity::Negative,
                baseline: 10,
            },
        );

        assert!(digitizer_event_list_message_buffer_has_identifier(
            fbb.finished_data()
        ));
        let event_message = root_as_digitizer_event_list_message(fbb.finished_data()).unwrap();

        assert_eq!(
            vec![0, 0],
            event_message.channel().unwrap().iter().collect::<Vec<_>>()
        );

        assert_eq!(
            vec![1, 7],
            event_message.time().unwrap().iter().collect::<Vec<_>>()
        );

        assert_eq!(
            vec![1, 4],
            event_message.voltage().unwrap().iter().collect::<Vec<_>>()
        );
    }
}
