//! Defines the event list type, used for both digitiser messages and frame messages.
use super::{Accumulate, DigitiserData};
use crate::frame::AggregatedFrame;
use digital_muon_common::{Channel, DigitizerId, Intensity, Time};
use digital_muon_streaming_types::{
    aev2_frame_assembled_event_v2_generated::{
        FrameAssembledEventListMessage, FrameAssembledEventListMessageArgs,
        finish_frame_assembled_event_list_message_buffer,
    },
    dev2_digitizer_event_v2_generated::DigitizerEventListMessage,
    flatbuffers::FlatBufferBuilder,
    frame_metadata_v2_generated::{FrameMetadataV2, FrameMetadataV2Args},
};

/// Event list, either for a digitiser message, or frame message.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct EventData {
    /// Time at which event occurred, relative to frame metadata timestamp (ns).
    time: Vec<Time>,
    /// Intensity of event.
    intensity: Vec<Intensity>,
    /// Id of the detector which registered the event.
    channel: Vec<Channel>,
}

impl EventData {
    #[cfg(test)]
    pub(crate) fn new(time: Vec<Time>, intensity: Vec<Intensity>, channel: Vec<Channel>) -> Self {
        Self {
            time,
            intensity,
            channel,
        }
    }

    #[cfg(test)]
    pub(crate) fn dummy_data(
        time_offset: Time,
        events_per_channel: usize,
        channels: &[Channel],
    ) -> Self {
        let time = std::iter::repeat_n(
            &(time_offset..(time_offset + events_per_channel as Time)).collect::<Vec<Time>>(),
            channels.len(),
        )
        .flatten()
        .copied()
        .collect();

        let intensity = vec![time_offset.try_into().unwrap(); channels.len() * events_per_channel];

        let channel = channels
            .iter()
            .flat_map(|c| vec![c; events_per_channel])
            .copied()
            .collect();

        Self {
            time,
            intensity,
            channel,
        }
    }

    /// Creates an event list with a specific reserved capacity.
    /// # Parameters
    /// - capacity: the number of events to reserve in the list.
    ///
    /// Note this does not affect the length of any of the fields, merely reserves space for data to be entered.
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            time: Vec::with_capacity(capacity),
            intensity: Vec::with_capacity(capacity),
            channel: Vec::with_capacity(capacity),
        }
    }

    /// Returns the number of events in the list.
    ///
    /// This assumes all fields are of equal length.
    /// This is not checked, so must be guaranteed by the whoever builds the list.
    pub(crate) fn event_count(&self) -> usize {
        self.time.len()
    }
}

impl<'a> From<DigitizerEventListMessage<'a>> for EventData {
    fn from(msg: DigitizerEventListMessage<'a>) -> Self {
        let time = msg.time().expect("data should have times").iter().collect();
        let intensity = msg
            .voltage()
            .expect("data should have intensities")
            .iter()
            .collect();
        let channel = msg
            .channel()
            .expect("data should have channel numbers")
            .iter()
            .collect();

        // The guarantee that all fields are of equal length depends on the inputs
        // having fields of equal length. This is guaranteed by the `trace-to-events`
        // unit so is not checked.
        Self {
            time,
            intensity,
            channel,
        }
    }
}

impl Accumulate<EventData> for DigitiserData<EventData> {
    fn accumulate(data: &mut DigitiserData<EventData>) -> EventData {
        // The guarantee that all fields are of equal length depends on all
        // inputs in the collection having fields of equal length.
        let total_len = data.iter().map(|(_, v)| v.event_count()).sum();

        data.iter_mut()
            .fold(EventData::with_capacity(total_len), |mut acc, value| {
                acc.time.append(&mut value.1.time);
                acc.intensity.append(&mut value.1.intensity);
                acc.channel.append(&mut value.1.channel);
                acc
            })
    }
}

impl From<AggregatedFrame<EventData>> for Vec<u8> {
    fn from(frame: AggregatedFrame<EventData>) -> Self {
        let mut fbb = FlatBufferBuilder::new();

        let timestamp = frame.metadata.timestamp.into();
        let metadata = FrameMetadataV2Args {
            timestamp: Some(&timestamp),
            period_number: frame.metadata.period_number,
            protons_per_pulse: frame.metadata.protons_per_pulse,
            running: frame.metadata.running,
            frame_number: frame.metadata.frame_number,
            veto_flags: frame.metadata.veto_flags,
        };
        let metadata = FrameMetadataV2::create(&mut fbb, &metadata);

        let message = FrameAssembledEventListMessageArgs {
            metadata: Some(metadata),
            time: Some(fbb.create_vector::<Time>(&frame.digitiser_data.time)),
            voltage: Some(fbb.create_vector::<Intensity>(&frame.digitiser_data.intensity)),
            channel: Some(fbb.create_vector::<Channel>(&frame.digitiser_data.channel)),
            complete: frame.complete,
            digitizers_present: Some(fbb.create_vector::<DigitizerId>(&frame.digitiser_ids)),
        };
        let message = FrameAssembledEventListMessage::create(&mut fbb, &message);

        finish_frame_assembled_event_list_message_buffer(&mut fbb, message);

        fbb.finished_data().to_vec()
    }
}

#[cfg(test)]
mod test {
    use chrono::Utc;
    use digital_muon_streaming_types::FrameMetadata;

    use super::*;

    #[test]
    fn dummy_data_creation() {
        let data = EventData::dummy_data(2, 5, &[0, 1, 2]);

        assert_eq!(data.time, [2, 3, 4, 5, 6, 2, 3, 4, 5, 6, 2, 3, 4, 5, 6]);

        assert_eq!(
            data.intensity,
            [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]
        );

        assert_eq!(data.channel, [0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2]);
    }

    #[test]
    fn aggregate_frame_to_flatbuffer_bytes() {
        let now = Utc::now();

        let reference = {
            let mut fbb = FlatBufferBuilder::new();

            let timestamp = now.into();
            let metadata = FrameMetadataV2Args {
                timestamp: Some(&timestamp),
                period_number: 1,
                protons_per_pulse: 8,
                running: true,
                frame_number: 1337,
                veto_flags: 4,
            };
            let metadata = FrameMetadataV2::create(&mut fbb, &metadata);

            let message = FrameAssembledEventListMessageArgs {
                metadata: Some(metadata),
                time: Some(fbb.create_vector::<Time>(&[1, 2, 8, 9, 7])),
                voltage: Some(fbb.create_vector::<Intensity>(&[2, 8, 8, 2, 7])),
                channel: Some(fbb.create_vector::<Channel>(&[1, 3, 1, 0, 4])),
                complete: true,
                digitizers_present: Some(fbb.create_vector::<DigitizerId>(&[0, 1])),
            };
            let message = FrameAssembledEventListMessage::create(&mut fbb, &message);

            finish_frame_assembled_event_list_message_buffer(&mut fbb, message);

            fbb.finished_data().to_vec()
        };

        let test: Vec<u8> = {
            let frame = AggregatedFrame::new(
                FrameMetadata {
                    timestamp: now,
                    period_number: 1,
                    protons_per_pulse: 8,
                    running: true,
                    frame_number: 1337,
                    veto_flags: 4,
                },
                true,
                vec![0, 1],
                EventData {
                    time: vec![1, 2, 8, 9, 7],
                    intensity: vec![2, 8, 8, 2, 7],
                    channel: vec![1, 3, 1, 0, 4],
                },
            );
            frame.into()
        };

        assert_eq!(test, reference);
    }
}
