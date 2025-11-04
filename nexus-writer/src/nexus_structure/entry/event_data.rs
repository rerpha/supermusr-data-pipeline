//! Defines [EventData] group structure which contains detection data of the muon events.
use crate::{
    error::FlatBufferMissingError,
    hdf5_handlers::{
        AttributeExt, ConvertResult, DatasetExt, GroupExt, HasAttributesExt, NexusHDF5Error,
        NexusHDF5Result,
    },
    nexus::{DatasetUnitExt, NexusClass, NexusUnits},
    nexus_structure::{NexusMessageHandler, NexusSchematic},
    run_engine::{
        EventChunkSize, FrameChunkSize, NexusDateTime,
        run_messages::{InitialiseNewNexusRun, PushFrameEventList},
    },
};
use digital_muon_common::{Channel, Time};
use digital_muon_streaming_types::aev2_frame_assembled_event_v2_generated::FrameAssembledEventListMessage;
use hdf5::{Attribute, Dataset, Group};

/// Field names for [EventData].
mod labels {
    pub(super) const PULSE_HEIGHT: &str = "pulse_height";
    pub(super) const EVENT_ID: &str = "event_id";
    pub(super) const EVENT_TIME_ZERO: &str = "event_time_zero";
    pub(super) const EVENT_TIME_ZERO_OFFSET: &str = "offset";
    pub(super) const EVENT_TIME_OFFSET: &str = "event_time_offset";
    pub(super) const EVENT_INDEX: &str = "event_index";
    pub(super) const PERIOD_NUMBER: &str = "period_number";
    pub(super) const FRAME_NUMBER: &str = "frame_number";
    pub(super) const FRAME_COMPLETE: &str = "frame_complete";
    pub(super) const RUNNING: &str = "running";
    pub(super) const VETO_FLAGS: &str = "veto_flags";
}

pub(crate) struct EventData {
    /// Number of messages pushed via [NexusMessageHandler<PushFrameEventList<'_>>]. This is equal to the number of frames.
    num_messages: usize,
    /// Number of muon events appended through the [NexusMessageHandler<PushFrameEventList<'_>>] messages.
    num_events: usize,
    /// Optional value stored in [Self::event_time_zero_offset].
    offset: Option<NexusDateTime>,
    /// Vector of muon event intensities.
    pulse_height: Dataset,
    /// Vector of muon event channels.
    event_id: Dataset,
    /// Vector of muon-event times specifying the time (in ns) at which each muon event occurs, relative to the appropriate entry in [Self::event_time_zero].
    event_time_offset: Dataset,
    /// Vector of frame times (in ns) specifying the start time of each frame (relative to this [Dataset]'s offset [Attribute]).
    event_time_zero: Dataset,
    /// Timestamp indicating the time which values in [Self::event_time_zero] are relative to.
    event_time_zero_offset: Attribute,
    /// Vector of indices in [Self::pulse_height], [Self::event_id], and [Self::event_time_offset] which denote the start of each frame.
    event_index: Dataset,
    /// Vector of numbers specifying to period each frame belongs.
    period_number: Dataset,
    /// Vector of numbers specifying the number of each frame.
    frame_number: Dataset,
    /// Vector of booleans specifying whether each frame is marked as complete.
    frame_complete: Dataset,
    /// Vector of booleans specifying whether each frame is marked as running.
    running: Dataset,
    /// Vector specifying the veto_flags of each each frame.
    veto_flags: Dataset,
}

impl NexusSchematic for EventData {
    const CLASS: NexusClass = NexusClass::EventData;
    type Settings = (EventChunkSize, FrameChunkSize);

    fn build_group_structure(
        group: &Group,
        (event_chunk_size, frame_chunk_size): &Self::Settings,
    ) -> NexusHDF5Result<Self> {
        let event_time_zero = group
            .create_resizable_empty_dataset::<u64>(labels::EVENT_TIME_ZERO, *frame_chunk_size)?
            .with_units(NexusUnits::Nanoseconds)?;
        let event_time_zero_offset =
            event_time_zero.add_string_attribute(labels::EVENT_TIME_ZERO_OFFSET)?;

        Ok(Self {
            num_messages: Default::default(),
            num_events: Default::default(),
            offset: None,
            pulse_height: group
                .create_resizable_empty_dataset::<f64>(labels::PULSE_HEIGHT, *event_chunk_size)?,
            event_id: group
                .create_resizable_empty_dataset::<Channel>(labels::EVENT_ID, *event_chunk_size)?,
            event_time_offset: group
                .create_resizable_empty_dataset::<Time>(
                    labels::EVENT_TIME_OFFSET,
                    *event_chunk_size,
                )?
                .with_units(NexusUnits::Nanoseconds)?,
            event_time_zero,
            event_time_zero_offset,
            event_index: group
                .create_resizable_empty_dataset::<u64>(labels::EVENT_INDEX, *frame_chunk_size)?,
            period_number: group
                .create_resizable_empty_dataset::<u64>(labels::PERIOD_NUMBER, *frame_chunk_size)?,
            frame_number: group
                .create_resizable_empty_dataset::<u64>(labels::FRAME_NUMBER, *frame_chunk_size)?,
            frame_complete: group
                .create_resizable_empty_dataset::<u64>(labels::FRAME_COMPLETE, *frame_chunk_size)?,
            running: group
                .create_resizable_empty_dataset::<bool>(labels::RUNNING, *frame_chunk_size)?,
            veto_flags: group
                .create_resizable_empty_dataset::<u16>(labels::VETO_FLAGS, *frame_chunk_size)?,
        })
    }

    fn populate_group_structure(group: &Group) -> NexusHDF5Result<Self> {
        let pulse_height = group.get_dataset(labels::PULSE_HEIGHT)?;
        let event_id = group.get_dataset(labels::EVENT_ID)?;
        let event_time_offset = group.get_dataset(labels::EVENT_TIME_OFFSET)?;

        let event_index = group.get_dataset(labels::EVENT_INDEX)?;
        let event_time_zero = group.get_dataset(labels::EVENT_TIME_ZERO)?;
        let period_number = group.get_dataset(labels::PERIOD_NUMBER)?;
        let frame_number = group.get_dataset(labels::FRAME_NUMBER)?;
        let frame_complete = group.get_dataset(labels::FRAME_COMPLETE)?;
        let running = group.get_dataset(labels::RUNNING)?;
        let veto_flags = group.get_dataset(labels::VETO_FLAGS)?;

        let event_time_zero_offset =
            event_time_zero.get_attribute(labels::EVENT_TIME_ZERO_OFFSET)?;

        let offset = Some(event_time_zero_offset.get_datetime()?);

        Ok(Self {
            offset,
            num_messages: event_time_zero.size(),
            num_events: event_time_offset.size(),
            event_id,
            event_index,
            pulse_height,
            event_time_offset,
            event_time_zero,
            event_time_zero_offset,
            period_number,
            frame_number,
            frame_complete,
            running,
            veto_flags,
        })
    }
}

/// Sets up the `offset` attribute of the `event_time_zero` dataset.
impl NexusMessageHandler<InitialiseNewNexusRun<'_>> for EventData {
    fn handle_message(
        &mut self,
        &InitialiseNewNexusRun { parameters }: &InitialiseNewNexusRun<'_>,
    ) -> NexusHDF5Result<()> {
        self.offset = Some(parameters.collect_from);
        self.event_time_zero_offset
            .set_string(&parameters.collect_from.to_rfc3339())?;
        Ok(())
    }
}

impl EventData {
    /// Extracts the timestamp from the message's metadata and convert it to nanoseconds since [Self::offset].
    /// # Parameters
    /// - message: the frame event list to extract the timestamp from.
    /// # Return
    /// The nanoseconds since [Self::offset]
    ///
    /// [TimeDelta::num_nanoseconds()]: chrono::TimeDelta::num_nanoseconds()
    /// [TryFromIntError]: std::num::TryFromIntError
    pub(crate) fn get_time_zero(
        &self,
        message: &FrameAssembledEventListMessage,
    ) -> NexusHDF5Result<u64> {
        let timestamp: NexusDateTime = (*message
            .metadata()
            .timestamp()
            .ok_or(FlatBufferMissingError::Timestamp)?)
        .try_into()?;

        // Recalculate time_zero of the frame to be relative to the offset value
        // (set at the start of the run).
        let timedelta = timestamp - self.offset.ok_or(FlatBufferMissingError::Timestamp)?;
        let time_zero = timedelta
            .num_nanoseconds()
            .ok_or_else(|| NexusHDF5Error::timedelta_convert_to_ns(timedelta))?;
        Ok(time_zero.try_into()?)
    }
}

/// Appends data from the provided [FrameAssembledEventListMessage] message.
impl NexusMessageHandler<PushFrameEventList<'_>> for EventData {
    fn handle_message(
        &mut self,
        &PushFrameEventList { message }: &PushFrameEventList<'_>,
    ) -> NexusHDF5Result<()> {
        // Fields Indexed By Frame
        self.event_index.append_value(self.num_events)?;

        // Recalculate time_zero of the frame to be relative to the offset value
        // (set at the start of the run).
        let time_zero = self
            .get_time_zero(message)
            .err_dataset(&self.event_time_zero)?;

        self.event_time_zero.append_value(time_zero)?;
        self.period_number
            .append_value(message.metadata().period_number())?;
        self.frame_number
            .append_value(message.metadata().frame_number())?;
        self.frame_complete.append_value(message.complete())?;

        self.running.append_value(message.metadata().running())?;

        self.veto_flags
            .append_value(message.metadata().veto_flags())?;

        // Fields Indexed By Event

        let intensities = &message
            .voltage()
            .ok_or(FlatBufferMissingError::Intensities)?
            .iter()
            .collect::<Vec<_>>();

        let times = &message
            .time()
            .ok_or(FlatBufferMissingError::Times)?
            .iter()
            .collect::<Vec<_>>();

        let channels = &message
            .channel()
            .ok_or(FlatBufferMissingError::Channels)?
            .iter()
            .collect::<Vec<_>>();

        let num_new_events = channels.len();
        let total_events = self.num_events + num_new_events;

        self.pulse_height.append_slice(intensities)?;
        self.event_time_offset.append_slice(times)?;
        self.event_id.append_slice(channels)?;

        self.num_events = total_events;
        self.num_messages += 1;
        Ok(())
    }
}
