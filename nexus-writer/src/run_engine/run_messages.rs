//! Contains stucts used to pass messages to the `nexus_structure` module.
//!
//! Given a message type `M` and a type `T` implementing `NexusHandleMessage<M>`, we pass
//! the message to an instance of `T` via `T::handle_message(m)` where `m : M`.
use super::{ChunkSizeSettings, NexusConfiguration, NexusDateTime, RunParameters};
use crate::nexus::NexusMessageHandler;
use digital_muon_streaming_types::{
    aev2_frame_assembled_event_v2_generated::FrameAssembledEventListMessage,
    ecs_al00_alarm_generated::Alarm, ecs_f144_logdata_generated::f144_LogData,
    ecs_pl72_run_start_generated::RunStart, ecs_se00_data_generated::se00_SampleEnvironmentData,
};
use std::ops::Deref;

/// As Sample Environment Logs can be delivered via both f144 or se00 type messages,
/// a wrapper enum is required to handle them.
#[derive(Debug)]
pub(crate) enum SampleEnvironmentLog<'a> {
    LogData(f144_LogData<'a>),
    SampleEnvironmentData(se00_SampleEnvironmentData<'a>),
}

/// Initialises the fields which are initialised by [RunParameters] or [NexusConfiguration]
pub(crate) struct InitialiseNewNexusStructure<'a> {
    /// The parameters to initialise with.
    pub(crate) parameters: &'a RunParameters,
    /// The configuration to initialise with.
    pub(crate) configuration: &'a NexusConfiguration,
}

/// Tells [nexus_structure] to initialise fields based on values in [RunParameters]
///
/// [nexus_structure]: crate::nexus_structure
pub(crate) struct InitialiseNewNexusRun<'a> {
    /// The parameters to initialise with.
    pub(crate) parameters: &'a RunParameters,
}

/// Tells [nexus_structure] to process a [RunStart] message.
/// This is used to insert any data not covered by the [InitialiseNewNexusRun] message.
///
/// [nexus_structure]: crate::nexus_structure
pub(crate) struct PushRunStart<'a>(pub(crate) RunStart<'a>);

/// Tells [nexus_structure] to input values from a new [FrameAssembledEventListMessage].
/// Note this does not handle values in the `Period` hdf5 group.
///
/// [nexus_structure]: crate::nexus_structure
pub(crate) struct PushFrameEventList<'a> {
    /// The frame event list message to push.
    pub(crate) message: &'a FrameAssembledEventListMessage<'a>,
}

/// Tells [nexus_structure] to update the periods list in the `Periods` hdf5 group.
///
/// [nexus_structure]: crate::nexus_structure
pub(crate) struct UpdatePeriodList<'a> {
    /// The period list to update from.
    pub(crate) periods: &'a [u64],
}

/// Generic message used to tell [nexus_structure] a new log has been received.
///
/// [nexus_structure]: crate::nexus_structure
pub(crate) struct PushLog<'a, T> {
    /// The log message to push.
    pub(crate) message: T,
    /// The timestamp to which the log times should be relative to.
    pub(crate) origin: &'a NexusDateTime,
    /// The sizes of the chunks to use.
    pub(crate) settings: &'a ChunkSizeSettings,
}

impl<T> Deref for PushLog<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.message
    }
}

/// Tells [nexus_structure] a new RunLog has been received.
///
/// [nexus_structure]: crate::nexus_structure
pub(crate) type PushRunLog<'a> = PushLog<'a, &'a f144_LogData<'a>>;

/// Tells [nexus_structure] a new `SampleEnvironmentLog` has been received.
///
/// [nexus_structure]: crate::nexus_structure
pub(crate) type PushSampleEnvironmentLog<'a> = PushLog<'a, &'a SampleEnvironmentLog<'a>>;

/// Tells [nexus_structure] a new `Alarm` has been received.
///
/// [nexus_structure]: crate::nexus_structure
pub(crate) type PushAlarm<'a> = PushLog<'a, &'a Alarm<'a>>;

/// Enum for internally generated logs.
pub(crate) enum InternallyGeneratedLog<'a> {
    /// When a previously started run, is resumed.
    RunResume {
        /// The timestamp at which the run is resumed.
        resume_time: &'a NexusDateTime,
    },
    /// When an incomplete frame arrives.
    IncompleteFrame {
        /// The frame event list that is incomplete.
        frame: &'a FrameAssembledEventListMessage<'a>,
    },
    /// When an run should be aborted.
    AbortRun {
        /// The ms since epoch to record as the stop time.
        stop_time_ms: i64,
    },
}

/// Tells [nexus_structure] an internal warning has been generated.
///
/// [nexus_structure]: crate::nexus_structure
pub(crate) type PushInternallyGeneratedLogWarning<'a> = PushLog<'a, InternallyGeneratedLog<'a>>;

/// Tells [nexus_structure] to set the `end_time` hdf5 dataset.
///
/// [nexus_structure]: crate::nexus_structure
pub(crate) struct SetEndTime<'a> {
    /// The timestamp to set as the end time.
    pub(crate) end_time: &'a NexusDateTime,
}

/// Ensures anything implementing [NexusFileInterface] must implement the correct [NexusMessageHandler]s.
/// Any new message that is added to this module should be added here.
///
/// [NexusFileInterface]: crate::nexus::NexusFileInterface
pub(crate) trait HandlesAllNexusMessages:
    for<'a> NexusMessageHandler<InitialiseNewNexusStructure<'a>>
    + for<'a> NexusMessageHandler<PushFrameEventList<'a>>
    + for<'a> NexusMessageHandler<UpdatePeriodList<'a>>
    + for<'a> NexusMessageHandler<PushRunLog<'a>>
    + for<'a> NexusMessageHandler<PushRunStart<'a>>
    + for<'a> NexusMessageHandler<PushSampleEnvironmentLog<'a>>
    + for<'a> NexusMessageHandler<PushInternallyGeneratedLogWarning<'a>>
    + for<'a> NexusMessageHandler<PushAlarm<'a>>
    + for<'a> NexusMessageHandler<SetEndTime<'a>>
{
}
