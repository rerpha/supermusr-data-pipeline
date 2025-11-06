//! Top-level error handling.
use crate::{hdf5_handlers::NexusHDF5Error, run_engine::NexusDateTime};
use digital_muon_streaming_types::time_conversions::GpsTimeConversionError;
use glob::{GlobError, PatternError};
use rdkafka::error::KafkaError;
use std::{num::TryFromIntError, path::PathBuf};
use thiserror::Error;

pub(crate) type NexusWriterResult<T> = Result<T, NexusWriterError>;

/// Specifies where in the code base the error was emitted from
#[derive(Debug, strum::Display)]
pub(crate) enum ErrorCodeLocation {
    #[strum(to_string = "flush_to_archive")]
    FlushToArchive,
    #[strum(to_string = "RunParameters::new")]
    NewRunParameters,
    #[strum(to_string = "process_event_list")]
    ProcessEventList,
    #[strum(to_string = "resume_partial_runs file path")]
    ResumePartialRunsFilePath,
    #[strum(to_string = "resume_partial_runs local directory path")]
    ResumePartialRunsLocalDirectoryPath,
    #[strum(to_string = "set_aborted_run")]
    SetAbortedRun,
    #[strum(to_string = "set_stop_if_valid")]
    SetStopIfValid,
    #[strum(to_string = "stop_command")]
    StopCommand,
}

/// Error object used at the top level of the nexus-writer component.
///
/// These variants describes all possible errors and implment `Into<NexusWriterError>`
/// for some other errors. Some variants take an additional
/// `location: ` [ErrorCodeLocation] field, describing where in the code the error happened.
#[derive(Debug, Error)]
pub(crate) enum NexusWriterError {
    /// [NexusHDF5Error] error emitted from the [nexus_structure] module.
    ///
    /// [nexus_structure]: crate::nexus_structure
    #[error("{0}")]
    HDF5(#[from] NexusHDF5Error),
    /// Indicates a glob pattern error, most likely a file path handling error.
    #[error("Glob Pattern Error: {0}")]
    GlobPattern(#[from] PatternError),
    /// Indicates a glob error, most likely a file path handling error.
    #[error("Glob Error: {0}")]
    Glob(#[from] GlobError),
    /// Indicates an IO error, most likely a file handling error.
    #[error("IO Error: {0}")]
    IO(#[from] std::io::Error),
    /// Error converting one integer type to another, either trying to convert a negative to an unsigned, or a value is too large to fit into the bit-width.
    #[error("Integer Conversion Error")]
    TryFromInt(#[from] TryFromIntError),
    /// Error converting the `GpsTime` type, most likely a malformed timestamp.
    #[error("Flatbuffer Timestamp Conversion Error {0}")]
    FlatBufferTimestampConversion(#[from] GpsTimeConversionError),
    /// A missing element of a flatbuffer message.
    #[error("{0} at {1}")]
    FlatBufferMissing(FlatBufferMissingError, ErrorCodeLocation),
    /// A general Kafka error.
    #[error("Kafka Error: {0}")]
    KafkaError(#[from] KafkaError),
    /// An error converting a path to a string.
    #[error("Cannot convert local path to string: {path} at {location}")]
    CannotConvertPath {
        path: PathBuf,
        location: ErrorCodeLocation,
    },
    /// An unsigned long into is too large to be interpreted as nanoseconds since epoch.
    #[error("Start Time {int} Out of Range for DateTime at {location}")]
    IntOutOfRangeForDateTime {
        int: u64,
        location: ErrorCodeLocation,
    },
    /// A `RunStop` was received before a corresponding `RunStart`.
    #[error("Stop Command before Start Command at {0}")]
    StopCommandBeforeStartCommand(ErrorCodeLocation),
    /// A `RunStop` with a stop time earlier than the corresponding `RunStart` start time was received.
    #[error("Stop Time {stop} earlier than current Start Time {start} at {location}")]
    StopTimeEarlierThanStartTime {
        start: NexusDateTime,
        stop: NexusDateTime,
        location: ErrorCodeLocation,
    },
    /// A `RunStop` corresponding to a run that has already been stopped has been received.
    #[error("RunStop already set at {0}")]
    RunStopAlreadySet(ErrorCodeLocation),
    /// An unexpected `RunStop` has been received.
    #[error("Unexpected RunStop Command at {0}")]
    RunStopUnexpected(ErrorCodeLocation),
}

/// Specifies which type of log message an invalid flatbuffer data error pertains to.
#[derive(Debug, strum::Display)]
pub(crate) enum FlatBufferInvalidDataTypeContext {
    /// The invalid message is a `RunLog` type.
    #[strum(to_string = "Run Log")]
    RunLog,
    /// The invalid message is a `SELog` type.
    #[strum(to_string = "Sample Environment Log")]
    SELog,
}

/// Specifies which element of a flatbuffer message is missing.
#[derive(Debug, Error)]
pub(crate) enum FlatBufferMissingError {
    /// A timestamp is missing.
    #[error("Timestamp Missing from Flatbuffer FrameEventList Message")]
    Timestamp,
    /// A channels vector is missing.
    #[error("Channels Missing from Flatbuffer FrameEventList Message")]
    Channels,
    /// An intensities vector is missing.
    #[error("Intensities Missing from Flatbuffer FrameEventList Message")]
    Intensities,
    /// An times vector is missing.
    #[error("Times Missing from Flatbuffer FrameEventList Message")]
    Times,
    /// The run name string is missing.
    #[error("Run Name Missing from Flatbuffer RunStart Message")]
    RunName,
    /// The instrument name string is missing.
    #[error("Instrument Name Missing from Flatbuffer RunStart Message")]
    InstrumentName,
    /// The alarm name string is missing.
    #[error("Source Name Missing from Flatbuffer Alarm Message")]
    AlarmName,
    /// The alarm severity name string is missing.
    #[error("Severity Missing from Flatbuffer Alarm Message")]
    AlarmSeverity,
    /// The alarm status string is missing.
    #[error("Status Missing from Flatbuffer Alarm Message")]
    AlarmMessage,
    /// The file name string is missing.
    #[error("File name missing from flatbuffer RunStart Message")]
    FileName,
}
