//! Encapsulates that data of a run which persists directly in memory, rather than in the HDF5 file.
use crate::{
    error::{ErrorCodeLocation, FlatBufferMissingError, NexusWriterError, NexusWriterResult},
    run_engine::NexusDateTime,
};
use chrono::Utc;
use digital_muon_streaming_types::{
    ecs_6s4t_run_stop_generated::RunStop, ecs_pl72_run_start_generated::RunStart,
};
use std::path::{Path, PathBuf};

/// Encapsulates user-specified configuration data to be written to the NeXus file
#[derive(Clone, Default, Debug)]
pub(crate) struct NexusConfiguration {
    /// Data pipeline configuration to be written to the `/raw_data_1/program_name/configuration`
    /// attribute of the NeXus file.
    pub(crate) configuration: String,
}

impl NexusConfiguration {
    pub(crate) fn new(configuration: Option<String>) -> Self {
        Self {
            configuration: configuration.unwrap_or_default(),
        }
    }
}

/// Encapsulates all data for a run which has received a `RunStop` and hence can be deleted
/// when it is no longer receiving data
#[derive(Default, Debug, Clone)]
pub(crate) struct RunStopParameters {
    /// Timestamp of the moment the run officially ended
    pub(crate) collect_until: NexusDateTime,
    /// Timestamp of the last moment the run was modified (i.e. by receiving a message)
    pub(crate) last_modified: NexusDateTime,
}

/// Encapsulates all data for a run that persists in memory (outside of the NeXus file)
#[derive(Debug, Clone)]
pub(crate) struct RunParameters {
    /// Timestamp of the moment the run started
    pub(crate) collect_from: NexusDateTime,
    /// This is initially None, and set when a corresponding `RunStop` is received.
    pub(crate) run_stop_parameters: Option<RunStopParameters>,
    /// Name of the run, as appears in the `RunStart` message
    pub(crate) run_name: String,
    /// Vector of periods used within the run
    pub(crate) periods: Vec<u64>,
    /// Filename for the run
    pub(crate) file_name: String,
}

impl RunParameters {
    /// Creates new instance with parameters extracted from a flatbuffer `RunStart` message.
    /// # Parameters
    /// - data: A `RunStart` message
    #[tracing::instrument(skip_all, level = "trace", err(level = "warn"))]
    pub(crate) fn new(data: RunStart<'_>) -> NexusWriterResult<Self> {
        let run_name = data
            .run_name()
            .ok_or(NexusWriterError::FlatBufferMissing(
                FlatBufferMissingError::RunName,
                ErrorCodeLocation::NewRunParameters,
            ))?
            .to_owned();

        let file_name = data
            .filename()
            .ok_or(NexusWriterError::FlatBufferMissing(
                FlatBufferMissingError::FileName,
                ErrorCodeLocation::NewRunParameters,
            ))?
            .to_owned();

        Ok(Self {
            collect_from: NexusDateTime::from_timestamp_millis(data.start_time().try_into()?)
                .ok_or(NexusWriterError::IntOutOfRangeForDateTime {
                    int: data.start_time(),
                    location: ErrorCodeLocation::NewRunParameters,
                })?,
            run_stop_parameters: None,
            run_name,
            periods: Default::default(),
            file_name,
        })
    }

    /// Takes a `run_stop` message, and if the run is expecting one, then attempts to apply it to the run.
    /// # Parameters
    /// - data: A `RunStop` message
    /// # Error
    /// Emits [NexusWriterError::StopCommandBeforeStartCommand] if the [Self::run_stop_parameters] already exist.
    #[tracing::instrument(skip_all, level = "trace", err(level = "warn"))]
    pub(crate) fn set_stop_if_valid(&mut self, data: &RunStop<'_>) -> NexusWriterResult<()> {
        if self.run_stop_parameters.is_some() {
            Err(NexusWriterError::StopCommandBeforeStartCommand(
                ErrorCodeLocation::SetStopIfValid,
            ))
        } else {
            let stop_time = NexusDateTime::from_timestamp_millis(data.stop_time().try_into()?)
                .ok_or(NexusWriterError::IntOutOfRangeForDateTime {
                    int: data.stop_time(),
                    location: ErrorCodeLocation::SetStopIfValid,
                })?;
            if self.collect_from < stop_time {
                self.run_stop_parameters = Some(RunStopParameters {
                    collect_until: stop_time,
                    last_modified: Utc::now(),
                });
                Ok(())
            } else {
                Err(NexusWriterError::StopTimeEarlierThanStartTime {
                    start: self.collect_from,
                    stop: stop_time,
                    location: ErrorCodeLocation::SetStopIfValid,
                })
            }
        }
    }

    /// Stops a run, without a `run_stop` message.
    /// # Parameters
    /// - stop_time: time at which the abort should be recorded to occur.
    /// # Error Modes
    /// - Emits [RunStopAlreadySet] if the [Self::run_stop_parameters] already exist.
    ///
    /// [RunStopAlreadySet]: NexusWriterError::RunStopAlreadySet
    #[tracing::instrument(skip_all, level = "trace", err(level = "warn"))]
    pub(crate) fn set_aborted_run(&mut self, stop_time: u64) -> NexusWriterResult<()> {
        let collect_until = NexusDateTime::from_timestamp_millis(stop_time.try_into()?).ok_or(
            NexusWriterError::IntOutOfRangeForDateTime {
                int: stop_time,
                location: ErrorCodeLocation::SetAbortedRun,
            },
        )?;
        if self.run_stop_parameters.is_some() {
            return Err(NexusWriterError::RunStopAlreadySet(
                ErrorCodeLocation::SetAbortedRun,
            ));
        }
        {
            self.run_stop_parameters = Some(RunStopParameters {
                collect_until,
                last_modified: Utc::now(),
            });
        }
        Ok(())
    }

    /// Returns `true` if timestamp is strictly after collect_from and,
    /// if `run_stop_parameters` exist then, if timestamp is strictly
    /// before `params.collect_until`.
    /// # Parameters
    /// - timestamp: timestamp to test.
    #[tracing::instrument(skip_all, level = "trace")]
    pub(crate) fn is_message_timestamp_within_range(&self, timestamp: &NexusDateTime) -> bool {
        if self.collect_from < *timestamp {
            self.is_message_timestamp_not_after_end(timestamp)
        } else {
            false
        }
    }

    /// if `run_stop_parameters` exist then, return `true` if timestamp is
    /// strictly before `params.collect_until`, otherwise returns `true`.
    /// before `params.collect_until`.
    /// # Parameters
    /// - timestamp: timestamp to test.
    #[tracing::instrument(skip_all, level = "trace")]
    pub(crate) fn is_message_timestamp_not_after_end(&self, timestamp: &NexusDateTime) -> bool {
        self.run_stop_parameters
            .as_ref()
            .map(|params| *timestamp < params.collect_until)
            .unwrap_or(true)
    }

    /// If the run has a `run_stop` then set the last modified timestamp to the current time.
    #[tracing::instrument(skip_all, level = "trace")]
    pub(crate) fn update_last_modified(&mut self) {
        if let Some(params) = &mut self.run_stop_parameters {
            params.last_modified = Utc::now();
        }
    }

    /// Constructs the file path from a directory and string for the run name.
    pub(crate) fn get_hdf5_filename(path: &Path, file_name: &str) -> PathBuf {
        let mut path = path.to_owned();
        path.push(file_name);
        path.set_extension("nxs");
        path
    }
}
