//! Encapsulates a single run and provides methods for handling flatbuffer messages, intended for this run.
mod run_parameters;
mod run_spans;

use super::{
    NexusDateTime, NexusSettings,
    run_messages::{
        InitialiseNewNexusStructure, InternallyGeneratedLog, PushAlarm, PushFrameEventList,
        PushInternallyGeneratedLogWarning, PushRunLog, PushRunStart, PushSampleEnvironmentLog,
        SampleEnvironmentLog, SetEndTime, UpdatePeriodList,
    },
};
use crate::{error::NexusWriterResult, hdf5_handlers::NexusHDF5Result, nexus::NexusFileInterface};
use chrono::{Duration, Utc};
use digital_muon_common::spanned::SpanOnce;
use digital_muon_streaming_types::{
    aev2_frame_assembled_event_v2_generated::FrameAssembledEventListMessage,
    ecs_6s4t_run_stop_generated::RunStop, ecs_al00_alarm_generated::Alarm,
    ecs_f144_logdata_generated::f144_LogData, ecs_pl72_run_start_generated::RunStart,
};
pub(crate) use run_parameters::{NexusConfiguration, RunParameters, RunStopParameters};
pub(crate) use run_spans::RunSpan;
use std::{io, path::Path};
use tracing::{error, info, info_span};

/// Represents a single run.
///
/// This struct has one injected dependency,
/// a type implementing the `NexusFileInterface` trait. This allows `Run`
/// to interact with a HDF5 file whilst maintaining separation of concerns.
pub(crate) struct Run<I: NexusFileInterface> {
    /// Used by the implementation of [SpannedAggregator].
    ///
    /// [SpannedAggregator]: digital_muon_common::spanned::SpannedAggregator
    span: SpanOnce,
    /// Contains parameters of the run as specified by the `RunStart` message.
    parameters: RunParameters,
    /// Must implement the [NexusFileInterface] trait, allows for the creation of and interaction with HDF5 files.
    file: I,
}

impl<I: NexusFileInterface> Run<I> {
    /// Creates a new run.
    /// # Parameters
    /// - nexus_settings: settings pertaining to local storage and hdf5 file properties.
    /// - run_start: flatbuffer message prompting the run to start.
    /// - nexus_configuration: data to inject into the NeXus files.
    #[tracing::instrument(skip_all, level = "debug", err(level = "warn"))]
    pub(crate) fn new_run(
        nexus_settings: &NexusSettings,
        run_start: RunStart,
        nexus_configuration: &NexusConfiguration,
    ) -> NexusWriterResult<Self> {
        let parameters = RunParameters::new(run_start)?;
        let file_path = RunParameters::get_hdf5_filename(
            nexus_settings.get_local_path(),
            &parameters.file_name,
        );
        let mut file = I::build_new_file(&file_path, nexus_settings.get_chunk_sizes())?;

        file.handle_message(&InitialiseNewNexusStructure {
            parameters: &parameters,
            configuration: nexus_configuration,
        })?;
        file.handle_message(&PushRunStart(run_start))?;
        file.flush()?;

        let mut run = Self {
            span: Default::default(),
            parameters,
            file,
        };
        run.link_run_start_span();

        Ok(run)
    }

    /// Creates a run, and populates it from an existing NeXus file.
    /// # Parameters
    /// - nexus_settings: settings pertaining to local storage and hdf5 file properties.
    /// - filename: path of the NeXus file.
    pub(crate) fn resume_partial_run(
        nexus_settings: &NexusSettings,
        filename: &str,
    ) -> NexusWriterResult<Self> {
        let file_path = RunParameters::get_hdf5_filename(nexus_settings.get_local_path(), filename);
        let mut file = I::open_from_file(&file_path)?;
        let parameters = file.extract_run_parameters()?;
        file.handle_message(&PushInternallyGeneratedLogWarning {
            message: InternallyGeneratedLog::RunResume {
                resume_time: &Utc::now(),
            },
            origin: &parameters.collect_from,
            settings: nexus_settings.get_chunk_sizes(),
        })?;
        file.flush()?;

        Ok(Self {
            span: Default::default(),
            parameters,
            file,
        })
    }

    /// Returns a ref to the [RunParameters].
    pub(crate) fn parameters(&self) -> &RunParameters {
        &self.parameters
    }

    /// Renames the path of "LOCAL_PATH/temp/FILENAME.nxs" to "LOCAL_PATH/completed/FILENAME.nxs"
    /// As these paths are on the same mount, no actual file move occurs,
    /// So this does not need to be async.
    /// # Parameters
    /// - temp_path: path of now completed file.
    /// - completed_path: target path of file.
    pub(crate) fn move_to_completed(
        &self,
        temp_path: &Path,
        completed_path: &Path,
    ) -> io::Result<()> {
        let from_path = RunParameters::get_hdf5_filename(temp_path, &self.parameters.file_name);
        let to_path = RunParameters::get_hdf5_filename(completed_path, &self.parameters.file_name);

        info_span!(
            "Move To Completed",
            from_path = from_path.to_string_lossy().to_string(),
            to_path = to_path.to_string_lossy().to_string()
        )
        .in_scope(|| match std::fs::rename(from_path, to_path) {
            Ok(()) => {
                info!("File Move Succesful.");
                Ok(())
            }
            Err(e) => {
                error!("File Move Error {e}");
                Err(e)
            }
        })
    }

    /// Takes `frame_event_list` message and attempts to append it to the run.
    /// # Parameters
    /// - nexus_settings: settings pertaining to local storage and hdf5 file properties.
    /// - message: message to push.
    #[tracing::instrument(skip_all, level = "debug", err(level = "warn"))]
    pub(crate) fn push_frame_event_list(
        &mut self,
        nexus_settings: &NexusSettings,
        message: FrameAssembledEventListMessage,
    ) -> NexusWriterResult<()> {
        self.link_frame_event_list_span(message);
        self.file
            .handle_message(&PushFrameEventList { message: &message })?;

        if !self
            .parameters
            .periods
            .contains(&message.metadata().period_number())
        {
            self.parameters
                .periods
                .push(message.metadata().period_number());
            self.file.handle_message(&UpdatePeriodList {
                periods: &self.parameters.periods,
            })?;
        }

        if !message.complete() {
            self.file
                .handle_message(&PushInternallyGeneratedLogWarning {
                    message: InternallyGeneratedLog::IncompleteFrame { frame: &message },
                    origin: &self.parameters.collect_from,
                    settings: nexus_settings.get_chunk_sizes(),
                })?;
        }

        self.file.flush()?;

        self.parameters.update_last_modified();
        Ok(())
    }

    /// Takes `run_log` message and attempts to append it to the run.
    /// # Parameters
    /// - nexus_settings: settings pertaining to local storage and hdf5 file properties.
    /// - logdata: message to push.
    #[tracing::instrument(skip_all, level = "debug", err(level = "warn"))]
    pub(crate) fn push_run_log(
        &mut self,
        nexus_settings: &NexusSettings,
        logdata: &f144_LogData,
    ) -> NexusWriterResult<()> {
        self.link_run_log_span();

        self.file.handle_message(&PushRunLog {
            message: logdata,
            origin: &self.parameters.collect_from,
            settings: nexus_settings.get_chunk_sizes(),
        })?;
        self.file.flush()?;

        self.parameters.update_last_modified();
        Ok(())
    }

    /// Takes `sample_environment_log` message and attempts to append it to the run.
    /// # Parameters
    /// - nexus_settings: settings pertaining to local storage and hdf5 file properties.
    /// - selog: message to push.
    #[tracing::instrument(skip_all, level = "debug", err(level = "warn"))]
    pub(crate) fn push_sample_environment_log(
        &mut self,
        nexus_settings: &NexusSettings,
        selog: &SampleEnvironmentLog,
    ) -> NexusWriterResult<()> {
        self.link_sample_environment_log_span();

        self.file.handle_message(&PushSampleEnvironmentLog {
            message: selog,
            origin: &self.parameters.collect_from,
            settings: nexus_settings.get_chunk_sizes(),
        })?;
        self.file.flush()?;

        self.parameters.update_last_modified();
        Ok(())
    }

    /// Takes `alarm` message and attempts to append it to the run.
    /// # Parameters
    /// - nexus_settings: settings pertaining to local storage and hdf5 file properties.
    /// - alarm: message to push.
    #[tracing::instrument(skip_all, level = "debug", err(level = "warn"))]
    pub(crate) fn push_alarm(
        &mut self,
        nexus_settings: &NexusSettings,
        alarm: &Alarm,
    ) -> NexusWriterResult<()> {
        self.link_alarm_span();

        self.file.handle_message(&PushAlarm {
            message: alarm,
            origin: &self.parameters.collect_from,
            settings: nexus_settings.get_chunk_sizes(),
        })?;
        self.file.flush()?;

        self.parameters.update_last_modified();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn get_name(&self) -> &str {
        &self.parameters.run_name
    }

    /// Checks whether the run's parameters has a [RunStopParameters] set.
    pub(crate) fn has_run_stop(&self) -> bool {
        self.parameters.run_stop_parameters.is_some()
    }

    /// Takes a `run_stop` message, and if the run is expecting one, then attempts to apply it to the run.
    /// # Parameters
    /// - data: message to push.
    #[tracing::instrument(skip_all, level = "debug", err(level = "warn"))]
    pub(crate) fn set_stop_if_valid(&mut self, data: &RunStop<'_>) -> NexusWriterResult<()> {
        self.link_run_stop_span();

        self.parameters.set_stop_if_valid(data)?;

        self.file.handle_message(&SetEndTime {
            end_time: &self
                .parameters
                .run_stop_parameters
                .as_ref()
                .expect("RunStopParameters should exist, this should never happen")
                .collect_until,
        })?;
        self.file.flush()?;
        Ok(())
    }

    /// Stops a run, without a `run_stop` message.
    /// # Parameters
    /// - nexus_settings: settings pertaining to local storage and hdf5 file properties.
    /// - absolute_stop_time_ms: time at which the abort should be recorded to occur.
    pub(crate) fn abort_run(
        &mut self,
        nexus_settings: &NexusSettings,
        absolute_stop_time_ms: u64,
    ) -> NexusWriterResult<()> {
        self.parameters.set_aborted_run(absolute_stop_time_ms)?;

        let collect_until = self
            .parameters
            .run_stop_parameters
            .as_ref()
            .expect("RunStopParameters should exist, this should never happen")
            .collect_until;

        self.file.handle_message(&SetEndTime {
            end_time: &collect_until,
        })?;

        let relative_stop_time_ms =
            (collect_until - self.parameters.collect_from).num_milliseconds();
        self.file
            .handle_message(&PushInternallyGeneratedLogWarning {
                message: InternallyGeneratedLog::AbortRun {
                    stop_time_ms: relative_stop_time_ms,
                },
                origin: &self.parameters.collect_from,
                settings: nexus_settings.get_chunk_sizes(),
            })?;
        self.file.flush()?;

        Ok(())
    }

    /// Checks whether given timestamp is within range of this run.
    /// # Parameters
    /// - timestamp: timestamp to test.
    pub(crate) fn is_message_timestamp_within_range(&self, timestamp: &NexusDateTime) -> bool {
        self.parameters.is_message_timestamp_within_range(timestamp)
    }

    /// Checks whether given timestamp occurs before the end of this run.
    /// # Parameters
    /// - timestamp: timestamp to test.
    pub(crate) fn is_message_timestamp_before_end(&self, timestamp: &NexusDateTime) -> bool {
        self.parameters
            .is_message_timestamp_not_after_end(timestamp)
    }

    /// Checks whether this current run has completed. The criteria for completion is:
    /// - The run has received a run stop, and
    /// - at least `delay` time has passed since the run was last modified.
    /// # Parameters
    /// - delay: how long to wait after last modification to consider.
    pub(crate) fn has_completed(&self, delay: &Duration) -> bool {
        self.parameters
            .run_stop_parameters
            .as_ref()
            .map(|run_stop_parameters| Utc::now() - run_stop_parameters.last_modified > *delay)
            .unwrap_or(false)
    }

    /// Takes ownership of the [Run] and closes the hdf5 file.
    pub(crate) fn close(self) -> NexusHDF5Result<()> {
        self.file.close()
    }
}
