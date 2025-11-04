//! Defines and implements the [NexusEngine] struct.
use super::run_messages::SampleEnvironmentLog;
use crate::{
    TopicMode,
    error::{ErrorCodeLocation, FlatBufferMissingError, NexusWriterError, NexusWriterResult},
    kafka_topic_interface::KafkaTopicInterface,
    nexus::NexusFileInterface,
    run_engine::{NexusConfiguration, NexusDateTime, NexusSettings, Run},
};
use chrono::Duration;
use digital_muon_common::spanned::SpannedAggregator;
use digital_muon_streaming_types::{
    aev2_frame_assembled_event_v2_generated::FrameAssembledEventListMessage,
    ecs_6s4t_run_stop_generated::RunStop, ecs_al00_alarm_generated::Alarm,
    ecs_f144_logdata_generated::f144_LogData, ecs_pl72_run_start_generated::RunStart,
};
use glob::glob;
#[cfg(test)]
use std::collections::vec_deque;
use std::{collections::VecDeque, ffi::OsStr};
use tracing::{debug, info_span, warn};

/// Enables searching for a valid run based on a timestamp.
trait FindValidRun<I: NexusFileInterface> {
    /// Searches for a run whose start and end contains the timestamp,
    /// and returns a mut ref to the first such run found.
    /// This should be the only such run in the collection,
    /// but this is not checked.
    /// # Parameters
    /// - timestamp: Timestamp to check.
    /// # Return
    /// - A mut ref to the first valid timestamp, or
    /// - [None] if none are found.
    /// # Warnings
    /// If no valid timestamp is found, a debug message is emitted.
    fn find_run_containing(&mut self, timestamp: &NexusDateTime) -> Option<&mut Run<I>>;

    /// Searches for a run whose `end_time` follows the timestamp.
    /// and returns a mut ref to the first such run found.
    /// This should be the only such run in the collection,
    /// but this is not checked.
    /// The `has_run` field of the current span is set.
    /// If no valid timestamp is found, a debug message is emitted.
    /// # Parameters
    /// - timestamp: Timestamp to check.
    /// # Return
    /// - A mut ref to the first valid timestamp found, or
    /// - [None] if none are found.
    /// # Warnings
    /// If no valid timestamp is found, a debug message is emitted.
    fn find_run_not_ending_before(&mut self, timestamp: &NexusDateTime) -> Option<&mut Run<I>>;
}

impl<I: NexusFileInterface> FindValidRun<I> for VecDeque<Run<I>> {
    #[tracing::instrument(skip_all, level = "debug", fields(has_run))]
    fn find_run_containing(&mut self, timestamp: &NexusDateTime) -> Option<&mut Run<I>> {
        let maybe_run = self
            .iter_mut()
            .find(|run| run.is_message_timestamp_within_range(timestamp));

        if maybe_run.is_none() {
            debug!("No run found for message with timestamp: {timestamp}");
        }
        tracing::Span::current().record("has_run", maybe_run.is_some());
        maybe_run
    }

    #[tracing::instrument(skip_all, level = "debug", fields(has_run))]
    fn find_run_not_ending_before(&mut self, timestamp: &NexusDateTime) -> Option<&mut Run<I>> {
        let maybe_run = self
            .iter_mut()
            .find(|run| run.is_message_timestamp_before_end(timestamp));

        if maybe_run.is_none() {
            debug!("No run found for message with timestamp: {timestamp}");
        }
        tracing::Span::current().record("has_run", maybe_run.is_some());
        maybe_run
    }
}

/// Encapsulates all dependencies injected into NexusEngine.
///
/// For example, suppose we have types [NexusFile] and [TopicSubscriber]
/// implementing the [NexusFileInterface] and [KafkaTopicInterface] traits
/// respectively, then we encapsulate them in a blank struct, like:
/// ```rust
/// struct EngineDependencies;
///
/// impl NexusEngineDependencies for EngineDependencies {
///     type FileInterface = NexusFile;
///     type TopicInterface = TopicSubscriber;
/// }
/// ```
/// which is then injected into the type of `NexusEngine`:
/// ```rust
/// let engine = NexusEngine::<EngineDependencies>::new(...);
/// ```
///
/// [NexusFile]: crate::nexus::NexusFile
/// [TopicSubscriber]: crate::kafka_topic_interface::TopicSubscriber
pub(crate) trait NexusEngineDependencies {
    type FileInterface: NexusFileInterface;
    type TopicInterface: KafkaTopicInterface;
}

/// Owns and handles all runs, and handles messages from the Kafka broker.
pub(crate) struct NexusEngine<D: NexusEngineDependencies> {
    /// Settings pertaining to local storage and hdf5 file properties.
    nexus_settings: NexusSettings,
    /// Container for runs.
    run_cache: VecDeque<Run<D::FileInterface>>,
    /// Configuration data to inject into the NeXus files.
    nexus_configuration: NexusConfiguration,
    /// Interface to control Kafka topic subscriptions.
    kafka_topic_interface: D::TopicInterface,
}

impl<D: NexusEngineDependencies> NexusEngine<D> {
    /// Creates a new instance with empty `run_cache`.
    /// # Parameters
    /// - nexus_settings: settings pertaining to local storage and hdf5 file properties.
    /// - nexus_configuration: data to inject into the NeXus files.
    /// - kafka_topic_interface: object controlling Kafka topic subscriptions.
    #[tracing::instrument(skip_all)]
    pub(crate) fn new(
        nexus_settings: NexusSettings,
        nexus_configuration: NexusConfiguration,
        kafka_topic_interface: D::TopicInterface,
    ) -> Self {
        Self {
            nexus_settings,
            run_cache: Default::default(),
            nexus_configuration,
            kafka_topic_interface,
        }
    }

    /// Called shortly after initialisation,
    /// this method searches the local directory for
    /// .nxs files and creates Run instances for each file found.
    /// There should only be .nxs files if the nexus writer
    /// was previous interupted mid-run.
    pub(crate) fn resume_partial_runs(&mut self) -> NexusWriterResult<()> {
        let local_path_str = self
            .nexus_settings
            .get_local_temp_glob_pattern()
            .map_err(|path| NexusWriterError::CannotConvertPath {
                path: path.to_path_buf(),
                location: ErrorCodeLocation::ResumePartialRunsLocalDirectoryPath,
            })?;
        for file_path in glob(&local_path_str)? {
            let file_path = file_path?;
            let filename_str = file_path
                .file_stem()
                .and_then(OsStr::to_str)
                .ok_or_else(|| NexusWriterError::CannotConvertPath {
                    path: file_path.clone(),
                    location: ErrorCodeLocation::ResumePartialRunsFilePath,
                })?;
            let mut run = info_span!(
                "Partial Run Found",
                path = local_path_str,
                file_name = filename_str
            )
            .in_scope(|| Run::resume_partial_run(&self.nexus_settings, filename_str))?;
            if let Err(e) = run.span_init() {
                warn!("Run span initiation failed {e}")
            }
            self.run_cache.push_back(run);
        }
        Ok(())
    }

    #[cfg(test)]
    fn cache_iter(&self) -> vec_deque::Iter<'_, Run<D::FileInterface>> {
        self.run_cache.iter()
    }

    /// Returns the number of runs currently in the cache.
    ///
    /// In normal operation there should only be one (or possibly
    /// two if a second run starts quickly after the first has ended).
    /// A high value for this probably indicates a problem.
    pub(crate) fn get_num_cached_runs(&self) -> usize {
        self.run_cache.len()
    }

    /// This there is a run in the run cache, and the final one is still running,
    /// this method aborts it, and creates a new run
    /// # Parameters
    /// - run_start: the flatbuffers `RunStart` message.
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn push_run_start(
        &mut self,
        run_start: RunStart<'_>,
    ) -> NexusWriterResult<&mut Run<D::FileInterface>> {
        //  If a run is already in progress, and is missing a run-stop
        //  then call an abort run on the current run.
        if self.run_cache.back().is_some_and(|run| !run.has_run_stop()) {
            self.abort_back_run(&run_start)?;
        }

        let run = Run::new_run(&self.nexus_settings, run_start, &self.nexus_configuration)?;
        self.run_cache.push_back(run);

        //  Ensure Topic Subscription Mode is set to Full)
        self.kafka_topic_interface
            .ensure_subscription_mode_is(TopicMode::Full)?;

        Ok(self.run_cache.back_mut().expect("Run exists"))
    }

    /// This pushes a Frame Event List message to the first valid run it finds in the run cache.
    /// If no run is found then this method does nothing.
    /// Should a warning be emitted?
    /// # Parameters
    /// - data: the `RunLog` message to push.
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn push_frame_event_list(
        &mut self,
        message: FrameAssembledEventListMessage<'_>,
    ) -> NexusWriterResult<()> {
        let timestamp: NexusDateTime =
            (*message
                .metadata()
                .timestamp()
                .ok_or(NexusWriterError::FlatBufferMissing(
                    FlatBufferMissingError::Timestamp,
                    ErrorCodeLocation::ProcessEventList,
                ))?)
            .try_into()?;

        if let Some(run) = self.run_cache.find_run_containing(&timestamp) {
            run.push_frame_event_list(&self.nexus_settings, message)?;
        }
        Ok(())
    }

    /// This pushes a Run Log message to the first valid run it finds in the run cache.
    /// If no run is found then this method does nothing.
    /// Should a warning be emitted?
    /// # Parameters
    /// - data: the `RunLog` message to push.
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn push_run_log(&mut self, data: &f144_LogData<'_>) -> NexusWriterResult<()> {
        let timestamp = NexusDateTime::from_timestamp_nanos(data.timestamp());
        if let Some(run) = self.run_cache.find_run_containing(&timestamp) {
            run.push_run_log(&self.nexus_settings, data)?;
        }
        Ok(())
    }

    /// This pushes a Sample Environment Log message to the first valid run it finds in the run cache.
    /// If no run is found then this method does nothing.
    /// Should a warning be emitted?
    /// # Parameters
    /// - data: the SampleEnvironmentLog message to push.
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn push_sample_environment_log(
        &mut self,
        data: SampleEnvironmentLog,
    ) -> NexusWriterResult<()> {
        let timestamp = NexusDateTime::from_timestamp_nanos(match data {
            SampleEnvironmentLog::LogData(f144_log_data) => f144_log_data.timestamp(),
            SampleEnvironmentLog::SampleEnvironmentData(se00_sample_environment_data) => {
                se00_sample_environment_data.packet_timestamp()
            }
        });
        if let Some(run) = self.run_cache.find_run_not_ending_before(&timestamp) {
            run.push_sample_environment_log(&self.nexus_settings, &data)?;
        }
        Ok(())
    }

    /// This pushes an Alarm message to the first valid run it finds in the run cache.
    /// If no run is found then this method does nothing.
    /// Should a warning be emitted?
    /// # Parameters
    /// - data: the Alarm message to push.
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn push_alarm(&mut self, data: Alarm<'_>) -> NexusWriterResult<()> {
        let timestamp = NexusDateTime::from_timestamp_nanos(data.timestamp());
        if let Some(run) = self.run_cache.find_run_not_ending_before(&timestamp) {
            run.push_alarm(&self.nexus_settings, &data)?;
        }
        Ok(())
    }

    /// This pushes a RunStop message to the final run in the cache.
    /// # Parameters
    /// - data: the RunStop message to push.
    /// # Return
    /// A reference to the run.
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn push_run_stop(
        &mut self,
        data: RunStop<'_>,
    ) -> NexusWriterResult<&Run<D::FileInterface>> {
        if let Some(last_run) = self.run_cache.back_mut() {
            last_run.set_stop_if_valid(&data)?;

            Ok(last_run)
        } else {
            Err(NexusWriterError::RunStopUnexpected(
                ErrorCodeLocation::StopCommand,
            ))
        }
    }

    /// This tells the last run in the run cache that it is being aborted.
    #[tracing::instrument(skip_all, level = "warn", err(level = "warn")
        fields(
            run_name = data.run_name(),
            instrument_name = data.instrument_name(),
            start_time = data.start_time(),
        )
    )]
    fn abort_back_run(&mut self, data: &RunStart<'_>) -> NexusWriterResult<()> {
        self.run_cache
            .back_mut()
            .expect("run_cache::back_mut should exist")
            .abort_run(&self.nexus_settings, data.start_time())?;
        Ok(())
    }

    /// This moves all completed runs into the completed directory and removes them from the run cache.
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn flush(&mut self, delay: &Duration) -> NexusWriterResult<()> {
        // Moves the runs into a new vector, then consumes it,
        // directing completed runs to self.run_move_cache
        // and incomplete ones back to self.run_cache
        let temp: Vec<_> = self.run_cache.drain(..).collect();
        for run in temp.into_iter() {
            if run.has_completed(delay) {
                if let Err(e) = run.end_span() {
                    warn!("Run span drop failed {e}")
                }
                run.move_to_completed(
                    self.nexus_settings.get_local_path(),
                    self.nexus_settings.get_local_completed_path(),
                )?;
                run.close()?;
            } else {
                self.run_cache.push_back(run);
            }
        }

        if self.run_cache.is_empty() {
            //  Ensure Topic Subscription Mode is set to Continuous Only.
            self.kafka_topic_interface
                .ensure_subscription_mode_is(TopicMode::ConitinousOnly)?;
        }

        Ok(())
    }

    pub(crate) fn close_all(self) -> NexusWriterResult<()> {
        for run in self.run_cache.into_iter() {
            run.close()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{NexusEngine, NexusEngineDependencies};
    use crate::{
        NexusSettings, kafka_topic_interface::NoKafka, nexus::NexusNoFile,
        run_engine::NexusConfiguration,
    };
    use chrono::{DateTime, Duration, Utc};
    use digital_muon_streaming_types::{
        aev2_frame_assembled_event_v2_generated::{
            FrameAssembledEventListMessage, FrameAssembledEventListMessageArgs,
            finish_frame_assembled_event_list_message_buffer,
            root_as_frame_assembled_event_list_message,
        },
        ecs_6s4t_run_stop_generated::{
            RunStop, RunStopArgs, finish_run_stop_buffer, root_as_run_stop,
        },
        ecs_pl72_run_start_generated::{
            RunStart, RunStartArgs, finish_run_start_buffer, root_as_run_start,
        },
        flatbuffers::{FlatBufferBuilder, InvalidFlatbuffer},
        frame_metadata_v2_generated::{FrameMetadataV2, FrameMetadataV2Args, GpsTime},
    };

    fn create_start<'a, 'b: 'a>(
        fbb: &'b mut FlatBufferBuilder,
        name: &str,
        start_time: u64,
    ) -> Result<RunStart<'a>, InvalidFlatbuffer> {
        let args = RunStartArgs {
            start_time,
            run_name: Some(fbb.create_string(name)),
            instrument_name: Some(fbb.create_string("Super MuSR")),
            filename: Some(fbb.create_string(name)),
            ..Default::default()
        };
        let message = RunStart::create(fbb, &args);
        finish_run_start_buffer(fbb, message);
        root_as_run_start(fbb.finished_data())
    }

    fn create_stop<'a, 'b: 'a>(
        fbb: &'b mut FlatBufferBuilder,
        name: &str,
        stop_time: u64,
    ) -> Result<RunStop<'a>, InvalidFlatbuffer> {
        let args = RunStopArgs {
            stop_time,
            run_name: Some(fbb.create_string(name)),
            ..Default::default()
        };
        let message = RunStop::create(fbb, &args);
        finish_run_stop_buffer(fbb, message);
        root_as_run_stop(fbb.finished_data())
    }

    fn create_metadata(timestamp: &GpsTime) -> FrameMetadataV2Args<'_> {
        FrameMetadataV2Args {
            timestamp: Some(timestamp),
            period_number: 0,
            protons_per_pulse: 0,
            running: false,
            frame_number: 0,
            veto_flags: 0,
        }
    }

    fn create_frame_assembled_message<'a, 'b: 'a>(
        fbb: &'b mut FlatBufferBuilder,
        timestamp: &GpsTime,
    ) -> Result<FrameAssembledEventListMessage<'a>, InvalidFlatbuffer> {
        let metadata = FrameMetadataV2::create(fbb, &create_metadata(timestamp));
        let args = FrameAssembledEventListMessageArgs {
            metadata: Some(metadata),
            ..Default::default()
        };
        let message = FrameAssembledEventListMessage::create(fbb, &args);
        finish_frame_assembled_event_list_message_buffer(fbb, message);
        root_as_frame_assembled_event_list_message(fbb.finished_data())
    }

    struct MockDependencies;
    impl NexusEngineDependencies for MockDependencies {
        type FileInterface = NexusNoFile;
        type TopicInterface = NoKafka;
    }

    #[test]
    fn empty_run() {
        let mut nexus = NexusEngine::<MockDependencies>::new(
            NexusSettings::default(),
            NexusConfiguration::new(None),
            NoKafka,
        );
        let mut fbb = FlatBufferBuilder::new();
        let start = create_start(&mut fbb, "Test1", 16).unwrap();
        nexus.push_run_start(start).unwrap();

        assert_eq!(nexus.run_cache.len(), 1);
        assert_eq!(
            nexus.run_cache.front().unwrap().parameters().collect_from,
            DateTime::<Utc>::from_timestamp_millis(16).unwrap()
        );
        assert!(
            nexus
                .run_cache
                .front()
                .unwrap()
                .parameters()
                .run_stop_parameters
                .is_none()
        );

        fbb.reset();
        let stop = create_stop(&mut fbb, "Test1", 17).unwrap();
        nexus.push_run_stop(stop).unwrap();

        assert_eq!(nexus.cache_iter().len(), 1);

        let run = nexus.cache_iter().next();

        assert!(run.is_some());
        assert!(run.unwrap().parameters().run_stop_parameters.is_some());
        assert_eq!(run.unwrap().get_name(), "Test1");

        assert!(run.unwrap().parameters().run_stop_parameters.is_some());
        assert_eq!(
            run.unwrap()
                .parameters()
                .run_stop_parameters
                .as_ref()
                .unwrap()
                .collect_until,
            DateTime::<Utc>::from_timestamp_millis(17).unwrap()
        );
    }

    #[test]
    fn no_run_start() {
        let mut nexus = NexusEngine::<MockDependencies>::new(
            NexusSettings::default(),
            NexusConfiguration::new(None),
            NoKafka,
        );
        let mut fbb = FlatBufferBuilder::new();

        let stop = create_stop(&mut fbb, "Test1", 0).unwrap();
        assert!(nexus.push_run_stop(stop).is_err());
    }

    #[test]
    fn no_run_stop() {
        let mut nexus = NexusEngine::<MockDependencies>::new(
            NexusSettings::default(),
            NexusConfiguration::new(None),
            NoKafka,
        );
        let mut fbb = FlatBufferBuilder::new();

        let start1 = create_start(&mut fbb, "Test1", 0).unwrap();
        nexus.push_run_start(start1).unwrap();
        assert_eq!(nexus.get_num_cached_runs(), 1);

        fbb.reset();
        let start2 = create_start(&mut fbb, "Test2", 0).unwrap();
        nexus.push_run_start(start2).unwrap();
        assert_eq!(nexus.get_num_cached_runs(), 2);
    }

    #[test]
    fn frame_messages_correct() {
        let mut nexus = NexusEngine::<MockDependencies>::new(
            NexusSettings::default(),
            NexusConfiguration::new(None),
            NoKafka,
        );
        let mut fbb = FlatBufferBuilder::new();

        let ts = GpsTime::new(0, 1, 0, 0, 16, 0, 0, 0);
        let ts_start: DateTime<Utc> = GpsTime::new(0, 1, 0, 0, 15, 0, 0, 0).try_into().unwrap();
        let ts_end: DateTime<Utc> = GpsTime::new(0, 1, 0, 0, 17, 0, 0, 0).try_into().unwrap();

        let start = create_start(&mut fbb, "Test1", ts_start.timestamp_millis() as u64).unwrap();
        nexus.push_run_start(start).unwrap();

        fbb.reset();
        let message = create_frame_assembled_message(&mut fbb, &ts).unwrap();
        nexus.push_frame_event_list(message).unwrap();

        let mut fbb = FlatBufferBuilder::new(); //  Need to create a new instance as we use m1 later
        let stop = create_stop(&mut fbb, "Test1", ts_end.timestamp_millis() as u64).unwrap();
        nexus.push_run_stop(stop).unwrap();

        assert_eq!(nexus.cache_iter().len(), 1);

        let run = nexus.cache_iter().next();

        let timestamp: DateTime<Utc> = (*message.metadata().timestamp().unwrap())
            .try_into()
            .unwrap();

        assert!(run.unwrap().is_message_timestamp_within_range(&timestamp));

        let _ = nexus.flush(&Duration::zero());
        assert_eq!(nexus.cache_iter().len(), 0);
    }

    #[test]
    fn two_runs_flushed() {
        let mut nexus = NexusEngine::<MockDependencies>::new(
            NexusSettings::default(),
            NexusConfiguration::new(None),
            NoKafka,
        );
        let mut fbb = FlatBufferBuilder::new();

        let ts_start: DateTime<Utc> = GpsTime::new(0, 1, 0, 0, 15, 0, 0, 0).try_into().unwrap();
        let ts_end: DateTime<Utc> = GpsTime::new(0, 1, 0, 0, 17, 0, 0, 0).try_into().unwrap();

        let start = create_start(&mut fbb, "TestRun1", ts_start.timestamp_millis() as u64).unwrap();
        nexus.push_run_start(start).unwrap();

        fbb.reset();
        let stop = create_stop(&mut fbb, "TestRun1", ts_end.timestamp_millis() as u64).unwrap();
        nexus.push_run_stop(stop).unwrap();

        assert_eq!(nexus.cache_iter().len(), 1);

        fbb.reset();
        let start = create_start(&mut fbb, "TestRun2", ts_start.timestamp_millis() as u64).unwrap();
        nexus.push_run_start(start).unwrap();

        fbb.reset();
        let stop = create_stop(&mut fbb, "TestRun2", ts_end.timestamp_millis() as u64).unwrap();
        nexus.push_run_stop(stop).unwrap();

        assert_eq!(nexus.cache_iter().len(), 2);

        let _ = nexus.flush(&Duration::zero());
        assert_eq!(nexus.cache_iter().len(), 0);
    }
}
