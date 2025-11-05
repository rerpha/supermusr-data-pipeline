//! Defines [Entry] group structure which contains all data pertaining to the run.
mod event_data;
mod instrument;
mod period;
mod runlog;
mod sample;
mod selog;

use super::{NexusGroup, NexusMessageHandler, NexusSchematic};
use crate::{
    hdf5_handlers::{DatasetExt, GroupExt, HasAttributesExt, NexusHDF5Result},
    nexus::{DATETIME_FORMAT, DatasetUnitExt, NexusClass, NexusUnits},
    run_engine::{
        ChunkSizeSettings, RunParameters, RunStopParameters,
        run_messages::{
            InitialiseNewNexusRun, InitialiseNewNexusStructure, PushAlarm, PushFrameEventList,
            PushInternallyGeneratedLogWarning, PushRunLog, PushRunStart, PushSampleEnvironmentLog,
            SetEndTime, UpdatePeriodList,
        },
    },
};
use chrono::Utc;
use event_data::EventData;
use hdf5::{Dataset, Group};
use instrument::Instrument;
use period::Period;
use runlog::RunLog;
use sample::Sample;
use selog::SELog;
use tracing::warn;

/// Names of datasets/attribute and subgroups in the Entry struct
mod labels {
    pub(super) const IDF_VERSION: &str = "IDF_version";
    pub(super) const DEFINITION: &str = "definition";
    pub(super) const PROGRAM_NAME: &str = "program_name";
    pub(super) const PROGRAM_NAME_VERSION: &str = "version";
    pub(super) const PROGRAM_NAME_CONFIGURATION: &str = "configuration";
    pub(super) const RUN_NUMBER: &str = "run_number";
    pub(super) const PROTON_CHARGE: &str = "proton_charge";
    pub(super) const DURATION: &str = "duration";
    pub(super) const EXPERIMENT_IDENTIFIER: &str = "experiment_identifier";
    pub(super) const START_TIME: &str = "start_time";
    pub(super) const END_TIME: &str = "end_time";
    pub(super) const NAME: &str = "name";
    pub(super) const TITLE: &str = "title";
    pub(super) const INSTRUMENT: &str = "instrument";
    pub(super) const RUNLOGS: &str = "runlog";
    pub(super) const PERIODS: &str = "periods";
    pub(super) const SELOGS: &str = "selog";
    pub(super) const SAMPLE: &str = "sample";
    pub(super) const DETECTOR_1: &str = "detector_1_events";
}

// Values of Nexus Constant
/// The instrument definition file version, currently "2".
const IDF_VERSION: u32 = 2;
/// The template (DTD name) on which this entry is based, ‘muonTD’ (muon, time differential).
const DEFINITION: &str = "muonTD";
/// This may be changed to something more widely agreed upon. Possibly statically scraped from elsewhere.
const PROGRAM_NAME: &str = "SuperMuSR Data Pipeline Nexus Writer";
/// This should probably be taken statically from "cargo.toml" or something.
const PROGRAM_NAME_VERSION: &str = "1.0";

/// Handles all actual data.
pub(crate) struct Entry {
    /// Instrument Definition File number.
    _idf_version: Dataset,
    /// The template (DTD name) on which the entry was based, e.g. ‘muonTD’ (muon, time differential). It’s suggested that muon definitions always use the prefix ‘muon’, with a subsequent sequence of capitals defining the unique function of the definition.
    _definition: Dataset,
    /// The name of the creating program (i.e. the data pipeline).
    program_name: Dataset,
    /// Run number. Currently don't know where this data comes from.
    run_number: Dataset,
    /// Indicates whether we using protons or anti-protons. Currently don't know where this data comes from.
    _proton_charge: Dataset,
    /// Duration of measurement i.e. (endstart)
    _duration: Dataset,
    /// Experiment number, for ISIS, the RB number . Currently don't know where this data comes from.
    experiment_identifier: Dataset,
    /// Start time and date of measurement
    start_time: Dataset,
    /// End time and date of measurement
    end_time: Dataset,
    /// Don't know what this is, or whether it is actually supposed to be here as it doesn't appear in the spec.
    name: Dataset,
    ///extended title for the entry, e.g. string containing sample, temperature and field.
    title: Dataset,

    /// Container for log(s) of run parameters, inevitably specific to each facility.
    run_logs: NexusGroup<RunLog>,

    /// Details of the instrument used.
    instrument: NexusGroup<Instrument>,
    /// Log of period parameters, inevitably specific to each facility.
    periods: NexusGroup<Period>,
    /// Details of the sample under investigation.
    _sample: NexusGroup<Sample>,

    /// Container for log(s) of sample environment parameters, that may be specific to each experiment.
    selogs: NexusGroup<SELog>,

    /// The data collected.
    detector_1: NexusGroup<EventData>,
}

impl Entry {
    /// Extracts [RunParameters] data from an existing NeXus file.
    pub(super) fn extract_run_parameters(&self) -> NexusHDF5Result<RunParameters> {
        let collect_from = self.start_time.get_datetime()?;
        let run_name = self.name.get_string()?;
        let run_stop_parameters = self
            .end_time
            .get_datetime()
            .map(|collect_until| RunStopParameters {
                collect_until,
                last_modified: Utc::now(),
            })
            .ok();
        let filename = run_name.clone();
        Ok(RunParameters {
            collect_from,
            run_stop_parameters,
            run_name,
            periods: self.periods.extract(Period::extract_periods)?,
            file_name: filename,
        })
    }
}

impl NexusSchematic for Entry {
    const CLASS: NexusClass = NexusClass::Entry;
    type Settings = ChunkSizeSettings;

    fn build_group_structure(group: &Group, settings: &ChunkSizeSettings) -> NexusHDF5Result<Self> {
        Ok(Self {
            _idf_version: group
                .create_constant_scalar_dataset::<u32>(labels::IDF_VERSION, &IDF_VERSION)?,
            _definition: group.create_constant_string_dataset(labels::DEFINITION, DEFINITION)?,
            program_name: group
                .create_constant_string_dataset(labels::PROGRAM_NAME, PROGRAM_NAME)?
                .with_constant_string_attribute(
                    labels::PROGRAM_NAME_VERSION,
                    PROGRAM_NAME_VERSION,
                )?,
            run_number: group.create_scalar_dataset::<u32>(labels::RUN_NUMBER)?,
            _proton_charge: group
                .create_scalar_dataset::<f32>(labels::PROTON_CHARGE)?
                .with_units(NexusUnits::MicroAmpHours)?,
            _duration: group
                .create_scalar_dataset::<u32>(labels::DURATION)?
                .with_units(NexusUnits::Seconds)?,
            experiment_identifier: group.create_string_dataset(labels::EXPERIMENT_IDENTIFIER)?,
            start_time: group.create_string_dataset(labels::START_TIME)?,
            end_time: group.create_string_dataset(labels::END_TIME)?,
            name: group.create_constant_string_dataset(labels::NAME, "")?,
            title: group.create_constant_string_dataset(labels::TITLE, "")?,
            instrument: Instrument::build_new_group(group, labels::INSTRUMENT, &())?,
            run_logs: RunLog::build_new_group(group, labels::RUNLOGS, &())?,
            periods: Period::build_new_group(group, labels::PERIODS, &settings.period)?,
            selogs: SELog::build_new_group(group, labels::SELOGS, &())?,
            _sample: Sample::build_new_group(group, labels::SAMPLE, settings)?,
            detector_1: EventData::build_new_group(
                group,
                labels::DETECTOR_1,
                &(settings.event, settings.frame),
            )?,
        })
    }

    fn populate_group_structure(group: &Group) -> NexusHDF5Result<Self> {
        let _idf_version = group.get_dataset(labels::IDF_VERSION)?;
        let _definition = group.get_dataset(labels::DEFINITION)?;
        let run_number = group.get_dataset(labels::RUN_NUMBER)?;
        let program_name = group.get_dataset(labels::PROGRAM_NAME)?;
        let _proton_charge = group.get_dataset(labels::PROTON_CHARGE)?;
        let _duration = group.get_dataset(labels::DURATION)?;
        let experiment_identifier = group.get_dataset(labels::EXPERIMENT_IDENTIFIER)?;

        let start_time = group.get_dataset(labels::START_TIME)?;
        let end_time = group.get_dataset(labels::END_TIME)?;

        let name = group.get_dataset(labels::NAME)?;
        let title = group.get_dataset(labels::TITLE)?;

        let instrument = Instrument::open_group(group, labels::INSTRUMENT)?;
        let periods = Period::open_group(group, labels::PERIODS)?;
        let _sample = Sample::open_group(group, labels::SAMPLE)?;

        let run_logs = RunLog::open_group(group, labels::RUNLOGS)?;
        let selogs = SELog::open_group(group, labels::SELOGS)?;

        let detector_1 = EventData::open_group(group, labels::DETECTOR_1)?;

        Ok(Self {
            _idf_version,
            start_time,
            end_time,
            name,
            title,
            selogs,
            _definition,
            run_number,
            program_name,
            _duration,
            _proton_charge,
            experiment_identifier,
            run_logs,
            _sample,
            instrument,
            periods,
            detector_1,
        })
    }
}

/// Helper function to extract the run number from the run name.
///
/// Works by filtering out all non-digit characters and parsing the remaining string.
/// # Parameters
/// - run_name: string slice presumed to be alpha-numeric.
/// # Return
/// The number parsed from the remaining string.
/// # Warnings
/// - If no digit characters are present, the number returned defaults to zero.
///
/// This should probably be changed to force the caller to handle this case.
fn extract_run_number(run_name: &str) -> NexusHDF5Result<u32> {
    // Get Run Number by filtering out any non-integer ascii characters
    let string = run_name
        .chars()
        .filter(char::is_ascii_digit)
        .collect::<String>();

    // If there were no integer characters then return 0
    if string.is_empty() {
        warn!(
            "'Run Number' cannot be determined, defaulting to {}",
            u32::default()
        );
        Ok(u32::default())
    } else {
        Ok(string.parse::<u32>()?)
    }
}

/// Initialise nexus file.
impl NexusMessageHandler<InitialiseNewNexusStructure<'_>> for Entry {
    fn handle_message(&mut self, message: &InitialiseNewNexusStructure<'_>) -> NexusHDF5Result<()> {
        let InitialiseNewNexusStructure {
            parameters,
            configuration,
        } = message;

        self.run_number
            .set_scalar(&extract_run_number(&parameters.run_name)?)?;

        self.experiment_identifier.set_string("")?;

        self.program_name.add_constant_string_attribute(
            labels::PROGRAM_NAME_CONFIGURATION,
            &configuration.configuration,
        )?;

        let start_time = parameters.collect_from.format(DATETIME_FORMAT).to_string();

        self.start_time.set_string(&start_time)?;
        self.end_time.set_string("")?;

        self.name.set_string(&parameters.run_name)?;
        self.title.set_string("")?;

        self.detector_1
            .handle_message(&InitialiseNewNexusRun { parameters })?;
        Ok(())
    }
}

/// Direct `PushRunStart` to the group(s) that need it
impl NexusMessageHandler<PushRunStart<'_>> for Entry {
    fn handle_message(&mut self, message: &PushRunStart<'_>) -> NexusHDF5Result<()> {
        self.instrument.handle_message(message)
    }
}

/// Direct `PushFrameEventList` to the group(s) that need it
impl NexusMessageHandler<PushFrameEventList<'_>> for Entry {
    fn handle_message(&mut self, message: &PushFrameEventList<'_>) -> NexusHDF5Result<()> {
        self.detector_1.handle_message(message)
    }
}

// Direct `UpdatePeriodList` to the group(s) that need it
impl NexusMessageHandler<UpdatePeriodList<'_>> for Entry {
    fn handle_message(&mut self, message: &UpdatePeriodList<'_>) -> NexusHDF5Result<()> {
        self.periods.handle_message(message)
    }
}

// Direct `PushRunLog` to the group(s) that need it
impl NexusMessageHandler<PushRunLog<'_>> for Entry {
    fn handle_message(&mut self, message: &PushRunLog<'_>) -> NexusHDF5Result<()> {
        self.run_logs.handle_message(message)
    }
}

// Direct `PushSampleEnvironmentLog` to the group(s) that need it
impl NexusMessageHandler<PushSampleEnvironmentLog<'_>> for Entry {
    fn handle_message(&mut self, message: &PushSampleEnvironmentLog<'_>) -> NexusHDF5Result<()> {
        self.selogs.handle_message(message)
    }
}

// Direct `PushAlarm` to the group(s) that need it
impl NexusMessageHandler<PushAlarm<'_>> for Entry {
    fn handle_message(&mut self, message: &PushAlarm<'_>) -> NexusHDF5Result<()> {
        self.selogs.handle_message(message)
    }
}

// Direct `PushInternallyGeneratedLogWarning` to the group(s) that need it
impl NexusMessageHandler<PushInternallyGeneratedLogWarning<'_>> for Entry {
    fn handle_message(
        &mut self,
        message: &PushInternallyGeneratedLogWarning<'_>,
    ) -> NexusHDF5Result<()> {
        self.run_logs.handle_message(message)
    }
}

// Set `end_time` field
impl NexusMessageHandler<SetEndTime<'_>> for Entry {
    fn handle_message(&mut self, message: &SetEndTime<'_>) -> NexusHDF5Result<()> {
        let end_time = message.end_time.format(DATETIME_FORMAT).to_string();

        self.end_time.set_string(&end_time)
    }
}
