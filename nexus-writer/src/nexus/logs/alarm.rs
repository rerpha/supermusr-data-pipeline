//! Implementation allows flatbuffer [Alarm] messages to robustly write data to a [Dataset].
use super::{AlarmMessage, adjust_nanoseconds_by_origin_to_sec, remove_prefixes};
use crate::{
    error::FlatBufferMissingError,
    hdf5_handlers::{ConvertResult, DatasetExt, NexusHDF5Error, NexusHDF5Result},
    run_engine::NexusDateTime,
};
use digital_muon_streaming_types::ecs_al00_alarm_generated::Alarm;
use hdf5::{Dataset, types::VarLenUnicode};

impl<'a> AlarmMessage<'a> for Alarm<'a> {
    fn get_name(&self) -> NexusHDF5Result<String> {
        let name = self
            .source_name()
            .ok_or_else(|| NexusHDF5Error::FlatBufferMissing {
                error: FlatBufferMissingError::AlarmName,
                hdf5_path: None,
            })?;
        Ok(remove_prefixes(name))
    }

    fn append_timestamp_to(
        &self,
        dataset: &Dataset,
        origin_time: &NexusDateTime,
    ) -> NexusHDF5Result<()> {
        dataset
            .append_value(adjust_nanoseconds_by_origin_to_sec(
                self.timestamp(),
                origin_time,
            ))
            .err_dataset(dataset)
    }

    fn append_severity_to(&self, dataset: &Dataset) -> NexusHDF5Result<()> {
        let severity = self
            .severity()
            .variant_name()
            .ok_or(FlatBufferMissingError::AlarmSeverity)
            .err_dataset(dataset)?;
        let severity = severity.parse::<VarLenUnicode>().err_dataset(dataset)?;
        dataset.append_value(severity).err_dataset(dataset)
    }

    fn append_message_to(&self, dataset: &Dataset) -> NexusHDF5Result<()> {
        let severity = self
            .message()
            .ok_or(FlatBufferMissingError::AlarmMessage)
            .err_dataset(dataset)?;
        let severity = severity.parse::<VarLenUnicode>().err_dataset(dataset)?;
        dataset.append_value(severity).err_dataset(dataset)
    }
}
