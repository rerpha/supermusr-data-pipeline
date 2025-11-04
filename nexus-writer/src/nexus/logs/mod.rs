//! Defines traits which, when implemented for appropriate flatbuffer messages,
//! allow the messages to write their data into a given [Dataset].
mod alarm;
mod f114;
mod se00;

use crate::{hdf5_handlers::NexusHDF5Result, run_engine::NexusDateTime};
use hdf5::{Dataset, types::TypeDescriptor};
/// Is implemented on [f144_LogData] and [se00_SampleEnvironmentData].
///
/// [f144_LogData]: digital_muon_streaming_types::ecs_f144_logdata_generated::f144_LogData
/// [se00_SampleEnvironmentData]: digital_muon_streaming_types::ecs_se00_data_generated::se00_SampleEnvironmentData
pub(crate) trait LogMessage<'a>: Sized {
    /// Returns name of the log message.
    fn get_name(&self) -> String;

    /// Returns data type of the log message.
    fn get_type_descriptor(&self) -> NexusHDF5Result<TypeDescriptor>;

    /// Append given dataset with the log message time values.
    /// # Parameters
    /// - dataset: [Dataset] to write data to.
    /// - origin_time: the time by which the timestamps should be written relative to. Usually the start time of the run.
    /// # Error
    /// Emits an error if either of the following requirements on the given [Dataset] are violated:
    /// - has data type equal to [get_type_descriptor].
    /// - is one-dimentional.
    ///
    /// [get_type_descriptor]: LogMessage::get_type_descriptor
    fn append_timestamps_to(
        &self,
        dataset: &Dataset,
        origin_time: &NexusDateTime,
    ) -> NexusHDF5Result<()>;

    /// Appends given dataset with the log message data values.
    /// # Parameters
    /// - dataset: [Dataset] to write data to.
    /// # Error
    /// Emits an error if either of the following requirements on the given [Dataset] are violated:
    /// - has data type equal to [get_type_descriptor].
    /// - is one-dimentional.
    ///
    /// [get_type_descriptor]: LogMessage::get_type_descriptor
    fn append_values_to(&self, dataset: &Dataset) -> NexusHDF5Result<()>;
}

/// Is implemented on [Alarm].
///
/// [Alarm]: digital_muon_streaming_types::ecs_al00_alarm_generated::Alarm
pub(crate) trait AlarmMessage<'a>: Sized {
    fn get_name(&self) -> NexusHDF5Result<String>;

    /// Append given dataset with the alarm message time values.
    /// # Parameters
    /// - dataset: [Dataset] to write data to.
    /// - origin_time: the time by which the timestamps should be written relative to. Usually the start time of the run.
    /// # Error
    /// Emits an error if either of the following requirements on the given [Dataset] are violated:
    /// - has data type equal to [f64].
    /// - is one-dimentional.
    fn append_timestamp_to(
        &self,
        dataset: &Dataset,
        origin_time: &NexusDateTime,
    ) -> NexusHDF5Result<()>;

    /// Appends given dataset with the alarm message severity value.
    /// # Parameters
    /// - dataset: [Dataset] to write data to.
    /// # Error
    /// Emits an error if either of the following requirements on the given [Dataset] are violated:
    /// - has data type equal to [VarLenUnicode].
    /// - is one-dimentional.
    ///
    /// [VarLenUnicode]: hdf5::types::VarLenUnicode
    fn append_severity_to(&self, dataset: &Dataset) -> NexusHDF5Result<()>;

    /// Appends given dataset with the alarm message status.
    /// # Parameters
    /// - dataset: [Dataset] to write data to.
    /// # Error
    /// Emits an error if either of the following requirements on the given [Dataset] are violated:
    /// - has data type equal to [VarLenUnicode].
    /// - is one-dimentional.
    ///
    /// [VarLenUnicode]: hdf5::types::VarLenUnicode
    fn append_message_to(&self, dataset: &Dataset) -> NexusHDF5Result<()>;
}

/// Coverts ns since epoch to ns since `origin_time`.
/// # Parameters
/// - nanoseconds: time since epoch to adjust.
/// - origin_time: timestamp to set the time relative to.
/// # Return
/// Time relative to `origin_time` in seconds.
fn adjust_nanoseconds_by_origin_to_sec(nanoseconds: i64, origin_time: &NexusDateTime) -> f64 {
    (origin_time
        .timestamp_nanos_opt()
        .map(|origin_time_ns| nanoseconds - origin_time_ns)
        .unwrap_or_default() as f64)
        / 1_000_000_000.0
}

/// Removes prefixes from block names or returns the whole PV address if not a block.
/// # Parameters
/// - text: a string slice of the form: "prefix_1:prefix_2:...:prefix_n:LOG_NAME".
/// # Return
/// A string containing "LOG_NAME".
fn remove_prefixes(text: &str) -> String {
    text.rsplit(":CS:SB:").next().unwrap_or(text).to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remove_block_prefix() {
        assert_eq!(&remove_prefixes("IN:POLREF:CS:SB:someBlock"), "someBlock")
    }

    #[test]
    fn test_remove_prefixes_with_no_block_prefix() {
        assert_eq!(
            &remove_prefixes("IN:IMAT:SOME:ARCHIVED:PV"),
            "IN:IMAT:SOME:ARCHIVED:PV"
        )
    }
}
