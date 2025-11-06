//! Implementation allows flatbuffer [se00_SampleEnvironmentData] messages to robustly write data to a [Dataset].
use super::{LogMessage, adjust_nanoseconds_by_origin_to_sec, remove_prefixes};
use crate::{
    error::FlatBufferInvalidDataTypeContext,
    hdf5_handlers::{
        ConvertResult, DatasetExt, DatasetFlatbuffersExt, NexusHDF5Error, NexusHDF5Result,
    },
    run_engine::{NexusDateTime, run_messages::SampleEnvironmentLog},
};
use digital_muon_streaming_types::ecs_se00_data_generated::{
    ValueUnion, se00_SampleEnvironmentData,
};
use hdf5::{
    Dataset,
    types::{FloatSize, IntSize, TypeDescriptor},
};
use tracing::{trace, warn};

fn get_se00_len(data: &se00_SampleEnvironmentData<'_>) -> NexusHDF5Result<usize> {
    let type_descriptor = data.get_type_descriptor()?;
    let error = || NexusHDF5Error::invalid_hdf5_type_conversion(type_descriptor.clone());
    match type_descriptor {
        TypeDescriptor::Integer(int_size) => match int_size {
            IntSize::U1 => data.values_as_int_8_array().map(|x| x.value().len()),
            IntSize::U2 => data.values_as_int_16_array().map(|x| x.value().len()),
            IntSize::U4 => data.values_as_int_32_array().map(|x| x.value().len()),
            IntSize::U8 => data.values_as_int_64_array().map(|x| x.value().len()),
        },
        TypeDescriptor::Unsigned(int_size) => match int_size {
            IntSize::U1 => data.values_as_uint_8_array().map(|x| x.value().len()),
            IntSize::U2 => data.values_as_uint_16_array().map(|x| x.value().len()),
            IntSize::U4 => data.values_as_uint_32_array().map(|x| x.value().len()),
            IntSize::U8 => data.values_as_uint_64_array().map(|x| x.value().len()),
        },
        TypeDescriptor::Float(float_size) => match float_size {
            FloatSize::U4 => data.values_as_float_array().map(|x| x.value().len()),
            FloatSize::U8 => data.values_as_double_array().map(|x| x.value().len()),
        },
        _ => unreachable!("Unreachable HDF5 TypeDescriptor reached, this should never happen"),
    }
    .ok_or_else(error)
}

impl<'a> LogMessage<'a> for se00_SampleEnvironmentData<'a> {
    fn get_name(&self) -> String {
        remove_prefixes(self.name())
    }

    fn get_type_descriptor(&self) -> Result<TypeDescriptor, NexusHDF5Error> {
        let error = |t: ValueUnion| {
            NexusHDF5Error::flatbuffer_invalid_data_type(
                FlatBufferInvalidDataTypeContext::SELog,
                t.variant_name().map(ToOwned::to_owned).unwrap_or_default(),
            )
        };
        let datatype = match self.values_type() {
            ValueUnion::Int8Array => TypeDescriptor::Integer(IntSize::U1),
            ValueUnion::UInt8Array => TypeDescriptor::Unsigned(IntSize::U1),
            ValueUnion::Int16Array => TypeDescriptor::Integer(IntSize::U2),
            ValueUnion::UInt16Array => TypeDescriptor::Unsigned(IntSize::U2),
            ValueUnion::Int32Array => TypeDescriptor::Integer(IntSize::U4),
            ValueUnion::UInt32Array => TypeDescriptor::Unsigned(IntSize::U4),
            ValueUnion::Int64Array => TypeDescriptor::Integer(IntSize::U8),
            ValueUnion::UInt64Array => TypeDescriptor::Unsigned(IntSize::U8),
            ValueUnion::FloatArray => TypeDescriptor::Float(FloatSize::U4),
            ValueUnion::DoubleArray => TypeDescriptor::Float(FloatSize::U8),
            value_union => return Err(error(value_union)),
        };
        Ok(datatype)
    }

    fn append_timestamps_to(
        &self,
        dataset: &Dataset,
        origin_time: &NexusDateTime,
    ) -> NexusHDF5Result<()> {
        let num_values = get_se00_len(self).err_dataset(dataset)?;
        if let Some(timestamps) = self.timestamps() {
            let timestamps = timestamps
                .iter()
                .map(|t| adjust_nanoseconds_by_origin_to_sec(t, origin_time))
                .collect::<Vec<_>>();

            if timestamps.len() != num_values {
                return Err(NexusHDF5Error::FlatBufferInconsistentSELogTimeValueSizes {
                    sizes: (timestamps.len(), num_values),
                    hdf5_path: None,
                })
                .err_dataset(dataset);
            }
            dataset.append_slice(timestamps.as_slice())
        } else if self.time_delta() > 0.0 {
            trace!("Calculate times automatically.");

            let timestamps = (0..num_values)
                .map(|v| (v as f64 * self.time_delta()) as i64)
                .map(|t| {
                    adjust_nanoseconds_by_origin_to_sec(t + self.packet_timestamp(), origin_time)
                })
                .collect::<Vec<_>>();

            dataset.append_slice(timestamps.as_slice())
        } else {
            warn!("No time data.");
            Ok(())
        }
        .err_dataset(dataset)
    }

    fn append_values_to(&self, dataset: &Dataset) -> NexusHDF5Result<()> {
        dataset.append_se00_value_slice(self).err_dataset(dataset)
    }
}

impl<'a> LogMessage<'a> for SampleEnvironmentLog<'a> {
    fn get_name(&self) -> String {
        match self {
            SampleEnvironmentLog::LogData(data) => data.get_name(),
            SampleEnvironmentLog::SampleEnvironmentData(data) => data.get_name(),
        }
    }

    fn get_type_descriptor(&self) -> Result<TypeDescriptor, NexusHDF5Error> {
        match self {
            SampleEnvironmentLog::LogData(data) => data.get_type_descriptor(),
            SampleEnvironmentLog::SampleEnvironmentData(data) => data.get_type_descriptor(),
        }
    }

    fn append_timestamps_to(
        &self,
        dataset: &Dataset,
        origin_time: &NexusDateTime,
    ) -> NexusHDF5Result<()> {
        match self {
            SampleEnvironmentLog::LogData(data) => data.append_timestamps_to(dataset, origin_time),
            SampleEnvironmentLog::SampleEnvironmentData(data) => {
                data.append_timestamps_to(dataset, origin_time)
            }
        }
    }

    fn append_values_to(&self, dataset: &Dataset) -> NexusHDF5Result<()> {
        match self {
            SampleEnvironmentLog::LogData(data) => data.append_values_to(dataset),
            SampleEnvironmentLog::SampleEnvironmentData(data) => data.append_values_to(dataset),
        }
    }
}
