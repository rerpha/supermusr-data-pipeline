//! Implementation allows flatbuffer [f144_LogData] messages to robustly write data to a [Dataset].
use super::{LogMessage, adjust_nanoseconds_by_origin_to_sec, remove_prefixes};
use crate::{
    error::FlatBufferInvalidDataTypeContext,
    hdf5_handlers::{
        ConvertResult, DatasetExt, DatasetFlatbuffersExt, NexusHDF5Error, NexusHDF5Result,
    },
    run_engine::NexusDateTime,
};
use digital_muon_streaming_types::ecs_f144_logdata_generated::{Value, f144_LogData};
use hdf5::{
    Dataset,
    types::{FloatSize, IntSize, TypeDescriptor},
};

fn var_len_array_type_descr(type_descriptor: TypeDescriptor) -> TypeDescriptor {
    TypeDescriptor::VarLenArray(Box::new(type_descriptor))
}

impl<'a> LogMessage<'a> for f144_LogData<'a> {
    fn get_name(&self) -> String {
        remove_prefixes(self.source_name())
    }

    fn get_type_descriptor(&self) -> NexusHDF5Result<TypeDescriptor> {
        let error = |value: Value| {
            NexusHDF5Error::flatbuffer_invalid_data_type(
                FlatBufferInvalidDataTypeContext::RunLog,
                value
                    .variant_name()
                    .map(ToOwned::to_owned)
                    .unwrap_or_default(),
            )
        };
        let datatype = match self.value_type() {
            Value::Byte => TypeDescriptor::Integer(IntSize::U1),
            Value::UByte => TypeDescriptor::Unsigned(IntSize::U1),
            Value::Short => TypeDescriptor::Integer(IntSize::U2),
            Value::UShort => TypeDescriptor::Unsigned(IntSize::U2),
            Value::Int => TypeDescriptor::Integer(IntSize::U4),
            Value::UInt => TypeDescriptor::Unsigned(IntSize::U4),
            Value::Long => TypeDescriptor::Integer(IntSize::U8),
            Value::ULong => TypeDescriptor::Unsigned(IntSize::U8),
            Value::Float => TypeDescriptor::Float(FloatSize::U4),
            Value::Double => TypeDescriptor::Float(FloatSize::U8),
            Value::ArrayByte => var_len_array_type_descr(TypeDescriptor::Integer(IntSize::U1)),
            Value::ArrayUByte => var_len_array_type_descr(TypeDescriptor::Unsigned(IntSize::U1)),
            Value::ArrayShort => var_len_array_type_descr(TypeDescriptor::Integer(IntSize::U2)),
            Value::ArrayUShort => var_len_array_type_descr(TypeDescriptor::Unsigned(IntSize::U2)),
            Value::ArrayInt => var_len_array_type_descr(TypeDescriptor::Integer(IntSize::U4)),
            Value::ArrayUInt => var_len_array_type_descr(TypeDescriptor::Unsigned(IntSize::U4)),
            Value::ArrayLong => var_len_array_type_descr(TypeDescriptor::Integer(IntSize::U8)),
            Value::ArrayULong => var_len_array_type_descr(TypeDescriptor::Unsigned(IntSize::U8)),
            Value::ArrayFloat => var_len_array_type_descr(TypeDescriptor::Float(FloatSize::U4)),
            Value::ArrayDouble => var_len_array_type_descr(TypeDescriptor::Float(FloatSize::U8)),
            value => return Err(error(value)),
        };
        Ok(datatype)
    }

    #[tracing::instrument(skip_all, level = "debug", err(level = "warn"))]
    fn append_timestamps_to(
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

    #[tracing::instrument(skip_all, level = "debug", err(level = "warn"))]
    fn append_values_to(&self, dataset: &Dataset) -> NexusHDF5Result<()> {
        dataset.append_f144_value(self).err_dataset(dataset)
    }
}
