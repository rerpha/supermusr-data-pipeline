//! This module implements the traits to extend the hdf5 [Dataset] type to provide robust, conventient methods.
//!
//! This trait assists writing of flatbuffer log messages into a [Dataset].
use super::{DatasetExt, DatasetFlatbuffersExt, NexusHDF5Error, NexusHDF5Result};
use crate::nexus::LogMessage;
use digital_muon_streaming_types::{
    ecs_f144_logdata_generated::f144_LogData, ecs_se00_data_generated::se00_SampleEnvironmentData,
};
use hdf5::{
    Dataset,
    types::{FloatSize, IntSize, TypeDescriptor, VarLenArray},
};

/// Extracts a value of type [Self] from a [f144_LogData] reference, returning the given error if conversion fails.
trait F144HDF5Value: Sized + Copy {
    fn f144_value_or_else(
        data: &f144_LogData<'_>,
        error: impl Fn() -> NexusHDF5Error,
    ) -> Result<Self, NexusHDF5Error>;

    fn f144_array_value_or_else(
        data: &f144_LogData<'_>,
        error: impl Fn() -> NexusHDF5Error,
    ) -> Result<VarLenArray<Self>, NexusHDF5Error>;
}

/// Extracts a value of type [Self] from a [se00_SampleEnvironmentData] reference, returning the given error if conversion fails.
trait Se00HDF5Value: Sized {
    fn se00_value_or_else(
        data: &se00_SampleEnvironmentData<'_>,
        error: impl Fn() -> NexusHDF5Error,
    ) -> Result<Vec<Self>, NexusHDF5Error>;
}

macro_rules! impl_hdf5_value_traits {
    ($T:ident, $f114_scalar_method:ident, $f114_array_method:ident, $se00_array_method:ident) => {
        impl F144HDF5Value for $T {
            fn f144_value_or_else(
                data: &f144_LogData<'_>,
                error: impl Fn() -> NexusHDF5Error,
            ) -> Result<Self, NexusHDF5Error> {
                Ok(data.$f114_scalar_method().ok_or_else(error)?.value())
            }

            fn f144_array_value_or_else(
                data: &f144_LogData<'_>,
                error: impl Fn() -> NexusHDF5Error,
            ) -> Result<VarLenArray<Self>, NexusHDF5Error> {
                Ok(VarLenArray::from_slice(
                    data.$f114_array_method()
                        .and_then(|val| val.value())
                        .ok_or_else(error)?
                        .into_iter()
                        .collect::<Vec<_>>()
                        .as_slice(),
                ))
            }
        }

        impl Se00HDF5Value for $T {
            fn se00_value_or_else(
                data: &se00_SampleEnvironmentData<'_>,
                error: impl Fn() -> NexusHDF5Error,
            ) -> Result<Vec<Self>, NexusHDF5Error> {
                Ok(data
                    .$se00_array_method()
                    .map(|val| val.value())
                    .ok_or_else(error)?
                    .into_iter()
                    .collect::<Vec<_>>())
            }
        }
    };
}

impl_hdf5_value_traits!(
    i8,
    value_as_byte,
    value_as_array_byte,
    values_as_int_8_array
);
impl_hdf5_value_traits!(
    i16,
    value_as_short,
    value_as_array_short,
    values_as_int_16_array
);
impl_hdf5_value_traits!(
    i32,
    value_as_int,
    value_as_array_int,
    values_as_int_32_array
);
impl_hdf5_value_traits!(
    i64,
    value_as_long,
    value_as_array_long,
    values_as_int_64_array
);
impl_hdf5_value_traits!(
    u8,
    value_as_ubyte,
    value_as_array_ubyte,
    values_as_uint_8_array
);
impl_hdf5_value_traits!(
    u16,
    value_as_ushort,
    value_as_array_ushort,
    values_as_uint_16_array
);
impl_hdf5_value_traits!(
    u32,
    value_as_uint,
    value_as_array_uint,
    values_as_uint_32_array
);
impl_hdf5_value_traits!(
    u64,
    value_as_ulong,
    value_as_array_ulong,
    values_as_uint_64_array
);
impl_hdf5_value_traits!(
    f32,
    value_as_float,
    value_as_array_float,
    values_as_float_array
);
impl_hdf5_value_traits!(
    f64,
    value_as_double,
    value_as_array_double,
    values_as_double_array
);

fn append_f144_array(
    dataset: &Dataset,
    type_descriptor: TypeDescriptor,
    data: &f144_LogData<'_>,
    error: impl Fn() -> NexusHDF5Error,
) -> Result<(), NexusHDF5Error> {
    match type_descriptor {
        TypeDescriptor::Integer(int_size) => match int_size {
            IntSize::U1 => dataset.append_value(i8::f144_array_value_or_else(data, error)?),
            IntSize::U2 => dataset.append_value(i16::f144_array_value_or_else(data, error)?),
            IntSize::U4 => dataset.append_value(i32::f144_array_value_or_else(data, error)?),
            IntSize::U8 => dataset.append_value(i64::f144_array_value_or_else(data, error)?),
        },
        TypeDescriptor::Unsigned(int_size) => match int_size {
            IntSize::U1 => dataset.append_value(u8::f144_array_value_or_else(data, error)?),
            IntSize::U2 => dataset.append_value(u16::f144_array_value_or_else(data, error)?),
            IntSize::U4 => dataset.append_value(u32::f144_array_value_or_else(data, error)?),
            IntSize::U8 => dataset.append_value(u64::f144_array_value_or_else(data, error)?),
        },
        TypeDescriptor::Float(float_size) => match float_size {
            FloatSize::U4 => dataset.append_value(f32::f144_array_value_or_else(data, error)?),
            FloatSize::U8 => dataset.append_value(f64::f144_array_value_or_else(data, error)?),
        },
        _ => unreachable!("Unreachable HDF5 TypeDescriptor reached, this should never happen"),
    }
}

impl DatasetFlatbuffersExt for Dataset {
    #[tracing::instrument(skip_all, level = "debug", err(level = "warn"))]
    fn append_f144_value(&self, data: &f144_LogData<'_>) -> NexusHDF5Result<()> {
        let type_descriptor = data.get_type_descriptor()?;
        let error = || NexusHDF5Error::invalid_hdf5_type_conversion(type_descriptor.clone());
        match type_descriptor.clone() {
            TypeDescriptor::Integer(int_size) => match int_size {
                IntSize::U1 => self.append_value(i8::f144_value_or_else(data, error)?),
                IntSize::U2 => self.append_value(i16::f144_value_or_else(data, error)?),
                IntSize::U4 => self.append_value(i32::f144_value_or_else(data, error)?),
                IntSize::U8 => self.append_value(i64::f144_value_or_else(data, error)?),
            },
            TypeDescriptor::Unsigned(int_size) => match int_size {
                IntSize::U1 => self.append_value(u8::f144_value_or_else(data, error)?),
                IntSize::U2 => self.append_value(u16::f144_value_or_else(data, error)?),
                IntSize::U4 => self.append_value(u32::f144_value_or_else(data, error)?),
                IntSize::U8 => self.append_value(u64::f144_value_or_else(data, error)?),
            },
            TypeDescriptor::Float(float_size) => match float_size {
                FloatSize::U4 => self.append_value(f32::f144_value_or_else(data, error)?),
                FloatSize::U8 => self.append_value(f64::f144_value_or_else(data, error)?),
            },
            TypeDescriptor::VarLenArray(inner_type_descriptor) => {
                append_f144_array(self, inner_type_descriptor.to_packed_repr(), data, error)
            }
            _ => unreachable!("Unreachable HDF5 TypeDescriptor reached, this should never happen"),
        }
    }

    #[tracing::instrument(skip_all, level = "debug", err(level = "warn"))]
    fn append_se00_value_slice(
        &self,
        data: &se00_SampleEnvironmentData<'_>,
    ) -> NexusHDF5Result<()> {
        let type_descriptor = data.get_type_descriptor()?;
        let error = || NexusHDF5Error::invalid_hdf5_type_conversion(type_descriptor.clone());
        match type_descriptor {
            TypeDescriptor::Integer(int_size) => match int_size {
                IntSize::U1 => self.append_slice(&i8::se00_value_or_else(data, error)?),
                IntSize::U2 => self.append_slice(&i16::se00_value_or_else(data, error)?),
                IntSize::U4 => self.append_slice(&i32::se00_value_or_else(data, error)?),
                IntSize::U8 => self.append_slice(&i64::se00_value_or_else(data, error)?),
            },
            TypeDescriptor::Unsigned(int_size) => match int_size {
                IntSize::U1 => self.append_slice(&u8::se00_value_or_else(data, error)?),
                IntSize::U2 => self.append_slice(&u16::se00_value_or_else(data, error)?),
                IntSize::U4 => self.append_slice(&u32::se00_value_or_else(data, error)?),
                IntSize::U8 => self.append_slice(&u64::se00_value_or_else(data, error)?),
            },
            TypeDescriptor::Float(float_size) => match float_size {
                FloatSize::U4 => self.append_slice(&f32::se00_value_or_else(data, error)?),
                FloatSize::U8 => self.append_slice(&f64::se00_value_or_else(data, error)?),
            },
            _ => unreachable!("Unreachable HDF5 TypeDescriptor reached, this should never happen"),
        }
    }
}
