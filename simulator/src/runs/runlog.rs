use clap::ValueEnum;
use digital_muon_streaming_types::{
    ecs_f144_logdata_generated::{
        ArrayByte, ArrayByteArgs, ArrayDouble, ArrayDoubleArgs, ArrayFloat, ArrayFloatArgs,
        ArrayInt, ArrayIntArgs, ArrayLong, ArrayLongArgs, ArrayShort, ArrayShortArgs, ArrayUByte,
        ArrayUByteArgs, ArrayUInt, ArrayUIntArgs, ArrayULong, ArrayULongArgs, ArrayUShort,
        ArrayUShortArgs, Byte, ByteArgs, Double, DoubleArgs, Float, FloatArgs, Int, IntArgs, Long,
        LongArgs, Short, ShortArgs, UByte, UByteArgs, UInt, UIntArgs, ULong, ULongArgs, UShort,
        UShortArgs, Value,
    },
    flatbuffers::{FlatBufferBuilder, Push, UnionWIPOffset, Vector, WIPOffset},
};
use serde::Deserialize;
use std::str::FromStr;

use super::RunCommandError;

#[derive(Clone, Debug, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum ValueType {
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    ArrayUint8,
    ArrayUint16,
    ArrayUint32,
    ArrayUint64,
    ArrayInt8,
    ArrayInt16,
    ArrayInt32,
    ArrayInt64,
    ArrayFloat32,
    ArrayFloat64,
}

impl From<ValueType> for Value {
    fn from(source: ValueType) -> Self {
        match source {
            ValueType::Uint8 => Value::UByte,
            ValueType::Uint16 => Value::UShort,
            ValueType::Uint32 => Value::UInt,
            ValueType::Uint64 => Value::ULong,
            ValueType::Int8 => Value::Byte,
            ValueType::Int16 => Value::Short,
            ValueType::Int32 => Value::Int,
            ValueType::Int64 => Value::Long,
            ValueType::Float32 => Value::Float,
            ValueType::Float64 => Value::Double,
            ValueType::ArrayUint8 => Value::ArrayUByte,
            ValueType::ArrayUint16 => Value::ArrayUShort,
            ValueType::ArrayUint32 => Value::ArrayUInt,
            ValueType::ArrayUint64 => Value::ArrayULong,
            ValueType::ArrayInt8 => Value::ArrayByte,
            ValueType::ArrayInt16 => Value::ArrayShort,
            ValueType::ArrayInt32 => Value::ArrayInt,
            ValueType::ArrayInt64 => Value::ArrayLong,
            ValueType::ArrayFloat32 => Value::ArrayFloat,
            ValueType::ArrayFloat64 => Value::ArrayDouble,
        }
    }
}

type GenericFBVector<'a, I> = WIPOffset<Vector<'a, <I as Push>::Output>>;

fn to_array<'a, 'fbb: 'a, I: FromStr + Push>(
    fbb: &mut FlatBufferBuilder<'fbb>,
    value: &[String],
) -> Result<Option<GenericFBVector<'a, I>>, <I as FromStr>::Err>
where
    <I as Push>::Output: 'fbb,
{
    Ok(Some(
        fbb.create_vector(
            value
                .iter()
                .map(|str| str.parse())
                .collect::<Result<Vec<I>, <I as FromStr>::Err>>()?
                .as_slice(),
        ),
    ))
}

pub(crate) fn make_value(
    fbb: &mut FlatBufferBuilder,
    value_type: Value,
    values: &[String],
) -> Result<WIPOffset<UnionWIPOffset>, RunCommandError> {
    let value = values.first().ok_or(RunCommandError::EmptyRunLogSlice)?;
    Ok(match value_type {
        Value::Byte => {
            let value = value.parse::<i8>()?;
            Byte::create(fbb, &ByteArgs { value }).as_union_value()
        }
        Value::Short => {
            let value = value.parse::<i16>()?;
            Short::create(fbb, &ShortArgs { value }).as_union_value()
        }
        Value::Int => {
            let value = value.parse::<i32>()?;
            Int::create(fbb, &IntArgs { value }).as_union_value()
        }
        Value::Long => {
            let value = value.parse::<i64>()?;
            Long::create(fbb, &LongArgs { value }).as_union_value()
        }
        Value::UByte => {
            let value = value.parse::<u8>()?;
            UByte::create(fbb, &UByteArgs { value }).as_union_value()
        }
        Value::UShort => {
            let value = value.parse::<u16>()?;
            UShort::create(fbb, &UShortArgs { value }).as_union_value()
        }
        Value::UInt => {
            let value = value.parse::<u32>()?;
            UInt::create(fbb, &UIntArgs { value }).as_union_value()
        }
        Value::ULong => {
            let value = value.parse::<u64>()?;
            ULong::create(fbb, &ULongArgs { value }).as_union_value()
        }
        Value::Float => {
            let value = value.parse::<f32>()?;
            Float::create(fbb, &FloatArgs { value }).as_union_value()
        }
        Value::Double => {
            let value = value.parse::<f64>()?;
            Double::create(fbb, &DoubleArgs { value }).as_union_value()
        }
        Value::ArrayByte => {
            let value = to_array::<i8>(fbb, values)?;
            ArrayByte::create(fbb, &ArrayByteArgs { value }).as_union_value()
        }
        Value::ArrayShort => {
            let value = to_array::<i16>(fbb, values)?;
            ArrayShort::create(fbb, &ArrayShortArgs { value }).as_union_value()
        }
        Value::ArrayInt => {
            let value = to_array::<i32>(fbb, values)?;
            ArrayInt::create(fbb, &ArrayIntArgs { value }).as_union_value()
        }
        Value::ArrayLong => {
            let value = to_array::<i64>(fbb, values)?;
            ArrayLong::create(fbb, &ArrayLongArgs { value }).as_union_value()
        }
        Value::ArrayUByte => {
            let value = to_array::<u8>(fbb, values)?;
            ArrayUByte::create(fbb, &ArrayUByteArgs { value }).as_union_value()
        }
        Value::ArrayUShort => {
            let value = to_array::<u16>(fbb, values)?;
            ArrayUShort::create(fbb, &ArrayUShortArgs { value }).as_union_value()
        }
        Value::ArrayUInt => {
            let value = to_array::<u32>(fbb, values)?;
            ArrayUInt::create(fbb, &ArrayUIntArgs { value }).as_union_value()
        }
        Value::ArrayULong => {
            let value = to_array::<u64>(fbb, values)?;
            ArrayULong::create(fbb, &ArrayULongArgs { value }).as_union_value()
        }
        Value::ArrayFloat => {
            let value = to_array::<f32>(fbb, values)?;
            ArrayFloat::create(fbb, &ArrayFloatArgs { value }).as_union_value()
        }
        Value::ArrayDouble => {
            let value = to_array::<f64>(fbb, values)?;
            ArrayDouble::create(fbb, &ArrayDoubleArgs { value }).as_union_value()
        }
        _ => unreachable!(),
    })
}

#[cfg(test)]
mod tests {
    use digital_muon_streaming_types::{
        ecs_f144_logdata_generated::{
            f144_LogData, f144_LogDataArgs, finish_f_144_log_data_buffer, root_as_f_144_log_data,
        },
        flatbuffers::InvalidFlatbuffer,
    };

    use super::*;

    fn process<'a>(
        fbb: &'a mut FlatBufferBuilder,
        value_type: Value,
        value: WIPOffset<UnionWIPOffset>,
    ) -> Result<f144_LogData<'a>, InvalidFlatbuffer> {
        let run_log = f144_LogDataArgs {
            source_name: Some(fbb.create_string("")),
            timestamp: 0,
            value_type,
            value: Some(value),
        };
        let message = f144_LogData::create(fbb, &run_log);
        finish_f_144_log_data_buffer(fbb, message);
        let bytes = fbb.finished_data();
        root_as_f_144_log_data(bytes)
    }

    fn do_test<'a>(
        fbb: &'a mut FlatBufferBuilder,
        value_type: Value,
    ) -> Result<f144_LogData<'a>, InvalidFlatbuffer> {
        let test_value = ["2".to_owned()];
        let val = make_value(fbb, value_type, &test_value).unwrap();
        process(fbb, value_type, val)
    }

    #[test]
    fn make_value_byte() {
        let mut fbb = FlatBufferBuilder::new();
        let obj = do_test(&mut fbb, Value::Byte).unwrap();

        assert_eq!(obj.value_type(), Value::Byte);
        assert_eq!(obj.value_as_byte().unwrap().value(), 2_i8);
    }

    #[test]
    fn make_value_short() {
        let mut fbb = FlatBufferBuilder::new();
        let obj = do_test(&mut fbb, Value::Short).unwrap();

        assert_eq!(obj.value_type(), Value::Short);
        assert_eq!(obj.value_as_short().unwrap().value(), 2_i16);
    }

    #[test]
    fn make_value_int() {
        let mut fbb = FlatBufferBuilder::new();
        let obj = do_test(&mut fbb, Value::Int).unwrap();

        assert_eq!(obj.value_type(), Value::Int);
        assert_eq!(obj.value_as_int().unwrap().value(), 2_i32);
    }

    #[test]
    fn make_value_long() {
        let mut fbb = FlatBufferBuilder::new();
        let obj = do_test(&mut fbb, Value::Long).unwrap();

        assert_eq!(obj.value_type(), Value::Long);
        assert_eq!(obj.value_as_long().unwrap().value(), 2_i64);
    }

    #[test]
    fn make_value_ubyte() {
        let mut fbb = FlatBufferBuilder::new();
        let obj = do_test(&mut fbb, Value::UByte).unwrap();

        assert_eq!(obj.value_type(), Value::UByte);
        assert_eq!(obj.value_as_ubyte().unwrap().value(), 2_u8);
    }

    #[test]
    fn make_value_ushort() {
        let mut fbb = FlatBufferBuilder::new();
        let obj = do_test(&mut fbb, Value::UShort).unwrap();

        assert_eq!(obj.value_type(), Value::UShort);
        assert_eq!(obj.value_as_ushort().unwrap().value(), 2_u16);
    }

    #[test]
    fn make_value_uint() {
        let mut fbb = FlatBufferBuilder::new();
        let obj = do_test(&mut fbb, Value::UInt).unwrap();

        assert_eq!(obj.value_type(), Value::UInt);
        assert_eq!(obj.value_as_uint().unwrap().value(), 2_u32);
    }

    #[test]
    fn make_value_ulong() {
        let mut fbb = FlatBufferBuilder::new();
        let obj = do_test(&mut fbb, Value::ULong).unwrap();

        assert_eq!(obj.value_type(), Value::ULong);
        assert_eq!(obj.value_as_ulong().unwrap().value(), 2_u64);
    }

    #[test]
    fn make_value_float() {
        let mut fbb = FlatBufferBuilder::new();
        let obj = do_test(&mut fbb, Value::Float).unwrap();

        assert_eq!(obj.value_type(), Value::Float);
        assert_eq!(obj.value_as_float().unwrap().value(), 2_f32);
    }

    #[test]
    fn make_value_double() {
        let mut fbb = FlatBufferBuilder::new();
        let obj = do_test(&mut fbb, Value::Double).unwrap();

        assert_eq!(obj.value_type(), Value::Double);
        assert_eq!(obj.value_as_double().unwrap().value(), 2_f64);
    }

    fn do_array_test<'a>(
        fbb: &'a mut FlatBufferBuilder,
        value_type: Value,
    ) -> Result<f144_LogData<'a>, InvalidFlatbuffer> {
        let test_value = ["2".to_owned(), "3".to_owned()];
        let val = make_value(fbb, value_type, &test_value).unwrap();
        process(fbb, value_type, val)
    }

    #[test]
    fn make_value_byte_array() {
        let mut fbb = FlatBufferBuilder::new();
        let obj = do_array_test(&mut fbb, Value::ArrayByte).unwrap();

        assert_eq!(obj.value_type(), Value::ArrayByte);

        let array = obj.value_as_array_byte().unwrap().value().unwrap();
        assert_eq!(array.len(), 2);
        assert_eq!(array.get(0), 2_i8);
        assert_eq!(array.get(1), 3_i8);
    }

    #[test]
    fn make_value_short_array() {
        let mut fbb = FlatBufferBuilder::new();
        let obj = do_array_test(&mut fbb, Value::ArrayShort).unwrap();

        assert_eq!(obj.value_type(), Value::ArrayShort);

        let array = obj.value_as_array_short().unwrap().value().unwrap();
        assert_eq!(array.len(), 2);
        assert_eq!(array.get(0), 2_i16);
        assert_eq!(array.get(1), 3_i16);
    }

    #[test]
    fn make_value_int_array() {
        let mut fbb = FlatBufferBuilder::new();
        let obj = do_array_test(&mut fbb, Value::ArrayInt).unwrap();

        assert_eq!(obj.value_type(), Value::ArrayInt);

        let array = obj.value_as_array_int().unwrap().value().unwrap();
        assert_eq!(array.len(), 2);
        assert_eq!(array.get(0), 2_i32);
        assert_eq!(array.get(1), 3_i32);
    }

    #[test]
    fn make_value_long_array() {
        let mut fbb = FlatBufferBuilder::new();
        let obj = do_array_test(&mut fbb, Value::ArrayLong).unwrap();

        assert_eq!(obj.value_type(), Value::ArrayLong);

        let array = obj.value_as_array_long().unwrap().value().unwrap();
        assert_eq!(array.len(), 2);
        assert_eq!(array.get(0), 2_i64);
        assert_eq!(array.get(1), 3_i64);
    }

    #[test]
    fn make_value_ubyte_array() {
        let mut fbb = FlatBufferBuilder::new();
        let obj = do_array_test(&mut fbb, Value::ArrayUByte).unwrap();

        assert_eq!(obj.value_type(), Value::ArrayUByte);

        let array = obj.value_as_array_ubyte().unwrap().value().unwrap();
        assert_eq!(array.len(), 2);
        assert_eq!(array.get(0), 2_u8);
        assert_eq!(array.get(1), 3_u8);
    }

    #[test]
    fn make_value_ushort_array() {
        let mut fbb = FlatBufferBuilder::new();
        let obj = do_array_test(&mut fbb, Value::ArrayUShort).unwrap();

        assert_eq!(obj.value_type(), Value::ArrayUShort);

        let array = obj.value_as_array_ushort().unwrap().value().unwrap();
        assert_eq!(array.len(), 2);
        assert_eq!(array.get(0), 2_u16);
        assert_eq!(array.get(1), 3_u16);
    }

    #[test]
    fn make_value_uint_array() {
        let mut fbb = FlatBufferBuilder::new();
        let obj = do_array_test(&mut fbb, Value::ArrayUInt).unwrap();

        assert_eq!(obj.value_type(), Value::ArrayUInt);

        let array = obj.value_as_array_uint().unwrap().value().unwrap();
        assert_eq!(array.len(), 2);
        assert_eq!(array.get(0), 2_u32);
        assert_eq!(array.get(1), 3_u32);
    }

    #[test]
    fn make_value_ulong_array() {
        let mut fbb = FlatBufferBuilder::new();
        let obj = do_array_test(&mut fbb, Value::ArrayULong).unwrap();

        assert_eq!(obj.value_type(), Value::ArrayULong);

        let array = obj.value_as_array_ulong().unwrap().value().unwrap();
        assert_eq!(array.len(), 2);
        assert_eq!(array.get(0), 2_u64);
        assert_eq!(array.get(1), 3_u64);
    }

    #[test]
    fn make_value_float_array() {
        let mut fbb = FlatBufferBuilder::new();
        let obj = do_array_test(&mut fbb, Value::ArrayFloat).unwrap();

        assert_eq!(obj.value_type(), Value::ArrayFloat);

        let array = obj.value_as_array_float().unwrap().value().unwrap();
        assert_eq!(array.len(), 2);
        assert_eq!(array.get(0), 2_f32);
        assert_eq!(array.get(1), 3_f32);
    }

    #[test]
    fn make_value_double_array() {
        let mut fbb = FlatBufferBuilder::new();
        let obj = do_array_test(&mut fbb, Value::ArrayDouble).unwrap();

        assert_eq!(obj.value_type(), Value::ArrayDouble);
        let array = obj.value_as_array_double().unwrap().value().unwrap();
        assert_eq!(array.len(), 2);
        assert_eq!(array.get(0), 2_f64);
        assert_eq!(array.get(1), 3_f64);
    }
}
