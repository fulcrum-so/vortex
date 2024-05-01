use std::sync::Arc;

use arrow_array::array::{
    Array as ArrowArray, ArrayRef as ArrowArrayRef, BooleanArray as ArrowBooleanArray,
    GenericByteArray, NullArray as ArrowNullArray, PrimitiveArray as ArrowPrimitiveArray,
    StructArray as ArrowStructArray,
};
use arrow_array::array::{ArrowPrimitiveType, OffsetSizeTrait};
use arrow_array::cast::{as_null_array, AsArray};
use arrow_array::types::{
    ByteArrayType, ByteViewType, Date32Type, Date64Type, DurationMicrosecondType,
    DurationMillisecondType, DurationNanosecondType, DurationSecondType, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use arrow_array::types::{
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{BinaryViewArray, GenericByteViewArray, StringViewArray};
use arrow_buffer::buffer::{NullBuffer, OffsetBuffer};
use arrow_buffer::{ArrowNativeType, Buffer, ScalarBuffer};
use arrow_schema::{DataType, TimeUnit};
use vortex_dtype::DType;
use vortex_dtype::NativePType;
use vortex_scalar::NullScalar;

use crate::array::bool::BoolArray;
use crate::array::constant::ConstantArray;
use crate::array::datetime::LocalDateTimeArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::r#struct::StructArray;
use crate::array::varbin::VarBinArray;
use crate::array::varbinview::VarBinViewArray;
use crate::arrow::FromArrowArray;
use crate::stats::{Stat, Statistics};
use crate::validity::Validity;
use crate::{ArrayData, IntoArray, IntoArrayData};

impl IntoArrayData for Buffer {
    fn into_array_data(self) -> ArrayData {
        let length = self.len();
        PrimitiveArray::try_new(
            ScalarBuffer::<u8>::new(self, 0, length),
            Validity::NonNullable,
        )
        .unwrap()
        .into_array_data()
    }
}

impl IntoArrayData for NullBuffer {
    fn into_array_data(self) -> ArrayData {
        BoolArray::try_new(self.into_inner(), Validity::NonNullable)
            .unwrap()
            .into_array_data()
    }
}

impl<T: ArrowNativeType + NativePType> IntoArrayData for ScalarBuffer<T> {
    fn into_array_data(self) -> ArrayData {
        let length = self.len();
        PrimitiveArray::try_new(
            ScalarBuffer::<T>::new(self.into_inner(), 0, length),
            Validity::NonNullable,
        )
        .unwrap()
        .into_array_data()
    }
}

impl<O: NativePType + OffsetSizeTrait> IntoArrayData for OffsetBuffer<O> {
    fn into_array_data(self) -> ArrayData {
        let length = self.len();
        let array = PrimitiveArray::try_new(
            ScalarBuffer::<O>::new(self.into_inner().into_inner(), 0, length),
            Validity::NonNullable,
        )
        .unwrap()
        .into_array_data();
        array.set(Stat::IsSorted, true.into());
        array.set(Stat::IsStrictSorted, true.into());
        array
    }
}

impl<T: ArrowPrimitiveType> FromArrowArray<&ArrowPrimitiveArray<T>> for ArrayData
where
    <T as ArrowPrimitiveType>::Native: NativePType,
{
    fn from_arrow(value: &ArrowPrimitiveArray<T>, nullable: bool) -> Self {
        let arr = PrimitiveArray::try_new(value.values().clone(), nulls(value.nulls(), nullable))
            .unwrap()
            .into_array_data();

        if T::DATA_TYPE.is_numeric() {
            return arr;
        }

        match T::DATA_TYPE {
            DataType::Timestamp(time_unit, tz) => match tz {
                // A timestamp with no timezone is the equivalent of an "unknown" timezone.
                // Therefore, we must treat it as a LocalDateTime and not an Instant.
                None => {
                    LocalDateTimeArray::new((&time_unit).into(), arr.into_array()).into_array_data()
                }
                Some(_tz) => todo!(),
            },
            DataType::Date32 => todo!(),
            DataType::Date64 => todo!(),
            DataType::Time32(_) => todo!(),
            DataType::Time64(_) => todo!(),
            DataType::Duration(_) => todo!(),
            DataType::Interval(_) => todo!(),
            _ => panic!("Invalid data type for PrimitiveArray"),
        }
    }
}

impl<T: ByteArrayType> FromArrowArray<&GenericByteArray<T>> for ArrayData
where
    <T as ByteArrayType>::Offset: NativePType,
{
    fn from_arrow(value: &GenericByteArray<T>, nullable: bool) -> Self {
        let dtype = match T::DATA_TYPE {
            DataType::Binary | DataType::LargeBinary => DType::Binary(nullable.into()),
            DataType::Utf8 | DataType::LargeUtf8 => DType::Utf8(nullable.into()),
            _ => panic!("Invalid data type for ByteArray"),
        };
        VarBinArray::try_new(
            value.offsets().clone().into_array_data().into_array(),
            value.values().clone().into_array_data().into_array(),
            dtype,
            nulls(value.nulls(), nullable),
        )
        .unwrap()
        .into_array_data()
    }
}

impl<T: ByteViewType> FromArrowArray<&GenericByteViewArray<T>> for ArrayData {
    fn from_arrow(value: &GenericByteViewArray<T>, nullable: bool) -> Self {
        let dtype = match T::DATA_TYPE {
            DataType::BinaryView => DType::Binary(nullable.into()),
            DataType::Utf8View => DType::Utf8(nullable.into()),
            _ => panic!("Invalid data type for ByteViewArray"),
        };
        VarBinViewArray::try_new(
            value.views().inner().clone().into_array_data().into_array(),
            value
                .data_buffers()
                .iter()
                .map(|b| b.clone().into_array_data().into_array())
                .collect::<Vec<_>>(),
            dtype,
            nulls(value.nulls(), nullable),
        )
        .unwrap()
        .into_array_data()
    }
}

impl FromArrowArray<&ArrowBooleanArray> for ArrayData {
    fn from_arrow(value: &ArrowBooleanArray, nullable: bool) -> Self {
        BoolArray::try_new(value.values().clone(), nulls(value.nulls(), nullable))
            .unwrap()
            .into_array_data()
    }
}

impl FromArrowArray<&ArrowStructArray> for ArrayData {
    fn from_arrow(value: &ArrowStructArray, nullable: bool) -> Self {
        // TODO(ngates): how should we deal with Arrow "logical nulls"?
        assert!(!nullable);
        StructArray::try_new(
            value
                .column_names()
                .iter()
                .map(|s| s.to_string())
                .map(Arc::new)
                .collect(),
            value
                .columns()
                .iter()
                .zip(value.fields())
                .map(|(c, field)| {
                    ArrayData::from_arrow(c.clone(), field.is_nullable()).into_array()
                })
                .collect(),
            value.len(),
        )
        .unwrap()
        .into_array_data()
    }
}

impl FromArrowArray<&ArrowNullArray> for ArrayData {
    fn from_arrow(value: &ArrowNullArray, nullable: bool) -> Self {
        assert!(nullable);
        ConstantArray::new(NullScalar::new(), value.len()).into_array_data()
    }
}

fn nulls(nulls: Option<&NullBuffer>, nullable: bool) -> Validity {
    if nullable {
        nulls
            .map(|nulls| {
                if nulls.null_count() == nulls.len() {
                    Validity::AllInvalid
                } else {
                    Validity::from(nulls.inner().clone())
                }
            })
            .unwrap_or_else(|| Validity::AllValid)
    } else {
        assert!(nulls.is_none());
        Validity::NonNullable
    }
}

impl FromArrowArray<ArrowArrayRef> for ArrayData {
    fn from_arrow(array: ArrowArrayRef, nullable: bool) -> Self {
        match array.data_type() {
            DataType::Boolean => ArrayData::from_arrow(array.as_boolean(), nullable),
            DataType::UInt8 => ArrayData::from_arrow(array.as_primitive::<UInt8Type>(), nullable),
            DataType::UInt16 => ArrayData::from_arrow(array.as_primitive::<UInt16Type>(), nullable),
            DataType::UInt32 => ArrayData::from_arrow(array.as_primitive::<UInt32Type>(), nullable),
            DataType::UInt64 => ArrayData::from_arrow(array.as_primitive::<UInt64Type>(), nullable),
            DataType::Int8 => ArrayData::from_arrow(array.as_primitive::<Int8Type>(), nullable),
            DataType::Int16 => ArrayData::from_arrow(array.as_primitive::<Int16Type>(), nullable),
            DataType::Int32 => ArrayData::from_arrow(array.as_primitive::<Int32Type>(), nullable),
            DataType::Int64 => ArrayData::from_arrow(array.as_primitive::<Int64Type>(), nullable),
            DataType::Float16 => {
                ArrayData::from_arrow(array.as_primitive::<Float16Type>(), nullable)
            }
            DataType::Float32 => {
                ArrayData::from_arrow(array.as_primitive::<Float32Type>(), nullable)
            }
            DataType::Float64 => {
                ArrayData::from_arrow(array.as_primitive::<Float64Type>(), nullable)
            }
            DataType::Utf8 => ArrayData::from_arrow(array.as_string::<i32>(), nullable),
            DataType::LargeUtf8 => ArrayData::from_arrow(array.as_string::<i64>(), nullable),
            DataType::Binary => ArrayData::from_arrow(array.as_binary::<i32>(), nullable),
            DataType::LargeBinary => ArrayData::from_arrow(array.as_binary::<i64>(), nullable),
            DataType::BinaryView => ArrayData::from_arrow(
                array.as_any().downcast_ref::<BinaryViewArray>().unwrap(),
                nullable,
            ),
            DataType::Utf8View => ArrayData::from_arrow(
                array.as_any().downcast_ref::<StringViewArray>().unwrap(),
                nullable,
            ),
            DataType::Struct(_) => ArrayData::from_arrow(array.as_struct(), nullable),
            DataType::Null => ArrayData::from_arrow(as_null_array(&array), nullable),
            DataType::Timestamp(u, _) => match u {
                TimeUnit::Second => {
                    ArrayData::from_arrow(array.as_primitive::<TimestampSecondType>(), nullable)
                }
                TimeUnit::Millisecond => ArrayData::from_arrow(
                    array.as_primitive::<TimestampMillisecondType>(),
                    nullable,
                ),
                TimeUnit::Microsecond => ArrayData::from_arrow(
                    array.as_primitive::<TimestampMicrosecondType>(),
                    nullable,
                ),
                TimeUnit::Nanosecond => {
                    ArrayData::from_arrow(array.as_primitive::<TimestampNanosecondType>(), nullable)
                }
            },
            DataType::Date32 => ArrayData::from_arrow(array.as_primitive::<Date32Type>(), nullable),
            DataType::Date64 => ArrayData::from_arrow(array.as_primitive::<Date64Type>(), nullable),
            DataType::Time32(u) => match u {
                TimeUnit::Second => {
                    ArrayData::from_arrow(array.as_primitive::<Time32SecondType>(), nullable)
                }
                TimeUnit::Millisecond => {
                    ArrayData::from_arrow(array.as_primitive::<Time32MillisecondType>(), nullable)
                }
                _ => unreachable!(),
            },
            DataType::Time64(u) => match u {
                TimeUnit::Microsecond => {
                    ArrayData::from_arrow(array.as_primitive::<Time64MicrosecondType>(), nullable)
                }
                TimeUnit::Nanosecond => {
                    ArrayData::from_arrow(array.as_primitive::<Time64NanosecondType>(), nullable)
                }
                _ => unreachable!(),
            },
            DataType::Duration(u) => match u {
                TimeUnit::Second => {
                    ArrayData::from_arrow(array.as_primitive::<DurationSecondType>(), nullable)
                }
                TimeUnit::Millisecond => {
                    ArrayData::from_arrow(array.as_primitive::<DurationMillisecondType>(), nullable)
                }
                TimeUnit::Microsecond => {
                    ArrayData::from_arrow(array.as_primitive::<DurationMicrosecondType>(), nullable)
                }
                TimeUnit::Nanosecond => {
                    ArrayData::from_arrow(array.as_primitive::<DurationNanosecondType>(), nullable)
                }
            },
            _ => panic!(
                "TODO(robert): Missing array encoding for dtype {}",
                array.data_type().clone()
            ),
        }
    }
}
