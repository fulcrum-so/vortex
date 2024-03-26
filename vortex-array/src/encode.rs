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

use vortex_schema::DType;

use crate::array::bool::BoolArray;
use crate::array::constant::ConstantArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::struct_::StructArray;
use crate::array::varbin::VarBinArray;
use crate::array::varbinview::VarBinViewArray;
use crate::array::IntoArray;
use crate::array::{Array, ArrayRef};
use crate::datetime::{LocalDateTime, LocalDateTimeArray};
use crate::ptype::{NativePType, PType};
use crate::scalar::NullScalar;
use crate::stats::Stat;
use crate::validity::Validity;

pub trait FromArrowArray<A> {
    fn from_arrow(array: A, nullable: bool) -> Self;
}

impl IntoArray for Buffer {
    fn into_array(self) -> ArrayRef {
        PrimitiveArray::new(PType::U8, self.to_owned(), None).into_array()
    }
}

impl IntoArray for NullBuffer {
    fn into_array(self) -> ArrayRef {
        BoolArray::new(self.into_inner(), None).into_array()
    }
}

impl<T: ArrowNativeType + NativePType> IntoArray for ScalarBuffer<T> {
    fn into_array(self) -> ArrayRef {
        PrimitiveArray::new(T::PTYPE, self.into_inner(), None).into_array()
    }
}

impl<O: OffsetSizeTrait> IntoArray for OffsetBuffer<O> {
    fn into_array(self) -> ArrayRef {
        let ptype = if O::IS_LARGE { PType::I64 } else { PType::I32 };
        let array = PrimitiveArray::new(ptype, self.into_inner().into_inner(), None).into_array();
        array.stats().set(Stat::IsSorted, true.into());
        array.stats().set(Stat::IsStrictSorted, true.into());
        array
    }
}

impl<T: ArrowPrimitiveType> FromArrowArray<&ArrowPrimitiveArray<T>> for ArrayRef {
    fn from_arrow(value: &ArrowPrimitiveArray<T>, nullable: bool) -> Self {
        let ptype: PType = (&T::DATA_TYPE).try_into().unwrap();
        let arr = PrimitiveArray::new(
            ptype,
            value.values().inner().to_owned(),
            nulls(value.nulls(), nullable, value.len()),
        )
        .into_array();

        if T::DATA_TYPE.is_numeric() {
            return arr;
        }

        match T::DATA_TYPE {
            DataType::Timestamp(time_unit, tz) => match tz {
                // A timestamp with no timezone is the equivalent of an "unknown" timezone.
                // Therefore, we must treat it as a LocalDateTime and not an Instant.
                None => LocalDateTimeArray::new(LocalDateTime::new((&time_unit).into()), arr)
                    .as_composite()
                    .into_array(),
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

impl<T: ByteArrayType> FromArrowArray<&GenericByteArray<T>> for ArrayRef {
    fn from_arrow(value: &GenericByteArray<T>, nullable: bool) -> Self {
        let dtype = match T::DATA_TYPE {
            DataType::Binary | DataType::LargeBinary => DType::Binary(nullable.into()),
            DataType::Utf8 | DataType::LargeUtf8 => DType::Utf8(nullable.into()),
            _ => panic!("Invalid data type for ByteArray"),
        };
        VarBinArray::new(
            value.offsets().clone().into_array(),
            value.values().clone().into_array(),
            dtype,
            nulls(value.nulls(), nullable, value.len()),
        )
        .into_array()
    }
}

impl<T: ByteViewType> FromArrowArray<&GenericByteViewArray<T>> for ArrayRef {
    fn from_arrow(value: &GenericByteViewArray<T>, nullable: bool) -> Self {
        let dtype = match T::DATA_TYPE {
            DataType::BinaryView => DType::Binary(nullable.into()),
            DataType::Utf8View => DType::Utf8(nullable.into()),
            _ => panic!("Invalid data type for ByteViewArray"),
        };

        VarBinViewArray::new(
            value.views().inner().clone().into_array(),
            value
                .data_buffers()
                .iter()
                .map(|b| b.clone().into_array())
                .collect::<Vec<_>>(),
            dtype,
            nulls(value.nulls(), nullable, value.len()),
        )
        .into_array()
    }
}

impl FromArrowArray<&ArrowBooleanArray> for ArrayRef {
    fn from_arrow(value: &ArrowBooleanArray, nullable: bool) -> Self {
        BoolArray::new(
            value.values().to_owned(),
            nulls(value.nulls(), nullable, value.len()),
        )
        .into_array()
    }
}

impl FromArrowArray<&ArrowStructArray> for ArrayRef {
    fn from_arrow(value: &ArrowStructArray, nullable: bool) -> Self {
        // TODO(ngates): how should we deal with Arrow "logical nulls"?
        assert!(!nullable);
        StructArray::new(
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
                .map(|(c, field)| ArrayRef::from_arrow(c.clone(), field.is_nullable()))
                .collect(),
        )
        .into_array()
    }
}

impl FromArrowArray<&ArrowNullArray> for ArrayRef {
    fn from_arrow(value: &ArrowNullArray, nullable: bool) -> Self {
        assert!(nullable);
        ConstantArray::new(NullScalar::new(), value.len()).into_array()
    }
}

fn nulls(nulls: Option<&NullBuffer>, nullable: bool, len: usize) -> Option<Validity> {
    if nullable {
        Some(
            nulls
                .map(|nulls| {
                    if nulls.null_count() == nulls.len() {
                        Validity::invalid(len)
                    } else {
                        Validity::from(nulls.inner().clone())
                    }
                })
                .unwrap_or_else(|| Validity::valid(len)),
        )
    } else {
        assert!(nulls.is_none());
        None
    }
}

impl FromArrowArray<ArrowArrayRef> for ArrayRef {
    fn from_arrow(array: ArrowArrayRef, nullable: bool) -> Self {
        match array.data_type() {
            DataType::Boolean => ArrayRef::from_arrow(array.as_boolean(), nullable),
            DataType::UInt8 => ArrayRef::from_arrow(array.as_primitive::<UInt8Type>(), nullable),
            DataType::UInt16 => ArrayRef::from_arrow(array.as_primitive::<UInt16Type>(), nullable),
            DataType::UInt32 => ArrayRef::from_arrow(array.as_primitive::<UInt32Type>(), nullable),
            DataType::UInt64 => ArrayRef::from_arrow(array.as_primitive::<UInt64Type>(), nullable),
            DataType::Int8 => ArrayRef::from_arrow(array.as_primitive::<Int8Type>(), nullable),
            DataType::Int16 => ArrayRef::from_arrow(array.as_primitive::<Int16Type>(), nullable),
            DataType::Int32 => ArrayRef::from_arrow(array.as_primitive::<Int32Type>(), nullable),
            DataType::Int64 => ArrayRef::from_arrow(array.as_primitive::<Int64Type>(), nullable),
            DataType::Float16 => {
                ArrayRef::from_arrow(array.as_primitive::<Float16Type>(), nullable)
            }
            DataType::Float32 => {
                ArrayRef::from_arrow(array.as_primitive::<Float32Type>(), nullable)
            }
            DataType::Float64 => {
                ArrayRef::from_arrow(array.as_primitive::<Float64Type>(), nullable)
            }
            DataType::Utf8 => ArrayRef::from_arrow(array.as_string::<i32>(), nullable),
            DataType::LargeUtf8 => ArrayRef::from_arrow(array.as_string::<i64>(), nullable),
            DataType::Binary => ArrayRef::from_arrow(array.as_binary::<i32>(), nullable),
            DataType::LargeBinary => ArrayRef::from_arrow(array.as_binary::<i64>(), nullable),
            DataType::BinaryView => ArrayRef::from_arrow(
                array.as_any().downcast_ref::<BinaryViewArray>().unwrap(),
                nullable,
            ),
            DataType::Utf8View => ArrayRef::from_arrow(
                array.as_any().downcast_ref::<StringViewArray>().unwrap(),
                nullable,
            ),
            DataType::Struct(_) => ArrayRef::from_arrow(array.as_struct(), nullable),
            DataType::Null => ArrayRef::from_arrow(as_null_array(&array), nullable),
            DataType::Timestamp(u, _) => match u {
                TimeUnit::Second => {
                    ArrayRef::from_arrow(array.as_primitive::<TimestampSecondType>(), nullable)
                }
                TimeUnit::Millisecond => {
                    ArrayRef::from_arrow(array.as_primitive::<TimestampMillisecondType>(), nullable)
                }
                TimeUnit::Microsecond => {
                    ArrayRef::from_arrow(array.as_primitive::<TimestampMicrosecondType>(), nullable)
                }
                TimeUnit::Nanosecond => {
                    ArrayRef::from_arrow(array.as_primitive::<TimestampNanosecondType>(), nullable)
                }
            },
            DataType::Date32 => ArrayRef::from_arrow(array.as_primitive::<Date32Type>(), nullable),
            DataType::Date64 => ArrayRef::from_arrow(array.as_primitive::<Date64Type>(), nullable),
            DataType::Time32(u) => match u {
                TimeUnit::Second => {
                    ArrayRef::from_arrow(array.as_primitive::<Time32SecondType>(), nullable)
                }
                TimeUnit::Millisecond => {
                    ArrayRef::from_arrow(array.as_primitive::<Time32MillisecondType>(), nullable)
                }
                _ => unreachable!(),
            },
            DataType::Time64(u) => match u {
                TimeUnit::Microsecond => {
                    ArrayRef::from_arrow(array.as_primitive::<Time64MicrosecondType>(), nullable)
                }
                TimeUnit::Nanosecond => {
                    ArrayRef::from_arrow(array.as_primitive::<Time64NanosecondType>(), nullable)
                }
                _ => unreachable!(),
            },
            DataType::Duration(u) => match u {
                TimeUnit::Second => {
                    ArrayRef::from_arrow(array.as_primitive::<DurationSecondType>(), nullable)
                }
                TimeUnit::Millisecond => {
                    ArrayRef::from_arrow(array.as_primitive::<DurationMillisecondType>(), nullable)
                }
                TimeUnit::Microsecond => {
                    ArrayRef::from_arrow(array.as_primitive::<DurationMicrosecondType>(), nullable)
                }
                TimeUnit::Nanosecond => {
                    ArrayRef::from_arrow(array.as_primitive::<DurationNanosecondType>(), nullable)
                }
            },
            _ => panic!(
                "TODO(robert): Missing array encoding for dtype {}",
                array.data_type().clone()
            ),
        }
    }
}
