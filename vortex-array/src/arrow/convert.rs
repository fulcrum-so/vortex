use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, SchemaRef, TimeUnit as ArrowTimeUnit};
use itertools::Itertools;

use crate::array::struct_::StructArray;
use crate::array::{Array, ArrayRef};
use crate::composite_dtypes::TimeUnit;
use crate::compute::cast::cast;
use crate::dtype::DType::*;
use crate::dtype::{DType, FloatWidth, IntWidth, Nullability};
use crate::encode::FromArrow;
use crate::error::{VortexError, VortexResult};
use crate::ptype::PType;

impl From<RecordBatch> for ArrayRef {
    fn from(value: RecordBatch) -> Self {
        StructArray::new(
            value
                .schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .map(|s| s.to_owned())
                .map(Arc::new)
                .collect(),
            value
                .columns()
                .iter()
                .zip(value.schema().fields())
                .map(|(array, field)| {
                    // The dtype of the child arrays infer their nullability from the array itself.
                    // In case the schema says something different, we cast into the schema's dtype.
                    let vortex_array = ArrayRef::from_arrow(array.clone(), field.is_nullable());
                    cast(vortex_array.as_ref(), &field.as_ref().into()).unwrap()
                })
                .collect(),
        )
        .boxed()
    }
}

impl TryFrom<SchemaRef> for DType {
    type Error = VortexError;

    fn try_from(value: SchemaRef) -> VortexResult<Self> {
        Ok(Struct(
            value
                .fields()
                .iter()
                .map(|f| Arc::new(f.name().clone()))
                .collect(),
            value
                .fields()
                .iter()
                .map(|f| f.as_ref().into())
                .collect_vec(),
        ))
    }
}

impl TryFrom<&DataType> for PType {
    type Error = VortexError;

    fn try_from(value: &DataType) -> VortexResult<Self> {
        match value {
            DataType::Int8 => Ok(PType::I8),
            DataType::Int16 => Ok(PType::I16),
            DataType::Int32 => Ok(PType::I32),
            DataType::Int64 => Ok(PType::I64),
            DataType::UInt8 => Ok(PType::U8),
            DataType::UInt16 => Ok(PType::U16),
            DataType::UInt32 => Ok(PType::U32),
            DataType::UInt64 => Ok(PType::U64),
            DataType::Float16 => Ok(PType::F16),
            DataType::Float32 => Ok(PType::F32),
            DataType::Float64 => Ok(PType::F64),
            DataType::Time32(_) => Ok(PType::I32),
            DataType::Time64(_) => Ok(PType::I64),
            DataType::Timestamp(_, _) => Ok(PType::I64),
            DataType::Date32 => Ok(PType::I32),
            DataType::Date64 => Ok(PType::I64),
            DataType::Duration(_) => Ok(PType::I64),
            _ => Err(VortexError::InvalidArrowDataType(value.clone())),
        }
    }
}

impl From<&Field> for DType {
    fn from(field: &Field) -> Self {
        use crate::dtype::Signedness::*;

        let nullability: Nullability = field.is_nullable().into();

        match field.data_type() {
            DataType::Null => Null,
            DataType::Boolean => Bool(nullability),
            DataType::Int8 => Int(IntWidth::_8, Signed, nullability),
            DataType::Int16 => Int(IntWidth::_16, Signed, nullability),
            DataType::Int32 => Int(IntWidth::_32, Signed, nullability),
            DataType::Int64 => Int(IntWidth::_64, Signed, nullability),
            DataType::UInt8 => Int(IntWidth::_8, Unsigned, nullability),
            DataType::UInt16 => Int(IntWidth::_16, Unsigned, nullability),
            DataType::UInt32 => Int(IntWidth::_32, Unsigned, nullability),
            DataType::UInt64 => Int(IntWidth::_64, Unsigned, nullability),
            DataType::Float16 => Float(FloatWidth::_16, nullability),
            DataType::Float32 => Float(FloatWidth::_32, nullability),
            DataType::Float64 => Float(FloatWidth::_64, nullability),
            DataType::Utf8 | DataType::LargeUtf8 => Utf8(nullability),
            DataType::Binary | DataType::LargeBinary => Binary(nullability),
            // TODO(robert): what to do about this timezone?
            // DataType::Timestamp(u, _) => zoneddatetime(u.into(), nullability),
            // DataType::Date32 => localdate(IntWidth::_32, nullability),
            // DataType::Date64 => localdate(IntWidth::_64, nullability),
            // DataType::Time32(u) => localtime(u.into(), IntWidth::_32, nullability),
            // DataType::Time64(u) => localtime(u.into(), IntWidth::_64, nullability),
            DataType::List(e) | DataType::LargeList(e) => {
                List(Box::new(e.as_ref().into()), nullability)
            }
            DataType::Struct(f) => Struct(
                f.iter().map(|f| Arc::new(f.name().clone())).collect(),
                f.iter().map(|f| f.as_ref().into()).collect_vec(),
            ),
            DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => Decimal(*p, *s, nullability),
            _ => unimplemented!("Arrow data type not yet supported: {:?}", field.data_type()),
        }
    }
}

impl From<&ArrowTimeUnit> for TimeUnit {
    fn from(value: &ArrowTimeUnit) -> Self {
        match value {
            ArrowTimeUnit::Second => TimeUnit::S,
            ArrowTimeUnit::Millisecond => TimeUnit::Ms,
            ArrowTimeUnit::Microsecond => TimeUnit::Us,
            ArrowTimeUnit::Nanosecond => TimeUnit::Ns,
        }
    }
}

impl From<TimeUnit> for ArrowTimeUnit {
    fn from(value: TimeUnit) -> Self {
        match value {
            TimeUnit::S => ArrowTimeUnit::Second,
            TimeUnit::Ms => ArrowTimeUnit::Millisecond,
            TimeUnit::Us => ArrowTimeUnit::Microsecond,
            TimeUnit::Ns => ArrowTimeUnit::Nanosecond,
        }
    }
}
