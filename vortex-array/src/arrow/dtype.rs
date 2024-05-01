use std::sync::Arc;

use arrow_schema::TimeUnit as ArrowTimeUnit;
use arrow_schema::{DataType, Field, SchemaRef};
use itertools::Itertools;
use vortex_dtype::PType;
use vortex_dtype::{DType, Nullability};
use vortex_error::{vortex_err, VortexResult};

use crate::array::datetime::{LocalDateTime, TimeUnit};
use crate::arrow::{FromArrowType, TryFromArrowType};

impl TryFromArrowType<&DataType> for PType {
    fn try_from_arrow(value: &DataType) -> VortexResult<Self> {
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
            DataType::Timestamp(..) => Ok(PType::I64),
            DataType::Date32 => Ok(PType::I32),
            DataType::Date64 => Ok(PType::I64),
            DataType::Duration(_) => Ok(PType::I64),
            _ => Err(vortex_err!(
                "Arrow datatype {:?} cannot be converted to ptype",
                value
            )),
        }
    }
}

impl FromArrowType<SchemaRef> for DType {
    fn from_arrow(value: SchemaRef) -> Self {
        DType::Struct(
            value
                .fields()
                .iter()
                .map(|f| Arc::new(f.name().clone()))
                .collect(),
            value
                .fields()
                .iter()
                .map(|f| DType::from_arrow(f.as_ref()))
                .collect_vec(),
        )
    }
}

impl FromArrowType<&Field> for DType {
    fn from_arrow(field: &Field) -> Self {
        use vortex_dtype::DType::*;

        let nullability: Nullability = field.is_nullable().into();

        if let Ok(ptype) = PType::try_from_arrow(field.data_type()) {
            return Primitive(ptype, nullability);
        }

        match field.data_type() {
            DataType::Null => Null,
            DataType::Boolean => Bool(nullability),
            DataType::Utf8 | DataType::LargeUtf8 => Utf8(nullability),
            DataType::Binary | DataType::LargeBinary => Binary(nullability),
            DataType::Timestamp(time_unit, tz) => match tz {
                None => LocalDateTime::dtype(time_unit.into(), nullability),
                Some(_) => unimplemented!("Timezone not yet supported"),
            },
            // DataType::Date32 => localdate(IntWidth::_32, nullability),
            // DataType::Date64 => localdate(IntWidth::_64, nullability),
            // DataType::Time32(u) => localtime(u.into(), IntWidth::_32, nullability),
            // DataType::Time64(u) => localtime(u.into(), IntWidth::_64, nullability),
            DataType::List(e) | DataType::LargeList(e) => {
                List(Box::new(DType::from_arrow(e.as_ref())), nullability)
            }
            DataType::Struct(f) => Struct(
                f.iter().map(|f| Arc::new(f.name().clone())).collect(),
                f.iter()
                    .map(|f| DType::from_arrow(f.as_ref()))
                    .collect_vec(),
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
