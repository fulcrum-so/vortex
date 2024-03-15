use arrow_array::{RecordBatch, RecordBatchReader};
use std::iter::zip;
use std::sync::Arc;

use arrow_schema::{
    DataType, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit as ArrowTimeUnit,
};
use itertools::Itertools;

use vortex::array::chunked::ChunkedArray;
use vortex::array::struct_::StructArray;
use vortex::array::typed::TypedArray;
use vortex::array::{Array, ArrayRef};
use vortex::dtype::DType::*;
use vortex::dtype::{DType, FloatWidth, IntWidth, Nullability, TimeUnit};
use vortex::error::{VortexError, VortexResult};
use vortex::ptype::PType;

#[allow(dead_code)]
trait CollectRecordBatches: IntoIterator<Item = RecordBatch> {
    fn collect_record_batches(&self, schema: &Schema) -> ArrayRef;
}

#[allow(dead_code)]
impl TryFrom<&mut dyn RecordBatchReader> for ArrayRef {
    type Error = VortexError;

    fn try_from(reader: &mut dyn RecordBatchReader) -> Result<Self, Self::Error> {
        let schema = reader.schema();
        let mut fields = vec![Vec::new(); schema.fields().len()];

        for batch_result in reader {
            let batch = batch_result?;
            for f in 0..schema.fields().len() {
                let col = batch.column(f).clone();
                fields[f].push(ArrayRef::from(col));
            }
        }

        let names = schema
            .fields()
            .iter()
            .map(|f| f.name())
            .cloned()
            .map(Arc::new)
            .collect_vec();

        let chunks: VortexResult<Vec<ArrayRef>> = fields
            .into_iter()
            .zip(schema.fields())
            .map(|(field_chunks, arrow_type)| {
                Ok(ChunkedArray::try_new(field_chunks, DType::try_from(arrow_type)?)?.boxed())
            })
            .try_collect();

        Ok(StructArray::new(names, chunks?).boxed())
    }
}

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
                    // In case the schema says something different, we wrap the array with the schema's dtype.
                    TypedArray::maybe_wrap(array.clone().into(), &field.try_into().unwrap())
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
                .map(|f| f.data_type().try_into_dtype(f.is_nullable()))
                .collect::<VortexResult<Vec<DType>>>()?,
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

pub trait TryIntoDType {
    fn try_into_dtype(self, is_nullable: bool) -> VortexResult<DType>;
}

impl TryIntoDType for &DataType {
    fn try_into_dtype(self, is_nullable: bool) -> VortexResult<DType> {
        use crate::dtype::Nullability::*;
        use crate::dtype::Signedness::*;

        let nullability: Nullability = is_nullable.into();

        match self {
            DataType::Null => Ok(Null),
            DataType::Boolean => Ok(Bool(nullability)),
            DataType::Int8 => Ok(Int(IntWidth::_8, Signed, nullability)),
            DataType::Int16 => Ok(Int(IntWidth::_16, Signed, nullability)),
            DataType::Int32 => Ok(Int(IntWidth::_32, Signed, nullability)),
            DataType::Int64 => Ok(Int(IntWidth::_64, Signed, nullability)),
            DataType::UInt8 => Ok(Int(IntWidth::_8, Unsigned, nullability)),
            DataType::UInt16 => Ok(Int(IntWidth::_16, Unsigned, nullability)),
            DataType::UInt32 => Ok(Int(IntWidth::_32, Unsigned, nullability)),
            DataType::UInt64 => Ok(Int(IntWidth::_64, Unsigned, nullability)),
            DataType::Float16 => Ok(Float(FloatWidth::_16, nullability)),
            DataType::Float32 => Ok(Float(FloatWidth::_32, nullability)),
            DataType::Float64 => Ok(Float(FloatWidth::_64, nullability)),
            DataType::Utf8 | DataType::LargeUtf8 => Ok(Utf8(nullability)),
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
                Ok(Binary(nullability))
            }
            // TODO(robert): what to do about this timezone?
            DataType::Timestamp(u, _) => Ok(ZonedDateTime(u.into(), nullability)),
            DataType::Date32 | DataType::Date64 => Ok(LocalDate(nullability)),
            DataType::Time32(u) | DataType::Time64(u) => Ok(LocalTime(u.into(), nullability)),
            DataType::List(e) | DataType::FixedSizeList(e, _) | DataType::LargeList(e) => {
                Ok(List(Box::new(e.try_into()?), nullability))
            }
            DataType::Struct(f) => Ok(Struct(
                f.iter().map(|f| Arc::new(f.name().clone())).collect(),
                f.iter()
                    .map(|f| f.data_type().try_into_dtype(f.is_nullable()))
                    .collect::<VortexResult<Vec<DType>>>()?,
            )),
            DataType::Dictionary(_, v) => v.as_ref().try_into_dtype(is_nullable),
            DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
                Ok(Decimal(*p, *s, nullability))
            }
            DataType::Map(e, _) => match e.data_type() {
                DataType::Struct(f) => Ok(Map(
                    Box::new(f.first().unwrap().try_into()?),
                    Box::new(f.get(1).unwrap().try_into()?),
                    Nullable,
                )),
                _ => Err(VortexError::InvalidArrowDataType(e.data_type().clone())),
            },
            DataType::RunEndEncoded(_, v) => v.try_into(),
            DataType::Duration(_) | DataType::Interval(_) | DataType::Union(_, _) => {
                Err(VortexError::InvalidArrowDataType(self.clone()))
            }
        }
    }
}

impl TryFrom<&FieldRef> for DType {
    type Error = VortexError;

    fn try_from(value: &FieldRef) -> VortexResult<Self> {
        value.data_type().try_into_dtype(value.is_nullable())
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

impl From<DType> for DataType {
    fn from(value: DType) -> Self {
        (&value).into()
    }
}

// TODO(ngates): we probably want to implement this for an arrow Field not a DataType?
impl From<&DType> for DataType {
    fn from(value: &DType) -> Self {
        use crate::dtype::Signedness::*;
        match value {
            Null => DataType::Null,
            Bool(_) => DataType::Boolean,
            Int(w, s, _) => match w {
                IntWidth::Unknown => match s {
                    Unknown => DataType::Int64,
                    Unsigned => DataType::UInt64,
                    Signed => DataType::Int64,
                },
                IntWidth::_8 => match s {
                    Unknown => DataType::Int8,
                    Unsigned => DataType::UInt8,
                    Signed => DataType::Int8,
                },
                IntWidth::_16 => match s {
                    Unknown => DataType::Int16,
                    Unsigned => DataType::UInt16,
                    Signed => DataType::Int16,
                },
                IntWidth::_32 => match s {
                    Unknown => DataType::Int32,
                    Unsigned => DataType::UInt32,
                    Signed => DataType::Int32,
                },
                IntWidth::_64 => match s {
                    Unknown => DataType::Int64,
                    Unsigned => DataType::UInt64,
                    Signed => DataType::Int64,
                },
            },
            Decimal(p, w, _) => DataType::Decimal128(*p, *w),
            Float(w, _) => match w {
                FloatWidth::Unknown => DataType::Float64,
                FloatWidth::_16 => DataType::Float16,
                FloatWidth::_32 => DataType::Float32,
                FloatWidth::_64 => DataType::Float64,
            },
            Utf8(_) => DataType::Utf8,
            Binary(_) => DataType::Binary,
            LocalTime(u, _) => DataType::Time64(match u {
                TimeUnit::Ns => ArrowTimeUnit::Nanosecond,
                TimeUnit::Us => ArrowTimeUnit::Microsecond,
                TimeUnit::Ms => ArrowTimeUnit::Millisecond,
                TimeUnit::S => ArrowTimeUnit::Second,
            }),
            LocalDate(_) => DataType::Date64,
            Instant(u, _) => DataType::Timestamp(
                match u {
                    TimeUnit::Ns => ArrowTimeUnit::Nanosecond,
                    TimeUnit::Us => ArrowTimeUnit::Microsecond,
                    TimeUnit::Ms => ArrowTimeUnit::Millisecond,
                    TimeUnit::S => ArrowTimeUnit::Second,
                },
                None,
            ),
            ZonedDateTime(_, _) => {
                unimplemented!("Converting ZoneDateTime to arrow datatype is not supported")
            }
            Struct(names, dtypes) => DataType::Struct(
                zip(names, dtypes)
                    .map(|(n, dt)| Field::new((**n).clone(), dt.into(), dt.is_nullable()))
                    .collect(),
            ),
            List(c, _) => DataType::List(Arc::new(Field::new(
                "element",
                c.as_ref().into(),
                c.is_nullable(),
            ))),
            Map(k, v, _) => DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", k.as_ref().into(), false),
                        Field::new("value", v.as_ref().into(), v.is_nullable()),
                    ])),
                    false,
                )),
                false,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::dtype::*;

    use super::*;

    #[test]
    fn test_dtype_to_datatype() {
        let dtype = Int(IntWidth::_32, Signedness::Signed, Nullability::Nullable);
        let data_type: DataType = dtype.into();
        assert_eq!(data_type, DataType::Int32);
    }
}
