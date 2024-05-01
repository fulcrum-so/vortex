use arrow_array::{
    ArrayRef as ArrowArrayRef, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use vortex_dtype::{ExtDType, ExtID, Nullability, PType};

use crate::array::datetime::TimeUnit;
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::cast::cast;
use crate::compute::ArrayCompute;
use crate::stats::ArrayStatisticsCompute;
use crate::validity::{ArrayValidity, LogicalValidity};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::{impl_encoding, ArrayDType, ArrayFlatten, FlattenedExtension, IntoArrayData};

impl_encoding!("vortex.localdatetime", LocalDateTime);

lazy_static! {
    static ref ID: ExtID = ExtID::from("vortex.localdatetime");
}

impl LocalDateTime {
    pub fn dtype(time_unit: TimeUnit, nullability: Nullability) -> DType {
        DType::Extension(
            ExtDType::new(ID.clone(), Some(time_unit.metadata().clone())),
            nullability,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalDateTimeMetadata {
    timestamp_dtype: DType,
}

impl LocalDateTimeArray<'_> {
    pub fn new(time_unit: TimeUnit, timestamps: Array) -> Self {
        Self::try_from_parts(
            LocalDateTime::dtype(time_unit, timestamps.dtype().nullability()),
            LocalDateTimeMetadata {
                timestamp_dtype: timestamps.dtype().clone(),
            },
            [timestamps.into_array_data()].into(),
            Default::default(),
        )
        .expect("Invalid LocalDateTimeArray")
    }

    pub fn time_unit(&self) -> TimeUnit {
        let DType::Extension(ext, _) = self.dtype() else {
            unreachable!();
        };
        let byte: [u8; 1] = ext
            .metadata()
            .expect("Missing metadata")
            .as_ref()
            .try_into()
            .expect("Invalid metadata");
        TimeUnit::try_from(byte[0]).expect("Invalid time unit")
    }

    pub fn timestamp(&self) -> Array {
        self.array()
            .child(0, &self.metadata().timestamp_dtype)
            .expect("Missing timestamp array")
    }
}

impl ArrayCompute for LocalDateTimeArray<'_> {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        Some(self)
    }
}

impl AsArrowArray for LocalDateTimeArray<'_> {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        // A LocalDateTime maps to an Arrow Timestamp array with no timezone.
        let timestamps = cast(&self.timestamp(), PType::I64.into())?.flatten_primitive()?;
        let validity = timestamps.logical_validity().to_null_buffer()?;
        let buffer = timestamps.scalar_buffer::<i64>();

        Ok(match self.time_unit() {
            TimeUnit::Ns => Arc::new(TimestampNanosecondArray::new(buffer, validity)),
            TimeUnit::Us => Arc::new(TimestampMicrosecondArray::new(buffer, validity)),
            TimeUnit::Ms => Arc::new(TimestampMillisecondArray::new(buffer, validity)),
            TimeUnit::S => Arc::new(TimestampSecondArray::new(buffer, validity)),
        })
    }
}

impl ArrayFlatten for LocalDateTimeArray<'_> {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a,
    {
        Ok(Flattened::Extension(FlattenedExtension::try_new(
            self.into_array(),
        )?))
    }
}

impl ArrayValidity for LocalDateTimeArray<'_> {
    fn is_valid(&self, index: usize) -> bool {
        self.timestamp().with_dyn(|a| a.is_valid(index))
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.timestamp().with_dyn(|a| a.logical_validity())
    }
}

impl AcceptArrayVisitor for LocalDateTimeArray<'_> {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("timestamp", &self.timestamp())
    }
}

impl ArrayStatisticsCompute for LocalDateTimeArray<'_> {
    // TODO(ngates): delegate all stats compute to timestamp array, then wrap in ext dtype.
}

impl ArrayTrait for LocalDateTimeArray<'_> {
    fn len(&self) -> usize {
        self.timestamp().len()
    }
}

impl EncodingCompression for LocalDateTimeEncoding {}
