use std::fmt::{Display, Formatter};
use std::sync::Arc;

use arrow_array::{
    ArrayRef as ArrowArrayRef, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};

use vortex_error::VortexResult;
use vortex_schema::CompositeID;

use crate::array::composite::{composite_impl, TypedCompositeArray};
use crate::arrow::wrappers::as_nulls;
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::cast::cast;
use crate::compute::flatten::flatten_primitive;
use crate::datetime::TimeUnit;
use crate::ptype::PType;
use crate::serde::BytesSerde;
use crate::validity::ArrayValidity;

#[derive(Debug, Clone)]
pub struct LocalDateTime {
    time_unit: TimeUnit,
}

composite_impl!("vortex.localdatetime", LocalDateTime);

impl LocalDateTime {
    pub fn new(time_unit: TimeUnit) -> Self {
        Self { time_unit }
    }

    #[inline]
    pub fn time_unit(&self) -> TimeUnit {
        self.time_unit
    }
}

impl Display for LocalDateTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.time_unit)
    }
}

impl BytesSerde for LocalDateTime {
    fn serialize(&self) -> Vec<u8> {
        self.time_unit.serialize()
    }

    fn deserialize(metadata: &[u8]) -> VortexResult<Self> {
        TimeUnit::deserialize(metadata).map(Self::new)
    }
}

pub type LocalDateTimeArray = TypedCompositeArray<LocalDateTime>;

impl ArrayCompute for LocalDateTimeArray {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        Some(self)
    }
}

impl AsArrowArray for LocalDateTimeArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        // A LocalDateTime maps to an Arrow Timestamp array with no timezone.
        let timestamps = flatten_primitive(cast(self.underlying(), PType::I64.into())?.as_ref())?;
        let validity = as_nulls(timestamps.validity())?;
        let buffer = timestamps.scalar_buffer::<i64>();

        Ok(match self.metadata().time_unit {
            TimeUnit::Ns => Arc::new(TimestampNanosecondArray::new(buffer, validity)),
            TimeUnit::Us => Arc::new(TimestampMicrosecondArray::new(buffer, validity)),
            TimeUnit::Ms => Arc::new(TimestampMillisecondArray::new(buffer, validity)),
            TimeUnit::S => Arc::new(TimestampSecondArray::new(buffer, validity)),
        })
    }
}
