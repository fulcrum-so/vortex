use std::fmt::{Display, Formatter};
use std::sync::Arc;

use arrow_array::{
    ArrayRef as ArrowArrayRef, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};

use crate::array::composite::typed::{composite_impl, TypedCompositeArray};
use crate::array::composite::CompositeID;
/// Arrow Datetime Types
/// time32/64 - time of day
///   => LocalTime
/// date32 - days since unix epoch
/// date64 - millis since unix epoch
///   => LocalDate
/// timestamp(unit, tz)
///   => Instant iff tz == UTC
///   => ZonedDateTime(Instant, tz)
/// timestamp(unit)
///   => LocalDateTime (tz is "unknown", not "UTC")
/// duration
///   => Duration
use crate::array::Array;
use crate::arrow::wrappers::as_nulls;
use crate::composite_dtypes::TimeUnit;
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::cast::cast;
use crate::compute::flatten::{flatten_primitive, FlattenFn};
use crate::compute::ArrayCompute;
use crate::error::VortexResult;
use crate::ptype::PType;
use crate::serde::BytesSerde;

#[derive(Debug, Clone)]
pub struct LocalDateTime {
    time_unit: TimeUnit,
}

composite_impl!("vortex.localdatetime", LocalDateTime);

impl LocalDateTime {
    pub fn new(time_unit: TimeUnit) -> Self {
        Self { time_unit }
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

    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }
}

impl AsArrowArray for LocalDateTimeArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        // A LocalDateTime maps to an Arrow Timestamp array with no timezone.
        let timestamps = flatten_primitive(cast(self.underlying(), &PType::I64.into())?.as_ref())?;
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