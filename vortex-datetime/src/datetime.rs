use std::sync::{Arc, RwLock};

use vortex::array::{Array, ArrayRef, Encoding, EncodingId, EncodingRef};
use vortex::compress::EncodingCompression;
use vortex::compute::ArrayCompute;
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::impl_array;
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stats, StatsCompute, StatsSet};
use vortex::validity::{ArrayValidity, Validity};
use vortex_error::{VortexError, VortexResult};
use vortex_schema::DType;

/// An array that decomposes a datetime into days, seconds, and nanoseconds.
#[derive(Debug, Clone)]
pub struct DateTimeArray {
    days: ArrayRef,
    seconds: ArrayRef,
    subsecond: ArrayRef,
    validity: Option<Validity>,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl DateTimeArray {
    pub fn new(
        days: ArrayRef,
        seconds: ArrayRef,
        subsecond: ArrayRef,
        validity: Option<Validity>,
        dtype: DType,
    ) -> Self {
        Self::try_new(days, seconds, subsecond, validity, dtype).unwrap()
    }

    pub fn try_new(
        days: ArrayRef,
        seconds: ArrayRef,
        subsecond: ArrayRef,
        validity: Option<Validity>,
        dtype: DType,
    ) -> VortexResult<Self> {
        if !matches!(days.dtype(), DType::Int(_, _, _)) {
            return Err(VortexError::InvalidDType(days.dtype().clone()));
        }
        if !matches!(seconds.dtype(), DType::Int(_, _, _)) {
            return Err(VortexError::InvalidDType(seconds.dtype().clone()));
        }
        if !matches!(subsecond.dtype(), DType::Int(_, _, _)) {
            return Err(VortexError::InvalidDType(subsecond.dtype().clone()));
        }

        Ok(Self {
            days,
            seconds,
            subsecond,
            validity,
            dtype,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    #[inline]
    pub fn days(&self) -> &ArrayRef {
        &self.days
    }

    #[inline]
    pub fn seconds(&self) -> &ArrayRef {
        &self.seconds
    }

    #[inline]
    pub fn subsecond(&self) -> &ArrayRef {
        &self.subsecond
    }
}

impl Array for DateTimeArray {
    impl_array!();

    fn len(&self) -> usize {
        self.days.len()
    }

    fn is_empty(&self) -> bool {
        self.days.is_empty()
    }

    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        Ok(Self::new(
            self.days.slice(start, stop)?,
            self.seconds.slice(start, stop)?,
            self.subsecond.slice(start, stop)?,
            self.validity().map(|v| v.slice(start, stop)),
            self.dtype.clone(),
        )
        .into_array())
    }

    fn encoding(&self) -> EncodingRef {
        &DateTimeEncoding
    }

    fn nbytes(&self) -> usize {
        self.days().nbytes() + self.seconds().nbytes() + self.subsecond().nbytes()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }
}

impl StatsCompute for DateTimeArray {}

impl ArrayCompute for DateTimeArray {}

impl ArrayDisplay for DateTimeArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.child("days", self.days())?;
        f.child("seconds", self.seconds())?;
        f.child("subsecond", self.subsecond())
    }
}

impl ArrayValidity for DateTimeArray {
    fn validity(&self) -> Option<Validity> {
        self.validity.clone()
    }
}

#[derive(Debug)]
pub struct DateTimeEncoding;

impl DateTimeEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.datetime");
}

impl Encoding for DateTimeEncoding {
    fn id(&self) -> EncodingId {
        Self::ID
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}
