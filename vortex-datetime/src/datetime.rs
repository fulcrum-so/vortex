use std::sync::{Arc, RwLock};

use vortex::array::{Array, ArrayRef, Encoding, EncodingId};
use vortex::compress::EncodingCompression;
use vortex::compute::ArrayCompute;
use vortex::error::{VortexError, VortexResult};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::impl_array;
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stats, StatsCompute, StatsSet};
use vortex_schema::DType;

/// An array that decomposes a datetime into days, seconds, and nanoseconds.
#[derive(Debug, Clone)]
pub struct DateTimeArray {
    days: ArrayRef,
    seconds: ArrayRef,
    subsecond: ArrayRef,
    validity: Option<ArrayRef>,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl DateTimeArray {
    pub fn new(
        days: ArrayRef,
        seconds: ArrayRef,
        subsecond: ArrayRef,
        validity: Option<ArrayRef>,
        dtype: DType,
    ) -> Self {
        Self::try_new(days, seconds, subsecond, validity, dtype).unwrap()
    }

    pub fn try_new(
        days: ArrayRef,
        seconds: ArrayRef,
        subsecond: ArrayRef,
        validity: Option<ArrayRef>,
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
    pub fn days(&self) -> &dyn Array {
        self.days.as_ref()
    }

    #[inline]
    pub fn seconds(&self) -> &dyn Array {
        self.seconds.as_ref()
    }

    #[inline]
    pub fn subsecond(&self) -> &dyn Array {
        self.subsecond.as_ref()
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
            self.validity
                .as_ref()
                .map(|v| v.slice(start, stop))
                .transpose()?,
            self.dtype.clone(),
        )
        .into_array())
    }

    fn encoding(&self) -> &'static dyn Encoding {
        &DateTimeEncoding
    }

    fn nbytes(&self) -> usize {
        self.days().nbytes() + self.seconds().nbytes() + self.subsecond().nbytes()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        None
    }
}

impl StatsCompute for DateTimeArray {}

impl ArrayCompute for DateTimeArray {}

impl<'arr> AsRef<(dyn Array + 'arr)> for DateTimeArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for DateTimeArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.child("days", self.days())?;
        f.child("seconds", self.seconds())?;
        f.child("subsecond", self.subsecond())
    }
}

#[derive(Debug)]
pub struct DateTimeEncoding;

pub const DATETIME_ENCODING: EncodingId = EncodingId::new("vortex.datetime");

impl Encoding for DateTimeEncoding {
    fn id(&self) -> &EncodingId {
        &DATETIME_ENCODING
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        None
    }
}
