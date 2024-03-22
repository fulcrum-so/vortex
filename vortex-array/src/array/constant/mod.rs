use std::sync::{Arc, RwLock};

use linkme::distributed_slice;
use vortex_schema::DType;

use crate::array::{
    check_slice_bounds, Array, ArrayRef, Encoding, EncodingId, EncodingRef, ENCODINGS,
};
use crate::error::VortexResult;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::impl_array;
use crate::scalar::Scalar;
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{Stat, Stats, StatsSet};

mod compute;
mod serde;
mod stats;

#[derive(Debug, Clone)]
pub struct ConstantArray {
    scalar: Scalar,
    length: usize,
    stats: Arc<RwLock<StatsSet>>,
}

impl ConstantArray {
    pub fn new(scalar: Scalar, length: usize) -> Self {
        let stats = StatsSet::from(
            [
                (Stat::Max, scalar.clone()),
                (Stat::Min, scalar.clone()),
                (Stat::IsConstant, true.into()),
                (Stat::IsSorted, true.into()),
                (Stat::RunCount, 1.into()),
            ]
            .into(),
        );
        Self {
            scalar,
            length,
            stats: Arc::new(RwLock::new(stats)),
        }
    }

    pub fn scalar(&self) -> &Scalar {
        &self.scalar
    }
}

impl Array for ConstantArray {
    impl_array!();

    #[inline]
    fn len(&self) -> usize {
        self.length
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.length == 0
    }

    #[inline]
    fn dtype(&self) -> &DType {
        self.scalar.dtype()
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;

        Ok(ConstantArray::new(self.scalar.clone(), stop - start).into_array())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &ConstantEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.scalar.nbytes()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }
}

impl ArrayDisplay for ConstantArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.property("scalar", self.scalar())
    }
}

#[derive(Debug)]
pub struct ConstantEncoding;

impl ConstantEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.constant");
}

#[distributed_slice(ENCODINGS)]
static ENCODINGS_CONSTANT: EncodingRef = &ConstantEncoding;

impl Encoding for ConstantEncoding {
    fn id(&self) -> &EncodingId {
        &Self::ID
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}
