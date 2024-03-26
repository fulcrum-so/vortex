use std::sync::{Arc, RwLock};

use arrow_buffer::buffer::BooleanBuffer;
use linkme::distributed_slice;

use vortex_error::VortexResult;
use vortex_schema::{DType, Nullability};

use crate::array::IntoArray;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::impl_array;
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{Stat, Stats, StatsSet};
use crate::validity::{ArrayValidity, Validity};

use super::{check_slice_bounds, Array, ArrayRef, Encoding, EncodingId, EncodingRef, ENCODINGS};

mod compute;
mod flatten;
mod serde;
mod stats;

#[derive(Debug, Clone)]
pub struct BoolArray {
    buffer: BooleanBuffer,
    stats: Arc<RwLock<StatsSet>>,
    validity: Option<Validity>,
}

impl BoolArray {
    pub fn new(buffer: BooleanBuffer, validity: Option<Validity>) -> Self {
        Self::try_new(buffer, validity).unwrap()
    }

    pub fn try_new(buffer: BooleanBuffer, validity: Option<Validity>) -> VortexResult<Self> {
        if let Some(v) = &validity {
            assert_eq!(v.len(), buffer.len());
        }
        Ok(Self {
            buffer,
            stats: Arc::new(RwLock::new(StatsSet::new())),
            validity,
        })
    }

    /// Create an all-null boolean array.
    pub fn null(n: usize) -> Self {
        BoolArray::new(
            BooleanBuffer::from(vec![false; n]),
            Some(Validity::invalid(n)),
        )
    }

    #[inline]
    pub fn buffer(&self) -> &BooleanBuffer {
        &self.buffer
    }

    pub fn into_buffer(self) -> BooleanBuffer {
        self.buffer
    }
}

impl Array for BoolArray {
    impl_array!();

    #[inline]
    fn len(&self) -> usize {
        self.buffer.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        if self.validity().is_some() {
            &DType::Bool(Nullability::Nullable)
        } else {
            &DType::Bool(Nullability::NonNullable)
        }
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;

        Ok(Self {
            buffer: self.buffer.slice(start, stop - start),
            stats: Arc::new(RwLock::new(StatsSet::new())),
            validity: self.validity.as_ref().map(|v| v.slice(start, stop)),
        }
        .into_array())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &BoolEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        (self.len() + 7) / 8
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }
}

impl ArrayValidity for BoolArray {
    fn validity(&self) -> Option<Validity> {
        self.validity.clone()
    }
}

#[derive(Debug)]
pub struct BoolEncoding;

impl BoolEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.bool");
}

#[distributed_slice(ENCODINGS)]
static ENCODINGS_BOOL: EncodingRef = &BoolEncoding;

impl Encoding for BoolEncoding {
    fn id(&self) -> EncodingId {
        Self::ID
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}

impl ArrayDisplay for BoolArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        let true_count = self.stats().get_or_compute_or(0usize, &Stat::TrueCount);
        let false_count = self.len() - true_count;
        f.property("n_true", true_count)?;
        f.property("n_false", false_count)?;
        f.validity(self.validity())
    }
}

impl From<Vec<bool>> for BoolArray {
    fn from(value: Vec<bool>) -> Self {
        BoolArray::new(BooleanBuffer::from(value), None)
    }
}

impl IntoArray for Vec<bool> {
    fn into_array(self) -> ArrayRef {
        Arc::new(BoolArray::from(self))
    }
}

impl FromIterator<Option<bool>> for BoolArray {
    fn from_iter<I: IntoIterator<Item = Option<bool>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();

        let mut validity: Vec<bool> = Vec::with_capacity(lower);
        let values: Vec<bool> = iter
            .map(|i| {
                if let Some(v) = i {
                    validity.push(true);
                    v
                } else {
                    validity.push(false);
                    false
                }
            })
            .collect::<Vec<_>>();

        if validity.is_empty() {
            BoolArray::from(values)
        } else {
            BoolArray::new(BooleanBuffer::from(values), Some(Validity::from(validity)))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::compute::scalar_at::scalar_at;

    #[test]
    fn slice() {
        let arr = BoolArray::from(vec![true, true, false, false, true])
            .slice(1, 4)
            .unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(scalar_at(&arr, 0).unwrap().try_into(), Ok(true));
        assert_eq!(scalar_at(&arr, 1).unwrap().try_into(), Ok(false));
        assert_eq!(scalar_at(&arr, 2).unwrap().try_into(), Ok(false));
    }

    #[test]
    fn nbytes() {
        assert_eq!(
            BoolArray::from(vec![true, true, false, false, true]).nbytes(),
            1
        );
    }
}
