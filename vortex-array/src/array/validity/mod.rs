use std::sync::Arc;

use arrow_buffer::{BooleanBuffer, NullBuffer};
use itertools::Itertools;
use linkme::distributed_slice;
use vortex_error::VortexResult;
use vortex_schema::{DType, Nullability};

use crate::array::bool::BoolArray;
use crate::array::{Array, ArrayRef};
use crate::compute::as_contiguous::as_contiguous;
use crate::encoding::{Encoding, EncodingId, EncodingRef, ENCODINGS};
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::stats::Stats;
use crate::{impl_array, impl_array_compute, ArrayWalker};
mod serde;
mod view;

pub use view::*;

use crate::compute::ArrayCompute;
use crate::serde::{ArraySerde, EncodingSerde};

#[derive(Debug, Clone)]
pub enum Validity {
    Valid(usize),
    Invalid(usize),
    Array(ArrayRef),
}

impl Validity {
    pub const DTYPE: DType = DType::Bool(Nullability::NonNullable);

    pub fn array(array: ArrayRef) -> Self {
        if !matches!(array.dtype(), &Validity::DTYPE) {
            panic!("Validity array must be of type bool");
        }
        Self::Array(array)
    }

    pub fn to_bool_array(&self) -> BoolArray {
        self.as_view().to_bool_array()
    }

    pub fn slice(&self, start: usize, stop: usize) -> Validity {
        self.as_view().slice(start, stop)
    }

    pub fn as_view(&self) -> ValidityView {
        match self {
            Self::Valid(len) => ValidityView::Valid(*len),
            Self::Invalid(len) => ValidityView::Invalid(*len),
            Self::Array(a) => ValidityView::Array(a.as_ref()),
        }
    }
}

impl From<NullBuffer> for Validity {
    fn from(value: NullBuffer) -> Self {
        if value.null_count() == 0 {
            Self::Valid(value.len())
        } else if value.null_count() == value.len() {
            Self::Invalid(value.len())
        } else {
            Self::Array(BoolArray::new(value.into_inner(), None).into_array())
        }
    }
}

impl From<BooleanBuffer> for Validity {
    fn from(value: BooleanBuffer) -> Self {
        if value.iter().all(|v| v) {
            Self::Valid(value.len())
        } else if value.iter().all(|v| !v) {
            Self::Invalid(value.len())
        } else {
            Self::Array(BoolArray::new(value, None).into_array())
        }
    }
}

impl From<Vec<bool>> for Validity {
    fn from(value: Vec<bool>) -> Self {
        if value.iter().all(|v| *v) {
            Self::Valid(value.len())
        } else if value.iter().all(|v| !*v) {
            Self::Invalid(value.len())
        } else {
            Self::Array(BoolArray::from(value).into_array())
        }
    }
}

impl PartialEq<Self> for Validity {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }

        match (self, other) {
            (Self::Valid(_), Self::Valid(_)) => true,
            (Self::Invalid(_), Self::Invalid(_)) => true,
            _ => {
                // TODO(ngates): use compute to dispatch an all() function.
                self.to_bool_array().buffer() == other.to_bool_array().buffer()
            }
        }
    }
}

impl Eq for Validity {}

impl FromIterator<Validity> for Validity {
    fn from_iter<T: IntoIterator<Item = Validity>>(iter: T) -> Self {
        let validities: Vec<Validity> = iter.into_iter().collect();
        let total_len = validities.iter().map(|v| v.len()).sum();

        // If they're all valid, then return a single validity.
        if validities.iter().all(|v| v.as_view().all_valid()) {
            return Self::Valid(total_len);
        }
        // If they're all invalid, then return a single invalidity.
        if validities.iter().all(|v| v.as_view().all_invalid()) {
            return Self::Invalid(total_len);
        }

        // Otherwise, map each to a bool array and concatenate them.
        let arrays = validities
            .iter()
            .map(|v| v.to_bool_array().into_array())
            .collect_vec();
        Self::Array(as_contiguous(&arrays).unwrap())
    }
}

impl Array for Validity {
    impl_array!();
    impl_array_compute!();

    fn len(&self) -> usize {
        match self {
            Validity::Valid(len) | Validity::Invalid(len) => *len,
            Validity::Array(a) => a.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Validity::Valid(len) | Validity::Invalid(len) => *len == 0,
            Validity::Array(a) => a.is_empty(),
        }
    }

    fn dtype(&self) -> &DType {
        &Validity::DTYPE
    }

    fn stats(&self) -> Stats {
        todo!()
    }

    fn validity(&self) -> Option<Validity> {
        None
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        Ok(self.slice(start, stop).into_array())
    }

    fn encoding(&self) -> EncodingRef {
        &ValidityEncoding
    }

    fn nbytes(&self) -> usize {
        match self {
            Validity::Valid(_) | Validity::Invalid(_) => 8,
            Validity::Array(a) => a.nbytes(),
        }
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }

    fn walk(&self, _walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        Ok(())
    }
}

impl ArrayDisplay for Validity {
    fn fmt(&self, fmt: &'_ mut ArrayFormatter) -> std::fmt::Result {
        match self {
            Validity::Valid(_) => fmt.property("all", "valid"),
            Validity::Invalid(_) => fmt.property("all", "invalid"),
            Validity::Array(a) => fmt.child("validity", a),
        }
    }
}

impl ArrayCompute for Validity {}

#[distributed_slice(ENCODINGS)]
static ENCODINGS_VALIDITY: EncodingRef = &ValidityEncoding;

#[derive(Debug)]
struct ValidityEncoding;

impl ValidityEncoding {
    const ID: EncodingId = EncodingId::new("vortex.validity");
}

impl Encoding for ValidityEncoding {
    fn id(&self) -> EncodingId {
        ValidityEncoding::ID
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}
