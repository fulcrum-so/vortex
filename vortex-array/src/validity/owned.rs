use arrow_buffer::{BooleanBuffer, NullBuffer};
use itertools::Itertools;
use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::{DType, Nullability};

use crate::array::bool::BoolArray;
use crate::array::{Array, ArrayRef};
use crate::compute::as_contiguous::as_contiguous;
use crate::view::AsView;

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

    pub fn try_from_logical(
        logical: Validity,
        nullability: Nullability,
    ) -> VortexResult<Option<Self>> {
        match nullability {
            Nullability::NonNullable => {
                if !logical.as_view().all_valid() {
                    vortex_bail!("Non-nullable validity must be all valid");
                }
                Ok(None)
            }
            Nullability::Nullable => Ok(Some(logical)),
        }
    }

    pub fn to_bool_array(&self) -> BoolArray {
        self.as_view().to_bool_array()
    }

    pub fn logical_validity(&self) -> Validity {
        if self.as_view().all_valid() {
            return Validity::Valid(self.len());
        }
        if self.as_view().all_invalid() {
            return Validity::Invalid(self.len());
        }
        self.clone()
    }

    pub fn slice(&self, start: usize, stop: usize) -> Validity {
        self.as_view().slice(start, stop)
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