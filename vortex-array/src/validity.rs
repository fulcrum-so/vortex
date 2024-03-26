use crate::array::bool::BoolArray;
use crate::array::constant::ConstantArray;
use crate::array::{Array, ArrayRef};
use crate::compute::as_contiguous::as_contiguous;
use crate::compute::flatten::flatten_bool;
use crate::compute::scalar_at::scalar_at;
use crate::stats::Stat;
use arrow_buffer::BooleanBuffer;
use vortex_schema::{DType, Nullability};

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

    pub fn invalid(len: usize) -> Self {
        Self::Invalid(len)
    }

    pub fn valid(len: usize) -> Self {
        Self::Valid(len)
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Valid(len) | Self::Invalid(len) => *len,
            Self::Array(a) => a.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Valid(len) | Self::Invalid(len) => *len == 0,
            Validity::Array(a) => a.is_empty(),
        }
    }

    pub fn all_valid(&self) -> bool {
        match self {
            Self::Valid(_) => true,
            Self::Invalid(_) => false,
            Self::Array(a) => a
                .stats()
                .get_or_compute_as::<usize>(&Stat::TrueCount)
                .map(|true_count| true_count == self.len())
                .unwrap_or(false),
        }
    }

    pub fn all_invalid(&self) -> bool {
        match self {
            Self::Valid(_) => false,
            Self::Invalid(_) => true,
            Self::Array(a) => a
                .stats()
                .get_or_compute_as::<usize>(&Stat::TrueCount)
                .map(|true_count| true_count == 0)
                .unwrap_or(false),
        }
    }

    pub fn to_array(&self) -> ArrayRef {
        match self {
            Self::Valid(len) => ConstantArray::new(true, *len).into_array(),
            Self::Invalid(len) => ConstantArray::new(false, *len).into_array(),
            Self::Array(a) => a.clone(),
        }
    }

    pub fn to_bool_array(&self) -> BoolArray {
        match self {
            Self::Valid(len) => BoolArray::from(vec![true; *len]),
            Self::Invalid(len) => BoolArray::from(vec![false; *len]),
            Self::Array(a) => flatten_bool(a).unwrap(),
        }
    }

    pub fn logical_validity(&self) -> Option<Validity> {
        match self.all_valid() {
            true => None,
            false => Some(self.clone()),
        }
    }

    // TODO(ngates): maybe we want to impl Array for Validity?
    pub fn slice(&self, start: usize, stop: usize) -> Self {
        match self {
            Self::Valid(_) => Self::valid(stop - start),
            Self::Invalid(_) => Self::invalid(stop - start),
            Self::Array(a) => Self::Array(a.slice(start, stop).unwrap()),
        }
    }

    pub fn nbytes(&self) -> usize {
        match self {
            Self::Valid(_) | Self::Invalid(_) => 4,
            Self::Array(a) => a.nbytes(),
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
        if validities.iter().all(|v| v.all_valid()) {
            return Self::Valid(total_len);
        }
        // If they're all invalid, then return a single invalidity.
        if validities.iter().all(|v| v.all_invalid()) {
            return Self::Invalid(total_len);
        }

        // Otherwise, map each to a bool array and concatenate them.
        Self::Array(
            as_contiguous(
                validities
                    .iter()
                    .map(|v| v.to_bool_array().into_array())
                    .collect(),
            )
            .unwrap(),
        )
    }
}

pub trait ArrayValidity {
    fn nullability(&self) -> Nullability {
        self.validity().is_some().into()
    }

    fn validity(&self) -> Option<Validity>;

    fn logical_validity(&self) -> Option<Validity> {
        self.validity().and_then(|v| v.logical_validity())
    }

    fn is_valid(&self, index: usize) -> bool {
        if let Some(v) = self.validity() {
            match v {
                Validity::Valid(_) => true,
                Validity::Invalid(_) => false,
                Validity::Array(a) => scalar_at(&a, index).unwrap().try_into().unwrap(),
            }
        } else {
            true
        }
    }
}
