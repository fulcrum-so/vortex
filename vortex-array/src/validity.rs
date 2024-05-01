use arrow_buffer::{BooleanBuffer, NullBuffer};
use serde::{Deserialize, Serialize};
use vortex_dtype::{DType, Nullability};
use vortex_error::{vortex_bail, VortexResult};

use crate::array::bool::BoolArray;
use crate::compute::as_contiguous::as_contiguous;
use crate::compute::scalar_at::scalar_at;
use crate::compute::slice::slice;
use crate::compute::take::take;
use crate::stats::ArrayStatistics;
use crate::{Array, ArrayData, IntoArray, IntoArrayData, OwnedArray, ToArray, ToArrayData};

pub trait ArrayValidity {
    fn is_valid(&self, index: usize) -> bool;
    fn logical_validity(&self) -> LogicalValidity;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ValidityMetadata {
    NonNullable,
    AllValid,
    AllInvalid,
    Array,
}

impl ValidityMetadata {
    pub fn to_validity<'v>(&self, array: Option<Array<'v>>) -> Validity<'v> {
        match self {
            ValidityMetadata::NonNullable => Validity::NonNullable,
            ValidityMetadata::AllValid => Validity::AllValid,
            ValidityMetadata::AllInvalid => Validity::AllInvalid,
            ValidityMetadata::Array => match array {
                None => panic!("Missing validity array"),
                Some(a) => Validity::Array(a),
            },
        }
    }
}

pub type OwnedValidity = Validity<'static>;

#[derive(Clone, Debug)]
pub enum Validity<'v> {
    NonNullable,
    AllValid,
    AllInvalid,
    Array(Array<'v>),
}

impl<'v> Validity<'v> {
    pub const DTYPE: DType = DType::Bool(Nullability::NonNullable);

    pub fn into_array_data(self) -> Option<ArrayData> {
        match self {
            Validity::Array(a) => Some(a.into_array_data()),
            _ => None,
        }
    }

    pub fn to_metadata(&self, length: usize) -> VortexResult<ValidityMetadata> {
        match self {
            Validity::NonNullable => Ok(ValidityMetadata::NonNullable),
            Validity::AllValid => Ok(ValidityMetadata::AllValid),
            Validity::AllInvalid => Ok(ValidityMetadata::AllInvalid),
            Validity::Array(a) => {
                // We force the caller to validate the length here.
                let validity_len = a.with_dyn(|a| a.len());
                if validity_len != length {
                    vortex_bail!(
                        "Validity array length {} doesn't match array length {}",
                        validity_len,
                        length
                    )
                }
                Ok(ValidityMetadata::Array)
            }
        }
    }

    pub fn array(&self) -> Option<&Array> {
        match self {
            Validity::Array(a) => Some(a),
            _ => None,
        }
    }

    pub fn nullability(&self) -> Nullability {
        match self {
            Validity::NonNullable => Nullability::NonNullable,
            _ => Nullability::Nullable,
        }
    }

    pub fn is_valid(&self, index: usize) -> bool {
        match self {
            Validity::NonNullable | Validity::AllValid => true,
            Validity::AllInvalid => false,
            Validity::Array(a) => scalar_at(a, index).unwrap().try_into().unwrap(),
        }
    }

    pub fn slice(&self, start: usize, stop: usize) -> VortexResult<Validity> {
        match self {
            Validity::Array(a) => Ok(Validity::Array(slice(a, start, stop)?)),
            _ => Ok(self.clone()),
        }
    }

    pub fn take(&self, indices: &Array) -> VortexResult<Validity> {
        match self {
            Validity::NonNullable => Ok(Validity::NonNullable),
            Validity::AllValid => Ok(Validity::AllValid),
            Validity::AllInvalid => Ok(Validity::AllInvalid),
            Validity::Array(a) => Ok(Validity::Array(take(a, indices)?)),
        }
    }

    pub fn to_logical(&self, length: usize) -> LogicalValidity {
        match self {
            Validity::NonNullable => LogicalValidity::AllValid(length),
            Validity::AllValid => LogicalValidity::AllValid(length),
            Validity::AllInvalid => LogicalValidity::AllInvalid(length),
            Validity::Array(a) => {
                // Logical validity should map into AllValid/AllInvalid where possible.
                if a.statistics().compute_min::<bool>().unwrap_or(false) {
                    LogicalValidity::AllValid(length)
                } else if a
                    .statistics()
                    .compute_max::<bool>()
                    .map(|m| !m)
                    .unwrap_or(false)
                {
                    LogicalValidity::AllInvalid(length)
                } else {
                    LogicalValidity::Array(a.to_array_data())
                }
            }
        }
    }

    pub fn to_static(&self) -> OwnedValidity {
        match self {
            Validity::NonNullable => Validity::NonNullable,
            Validity::AllValid => Validity::AllValid,
            Validity::AllInvalid => Validity::AllInvalid,
            Validity::Array(a) => Validity::Array(a.to_array_data().into_array()),
        }
    }
}

impl PartialEq for Validity<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Validity::NonNullable, Validity::NonNullable) => true,
            (Validity::AllValid, Validity::AllValid) => true,
            (Validity::AllInvalid, Validity::AllInvalid) => true,
            (Validity::Array(a), Validity::Array(b)) => {
                a.clone().flatten_bool().unwrap().boolean_buffer()
                    == b.clone().flatten_bool().unwrap().boolean_buffer()
            }
            _ => false,
        }
    }
}

impl From<Vec<bool>> for OwnedValidity {
    fn from(bools: Vec<bool>) -> Self {
        if bools.iter().all(|b| *b) {
            Validity::AllValid
        } else if !bools.iter().any(|b| *b) {
            Validity::AllInvalid
        } else {
            Validity::Array(BoolArray::from_vec(bools, Validity::NonNullable).into_array())
        }
    }
}

impl From<BooleanBuffer> for OwnedValidity {
    fn from(value: BooleanBuffer) -> Self {
        if value.count_set_bits() == value.len() {
            Validity::AllValid
        } else if value.count_set_bits() == 0 {
            Validity::AllInvalid
        } else {
            Validity::Array(BoolArray::from(value).into_array())
        }
    }
}

impl From<NullBuffer> for OwnedValidity {
    fn from(value: NullBuffer) -> Self {
        value.into_inner().into()
    }
}

impl FromIterator<LogicalValidity> for OwnedValidity {
    fn from_iter<T: IntoIterator<Item = LogicalValidity>>(iter: T) -> Self {
        let validities: Vec<LogicalValidity> = iter.into_iter().collect();

        // If they're all valid, then return a single validity.
        if validities.iter().all(|v| v.all_valid()) {
            return Self::AllValid;
        }
        // If they're all invalid, then return a single invalidity.
        if validities.iter().all(|v| v.all_invalid()) {
            return Self::AllInvalid;
        }

        // Otherwise, map each to a bool array and concatenate them.
        let arrays = validities
            .iter()
            .map(|v| {
                v.to_present_null_buffer()
                    .unwrap()
                    .into_array_data()
                    .into_array()
            })
            .collect::<Vec<_>>();
        Self::Array(as_contiguous(&arrays).unwrap())
    }
}

impl<'a, E> FromIterator<&'a Option<E>> for OwnedValidity {
    fn from_iter<T: IntoIterator<Item = &'a Option<E>>>(iter: T) -> Self {
        let bools: Vec<bool> = iter.into_iter().map(|option| option.is_some()).collect();
        Validity::from(bools)
    }
}

#[derive(Clone, Debug)]
pub enum LogicalValidity {
    AllValid(usize),
    AllInvalid(usize),
    Array(ArrayData),
}

impl LogicalValidity {
    pub fn to_null_buffer(&self) -> VortexResult<Option<NullBuffer>> {
        match self {
            LogicalValidity::AllValid(_) => Ok(None),
            LogicalValidity::AllInvalid(l) => Ok(Some(NullBuffer::new_null(*l))),
            LogicalValidity::Array(a) => Ok(Some(NullBuffer::new(
                a.to_array().flatten_bool()?.boolean_buffer(),
            ))),
        }
    }

    pub fn to_present_null_buffer(&self) -> VortexResult<NullBuffer> {
        match self {
            LogicalValidity::AllValid(l) => Ok(NullBuffer::new_valid(*l)),
            LogicalValidity::AllInvalid(l) => Ok(NullBuffer::new_null(*l)),
            LogicalValidity::Array(a) => Ok(NullBuffer::new(
                a.to_array().flatten_bool()?.boolean_buffer(),
            )),
        }
    }

    pub fn all_valid(&self) -> bool {
        matches!(self, LogicalValidity::AllValid(_))
    }

    pub fn all_invalid(&self) -> bool {
        matches!(self, LogicalValidity::AllInvalid(_))
    }

    pub fn len(&self) -> usize {
        match self {
            LogicalValidity::AllValid(n) => *n,
            LogicalValidity::AllInvalid(n) => *n,
            LogicalValidity::Array(a) => a.to_array().len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            LogicalValidity::AllValid(n) => *n == 0,
            LogicalValidity::AllInvalid(n) => *n == 0,
            LogicalValidity::Array(a) => a.to_array().is_empty(),
        }
    }

    pub fn into_validity<'a>(self) -> Validity<'a> {
        match self {
            LogicalValidity::AllValid(_) => Validity::AllValid,
            LogicalValidity::AllInvalid(_) => Validity::AllInvalid,
            LogicalValidity::Array(a) => Validity::Array(a.into_array()),
        }
    }
}

impl IntoArray<'static> for LogicalValidity {
    fn into_array(self) -> OwnedArray {
        match self {
            LogicalValidity::AllValid(len) => BoolArray::from(vec![true; len]).into_array(),
            LogicalValidity::AllInvalid(len) => BoolArray::from(vec![false; len]).into_array(),
            LogicalValidity::Array(a) => a.into_array(),
        }
    }
}
