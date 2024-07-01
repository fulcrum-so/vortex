use std::fmt::{Display, Formatter};
use std::hash::Hash;

use enum_iterator::Sequence;
use itertools::Itertools;
pub use statsset::*;
use vortex_dtype::Nullability::NonNullable;
use vortex_dtype::{DType, NativePType};
use vortex_error::{VortexError, VortexResult};
use vortex_scalar::Scalar;

use crate::Array;

pub mod flatbuffers;
mod statsset;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Sequence)]
pub enum Stat {
    BitWidthFreq,
    TrailingZeroFreq,
    IsConstant,
    IsSorted,
    IsStrictSorted,
    Max,
    Min,
    RunCount,
    TrueCount,
    NullCount,
}

impl Display for Stat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BitWidthFreq => write!(f, "bit_width_frequency"),
            Self::TrailingZeroFreq => write!(f, "trailing_zero_frequency"),
            Self::IsConstant => write!(f, "is_constant"),
            Self::IsSorted => write!(f, "is_sorted"),
            Self::IsStrictSorted => write!(f, "is_strict_sorted"),
            Self::Max => write!(f, "max"),
            Self::Min => write!(f, "min"),
            Self::RunCount => write!(f, "run_count"),
            Self::TrueCount => write!(f, "true_count"),
            Self::NullCount => write!(f, "null_count"),
        }
    }
}

pub trait Statistics {
    /// Returns the value of the statistic only if it's present
    fn get(&self, stat: Stat) -> Option<Scalar>;

    /// Get all existing statistics
    fn to_set(&self) -> StatsSet;

    fn set(&self, stat: Stat, value: Scalar);

    /// Computes the value of the stat if it's not present
    fn compute(&self, stat: Stat) -> Option<Scalar>;
}

pub struct EmptyStatistics;

impl Statistics for EmptyStatistics {
    fn get(&self, _stat: Stat) -> Option<Scalar> {
        None
    }

    fn to_set(&self) -> StatsSet {
        StatsSet::new()
    }

    fn set(&self, _stat: Stat, _value: Scalar) {}

    fn compute(&self, _stat: Stat) -> Option<Scalar> {
        None
    }
}

pub trait ArrayStatistics {
    fn statistics(&self) -> &dyn Statistics;
}

pub trait ArrayStatisticsCompute {
    /// Compute the requested statistic. Can return additional stats.
    fn compute_statistics(&self, _stat: Stat) -> VortexResult<StatsSet> {
        Ok(StatsSet::new())
    }
}

impl dyn Statistics + '_ {
    pub fn get_as<U: for<'a> TryFrom<&'a Scalar, Error = VortexError>>(
        &self,
        stat: Stat,
    ) -> Option<U> {
        self.get(stat)
            .map(|s| U::try_from(&s).expect("Invalid stats cast"))
    }

    pub fn get_as_cast<U: NativePType + for<'a> TryFrom<&'a Scalar, Error = VortexError>>(
        &self,
        stat: Stat,
    ) -> Option<U> {
        self.get(stat)
            .map(|s| {
                s.cast(&DType::Primitive(U::PTYPE, NonNullable))
                    .expect("Invalid scalar cast")
            })
            .map(|s| U::try_from(&s).expect("Invalid stats cast"))
    }

    pub fn compute_as<U: for<'a> TryFrom<&'a Scalar, Error = VortexError>>(
        &self,
        stat: Stat,
    ) -> Option<U> {
        self.compute(stat)
            .map(|s| U::try_from(&s).expect("Invalid stats cast"))
    }

    pub fn compute_as_cast<U: NativePType + for<'a> TryFrom<&'a Scalar, Error = VortexError>>(
        &self,
        stat: Stat,
    ) -> Option<U> {
        self.compute(stat)
            .map(|s| {
                s.cast(&DType::Primitive(U::PTYPE, NonNullable))
                    .expect("Invalid scalar cast")
            })
            .map(|s| U::try_from(&s).expect("Invalid stats cast"))
    }

    pub fn compute_min<U: for<'a> TryFrom<&'a Scalar, Error = VortexError>>(&self) -> Option<U> {
        self.compute_as(Stat::Min)
    }

    pub fn compute_max<U: for<'a> TryFrom<&'a Scalar, Error = VortexError>>(&self) -> Option<U> {
        self.compute_as(Stat::Max)
    }

    pub fn compute_is_strict_sorted(&self) -> Option<bool> {
        self.compute_as(Stat::IsStrictSorted)
    }

    pub fn compute_is_sorted(&self) -> Option<bool> {
        self.compute_as(Stat::IsSorted)
    }

    pub fn compute_is_constant(&self) -> Option<bool> {
        self.compute_as(Stat::IsConstant)
    }

    pub fn compute_true_count(&self) -> Option<usize> {
        self.compute_as(Stat::TrueCount)
    }

    pub fn compute_null_count(&self) -> Option<usize> {
        self.compute_as(Stat::NullCount)
    }

    pub fn compute_run_count(&self) -> Option<usize> {
        self.compute_as(Stat::RunCount)
    }

    pub fn compute_bit_width_freq(&self) -> Option<Vec<usize>> {
        self.compute_as::<Vec<usize>>(Stat::BitWidthFreq)
    }

    pub fn compute_trailing_zero_freq(&self) -> Option<Vec<usize>> {
        self.compute_as::<Vec<usize>>(Stat::TrailingZeroFreq)
    }
}

pub fn trailing_zeros(array: &Array) -> u8 {
    let tz_freq = array
        .statistics()
        .compute_trailing_zero_freq()
        .unwrap_or_else(|| vec![0]);
    tz_freq
        .iter()
        .enumerate()
        .find_or_first(|(_, &v)| v > 0)
        .map(|(i, _)| i)
        .unwrap_or(0) as u8
}
