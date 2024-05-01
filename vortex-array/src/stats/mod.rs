use std::fmt::{Display, Formatter};
use std::hash::Hash;

use enum_iterator::Sequence;
pub use statsset::*;
use vortex_dtype::{DType, NativePType};
use vortex_error::{VortexError, VortexResult};
use vortex_scalar::{ListScalarVec, Scalar};

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
            Stat::BitWidthFreq => write!(f, "bit_width_frequency"),
            Stat::TrailingZeroFreq => write!(f, "trailing_zero_frequency"),
            Stat::IsConstant => write!(f, "is_constant"),
            Stat::IsSorted => write!(f, "is_sorted"),
            Stat::IsStrictSorted => write!(f, "is_strict_sorted"),
            Stat::Max => write!(f, "max"),
            Stat::Min => write!(f, "min"),
            Stat::RunCount => write!(f, "run_count"),
            Stat::TrueCount => write!(f, "true_count"),
            Stat::NullCount => write!(f, "null_count"),
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

    fn with_stat_value<'a>(
        &self,
        stat: Stat,
        f: &'a mut dyn FnMut(&Scalar) -> VortexResult<()>,
    ) -> VortexResult<()>;

    fn with_computed_stat_value<'a>(
        &self,
        stat: Stat,
        f: &'a mut dyn FnMut(&Scalar) -> VortexResult<()>,
    ) -> VortexResult<()>;
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

    #[inline]
    fn with_stat_value<'a>(
        &self,
        _stat: Stat,
        _f: &'a mut dyn FnMut(&Scalar) -> VortexResult<()>,
    ) -> VortexResult<()> {
        Ok(())
    }

    #[inline]
    fn with_computed_stat_value<'a>(
        &self,
        _stat: Stat,
        _f: &'a mut dyn FnMut(&Scalar) -> VortexResult<()>,
    ) -> VortexResult<()> {
        Ok(())
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
    ) -> VortexResult<U> {
        let mut res: Option<U> = None;
        self.with_stat_value(stat, &mut |s| {
            res = Some(U::try_from(s)?);
            Ok(())
        })?;
        Ok(res.expect("Result should have been populated by previous call"))
    }

    pub fn compute_as<U: for<'a> TryFrom<&'a Scalar, Error = VortexError>>(
        &self,
        stat: Stat,
    ) -> VortexResult<U> {
        let mut res: Option<U> = None;
        self.with_computed_stat_value(stat, &mut |s| {
            res = Some(U::try_from(s)?);
            Ok(())
        })?;
        Ok(res.expect("Result should have been populated by previous call"))
    }

    pub fn compute_as_cast<U: NativePType + TryFrom<Scalar, Error = VortexError>>(
        &self,
        stat: Stat,
    ) -> VortexResult<U> {
        let mut res: Option<U> = None;
        self.with_computed_stat_value(stat, &mut |s| {
            res = Some(U::try_from(s.cast(&DType::from(U::PTYPE))?)?);
            Ok(())
        })?;
        Ok(res.expect("Result should have been populated by previous call"))
    }

    pub fn compute_min<U: for<'a> TryFrom<&'a Scalar, Error = VortexError>>(
        &self,
    ) -> VortexResult<U> {
        self.compute_as(Stat::Min)
    }

    pub fn compute_max<U: for<'a> TryFrom<&'a Scalar, Error = VortexError>>(
        &self,
    ) -> VortexResult<U> {
        self.compute_as(Stat::Max)
    }

    pub fn compute_is_strict_sorted(&self) -> VortexResult<bool> {
        self.compute_as(Stat::IsStrictSorted)
    }

    pub fn compute_is_sorted(&self) -> VortexResult<bool> {
        self.compute_as(Stat::IsSorted)
    }

    pub fn compute_is_constant(&self) -> VortexResult<bool> {
        self.compute_as(Stat::IsConstant)
    }

    pub fn compute_true_count(&self) -> VortexResult<usize> {
        self.compute_as(Stat::TrueCount)
    }

    pub fn compute_null_count(&self) -> VortexResult<usize> {
        self.compute_as(Stat::NullCount)
    }

    pub fn compute_run_count(&self) -> VortexResult<usize> {
        self.compute_as(Stat::RunCount)
    }

    pub fn compute_bit_width_freq(&self) -> VortexResult<Vec<usize>> {
        self.compute_as::<ListScalarVec<usize>>(Stat::BitWidthFreq)
            .map(|s| s.0)
    }

    pub fn compute_trailing_zero_freq(&self) -> VortexResult<Vec<usize>> {
        self.compute_as::<ListScalarVec<usize>>(Stat::TrailingZeroFreq)
            .map(|s| s.0)
    }
}
