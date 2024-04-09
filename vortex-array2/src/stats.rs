use std::collections::HashMap;

use vortex::ptype::NativePType;
use vortex::scalar::Scalar;
use vortex_error::VortexResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

pub trait ArrayStatistics {
    fn statistics(&self) -> &dyn Statistics {
        &EmptyStatistics
    }

    /// Compute the requested statistic. Can return additional stats.
    fn compute_statistics(&self, _stat: Stat) -> VortexResult<HashMap<Stat, Scalar>> {
        Ok(HashMap::new())
    }
}

pub trait Statistics {
    fn compute(&self, stat: Stat) -> VortexResult<Option<Scalar>>;
    fn get(&self, stat: Stat) -> Option<Scalar>;
    fn set(&self, stat: Stat, value: Scalar);
}

impl dyn Statistics {
    fn compute_as<T: TryFrom<Scalar>>(&self, _stat: Stat) -> VortexResult<Option<T>> {
        // TODO(ngates): should we panic if conversion fails?
        todo!()
    }

    fn get_as<T: TryFrom<Scalar>>(&self, _stat: Stat) -> Option<T> {
        todo!()
    }

    fn compute_min<T: NativePType>(&self, default: T) -> VortexResult<T> {
        Ok(self.compute_as::<T>(Stat::Min)?.unwrap_or(default))
    }
}

pub struct EmptyStatistics;
impl Statistics for EmptyStatistics {
    fn compute(&self, _stat: Stat) -> VortexResult<Option<Scalar>> {
        Ok(None)
    }

    fn get(&self, _stat: Stat) -> Option<Scalar> {
        None
    }

    fn set(&self, _stat: Stat, _value: Scalar) {
        // No-op
    }
}
