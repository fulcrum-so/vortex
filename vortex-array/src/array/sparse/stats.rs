use crate::array::sparse::SparseArray;
use crate::error::VortexResult;
use crate::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for SparseArray {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        Ok(StatsSet::default())
    }
}
