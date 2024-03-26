use vortex::stats::{Stat, StatsCompute, StatsSet};
use vortex_error::VortexResult;

use crate::dict::DictArray;

impl StatsCompute for DictArray {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        let mut stats = StatsSet::new();

        if let Some(rc) = self.codes().stats().get_or_compute(&Stat::RunCount) {
            stats.set(Stat::RunCount, rc);
        }
        if let Some(min) = self.dict().stats().get_or_compute(&Stat::Min) {
            stats.set(Stat::Min, min);
        }
        if let Some(max) = self.dict().stats().get_or_compute(&Stat::Max) {
            stats.set(Stat::Max, max);
        }
        if let Some(is_constant) = self.codes().stats().get_or_compute(&Stat::IsConstant) {
            stats.set(Stat::IsConstant, is_constant);
        }
        if let Some(null_count) = self.codes().stats().get_or_compute(&Stat::NullCount) {
            stats.set(Stat::NullCount, null_count);
        }

        // if dictionary is sorted
        if self
            .dict()
            .stats()
            .get_or_compute_as::<bool>(&Stat::IsSorted)
            .unwrap_or(false)
        {
            if let Some(codes_are_sorted) = self
                .codes()
                .stats()
                .get_or_compute_as::<bool>(&Stat::IsSorted)
            {
                stats.set(Stat::IsSorted, codes_are_sorted.into());
            }

            if let Some(codes_are_strict_sorted) = self
                .codes()
                .stats()
                .get_or_compute_as::<bool>(&Stat::IsStrictSorted)
            {
                stats.set(Stat::IsStrictSorted, codes_are_strict_sorted.into());
            }
        }

        Ok(stats)
    }
}
