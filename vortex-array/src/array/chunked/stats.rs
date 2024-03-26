use vortex_error::VortexResult;

use crate::array::chunked::ChunkedArray;
use crate::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for ChunkedArray {
    fn compute(&self, stat: &Stat) -> VortexResult<StatsSet> {
        Ok(self
            .chunks()
            .iter()
            .map(|c| {
                let s = c.stats();
                // HACK(robert): This will compute all stats but we could just compute one
                s.get_or_compute(stat);
                s.get_all()
            })
            .fold(StatsSet::new(), |mut acc, x| {
                acc.merge(&x);
                acc
            }))
    }
}
