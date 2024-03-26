use std::collections::HashMap;
use vortex_error::VortexResult;

use crate::ALPArray;
use vortex::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for ALPArray {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        // TODO(ngates): implement based on the encoded array
        Ok(StatsSet::from(HashMap::new()))
    }
}
