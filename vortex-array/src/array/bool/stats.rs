use std::collections::HashMap;

use vortex_error::VortexResult;

use crate::array::bool::BoolArray;
use crate::array::Array;
use crate::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for BoolArray {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        if self.len() == 0 {
            return Ok(StatsSet::from(HashMap::from([
                (Stat::TrueCount, 0.into()),
                (Stat::RunCount, 0.into()),
            ])));
        }

        let mut prev_bit = self.buffer().value(0);
        let mut true_count: usize = if prev_bit { 1 } else { 0 };
        let mut run_count: usize = 0;
        for bit in self.buffer().iter().skip(1) {
            if bit {
                true_count += 1
            }
            if bit != prev_bit {
                run_count += 1;
                prev_bit = bit;
            }
        }
        run_count += 1;

        Ok(StatsSet::from(HashMap::from([
            (Stat::Min, (true_count == self.len()).into()),
            (Stat::Max, (true_count > 0).into()),
            (
                Stat::IsConstant,
                (true_count == self.len() || true_count == 0).into(),
            ),
            (Stat::RunCount, run_count.into()),
            (Stat::TrueCount, true_count.into()),
        ])))
    }
}
