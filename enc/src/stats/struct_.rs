use crate::array::struct_::StructArray;
use crate::stats::{Stat, StatsCompute, StatsSet};

impl StatsCompute for StructArray {
    fn compute(&self, _stat: &Stat) -> StatsSet {
        todo!()
    }
}
