use std::collections::HashMap;

use vortex_dtype::DType;
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::array::constant::ConstantArray;
use crate::stats::{ArrayStatisticsCompute, Stat, StatsSet};
use crate::{ArrayDType, ArrayTrait};

impl ArrayStatisticsCompute for ConstantArray<'_> {
    fn compute_statistics(&self, _stat: Stat) -> VortexResult<StatsSet> {
        if matches!(self.dtype(), &DType::Bool(_)) {
            let Scalar::Bool(b) = self.scalar() else {
                unreachable!("Got bool dtype without bool scalar")
            };
            return Ok(StatsSet::from(HashMap::from([(
                Stat::TrueCount,
                (self.len() as u64 * b.value().cloned().map(|v| v as u64).unwrap_or(0)).into(),
            )])));
        }

        Ok(StatsSet::new())
    }
}
