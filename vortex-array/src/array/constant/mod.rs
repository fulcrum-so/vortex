use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use vortex_scalar::Scalar;

use crate::impl_encoding;
use crate::stats::Stat;
use crate::validity::{ArrayValidity, LogicalValidity};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
mod canonical;
mod compute;
mod stats;

impl_encoding!("vortex.constant", Constant);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstantMetadata {
    scalar: Scalar,
    length: usize,
}

impl ConstantArray {
    pub fn new<S>(scalar: S, length: usize) -> Self
    where
        Scalar: From<S>,
    {
        let scalar: Scalar = scalar.into();
        // TODO(aduffy): add stats for bools, ideally there should be a
        //  StatsSet::constant(Scalar) constructor that does this for us, like StatsSet::nulls.
        let stats = StatsSet::from(HashMap::from([
            (Stat::Max, scalar.clone()),
            (Stat::Min, scalar.clone()),
            (Stat::IsConstant, true.into()),
            (Stat::IsSorted, true.into()),
            (Stat::RunCount, 1.into()),
        ]));
        Self::try_from_parts(
            scalar.dtype().clone(),
            ConstantMetadata { scalar, length },
            [].into(),
            stats,
        )
        .unwrap()
    }

    pub fn scalar(&self) -> &Scalar {
        &self.metadata().scalar
    }
}

impl ArrayValidity for ConstantArray {
    fn is_valid(&self, _index: usize) -> bool {
        match self.metadata().scalar.dtype().is_nullable() {
            true => !self.scalar().is_null(),
            false => true,
        }
    }

    fn logical_validity(&self) -> LogicalValidity {
        match self.scalar().is_null() {
            true => LogicalValidity::AllInvalid(self.len()),
            false => LogicalValidity::AllValid(self.len()),
        }
    }
}

impl AcceptArrayVisitor for ConstantArray {
    fn accept(&self, _visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        Ok(())
    }
}

impl ArrayTrait for ConstantArray {
    fn len(&self) -> usize {
        self.metadata().length
    }
}
