use serde::{Deserialize, Serialize};

use crate::stats::{ArrayStatisticsCompute, Stat};
use crate::validity::{ArrayValidity, LogicalValidity, Validity};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::{impl_encoding, Canonical, IntoCanonical};

mod compute;

impl_encoding!("vortex.null", Null);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NullMetadata {
    len: usize,
}

impl NullArray {
    pub fn new(len: usize) -> Self {
        Self::try_from_parts(
            DType::Null,
            NullMetadata { len },
            Arc::new([]),
            StatsSet::nulls(len, &DType::Null),
        )
        .expect("NullArray::new cannot fail")
    }
}

impl IntoCanonical for NullArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        Ok(Canonical::Null(self))
    }
}

impl ArrayValidity for NullArray {
    fn is_valid(&self, _: usize) -> bool {
        false
    }

    fn logical_validity(&self) -> LogicalValidity {
        LogicalValidity::AllInvalid(self.len())
    }
}

impl ArrayStatisticsCompute for NullArray {
    fn compute_statistics(&self, _stat: Stat) -> VortexResult<StatsSet> {
        Ok(StatsSet::nulls(self.len(), &DType::Null))
    }
}

impl AcceptArrayVisitor for NullArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_validity(&Validity::AllInvalid)
    }
}

impl ArrayTrait for NullArray {
    fn len(&self) -> usize {
        self.metadata().len
    }

    fn nbytes(&self) -> usize {
        0
    }
}
