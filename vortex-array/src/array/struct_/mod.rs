use std::sync::{Arc, RwLock};

use itertools::Itertools;
use linkme::distributed_slice;
use vortex_error::VortexResult;
use vortex_schema::{DType, FieldNames};

use super::{check_slice_bounds, Array, ArrayRef};
use crate::array::validity::Validity;
use crate::compress::EncodingCompression;
use crate::encoding::{Encoding, EncodingId, EncodingRef, ENCODINGS};
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{Stats, StatsCompute, StatsSet};
use crate::{impl_array, impl_array_compute, ArrayWalker};

mod compress;
mod compute;
mod serde;

#[derive(Debug, Clone)]
pub struct StructArray {
    fields: Vec<ArrayRef>,
    dtype: DType,
    len: usize,
    stats: Arc<RwLock<StatsSet>>,
}

impl StructArray {
    pub fn new(names: FieldNames, fields: Vec<ArrayRef>, len: usize) -> Self {
        assert!(
            fields.iter().all(|v| v.len() == len),
            "Fields didn't have the same length"
        );
        let dtype = DType::Struct(names, fields.iter().map(|a| a.dtype().clone()).collect());
        Self {
            fields,
            dtype,
            len,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
    }

    #[inline]
    pub fn fields(&self) -> &[ArrayRef] {
        &self.fields
    }

    pub fn names(&self) -> &FieldNames {
        if let DType::Struct(names, _fields) = self.dtype() {
            names
        } else {
            panic!("dtype is not a struct")
        }
    }

    pub fn field_dtypes(&self) -> &[DType] {
        if let DType::Struct(_names, fields) = self.dtype() {
            fields
        } else {
            panic!("dtype is not a struct")
        }
    }
}

impl Array for StructArray {
    impl_array!();
    impl_array_compute!();

    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;

        let fields = self
            .fields
            .iter()
            .map(|field| field.slice(start, stop))
            .try_collect()?;
        Ok(Self {
            fields,
            dtype: self.dtype.clone(),
            len: stop - start,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
        .into_array())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &StructEncoding
    }

    #[inline]
    fn with_compute_mut(
        &self,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()> {
        f(self)
    }

    fn nbytes(&self) -> usize {
        self.fields.iter().map(|arr| arr.nbytes()).sum()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }

    fn validity(&self) -> Option<Validity> {
        todo!()
    }

    fn walk(&self, walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        for field in self.fields() {
            walker.visit_child(field)?;
        }
        Ok(())
    }
}

impl StatsCompute for StructArray {}

#[derive(Debug)]
pub struct StructEncoding;

impl StructEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.struct");
}

#[distributed_slice(ENCODINGS)]
static ENCODINGS_STRUCT: EncodingRef = &StructEncoding;

impl Encoding for StructEncoding {
    fn id(&self) -> EncodingId {
        Self::ID
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}

impl ArrayDisplay for StructArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        let DType::Struct(n, _) = self.dtype() else {
            unreachable!()
        };
        for (name, field) in n.iter().zip(self.fields()) {
            f.child(&format!("\"{}\"", name), field.as_ref())?;
        }
        Ok(())
    }
}
