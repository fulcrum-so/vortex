use std::sync::{Arc, RwLock};

use itertools::Itertools;
use linkme::distributed_slice;

use vortex_error::VortexResult;
use vortex_schema::{DType, FieldNames};

use crate::compress::EncodingCompression;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::impl_array;
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{Stats, StatsCompute, StatsSet};
use crate::validity::{ArrayValidity, Validity};

use super::{check_slice_bounds, Array, ArrayRef, Encoding, EncodingId, EncodingRef, ENCODINGS};

mod compress;
mod compute;
mod serde;

#[derive(Debug, Clone)]
pub struct StructArray {
    fields: Vec<ArrayRef>,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl StructArray {
    pub fn new(names: FieldNames, fields: Vec<ArrayRef>) -> Self {
        assert!(
            fields.iter().map(|v| v.len()).all_equal(),
            "Fields didn't have the same length"
        );
        let dtype = DType::Struct(names, fields.iter().map(|a| a.dtype().clone()).collect());
        Self {
            fields,
            dtype,
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

    fn len(&self) -> usize {
        self.fields.first().map_or(0, |a| a.len())
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
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
        .into_array())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &StructEncoding
    }

    fn nbytes(&self) -> usize {
        self.fields.iter().map(|arr| arr.nbytes()).sum()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }
}

impl ArrayValidity for StructArray {
    fn validity(&self) -> Option<Validity> {
        todo!()
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
