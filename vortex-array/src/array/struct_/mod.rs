use std::any::Any;
use std::sync::{Arc, RwLock};

use itertools::Itertools;
use linkme::distributed_slice;

use arrow_array::array::StructArray as ArrowStructArray;
use arrow_array::array::{Array as ArrowArray, ArrayRef as ArrowArrayRef};
use arrow_schema::{Field, Fields};

use crate::arrow::aligned_iter::AlignedArrowArrayIterator;
use crate::compress::EncodingCompression;
use crate::dtype::{DType, FieldNames};
use crate::error::VortexResult;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{Stats, StatsCompute, StatsSet};

use super::{
    check_slice_bounds, Array, ArrayRef, ArrowIterator, Encoding, EncodingId, EncodingRef,
    ENCODINGS,
};

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

    fn arrow_fields(&self) -> Fields {
        self.names()
            .iter()
            .zip(self.field_dtypes())
            .map(|(name, dtype)| Field::new(name.as_str(), dtype.into(), dtype.is_nullable()))
            .map(Arc::new)
            .collect()
    }
}

impl Array for StructArray {
    #[inline]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn boxed(self) -> ArrayRef {
        Box::new(self)
    }

    #[inline]
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

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

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        let fields = self.arrow_fields();
        Box::new(
            AlignedArrowArrayIterator::new(
                self.fields
                    .iter()
                    .map(|f| f.iter_arrow())
                    .collect::<Vec<_>>(),
            )
            .map(move |items| {
                Arc::new(ArrowStructArray::new(
                    fields.clone(),
                    items.into_iter().map(ArrowArrayRef::from).collect(),
                    None,
                )) as Arc<dyn ArrowArray>
            }),
        )
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
        .boxed())
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

impl<'arr> AsRef<(dyn Array + 'arr)> for StructArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
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
    fn id(&self) -> &EncodingId {
        &Self::ID
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
