use std::sync::{Arc, RwLock};

use vortex_dtype::DType;
use vortex_error::{vortex_err, VortexResult};
use vortex_scalar::Scalar;

use crate::buffer::{Buffer, OwnedBuffer};
use crate::encoding::EncodingRef;
use crate::stats::{Stat, Statistics, StatsSet};
use crate::{Array, ArrayMetadata, IntoArray, OwnedArray, ToArray};

#[derive(Clone, Debug)]
pub struct ArrayData {
    encoding: EncodingRef,
    dtype: DType, // FIXME(ngates): Arc?
    metadata: Arc<dyn ArrayMetadata>,
    buffer: Option<OwnedBuffer>,
    children: Arc<[ArrayData]>,
    stats_map: Arc<RwLock<StatsSet>>,
}

impl ArrayData {
    pub fn try_new(
        encoding: EncodingRef,
        dtype: DType,
        metadata: Arc<dyn ArrayMetadata>,
        buffer: Option<OwnedBuffer>,
        children: Arc<[ArrayData]>,
        statistics: StatsSet,
    ) -> VortexResult<Self> {
        let data = Self {
            encoding,
            dtype,
            metadata,
            buffer,
            children,
            stats_map: Arc::new(RwLock::new(statistics)),
        };

        // Validate here that the metadata correctly parses, so that an encoding can infallibly
        let array = data.to_array();
        // FIXME(ngates): run some validation function
        encoding.with_dyn(&array, &mut |_| Ok(()))?;

        Ok(data)
    }

    pub fn encoding(&self) -> EncodingRef {
        self.encoding
    }

    pub fn dtype(&self) -> &DType {
        &self.dtype
    }

    pub fn metadata(&self) -> &Arc<dyn ArrayMetadata> {
        &self.metadata
    }

    pub fn buffer(&self) -> Option<&Buffer> {
        self.buffer.as_ref()
    }

    pub fn into_buffer(self) -> Option<OwnedBuffer> {
        self.buffer
    }

    pub fn child(&self, index: usize, dtype: &DType) -> Option<&ArrayData> {
        match self.children.get(index) {
            None => None,
            Some(child) => {
                assert_eq!(child.dtype(), dtype);
                Some(child)
            }
        }
    }

    pub fn children(&self) -> &[ArrayData] {
        &self.children
    }

    pub fn statistics(&self) -> &dyn Statistics {
        self
    }

    pub fn depth_first_traversal(&self) -> ArrayDataIterator {
        ArrayDataIterator { stack: vec![self] }
    }

    /// Return the buffer offsets and the total length of all buffers, assuming the given alignment.
    /// This includes all child buffers.
    pub fn all_buffer_offsets(&self, alignment: usize) -> Vec<u64> {
        let mut offsets = vec![];
        let mut offset = 0;

        for col_data in self.depth_first_traversal() {
            if let Some(buffer) = col_data.buffer() {
                offsets.push(offset as u64);

                let buffer_size = buffer.len();
                let aligned_size = (buffer_size + (alignment - 1)) & !(alignment - 1);
                offset += aligned_size;
            }
        }
        offsets.push(offset as u64);

        offsets
    }
}

/// A depth-first pre-order iterator over a ArrayData.
pub struct ArrayDataIterator<'a> {
    stack: Vec<&'a ArrayData>,
}

impl<'a> Iterator for ArrayDataIterator<'a> {
    type Item = &'a ArrayData;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.stack.pop()?;
        for child in next.children.as_ref().iter().rev() {
            self.stack.push(child);
        }
        Some(next)
    }
}

impl ToArray for ArrayData {
    fn to_array(&self) -> Array {
        Array::Data(self.clone())
    }
}

impl IntoArray<'static> for ArrayData {
    fn into_array(self) -> OwnedArray {
        Array::Data(self)
    }
}

impl Statistics for ArrayData {
    fn get(&self, stat: Stat) -> Option<Scalar> {
        self.stats_map.read().unwrap().get(stat).cloned()
    }

    fn to_set(&self) -> StatsSet {
        self.stats_map.read().unwrap().clone()
    }

    fn set(&self, stat: Stat, value: Scalar) {
        self.stats_map.write().unwrap().set(stat, value);
    }

    fn compute(&self, stat: Stat) -> Option<Scalar> {
        if let Some(s) = self.get(stat) {
            return Some(s);
        }

        self.stats_map.write().unwrap().extend(
            self.to_array()
                .with_dyn(|a| a.compute_statistics(stat))
                .ok()?,
        );
        self.get(stat)
    }

    #[inline]
    fn with_stat_value<'a>(
        &self,
        stat: Stat,
        f: &'a mut dyn FnMut(&Scalar) -> VortexResult<()>,
    ) -> VortexResult<()> {
        self.stats_map
            .read()
            .unwrap()
            .get(stat)
            .ok_or_else(|| vortex_err!(ComputeError: "statistic {} missing", stat))
            .and_then(f)
    }

    #[inline]
    fn with_computed_stat_value<'a>(
        &self,
        stat: Stat,
        f: &'a mut dyn FnMut(&Scalar) -> VortexResult<()>,
    ) -> VortexResult<()> {
        if let Some(s) = self.stats_map.read().unwrap().get(stat) {
            return f(s);
        }

        self.stats_map
            .write()
            .unwrap()
            .extend(self.to_array().with_dyn(|a| a.compute_statistics(stat))?);
        self.with_stat_value(stat, f)
    }
}
