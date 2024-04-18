use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::buffer::{Buffer, OwnedBuffer};
use crate::encoding::EncodingRef;
use crate::scalar::Scalar;
use crate::stats::Stat;
use crate::stats::Statistics;
use crate::{Array, ArrayMetadata, IntoArray, ToArray};

#[derive(Clone, Debug)]
pub struct ArrayData {
    encoding: EncodingRef,
    dtype: DType,
    metadata: Arc<dyn ArrayMetadata>,
    buffers: Box<[OwnedBuffer]>, // Should this just be an Option, not an Arc? How many multi-buffer arrays are there?
    children: Box<[ArrayData]>,
    stats_map: Arc<RwLock<HashMap<Stat, Scalar>>>,
}

impl ArrayData {
    pub fn try_new(
        encoding: EncodingRef,
        dtype: DType,
        metadata: Arc<dyn ArrayMetadata>,
        buffers: Box<[OwnedBuffer]>,
        children: Box<[ArrayData]>,
        statistics: HashMap<Stat, Scalar>,
    ) -> VortexResult<Self> {
        let data = Self {
            encoding,
            dtype,
            metadata,
            buffers,
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

    pub fn buffers(&self) -> &[Buffer] {
        &self.buffers
    }

    pub fn into_buffers(self) -> Vec<OwnedBuffer> {
        self.buffers.into_vec()
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
        let mut offsets = Vec::with_capacity(self.buffers.len() + 1);
        let mut offset = 0;

        for col_data in self.depth_first_traversal() {
            for buffer in col_data.buffers() {
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

/// A depth-first iterator over a ArrayData.
pub struct ArrayDataIterator<'a> {
    stack: Vec<&'a ArrayData>,
}

impl<'a> Iterator for ArrayDataIterator<'a> {
    type Item = &'a ArrayData;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.stack.pop()?;
        for child in next.children.as_ref().iter() {
            self.stack.push(child);
        }
        Some(next)
    }
}

impl ToArray for ArrayData {
    fn to_array(&self) -> Array {
        Array::DataRef(self)
    }
}

impl IntoArray<'static> for ArrayData {
    fn into_array(self) -> Array<'static> {
        Array::Data(self)
    }
}

impl Statistics for ArrayData {
    fn compute(&self, stat: Stat) -> Option<Scalar> {
        let mut locked = self.stats_map.write().unwrap();
        let stats = self
            .to_array()
            .with_dyn(|a| a.compute_statistics(stat))
            .ok()?;
        for (k, v) in &stats {
            locked.insert(*k, v.clone());
        }
        stats.get(&stat).cloned()
    }

    fn get(&self, stat: Stat) -> Option<Scalar> {
        let locked = self.stats_map.read().unwrap();
        locked.get(&stat).cloned()
    }

    fn set(&self, stat: Stat, value: Scalar) {
        let mut locked = self.stats_map.write().unwrap();
        locked.insert(stat, value);
    }

    fn to_map(&self) -> HashMap<Stat, Scalar> {
        self.stats_map.read().unwrap().clone()
    }
}
