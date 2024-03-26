use num_traits::AsPrimitive;

use crate::array::primitive::PrimitiveArray;
use crate::array::Array;
use crate::compute::flatten::flatten_primitive;
use crate::compute::scalar_at::scalar_at;
use crate::match_each_native_ptype;

#[derive(Debug)]
pub struct VarBinPrimitiveIter<'a> {
    bytes: &'a [u8],
    offsets: &'a PrimitiveArray,
    last_offset: usize,
    idx: usize,
}

impl<'a> VarBinPrimitiveIter<'a> {
    pub fn new(bytes: &'a [u8], offsets: &'a PrimitiveArray) -> Self {
        assert!(offsets.len() > 1);
        let last_offset = Self::offset_at(offsets, 0);
        Self {
            bytes,
            offsets,
            last_offset,
            idx: 1,
        }
    }

    pub(self) fn offset_at(array: &'a PrimitiveArray, index: usize) -> usize {
        match_each_native_ptype!(array.ptype(), |$P| {
            array.typed_data::<$P>()[index].as_()
        })
    }
}

impl<'a> Iterator for VarBinPrimitiveIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx == self.offsets.len() {
            return None;
        }

        let next_offset = Self::offset_at(self.offsets, self.idx);
        let slice_bytes = &self.bytes[self.last_offset..next_offset];
        self.last_offset = next_offset;
        self.idx += 1;
        Some(slice_bytes)
    }
}

#[derive(Debug)]
pub struct VarBinIter<'a> {
    bytes: &'a dyn Array,
    offsets: &'a dyn Array,
    last_offset: usize,
    idx: usize,
}

impl<'a> VarBinIter<'a> {
    pub fn new(bytes: &'a dyn Array, offsets: &'a dyn Array) -> Self {
        assert!(offsets.len() > 1);
        let last_offset = scalar_at(offsets, 0).unwrap().try_into().unwrap();
        Self {
            bytes,
            offsets,
            last_offset,
            idx: 1,
        }
    }
}

impl<'a> Iterator for VarBinIter<'a> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx == self.offsets.len() {
            return None;
        }

        let next_offset: usize = scalar_at(self.offsets, self.idx)
            .unwrap()
            .try_into()
            .unwrap();
        let slice_bytes = self.bytes.slice(self.last_offset, next_offset).unwrap();
        let slice_bytes = flatten_primitive(slice_bytes.as_ref())
            .unwrap()
            .typed_data::<u8>()
            .to_vec();
        self.last_offset = next_offset;
        self.idx += 1;
        Some(slice_bytes)
    }
}
