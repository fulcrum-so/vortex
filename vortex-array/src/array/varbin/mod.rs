use std::any::Any;
use std::iter;
use std::sync::{Arc, RwLock};

use arrow::array::{make_array, Array as ArrowArray, ArrayData, AsArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::UInt8Type;
use itertools::Itertools;
use linkme::distributed_slice;
use num_traits::{FromPrimitive, Unsigned};

use crate::array::bool::BoolArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::{
    check_slice_bounds, check_validity_buffer, Array, ArrayRef, ArrowIterator, Encoding,
    EncodingId, EncodingRef, ENCODINGS,
};
use crate::arrow::CombineChunks;
use crate::compress::EncodingCompression;
use crate::compute::cast::cast_primitive;
use crate::compute::scalar_at::scalar_at;
use crate::dtype::{DType, IntWidth, Nullability, Signedness};
use crate::error::{VortexError, VortexResult};
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::ptype::{NativePType, PType};
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{Stats, StatsSet};

mod compress;
mod compute;
mod serde;
mod stats;

#[derive(Debug, Clone)]
pub struct VarBinArray {
    offsets: ArrayRef,
    bytes: ArrayRef,
    dtype: DType,
    validity: Option<ArrayRef>,
    stats: Arc<RwLock<StatsSet>>,
}

impl VarBinArray {
    pub fn new(
        offsets: ArrayRef,
        bytes: ArrayRef,
        dtype: DType,
        validity: Option<ArrayRef>,
    ) -> Self {
        Self::try_new(offsets, bytes, dtype, validity).unwrap()
    }

    pub fn try_new(
        offsets: ArrayRef,
        bytes: ArrayRef,
        dtype: DType,
        validity: Option<ArrayRef>,
    ) -> VortexResult<Self> {
        if !matches!(offsets.dtype(), DType::Int(_, _, Nullability::NonNullable)) {
            return Err(VortexError::UnsupportedOffsetsArrayDType(
                offsets.dtype().clone(),
            ));
        }
        if !matches!(
            bytes.dtype(),
            DType::Int(IntWidth::_8, Signedness::Unsigned, Nullability::NonNullable)
        ) {
            return Err(VortexError::UnsupportedDataArrayDType(
                bytes.dtype().clone(),
            ));
        }
        if !matches!(dtype, DType::Binary(_) | DType::Utf8(_)) {
            return Err(VortexError::InvalidDType(dtype));
        }

        let validity = validity.filter(|v| !v.is_empty());
        check_validity_buffer(validity.as_deref())?;

        let dtype = if validity.is_some() && !dtype.is_nullable() {
            dtype.as_nullable()
        } else {
            dtype
        };

        Ok(Self {
            offsets,
            bytes,
            dtype,
            validity,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    fn is_valid(&self, index: usize) -> bool {
        self.validity
            .as_deref()
            .map(|v| scalar_at(v, index).unwrap().try_into().unwrap())
            .unwrap_or(true)
    }

    #[inline]
    pub fn offsets(&self) -> &dyn Array {
        // FIXME(ngates): given our zero-copy slicing, this is kind of a useless array...
        self.offsets.as_ref()
    }

    pub fn sliced_offsets(&self) -> VortexResult<ArrayRef> {
        let first_offset: i64 = scalar_at(self.offsets(), 0)?.try_into()?;
        let offsets = cast_primitive(self.offsets(), &PType::I64)?
            .typed_data::<i64>()
            .iter()
            .map(|o| o - first_offset)
            .collect_vec();
        Ok(PrimitiveArray::from_vec(offsets).boxed())
    }

    #[inline]
    pub fn bytes(&self) -> &dyn Array {
        // FIXME(ngates): given our zero-copy slicing, this is kind of a useless array...
        self.bytes.as_ref()
    }

    pub fn sliced_bytes(&self) -> VortexResult<ArrayRef> {
        let first_offset: usize = scalar_at(self.offsets(), 0)?.try_into()?;
        let last_offset: usize = scalar_at(self.offsets(), self.offsets().len() - 1)?.try_into()?;
        self.bytes().slice(first_offset, last_offset)
    }

    #[inline]
    pub fn validity(&self) -> Option<&dyn Array> {
        self.validity.as_deref()
    }

    pub fn from_vec<T: AsRef<[u8]>>(vec: Vec<T>, dtype: DType) -> Self {
        let size: usize = vec.iter().map(|v| v.as_ref().len()).sum();
        if size < u32::MAX as usize {
            Self::from_vec_sized::<u32, T>(vec, dtype)
        } else {
            Self::from_vec_sized::<u64, T>(vec, dtype)
        }
    }

    fn from_vec_sized<K, T>(vec: Vec<T>, dtype: DType) -> Self
    where
        K: NativePType + FromPrimitive + Unsigned,
        T: AsRef<[u8]>,
    {
        let mut offsets: Vec<K> = Vec::with_capacity(vec.len() + 1);
        let mut values: Vec<u8> = Vec::new();
        offsets.push(K::zero());
        for v in vec {
            values.extend_from_slice(v.as_ref());
            offsets.push(<K as FromPrimitive>::from_usize(values.len()).unwrap());
        }

        VarBinArray::new(
            PrimitiveArray::from_vec(offsets).boxed(),
            PrimitiveArray::from_vec(values).boxed(),
            dtype,
            None,
        )
    }

    pub fn from_iter<T: AsRef<[u8]>, I: IntoIterator<Item = Option<T>>>(
        iter: I,
        dtype: DType,
    ) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();

        let mut validity: Vec<bool> = Vec::with_capacity(lower);
        let mut offsets: Vec<u64> = Vec::with_capacity(lower + 1);
        offsets.push(0);
        let mut bytes: Vec<u8> = Vec::new();
        for i in iter {
            if let Some(v) = i {
                validity.push(true);
                bytes.extend_from_slice(v.as_ref());
                offsets.push(bytes.len() as u64);
            } else {
                validity.push(false);
                offsets.push(bytes.len() as u64);
            }
        }

        let offsets_ref = PrimitiveArray::from_vec(offsets).boxed();
        let bytes_ref = PrimitiveArray::from_vec(bytes).boxed();
        if validity.is_empty() {
            VarBinArray::new(offsets_ref, bytes_ref, dtype, None)
        } else {
            VarBinArray::new(
                offsets_ref,
                bytes_ref,
                dtype.as_nullable(),
                Some(BoolArray::from(validity).boxed()),
            )
        }
    }

    pub fn bytes_at(&self, index: usize) -> VortexResult<Vec<u8>> {
        // check_index_bounds(self, index)?;

        let (start, end): (usize, usize) = (
            scalar_at(self.offsets(), index)?.try_into()?,
            scalar_at(self.offsets(), index + 1)?.try_into()?,
        );
        let byte_len = self.bytes().len();
        print!("start: {}, end: {}, byte_len: {}", start, end, byte_len);
        let sliced = self.bytes().slice(start, end)?;
        let arr_ref = sliced.iter_arrow().combine_chunks();
        Ok(arr_ref.as_primitive::<UInt8Type>().values().to_vec())
    }
}

impl Array for VarBinArray {
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

    #[inline]
    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.offsets.len() <= 1
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
        let offsets_data = self.offsets.iter_arrow().combine_chunks().into_data();
        let bytes_data = self.bytes.iter_arrow().combine_chunks().into_data();

        let data = ArrayData::builder(self.dtype.clone().into())
            .len(self.len())
            .nulls(self.validity().map(|v| {
                NullBuffer::new(
                    v.iter_arrow()
                        .combine_chunks()
                        .as_boolean()
                        .values()
                        .clone(),
                )
            }))
            .add_buffer(offsets_data.buffers()[0].to_owned())
            .add_buffer(bytes_data.buffers()[0].to_owned())
            .build()
            .unwrap();

        Box::new(iter::once(make_array(data)))
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;

        Ok(VarBinArray::new(
            self.offsets.slice(start, stop + 1)?,
            self.bytes.clone(),
            self.dtype.clone(),
            self.validity
                .as_ref()
                .map(|v| v.slice(start, stop + 1))
                .transpose()?,
        )
        .boxed())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &VarBinEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.bytes.nbytes() + self.offsets.nbytes()
    }

    fn serde(&self) -> &dyn ArraySerde {
        self
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for VarBinArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

#[derive(Debug)]
pub struct VarBinEncoding;

impl VarBinEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.varbin");
}

#[distributed_slice(ENCODINGS)]
static ENCODINGS_VARBIN: EncodingRef = &VarBinEncoding;

impl Encoding for VarBinEncoding {
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

impl ArrayDisplay for VarBinArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.writeln("offsets:")?;
        f.indent(|ind| ind.array(self.offsets()))?;
        f.writeln("bytes:")?;
        f.indent(|ind| ind.array(self.bytes()))
    }
}

impl From<Vec<&[u8]>> for VarBinArray {
    fn from(value: Vec<&[u8]>) -> Self {
        VarBinArray::from_vec(value, DType::Binary(Nullability::NonNullable))
    }
}

impl From<Vec<Vec<u8>>> for VarBinArray {
    fn from(value: Vec<Vec<u8>>) -> Self {
        VarBinArray::from_vec(value, DType::Binary(Nullability::NonNullable))
    }
}

impl From<Vec<String>> for VarBinArray {
    fn from(value: Vec<String>) -> Self {
        VarBinArray::from_vec(value, DType::Utf8(Nullability::NonNullable))
    }
}

impl From<Vec<&str>> for VarBinArray {
    fn from(value: Vec<&str>) -> Self {
        VarBinArray::from_vec(value, DType::Utf8(Nullability::NonNullable))
    }
}

impl<'a> FromIterator<Option<&'a [u8]>> for VarBinArray {
    fn from_iter<T: IntoIterator<Item = Option<&'a [u8]>>>(iter: T) -> Self {
        VarBinArray::from_iter(iter, DType::Binary(Nullability::NonNullable))
    }
}

impl FromIterator<Option<Vec<u8>>> for VarBinArray {
    fn from_iter<T: IntoIterator<Item = Option<Vec<u8>>>>(iter: T) -> Self {
        VarBinArray::from_iter(iter, DType::Binary(Nullability::NonNullable))
    }
}

impl FromIterator<Option<String>> for VarBinArray {
    fn from_iter<T: IntoIterator<Item = Option<String>>>(iter: T) -> Self {
        VarBinArray::from_iter(iter, DType::Utf8(Nullability::NonNullable))
    }
}

impl<'a> FromIterator<Option<&'a str>> for VarBinArray {
    fn from_iter<T: IntoIterator<Item = Option<&'a str>>>(iter: T) -> Self {
        VarBinArray::from_iter(iter, DType::Utf8(Nullability::NonNullable))
    }
}

#[cfg(test)]
mod test {
    use crate::array::Array;
    use arrow::array::{AsArray, GenericStringArray as ArrowStringArray};

    use crate::array::primitive::PrimitiveArray;
    use crate::array::varbin::VarBinArray;
    use crate::arrow::CombineChunks;
    use crate::compute::scalar_at::scalar_at;
    use crate::dtype::{DType, Nullability};

    fn binary_array() -> VarBinArray {
        let values = PrimitiveArray::from_vec(
            "hello worldhello world this is a long string"
                .as_bytes()
                .to_vec(),
        );
        let offsets = PrimitiveArray::from_vec(vec![0, 11, 44]);

        VarBinArray::new(
            offsets.boxed(),
            values.boxed(),
            DType::Utf8(Nullability::NonNullable),
            None,
        )
    }

    #[test]
    pub fn test_scalar_at() {
        let binary_arr = binary_array();
        assert_eq!(binary_arr.len(), 2);
        assert_eq!(scalar_at(binary_arr.as_ref(), 0), Ok("hello world".into()));
        assert_eq!(
            scalar_at(binary_arr.as_ref(), 1),
            Ok("hello world this is a long string".into())
        )
    }

    #[test]
    pub fn slice() {
        let binary_arr = binary_array().slice(1, 2).unwrap();
        assert_eq!(
            scalar_at(binary_arr.as_ref(), 0),
            Ok("hello world this is a long string".into())
        );
    }

    #[test]
    pub fn iter() {
        let binary_array = binary_array();
        assert_eq!(
            binary_array
                .iter_arrow()
                .combine_chunks()
                .as_string::<i32>(),
            &ArrowStringArray::<i32>::from(vec![
                "hello world",
                "hello world this is a long string",
            ])
        );
    }
}
