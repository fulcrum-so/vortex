use std::sync::{Arc, RwLock};

use linkme::distributed_slice;
use num_traits::{FromPrimitive, Unsigned};

use vortex_error::{VortexError, VortexResult};
use vortex_schema::{DType, IntWidth, Nullability, Signedness};

use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::primitive::PrimitiveArray;
use crate::array::varbin::values_iter::{VarBinIter, VarBinPrimitiveIter};
use crate::array::{
    check_slice_bounds, Array, ArrayRef, Encoding, EncodingId, EncodingRef, ENCODINGS,
};
use crate::compress::EncodingCompression;
use crate::compute::flatten::flatten_primitive;
use crate::compute::scalar_at::scalar_at;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::impl_array;
use crate::ptype::NativePType;
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{Stats, StatsSet};
use crate::validity::{ArrayValidity, Validity};

mod compress;
mod compute;
mod serde;
mod stats;
mod values_iter;

#[derive(Debug, Clone)]
pub struct VarBinArray {
    offsets: ArrayRef,
    bytes: ArrayRef,
    dtype: DType,
    validity: Option<Validity>,
    stats: Arc<RwLock<StatsSet>>,
}

impl VarBinArray {
    pub fn new(
        offsets: ArrayRef,
        bytes: ArrayRef,
        dtype: DType,
        validity: Option<Validity>,
    ) -> Self {
        Self::try_new(offsets, bytes, dtype, validity).unwrap()
    }

    pub fn try_new(
        offsets: ArrayRef,
        bytes: ArrayRef,
        dtype: DType,
        validity: Option<Validity>,
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

        if let Some(v) = &validity {
            assert_eq!(v.len(), offsets.len() - 1);
        }
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

    #[inline]
    pub fn offsets(&self) -> &ArrayRef {
        &self.offsets
    }

    pub fn first_offset<T: NativePType>(&self) -> VortexResult<T> {
        scalar_at(self.offsets(), 0)?
            .cast(&DType::from(T::PTYPE))?
            .try_into()
    }

    #[inline]
    pub fn bytes(&self) -> &ArrayRef {
        &self.bytes
    }

    pub fn sliced_bytes(&self) -> VortexResult<ArrayRef> {
        let first_offset: usize = scalar_at(self.offsets(), 0)?.try_into()?;
        let last_offset: usize = scalar_at(self.offsets(), self.offsets().len() - 1)?.try_into()?;
        self.bytes().slice(first_offset, last_offset)
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
            PrimitiveArray::from(offsets).into_array(),
            PrimitiveArray::from(values).into_array(),
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

        let offsets_ref = PrimitiveArray::from(offsets).into_array();
        let bytes_ref = PrimitiveArray::from(bytes).into_array();
        if validity.is_empty() {
            VarBinArray::new(offsets_ref, bytes_ref, dtype, None)
        } else {
            VarBinArray::new(
                offsets_ref,
                bytes_ref,
                dtype.as_nullable(),
                Some(validity.into()),
            )
        }
    }

    pub fn iter_primitive(&self) -> VortexResult<VarBinPrimitiveIter> {
        self.bytes()
            .maybe_primitive()
            .zip(self.offsets().maybe_primitive())
            .ok_or_else(|| {
                VortexError::ComputeError("Bytes array was not a primitive array".into())
            })
            .map(|(b, o)| VarBinPrimitiveIter::new(b.typed_data::<u8>(), o))
    }

    pub fn iter(&self) -> VarBinIter {
        VarBinIter::new(self.bytes(), self.offsets())
    }

    pub fn bytes_at(&self, index: usize) -> VortexResult<Vec<u8>> {
        let start = scalar_at(self.offsets(), index)?.try_into()?;
        let end = scalar_at(self.offsets(), index + 1)?.try_into()?;
        let sliced = self.bytes().slice(start, end)?;
        Ok(flatten_primitive(sliced.as_ref())?
            .typed_data::<u8>()
            .to_vec())
    }
}

impl Array for VarBinArray {
    impl_array!();

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

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;

        Ok(VarBinArray::new(
            self.offsets.slice(start, stop + 1)?,
            self.bytes.clone(),
            self.dtype.clone(),
            self.validity.as_ref().map(|v| v.slice(start, stop)),
        )
        .into_array())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &VarBinEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.bytes.nbytes() + self.offsets.nbytes()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }
}

impl ArrayValidity for VarBinArray {
    fn validity(&self) -> Option<Validity> {
        self.validity.clone()
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

impl ArrayDisplay for VarBinArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.child("offsets", self.offsets())?;
        f.child("bytes", self.bytes())?;
        f.validity(self.validity())
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
    use vortex_schema::{DType, Nullability};

    use crate::array::primitive::PrimitiveArray;
    use crate::array::varbin::VarBinArray;
    use crate::array::Array;
    use crate::compute::scalar_at::scalar_at;

    fn binary_array() -> VarBinArray {
        let values = PrimitiveArray::from(
            "hello worldhello world this is a long string"
                .as_bytes()
                .to_vec(),
        );
        let offsets = PrimitiveArray::from(vec![0, 11, 44]);

        VarBinArray::new(
            offsets.into_array(),
            values.into_array(),
            DType::Utf8(Nullability::NonNullable),
            None,
        )
    }

    #[test]
    pub fn test_scalar_at() {
        let binary_arr = binary_array();
        assert_eq!(binary_arr.len(), 2);
        assert_eq!(scalar_at(&binary_arr, 0), Ok("hello world".into()));
        assert_eq!(
            scalar_at(&binary_arr, 1),
            Ok("hello world this is a long string".into())
        )
    }

    #[test]
    pub fn slice() {
        let binary_arr = binary_array().slice(1, 2).unwrap();
        assert_eq!(
            scalar_at(&binary_arr, 0),
            Ok("hello world this is a long string".into())
        );
    }
}
