use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};
use vortex_dtype::Nullability;
use vortex_dtype::{match_each_native_ptype, NativePType};
use vortex_error::vortex_bail;
use vortex_scalar::{BinaryScalar, Scalar, Utf8Scalar};

use crate::array::varbin::builder::VarBinBuilder;
use crate::compute::scalar_at::scalar_at;
use crate::compute::slice::slice;
use crate::validity::{Validity, ValidityMetadata};
use crate::{impl_encoding, ArrayDType, OwnedArray, ToArrayData};

mod accessor;
mod array;
pub mod builder;
mod compute;
mod flatten;
mod stats;

pub use stats::compute_stats;

use crate::array::primitive::PrimitiveArray;

impl_encoding!("vortex.varbin", VarBin);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VarBinMetadata {
    validity: ValidityMetadata,
    offsets_dtype: DType,
}

impl VarBinArray<'_> {
    pub fn try_new(
        offsets: Array,
        bytes: Array,
        dtype: DType,
        validity: Validity,
    ) -> VortexResult<Self> {
        if !offsets.dtype().is_int() || offsets.dtype().is_nullable() {
            vortex_bail!(MismatchedTypes: "non nullable int", offsets.dtype());
        }
        if !matches!(bytes.dtype(), &DType::BYTES,) {
            vortex_bail!(MismatchedTypes: "u8", bytes.dtype());
        }
        if !matches!(dtype, DType::Binary(_) | DType::Utf8(_)) {
            vortex_bail!(MismatchedTypes: "utf8 or binary", dtype);
        }
        if dtype.is_nullable() == (validity == Validity::NonNullable) {
            vortex_bail!("incorrect validity {:?}", validity);
        }

        let metadata = VarBinMetadata {
            validity: validity.to_metadata(offsets.len() - 1)?,
            offsets_dtype: offsets.dtype().clone(),
        };

        let mut children = Vec::with_capacity(3);
        children.push(offsets.to_array_data());
        children.push(bytes.to_array_data());
        if let Some(a) = validity.into_array_data() {
            children.push(a)
        }

        Self::try_from_parts(dtype, metadata, children.into(), StatsSet::new())
    }

    #[inline]
    pub fn offsets(&self) -> Array {
        self.array()
            .child(0, &self.metadata().offsets_dtype)
            .expect("missing offsets")
    }

    pub fn first_offset<T: NativePType + TryFrom<Scalar, Error = VortexError>>(
        &self,
    ) -> VortexResult<T> {
        scalar_at(&self.offsets(), 0)?
            .cast(&DType::from(T::PTYPE))?
            .try_into()
    }

    #[inline]
    pub fn bytes(&self) -> Array {
        self.array().child(1, &DType::BYTES).expect("missing bytes")
    }

    pub fn validity(&self) -> Validity {
        self.metadata()
            .validity
            .to_validity(self.array().child(2, &Validity::DTYPE))
    }

    pub fn sliced_bytes(&self) -> VortexResult<OwnedArray> {
        let first_offset: usize = scalar_at(&self.offsets(), 0)?.try_into()?;
        let last_offset: usize =
            scalar_at(&self.offsets(), self.offsets().len() - 1)?.try_into()?;
        slice(&self.bytes(), first_offset, last_offset)
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
        K: NativePType,
        T: AsRef<[u8]>,
    {
        let mut builder = VarBinBuilder::<K>::with_capacity(vec.len());
        for v in vec {
            builder.push_value(v.as_ref());
        }
        builder.finish(dtype)
    }

    pub fn from_iter<T: AsRef<[u8]>, I: IntoIterator<Item = Option<T>>>(
        iter: I,
        dtype: DType,
    ) -> Self {
        let iter = iter.into_iter();
        let mut builder = VarBinBuilder::<u64>::with_capacity(iter.size_hint().0);
        for v in iter {
            builder.push(v.as_ref().map(|o| o.as_ref()));
        }
        builder.finish(dtype)
    }

    pub fn offset_at(&self, index: usize) -> usize {
        PrimitiveArray::try_from(self.offsets())
            .ok()
            .map(|p| {
                match_each_native_ptype!(p.ptype(), |$P| {
                    p.typed_data::<$P>()[index].as_()
                })
            })
            .unwrap_or_else(|| {
                scalar_at(&self.offsets(), index)
                    .unwrap()
                    .try_into()
                    .unwrap()
            })
    }

    pub fn bytes_at(&self, index: usize) -> VortexResult<Vec<u8>> {
        let start = self.offset_at(index);
        let end = self.offset_at(index + 1);
        let sliced = slice(&self.bytes(), start, end)?;
        Ok(sliced.flatten_primitive()?.buffer().as_slice().to_vec())
    }
}

impl From<Vec<&[u8]>> for VarBinArray<'_> {
    fn from(value: Vec<&[u8]>) -> Self {
        VarBinArray::from_vec(value, DType::Binary(Nullability::NonNullable))
    }
}

impl From<Vec<Vec<u8>>> for VarBinArray<'_> {
    fn from(value: Vec<Vec<u8>>) -> Self {
        VarBinArray::from_vec(value, DType::Binary(Nullability::NonNullable))
    }
}

impl From<Vec<String>> for VarBinArray<'_> {
    fn from(value: Vec<String>) -> Self {
        VarBinArray::from_vec(value, DType::Utf8(Nullability::NonNullable))
    }
}

impl From<Vec<&str>> for VarBinArray<'_> {
    fn from(value: Vec<&str>) -> Self {
        VarBinArray::from_vec(value, DType::Utf8(Nullability::NonNullable))
    }
}

impl<'a> FromIterator<Option<&'a [u8]>> for VarBinArray<'_> {
    fn from_iter<T: IntoIterator<Item = Option<&'a [u8]>>>(iter: T) -> Self {
        VarBinArray::from_iter(iter, DType::Binary(Nullability::Nullable))
    }
}

impl FromIterator<Option<Vec<u8>>> for VarBinArray<'_> {
    fn from_iter<T: IntoIterator<Item = Option<Vec<u8>>>>(iter: T) -> Self {
        VarBinArray::from_iter(iter, DType::Binary(Nullability::Nullable))
    }
}

impl FromIterator<Option<String>> for VarBinArray<'_> {
    fn from_iter<T: IntoIterator<Item = Option<String>>>(iter: T) -> Self {
        VarBinArray::from_iter(iter, DType::Utf8(Nullability::Nullable))
    }
}

impl<'a> FromIterator<Option<&'a str>> for VarBinArray<'_> {
    fn from_iter<T: IntoIterator<Item = Option<&'a str>>>(iter: T) -> Self {
        VarBinArray::from_iter(iter, DType::Utf8(Nullability::Nullable))
    }
}

pub fn varbin_scalar(value: Vec<u8>, dtype: &DType) -> Scalar {
    if matches!(dtype, DType::Utf8(_)) {
        let str = unsafe { String::from_utf8_unchecked(value) };
        Utf8Scalar::try_new(Some(str), dtype.nullability())
            .unwrap()
            .into()
    } else {
        BinaryScalar::try_new(Some(value), dtype.nullability())
            .unwrap()
            .into()
    }
}

impl EncodingCompression for VarBinEncoding {}

#[cfg(test)]
mod test {
    use vortex_dtype::{DType, Nullability};

    use crate::array::primitive::PrimitiveArray;
    use crate::array::varbin::VarBinArray;
    use crate::compute::scalar_at::scalar_at;
    use crate::compute::slice::slice;
    use crate::validity::Validity;
    use crate::{IntoArray, OwnedArray};

    fn binary_array() -> OwnedArray {
        let values = PrimitiveArray::from(
            "hello worldhello world this is a long string"
                .as_bytes()
                .to_vec(),
        );
        let offsets = PrimitiveArray::from(vec![0, 11, 44]);

        VarBinArray::try_new(
            offsets.into_array(),
            values.into_array(),
            DType::Utf8(Nullability::NonNullable),
            Validity::NonNullable,
        )
        .unwrap()
        .into_array()
    }

    #[test]
    pub fn test_scalar_at() {
        let binary_arr = binary_array();
        assert_eq!(binary_arr.len(), 2);
        assert_eq!(scalar_at(&binary_arr, 0).unwrap(), "hello world".into());
        assert_eq!(
            scalar_at(&binary_arr, 1).unwrap(),
            "hello world this is a long string".into()
        )
    }

    #[test]
    pub fn slice_array() {
        let binary_arr = slice(&binary_array(), 1, 2).unwrap();
        assert_eq!(
            scalar_at(&binary_arr, 0).unwrap(),
            "hello world this is a long string".into()
        );
    }
}
