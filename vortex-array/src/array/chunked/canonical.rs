use arrow_buffer::{BooleanBuffer, MutableBuffer, ScalarBuffer};
use itertools::Itertools;
use vortex_dtype::{match_each_native_ptype, DType, Nullability, PType, StructDType};
use vortex_error::{vortex_bail, ErrString, VortexResult};

use crate::accessor::ArrayAccessor;
use crate::array::bool::BoolArray;
use crate::array::chunked::ChunkedArray;
use crate::array::extension::ExtensionArray;
use crate::array::null::NullArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::struct_::StructArray;
use crate::array::varbin::builder::VarBinBuilder;
use crate::array::varbin::VarBinArray;
use crate::validity::Validity;
use crate::{
    Array, ArrayDType, ArrayTrait, ArrayValidity, Canonical, IntoArray, IntoArrayVariant,
    IntoCanonical,
};

impl IntoCanonical for ChunkedArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        try_canonicalize_chunks(self.chunks().collect(), self.dtype())
    }
}

pub(crate) fn try_canonicalize_chunks(
    chunks: Vec<Array>,
    dtype: &DType,
) -> VortexResult<Canonical> {
    let mismatched = chunks
        .iter()
        .filter(|chunk| !chunk.dtype().eq(dtype))
        .collect::<Vec<_>>();
    if !mismatched.is_empty() {
        vortex_bail!(MismatchedTypes: dtype.clone(), ErrString::from(format!("{:?}", mismatched)))
    }

    match dtype {
        // Structs can have their internal field pointers swizzled to push the chunking down
        // one level internally without copying or decompressing any data.
        DType::Struct(struct_dtype, _) => {
            let struct_array = swizzle_struct_chunks(chunks.as_slice(), struct_dtype)?;
            Ok(Canonical::Struct(struct_array))
        }

        // Extension arrays wrap an internal storage array, which can hold a ChunkedArray until
        // it is safe to unpack them.
        DType::Extension(ext_dtype, _) => {
            let ext_array = ExtensionArray::new(
                ext_dtype.clone(),
                ChunkedArray::try_new(chunks, dtype.clone())?.into_array(),
            );

            Ok(Canonical::Extension(ext_array))
        }

        // TODO(aduffy): better list support
        DType::List(..) => {
            todo!()
        }

        DType::Bool(nullability) => {
            let bool_array = pack_bools(chunks.as_slice(), *nullability)?;
            Ok(Canonical::Bool(bool_array))
        }
        DType::Primitive(ptype, nullability) => {
            let prim_array = pack_primitives(chunks.as_slice(), *ptype, *nullability)?;
            Ok(Canonical::Primitive(prim_array))
        }
        DType::Utf8(nullability) => {
            let varbin_array = pack_varbin(chunks.as_slice(), dtype, *nullability)?;
            Ok(Canonical::VarBin(varbin_array))
        }
        DType::Binary(nullability) => {
            let varbin_array = pack_varbin(chunks.as_slice(), dtype, *nullability)?;
            Ok(Canonical::VarBin(varbin_array))
        }
        DType::Null => {
            let len = chunks.iter().map(|chunk| chunk.len()).sum();
            let null_array = NullArray::new(len);
            Ok(Canonical::Null(null_array))
        }
    }
}

/// Swizzle the pointers within a ChunkedArray of StructArrays to instead be a single
/// StructArray, where the Array for each Field is a ChunkedArray.
///
/// It is expected this function is only called from [try_canonicalize_chunks], and thus all chunks have
/// been checked to have the same DType already.
fn swizzle_struct_chunks(
    chunks: &[Array],
    struct_dtype: &StructDType,
) -> VortexResult<StructArray> {
    let chunks: Vec<StructArray> = chunks.iter().map(StructArray::try_from).try_collect()?;

    let len = chunks.iter().map(|chunk| chunk.len()).sum();
    let validity = chunks
        .iter()
        .map(|chunk| chunk.logical_validity())
        .collect::<Validity>();

    let mut field_arrays = Vec::new();

    for (field_idx, field_dtype) in struct_dtype.dtypes().iter().enumerate() {
        let mut field_chunks = Vec::new();
        for chunk in &chunks {
            field_chunks.push(
                chunk
                    .field(field_idx)
                    .expect("all chunks must have same dtype"),
            );
        }
        let field_array = ChunkedArray::try_new(field_chunks, field_dtype.clone())?;
        field_arrays.push(field_array.into_array());
    }

    StructArray::try_new(struct_dtype.names().clone(), field_arrays, len, validity)
}

/// Builds a new [BoolArray] by repacking the values from the chunks in a single contiguous array.
///
/// It is expected this function is only called from [try_canonicalize_chunks], and thus all chunks have
/// been checked to have the same DType already.
fn pack_bools(chunks: &[Array], nullability: Nullability) -> VortexResult<BoolArray> {
    let len = chunks.iter().map(|chunk| chunk.len()).sum();
    let validity = validity_from_chunks(chunks, nullability);
    let mut bools = Vec::with_capacity(len);
    for chunk in chunks {
        let chunk = chunk.clone().into_bool()?;
        bools.extend(chunk.boolean_buffer().iter());
    }

    BoolArray::try_new(BooleanBuffer::from(bools), validity)
}

/// Builds a new [PrimitiveArray] by repacking the values from the chunks into a single
/// contiguous array.
///
/// It is expected this function is only called from [try_canonicalize_chunks], and thus all chunks have
/// been checked to have the same DType already.
fn pack_primitives(
    chunks: &[Array],
    ptype: PType,
    nullability: Nullability,
) -> VortexResult<PrimitiveArray> {
    let len: usize = chunks.iter().map(|chunk| chunk.len()).sum();
    let validity = validity_from_chunks(chunks, nullability);
    let mut buffer = MutableBuffer::with_capacity(len * ptype.byte_width());
    for chunk in chunks {
        let chunk = chunk.clone().into_primitive()?;
        buffer.extend_from_slice(chunk.buffer());
    }

    match_each_native_ptype!(ptype, |$T| {
        Ok(PrimitiveArray::try_new(
            ScalarBuffer::<$T>::from(buffer),
            validity,
        )?)
    })
}

/// Builds a new [VarBinArray] by repacking the values from the chunks into a single
/// contiguous array.
///
/// It is expected this function is only called from [try_canonicalize_chunks], and thus all chunks have
/// been checked to have the same DType already.
fn pack_varbin(
    chunks: &[Array],
    dtype: &DType,
    _nullability: Nullability,
) -> VortexResult<VarBinArray> {
    let len = chunks.iter().map(|chunk| chunk.len()).sum();
    let mut builder = VarBinBuilder::<i32>::with_capacity(len);

    for chunk in chunks {
        let chunk = chunk.clone().into_varbin()?;
        chunk.with_iterator(|iter| {
            for datum in iter {
                builder.push(datum);
            }
        })?;
    }

    Ok(builder.finish(dtype.clone()))
}

fn validity_from_chunks(chunks: &[Array], nullability: Nullability) -> Validity {
    if nullability == Nullability::NonNullable {
        Validity::NonNullable
    } else {
        chunks
            .iter()
            .map(|chunk| chunk.with_dyn(|a| a.logical_validity()))
            .collect()
    }
}
