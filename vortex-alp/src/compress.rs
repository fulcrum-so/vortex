use itertools::Itertools;
use std::sync::Arc;

use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::sparse::SparseArray;
use vortex::array::{Array, ArrayRef};
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::compute::flatten::flatten_primitive;
use vortex::compute::patch::patch;
use vortex::error::{VortexError, VortexResult};
use vortex::ptype::{NativePType, PType};

use crate::alp::ALPFloat;
use crate::array::{ALPArray, ALPEncoding};
use crate::downcast::DowncastALP;
use crate::Exponents;

#[macro_export]
macro_rules! match_each_alp_float_ptype {
    ($self:expr, | $_:tt $enc:ident | $($body:tt)*) => ({
        macro_rules! __with__ {( $_ $enc:ident ) => ( $($body)* )}
        use vortex::error::VortexError;
        use vortex::ptype::PType;
        let ptype = $self;
        match ptype {
            PType::F32 => Ok(__with__! { f32 }),
            PType::F64 => Ok(__with__! { f64 }),
            _ => Err(VortexError::InvalidPType(ptype))
        }
    })
}

impl EncodingCompression for ALPEncoding {
    fn can_compress(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        // Only support primitive arrays
        let parray = array.maybe_primitive()?;

        // Only supports f32 and f64
        if !matches!(parray.ptype(), PType::F32 | PType::F64) {
            return None;
        }

        Some(self)
    }

    fn compress(
        &self,
        array: &dyn Array,
        like: Option<&dyn Array>,
        ctx: CompressCtx,
    ) -> VortexResult<ArrayRef> {
        let like_alp = like.map(|like_array| like_array.as_alp());

        // TODO(ngates): fill forward nulls
        let parray = array.as_primitive();

        let (exponents, encoded, patches) = match_each_alp_float_ptype!(
            *parray.ptype(), |$T| {
            encode_to_array(parray.typed_data::<$T>(), like_alp.map(|l| l.exponents()))
        })?;

        let compressed_encoded = ctx
            .named("packed")
            .excluding(&ALPEncoding::ID)
            .compress(encoded.as_ref(), like_alp.map(|a| a.encoded()))?;

        let compressed_patches = patches
            .map(|p| {
                ctx.auxiliary("patches")
                    .excluding(&ALPEncoding::ID)
                    .compress(p.as_ref(), like_alp.and_then(|a| a.patches()))
            })
            .transpose()?;

        Ok(ALPArray::new(compressed_encoded, exponents, compressed_patches).into_array())
    }
}

fn encode_to_array<T>(
    values: &[T],
    exponents: Option<&Exponents>,
) -> (Exponents, ArrayRef, Option<ArrayRef>)
where
    T: ALPFloat + NativePType,
    T::ALPInt: NativePType,
{
    let (exponents, values, exc_pos, exc) = T::encode(values, exponents);
    let len = values.len();
    (
        exponents,
        PrimitiveArray::from(values).into_array(),
        (!exc.is_empty()).then(|| {
            SparseArray::new(
                PrimitiveArray::from(exc_pos).into_array(),
                PrimitiveArray::from(exc).into_array(),
                len,
            )
            .into_array()
        }),
    )
}

pub(crate) fn alp_encode(parray: &PrimitiveArray) -> VortexResult<ALPArray> {
    let (exponents, encoded, patches) = match parray.ptype() {
        PType::F32 => encode_to_array(parray.typed_data::<f32>(), None),
        PType::F64 => encode_to_array(parray.typed_data::<f64>(), None),
        _ => return Err(VortexError::InvalidPType(*parray.ptype())),
    };
    Ok(ALPArray::new(encoded, exponents, patches))
}

pub fn decompress(array: &ALPArray) -> VortexResult<PrimitiveArray> {
    let encoded = flatten_primitive(array.encoded())?;
    let decoded = match_each_alp_float_ptype!(array.dtype().try_into().unwrap(), |$T| {
        PrimitiveArray::from_nullable(
            decompress_primitive::<$T>(encoded.typed_data(), array.exponents()),
            encoded.validity().cloned(),
        )
    })?;
    if let Some(patches) = array.patches() {
        // TODO(#121): right now, applying patches forces an extraneous copy of the array data
        let patched = patch(&decoded, patches)?;
        let patched_encoding_id = patched.encoding().id().clone();
        patched
            .into_any()
            .downcast::<PrimitiveArray>()
            .map_err(|_| VortexError::InvalidEncoding(patched_encoding_id))
            .map(|p| {
                Arc::into_inner(p).expect("Patches were just constructed, expected one strong ref")
            })
    } else {
        Ok(decoded)
    }
}

fn decompress_primitive<T: NativePType + ALPFloat>(
    values: &[T::ALPInt],
    exponents: &Exponents,
) -> Vec<T> {
    values
        .iter()
        .map(|&v| T::decode_single(v, exponents))
        .collect_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress() {
        let array = PrimitiveArray::from(vec![1.234f32; 1025]);
        let encoded = alp_encode(&array).unwrap();
        assert!(encoded.patches().is_none());
        assert_eq!(
            encoded.encoded().as_primitive().typed_data::<i32>(),
            vec![1234; 1025]
        );
        assert_eq!(encoded.exponents(), &Exponents { e: 4, f: 1 });

        let decoded = decompress(&encoded).unwrap();
        assert_eq!(array.typed_data::<f32>(), decoded.typed_data::<f32>());
    }

    #[test]
    fn test_nullable_compress() {
        let array = PrimitiveArray::from_iter(vec![None, Some(1.234f32), None]);
        let encoded = alp_encode(&array).unwrap();
        println!("Encoded {:?}", encoded);
        assert!(encoded.patches().is_none());
        assert_eq!(
            encoded.encoded().as_primitive().typed_data::<i32>(),
            vec![0, 1234, 0]
        );
        assert_eq!(encoded.exponents(), &Exponents { e: 4, f: 1 });

        let decoded = decompress(&encoded).unwrap();
        let expected = vec![0f32, 1.234f32, 0f32];
        assert_eq!(decoded.typed_data::<f32>(), expected.as_slice());
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_patched_compress() {
        let values = vec![1.234f64, 2.718, std::f64::consts::PI, 4.0];
        let array = PrimitiveArray::from(values.clone());
        let encoded = alp_encode(&array).unwrap();
        println!("Encoded {:?}", encoded);
        assert!(encoded.patches().is_some());
        assert_eq!(
            encoded.encoded().as_primitive().typed_data::<i64>(),
            vec![1234i64, 2718, 2718, 4000] // fill forward
        );
        assert_eq!(encoded.exponents(), &Exponents { e: 3, f: 0 });

        let decoded = decompress(&encoded).unwrap();
        assert_eq!(values, decoded.typed_data::<f64>());
    }
}
