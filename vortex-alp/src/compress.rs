use itertools::Itertools;
// use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::sparse::SparseArray;
// use vortex::array::{Array, ArrayRef};
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::compute::ArrayCompute;
// use vortex::encoding::ArrayEncodingRef;
// use vortex::compute::ArrayCompute;
// use vortex::compute::flatten::flatten_primitive;
use vortex::ptype::{NativePType, PType};
use vortex::scalar::Scalar;
use vortex::{Array, ArrayDType, AsArray, IntoArray};
// use vortex::compute::patch::PatchFn;
// use vortex::view::ToOwnedView;
use vortex_error::{vortex_bail, vortex_err, VortexResult};

use crate::alp::ALPFloat;
use crate::array::{ALPArray, ALPEncoding};
// use crate::downcast::DowncastALP;
use crate::Exponents;

#[macro_export]
macro_rules! match_each_alp_float_ptype {
    ($self:expr, | $_:tt $enc:ident | $($body:tt)*) => ({
        macro_rules! __with__ {( $_ $enc:ident ) => ( $($body)* )}
        use vortex_error::vortex_err;
        use vortex::ptype::PType;
        let ptype = $self;
        match ptype {
            PType::F32 => Ok(__with__! { f32 }),
            PType::F64 => Ok(__with__! { f64 }),
            _ => Err(vortex_err!("ALP can only encode f32 and f64")),
        }
    })
}

impl EncodingCompression for ALPEncoding {
    fn can_compress(
        &self,
        array: &Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        // Only support primitive arrays
        let parray = array.clone().flatten_primitive().unwrap();

        // Only supports f32 and f64
        if !matches!(parray.ptype(), PType::F32 | PType::F64) {
            return None;
        }

        Some(self)
    }

    fn compress(
        &self,
        array: &Array,
        like: Option<&Array>,
        ctx: CompressCtx,
    ) -> VortexResult<Array<'static>> {
        let like_alp = like.map(|like_array| like_array.as_array_ref());

        // TODO(ngates): fill forward nulls
        let parray = array.clone().flatten_primitive()?;

        let (exponents, encoded, patches) = match_each_alp_float_ptype!(
            parray.ptype(), |$T| {
            encode_to_array::<$T>(&parray, None)
        })?;

        let compressed_encoded = ctx
            .named("packed")
            .excluding(&ALPEncoding)
            .compress(encoded.as_array_ref(), like_alp)?;

        let compressed_patches = patches
            .map(|p| {
                ctx.auxiliary("patches")
                    .excluding(&ALPEncoding)
                    .compress(p.as_array_ref(), like_alp)
            })
            .transpose()?;

        ALPArray::try_new(compressed_encoded, exponents, compressed_patches).map(|a| a.into_array())
    }
}

fn encode_to_array<'a, T>(
    values: &'a PrimitiveArray<'a>,
    exponents: Option<&'a Exponents>,
) -> (Exponents, Array<'a>, Option<Array<'a>>)
where
    T: ALPFloat + NativePType,
    T::ALPInt: NativePType,
{
    let (exponents, encoded, exc_pos, exc) = T::encode(values.typed_data::<T>(), exponents);
    let len = encoded.len();
    (
        exponents,
        PrimitiveArray::from(encoded)
            // .into_nullable(values.nullability())
            .into_array(),
        (!exc.is_empty()).then(|| {
            SparseArray::new(
                PrimitiveArray::from(exc_pos).into_array(),
                PrimitiveArray::from(exc).into_array(),
                len,
                Scalar::null(&values.dtype().as_nullable()),
            )
            .into_array()
        }),
    )
}

pub(crate) fn alp_encode(parray: &PrimitiveArray) -> VortexResult<ALPArray<'static>> {
    let (exponents, encoded, patches) = match parray.ptype() {
        PType::F32 => encode_to_array::<f32>(parray, None),
        PType::F64 => encode_to_array::<f64>(parray, None),
        _ => vortex_bail!("ALP can only encode f32 and f64"),
    };
    ALPArray::try_new(encoded, exponents, patches)
}

pub fn decompress(array: ALPArray) -> VortexResult<PrimitiveArray> {
    let binding = array.clone();
    let encoded = binding.encoded().clone().flatten_primitive()?;
    let decoded = match_each_alp_float_ptype!(array.dtype().try_into().unwrap(), |$T| {
        PrimitiveArray::from_vec(
            decompress_primitive::<$T>(encoded.typed_data(), array.exponents()),
            encoded.validity(),
        )
    })?;

    if let Some(patches) = array.patches() {
        patch_decoded(decoded, &patches)
    } else {
        Ok(decoded)
    }
}

fn patch_decoded<'a>(
    array: PrimitiveArray<'a>,
    patches: &Array,
) -> VortexResult<PrimitiveArray<'a>> {
    match array.patch() {
        None => Ok(array),
        Some(some_fn) => some_fn.patch(patches)?.flatten_primitive(),
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
            encoded
                .encoded()
                .clone()
                .flatten_primitive()
                .unwrap()
                .typed_data::<i32>(),
            vec![1234; 1025]
        );
        assert_eq!(encoded.exponents(), &Exponents { e: 4, f: 1 });

        let decoded = decompress(encoded).unwrap();
        assert_eq!(array.typed_data::<f32>(), decoded.typed_data::<f32>());
    }

    #[test]
    fn test_nullable_compress() {
        let array = PrimitiveArray::from_nullable_vec(vec![None, Some(1.234f32), None]);
        let encoded = alp_encode(&array).unwrap();
        println!("Encoded {:?}", encoded);
        assert!(encoded.patches().is_none());
        assert_eq!(
            encoded
                .encoded()
                .clone()
                .flatten_primitive()
                .unwrap()
                .typed_data::<i32>(),
            vec![0, 1234, 0]
        );
        assert_eq!(encoded.exponents(), &Exponents { e: 4, f: 1 });

        let decoded = decompress(encoded).unwrap();
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
            encoded
                .encoded()
                .clone()
                .flatten_primitive()
                .unwrap()
                .typed_data::<i64>(),
            vec![1234i64, 2718, 2718, 4000] // fill forward
        );
        assert_eq!(encoded.exponents(), &Exponents { e: 3, f: 0 });

        let decoded = decompress(encoded).unwrap();
        assert_eq!(values, decoded.typed_data::<f64>());
    }
}
