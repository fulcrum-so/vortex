use std::hash::{Hash, Hasher};

use ahash::RandomState;
use hashbrown::hash_map::{Entry, RawEntryMut};
use hashbrown::HashMap;
use log::debug;
use num_traits::{AsPrimitive, FromPrimitive, Unsigned};

use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::varbin::VarBinArray;
use vortex::array::{Array, ArrayKind, ArrayRef, CloneOptionalArray};
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::compute::scalar_at::scalar_at;
use vortex::dtype::DType;
use vortex::error::VortexResult;
use vortex::match_each_native_ptype;
use vortex::ptype::NativePType;
use vortex::scalar::AsBytes;
use vortex::stats::Stat;

use crate::dict::{DictArray, DictEncoding};
use crate::downcast::DowncastDict;

impl EncodingCompression for DictEncoding {
    fn can_compress(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        // TODO(robert): Add support for VarBinView
        if !matches!(
            ArrayKind::from(array),
            ArrayKind::Primitive(_) | ArrayKind::VarBin(_)
        ) {
            return None;
        };

        // No point dictionary coding if the array is unique.
        // We don't have a unique stat yet, but strict-sorted implies unique.
        if array
            .stats()
            .get_or_compute_as(&Stat::IsStrictSorted)
            .unwrap_or(false)
        {
            debug!("Skipping Dict: array is strict_sorted");
            return None;
        }

        Some(self)
    }

    fn compress(
        &self,
        array: &dyn Array,
        like: Option<&dyn Array>,
        ctx: &CompressCtx,
    ) -> VortexResult<ArrayRef> {
        let dict_like = like.map(|like_arr| like_arr.as_dict());

        // Exclude dict encoding from the next level
        let ctx = ctx.next_level().excluding(&DictEncoding::ID);

        let (codes, dict) = match ArrayKind::from(array) {
            ArrayKind::Primitive(p) => {
                let (codes, dict) = dict_encode_primitive(p);
                (
                    ctx.compress(codes.as_ref(), dict_like.map(|dict| dict.codes()))?,
                    ctx.compress(dict.as_ref(), dict_like.map(|dict| dict.dict()))?,
                )
            }
            ArrayKind::VarBin(vb) => {
                let (codes, dict) = dict_encode_varbin(vb);
                (
                    ctx.compress(codes.as_ref(), dict_like.map(|dict| dict.codes()))?,
                    ctx.compress(dict.as_ref(), dict_like.map(|dict| dict.dict()))?,
                )
            }

            _ => unreachable!("This array kind should have been filtered out"),
        };

        Ok(DictArray::new(codes, dict).boxed())
    }
}

#[derive(Debug)]
struct Value<T>(T);

impl<T: AsBytes> Hash for Value<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.as_bytes().hash(state)
    }
}

impl<T: AsBytes> PartialEq<Self> for Value<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_bytes().eq(other.0.as_bytes())
    }
}

impl<T: AsBytes> Eq for Value<T> {}

// TODO(robert): Use distinct count instead of len for width estimation
pub fn dict_encode_primitive(array: &PrimitiveArray) -> (PrimitiveArray, PrimitiveArray) {
    match_each_native_ptype!(array.ptype(), |$P| {
        if array.len() < u8::MAX as usize {
            dict_encode_typed_primitive::<u8, $P>(array)
        } else if array.len() < u16::MAX as usize {
            dict_encode_typed_primitive::<u16, $P>(array)
        } else if array.len() < u32::MAX as usize {
            dict_encode_typed_primitive::<u32, $P>(array)
        } else {
            dict_encode_typed_primitive::<u64, $P>(array)
        }
    })
}

/// Dictionary encode primitive array with given PType.
/// Null values in the original array are encoded in the dictionary.
fn dict_encode_typed_primitive<
    K: NativePType + Unsigned + FromPrimitive + AsPrimitive<u64>,
    T: NativePType,
>(
    array: &PrimitiveArray,
) -> (PrimitiveArray, PrimitiveArray) {
    let mut lookup_dict: HashMap<Value<T>, u64> = HashMap::new();
    let mut codes: Vec<K> = Vec::new();
    let mut values: Vec<T> = Vec::new();
    for v in array.buffer().typed_data::<T>() {
        let code: K = match lookup_dict.entry(Value(*v)) {
            Entry::Occupied(o) => K::from_u64(*o.get()).unwrap(),
            Entry::Vacant(vac) => {
                let next_code = <K as FromPrimitive>::from_usize(values.len()).unwrap();
                vac.insert(next_code.as_());
                values.push(*v);
                next_code
            }
        };
        codes.push(code)
    }

    (
        PrimitiveArray::from_nullable(codes, array.validity().clone_optional()),
        PrimitiveArray::from(values),
    )
}

// TODO(robert): Estimation of offsets array width could be better if we had average size and distinct count
macro_rules! dict_encode_offsets_codes {
    ($bytes_len:expr, $offsets_len:expr, | $_1:tt $codes:ident, $_2:tt $offsets:ident | $($body:tt)*) => ({
        macro_rules! __with__ {( $_1 $codes:ident, $_2 $offsets:ident ) => ( $($body)* )}
        if $bytes_len < u32::MAX as usize {
            if $offsets_len < u8::MAX as usize {
                __with__! { u32, u8 }
            } else if $offsets_len < u16::MAX as usize {
                __with__! { u32, u16 }
            } else if $offsets_len < u32::MAX as usize {
                __with__! { u32, u32 }
            } else {
                __with__! { u32, u64 }
            }
        } else {
            if $offsets_len < u8::MAX as usize {
                __with__! { u64, u8 }
            } else if $offsets_len < u16::MAX as usize {
                __with__! { u64, u16 }
            } else if $offsets_len < u32::MAX as usize {
                __with__! { u64, u32 }
            } else {
                __with__! { u64, u64 }
            }
        }
    })
}

/// Dictionary encode varbin array. Specializes for primitive byte arrays to avoid double copying
pub fn dict_encode_varbin(array: &VarBinArray) -> (PrimitiveArray, VarBinArray) {
    if let Some(bytes) = array.bytes().maybe_primitive() {
        let bytes = bytes.buffer().typed_data::<u8>();
        return if let Some(offsets) = array.offsets().maybe_primitive() {
            match_each_native_ptype!(offsets.ptype(), |$P| {
                let offsets = offsets.buffer().typed_data::<$P>();

                dict_encode_offsets_codes!(bytes.len(), array.offsets().len(), |$O, $C| {
                    dict_encode_typed_varbin::<$O, $C, _, &[u8]>(
                        array.dtype().clone(),
                        |idx| bytes_at_primitive(offsets, bytes, idx),
                        array.len(),
                        array.validity()
                    )
                })
            })
        } else {
            dict_encode_offsets_codes!(bytes.len(), array.offsets().len(), |$O, $C| {
                dict_encode_typed_varbin::<$O, $C, _, &[u8]>(
                    array.dtype().clone(),
                    |idx| bytes_at(array.offsets(), bytes, idx),
                    array.len(),
                    array.validity()
                )
            })
        };
    }

    dict_encode_offsets_codes!(array.bytes().len(), array.offsets().len(), |$O, $C| {
        dict_encode_typed_varbin::<$O, $C, _, Vec<u8>>(
            array.dtype().clone(),
            |idx| array.bytes_at(idx).unwrap(),
            array.len(),
            array.validity()
        )
    })
}

fn bytes_at_primitive<'a, T: NativePType + AsPrimitive<usize>>(
    offsets: &'a [T],
    bytes: &'a [u8],
    idx: usize,
) -> &'a [u8] {
    let begin: usize = offsets[idx].as_();
    let end: usize = offsets[idx + 1].as_();
    &bytes[begin..end]
}

fn bytes_at<'a>(offsets: &'a dyn Array, bytes: &'a [u8], idx: usize) -> &'a [u8] {
    let start: usize = scalar_at(offsets, idx).unwrap().try_into().unwrap();
    let stop: usize = scalar_at(offsets, idx + 1).unwrap().try_into().unwrap();
    &bytes[start..stop]
}

fn dict_encode_typed_varbin<O, K, V, U>(
    dtype: DType,
    value_lookup: V,
    len: usize,
    validity: Option<&dyn Array>,
) -> (PrimitiveArray, VarBinArray)
where
    O: NativePType + Unsigned + FromPrimitive + AsPrimitive<usize>,
    K: NativePType + Unsigned + FromPrimitive + AsPrimitive<usize>,
    V: Fn(usize) -> U,
    U: AsRef<[u8]>,
{
    let hasher = RandomState::new();
    let mut lookup_dict: HashMap<K, (), ()> = HashMap::with_hasher(());
    let mut codes: Vec<K> = Vec::with_capacity(len);
    let mut bytes: Vec<u8> = Vec::new();
    let mut offsets: Vec<O> = Vec::new();
    offsets.push(O::zero());

    for i in 0..len {
        let byte_val = value_lookup(i);
        let byte_ref = byte_val.as_ref();
        let value_hash = hasher.hash_one(byte_ref);
        let raw_entry = lookup_dict.raw_entry_mut().from_hash(value_hash, |idx| {
            byte_ref == bytes_at_primitive(offsets.as_slice(), bytes.as_slice(), idx.as_())
        });

        let code: K = match raw_entry {
            RawEntryMut::Occupied(o) => *o.into_key(),
            RawEntryMut::Vacant(vac) => {
                let next_code = <K as FromPrimitive>::from_usize(offsets.len() - 1).unwrap();
                bytes.extend_from_slice(byte_ref);
                offsets.push(<O as FromPrimitive>::from_usize(bytes.len()).unwrap());
                vac.insert_with_hasher(value_hash, next_code, (), |idx| {
                    hasher.hash_one(bytes_at_primitive(
                        offsets.as_slice(),
                        bytes.as_slice(),
                        idx.as_(),
                    ))
                });
                next_code
            }
        };
        codes.push(code)
    }
    (
        PrimitiveArray::from_nullable(codes, validity.clone_optional()),
        VarBinArray::new(
            PrimitiveArray::from(offsets).boxed(),
            PrimitiveArray::from(bytes).boxed(),
            dtype,
            None,
        ),
    )
}

#[cfg(test)]
mod test {
    use vortex::array::downcast::DowncastArrayBuiltin;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::array::varbin::VarBinArray;
    use vortex::compute::scalar_at::scalar_at;

    use crate::compress::{dict_encode_typed_primitive, dict_encode_varbin};

    #[test]
    fn encode_primitive() {
        let arr = PrimitiveArray::from(vec![1, 1, 3, 3, 3]);
        let (codes, values) = dict_encode_typed_primitive::<u8, i32>(&arr);
        assert_eq!(codes.buffer().typed_data::<u8>(), &[0, 0, 1, 1, 1]);
        assert_eq!(values.buffer().typed_data::<i32>(), &[1, 3]);
    }

    #[test]
    fn encode_primitive_nulls() {
        let arr = PrimitiveArray::from_iter(vec![
            Some(1),
            Some(1),
            None,
            Some(3),
            Some(3),
            None,
            Some(3),
            None,
        ]);
        let (codes, values) = dict_encode_typed_primitive::<u8, i32>(&arr);
        assert_eq!(codes.buffer().typed_data::<u8>(), &[0, 0, 1, 2, 2, 1, 2, 1]);
        assert!(!codes.is_valid(2));
        assert!(!codes.is_valid(5));
        assert!(!codes.is_valid(7));
        assert_eq!(scalar_at(values.as_ref(), 0), Ok(1.into()));
        assert_eq!(scalar_at(values.as_ref(), 2), Ok(3.into()));
    }

    #[test]
    fn encode_varbin() {
        let arr = VarBinArray::from(vec!["hello", "world", "hello", "again", "world"]);
        let (codes, values) = dict_encode_varbin(&arr);
        assert_eq!(codes.buffer().typed_data::<u8>(), &[0, 1, 0, 2, 1]);
        assert_eq!(
            String::from_utf8(values.bytes_at(0).unwrap()).unwrap(),
            "hello"
        );
        assert_eq!(
            String::from_utf8(values.bytes_at(1).unwrap()).unwrap(),
            "world"
        );
        assert_eq!(
            String::from_utf8(values.bytes_at(2).unwrap()).unwrap(),
            "again"
        );
    }

    #[test]
    fn encode_varbin_nulls() {
        let arr: VarBinArray = vec![
            Some("hello"),
            None,
            Some("world"),
            Some("hello"),
            None,
            Some("again"),
            Some("world"),
            None,
        ]
        .into_iter()
        .collect();
        let (codes, values) = dict_encode_varbin(&arr);
        assert_eq!(codes.buffer().typed_data::<u8>(), &[0, 1, 2, 0, 1, 3, 2, 1]);
        assert!(!codes.is_valid(1));
        assert!(!codes.is_valid(4));
        assert!(!codes.is_valid(7));
        assert_eq!(
            String::from_utf8(values.bytes_at(0).unwrap()).unwrap(),
            "hello"
        );
        assert_eq!(String::from_utf8(values.bytes_at(1).unwrap()).unwrap(), "");
        assert_eq!(
            String::from_utf8(values.bytes_at(2).unwrap()).unwrap(),
            "world"
        );
        assert_eq!(
            String::from_utf8(values.bytes_at(3).unwrap()).unwrap(),
            "again"
        );
    }

    #[test]
    fn repeated_values() {
        let arr = VarBinArray::from(vec!["a", "a", "b", "b", "a", "b", "a", "b"]);
        let (codes, values) = dict_encode_varbin(&arr);
        assert_eq!(
            values.bytes().as_primitive().typed_data::<u8>(),
            "ab".as_bytes()
        );
        assert_eq!(
            values.offsets().as_primitive().typed_data::<u32>(),
            &[0, 1, 2]
        );
        assert_eq!(codes.typed_data::<u8>(), &[0u8, 0, 1, 1, 0, 1, 0, 1]);
    }
}
