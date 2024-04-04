use std::hash::{Hash, Hasher};

use ahash::RandomState;
use hashbrown::hash_map::{Entry, RawEntryMut};
use hashbrown::HashMap;
use num_traits::AsPrimitive;
use vortex::array::bool::BoolArray;
use vortex::array::primitive::{PrimitiveArray, PrimitiveEncoding, PrimitiveTrait};
use vortex::array::validity::Validity;
use vortex::array::varbin::{VarBinArray, VarBinEncoding};
use vortex::array::{Array, ArrayKind, ArrayRef};
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::match_each_native_ptype;
use vortex::ptype::NativePType;
use vortex::scalar::AsBytes;
use vortex::stats::Stat;
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::dict::{DictArray, DictEncoding};
use crate::downcast::DowncastDict;

impl EncodingCompression for DictEncoding {
    fn can_compress(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        // TODO(robert): Add support for VarBinView
        if array.encoding().id() != PrimitiveEncoding::ID
            && array.encoding().id() != VarBinEncoding::ID
        {
            return None;
        };

        // No point dictionary coding if the array is unique.
        // We don't have a unique stat yet, but strict-sorted implies unique.
        if array
            .stats()
            .get_or_compute_as(&Stat::IsStrictSorted)
            .unwrap_or(false)
        {
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
        let dict_like = like.map(|like_arr| like_arr.as_dict());

        let (codes, dict) = match ArrayKind::from(array) {
            ArrayKind::Primitive(p) => {
                let (codes, dict) = match_each_native_ptype!(p.ptype(), |$P| {
                    dict_encode_typed_primitive::<$P>(p)
                });
                (
                    ctx.auxiliary("codes")
                        .excluding(&DictEncoding)
                        .compress(&codes, dict_like.map(|dict| dict.codes()))?,
                    ctx.named("values")
                        .excluding(&DictEncoding)
                        .compress(&dict, dict_like.map(|dict| dict.values()))?,
                )
            }
            ArrayKind::VarBin(vb) => {
                let (codes, dict) = dict_encode_varbin(vb);
                (
                    ctx.auxiliary("codes")
                        .excluding(&DictEncoding)
                        .compress(&codes, dict_like.map(|dict| dict.codes()))?,
                    ctx.named("values")
                        .excluding(&DictEncoding)
                        .compress(&dict, dict_like.map(|dict| dict.values()))?,
                )
            }

            _ => unreachable!("This array kind should have been filtered out"),
        };

        Ok(DictArray::new(codes, dict).into_array())
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

/// Dictionary encode primitive array with given PType.
/// Null values in the original array are encoded in the dictionary.
pub fn dict_encode_typed_primitive<T: NativePType>(
    array: &PrimitiveArray,
) -> (PrimitiveArray, PrimitiveArray) {
    let mut lookup_dict: HashMap<Value<T>, u64> = HashMap::new();
    let mut codes: Vec<u64> = Vec::new();
    let mut values: Vec<T> = Vec::new();

    if array.dtype().is_nullable() {
        values.push(T::zero());
    }

    for ov in array.iter() {
        match ov {
            None => codes.push(0),
            Some(v) => {
                let code = match lookup_dict.entry(Value(v)) {
                    Entry::Occupied(o) => *o.get(),
                    Entry::Vacant(vac) => {
                        let next_code = values.len() as u64;
                        vac.insert(next_code.as_());
                        values.push(v);
                        next_code
                    }
                };
                codes.push(code);
            }
        }
    }

    let values_validity = if array.dtype().is_nullable() {
        let mut validity = Vec::with_capacity(values.len());
        validity.push(false);
        validity.extend(vec![true; values.len() - 1]);

        Some(Validity::array(BoolArray::from(validity).into_array()))
    } else {
        None
    };

    (
        PrimitiveArray::from(codes),
        PrimitiveArray::from_nullable(values, values_validity),
    )
}

/// Dictionary encode varbin array. Specializes for primitive byte arrays to avoid double copying
pub fn dict_encode_varbin(array: &VarBinArray) -> (PrimitiveArray, VarBinArray) {
    array
        .iter_primitive()
        .map(|prim_iter| dict_encode_typed_varbin(array.dtype().clone(), prim_iter))
        .unwrap_or_else(|_| dict_encode_typed_varbin(array.dtype().clone(), array.iter()))
}

fn lookup_bytes<'a, T: NativePType + AsPrimitive<usize>>(
    offsets: &'a [T],
    bytes: &'a [u8],
    idx: usize,
) -> &'a [u8] {
    let begin: usize = offsets[idx].as_();
    let end: usize = offsets[idx + 1].as_();
    &bytes[begin..end]
}

fn dict_encode_typed_varbin<I, U>(dtype: DType, values: I) -> (PrimitiveArray, VarBinArray)
where
    I: Iterator<Item = Option<U>>,
    U: AsRef<[u8]>,
{
    let (lower, _) = values.size_hint();
    let hasher = RandomState::new();
    let mut lookup_dict: HashMap<u64, (), ()> = HashMap::with_hasher(());
    let mut codes: Vec<u64> = Vec::with_capacity(lower);
    let mut bytes: Vec<u8> = Vec::new();
    let mut offsets: Vec<u64> = Vec::new();
    offsets.push(0);

    if dtype.is_nullable() {
        offsets.push(0);
    }

    for o_val in values {
        match o_val {
            None => codes.push(0),
            Some(val) => {
                let byte_ref = val.as_ref();
                let value_hash = hasher.hash_one(byte_ref);
                let raw_entry = lookup_dict.raw_entry_mut().from_hash(value_hash, |idx| {
                    byte_ref == lookup_bytes(offsets.as_slice(), bytes.as_slice(), idx.as_())
                });

                let code = match raw_entry {
                    RawEntryMut::Occupied(o) => *o.into_key(),
                    RawEntryMut::Vacant(vac) => {
                        let next_code = offsets.len() as u64 - 1;
                        bytes.extend_from_slice(byte_ref);
                        offsets.push(bytes.len() as u64);
                        vac.insert_with_hasher(value_hash, next_code, (), |idx| {
                            hasher.hash_one(lookup_bytes(
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
        }
    }

    let values_validity = if dtype.is_nullable() {
        let mut validity = Vec::with_capacity(offsets.len() - 1);
        validity.push(false);
        validity.extend(vec![true; offsets.len() - 2]);

        Some(Validity::array(BoolArray::from(validity).into_array()))
    } else {
        None
    };

    (
        PrimitiveArray::from(codes),
        VarBinArray::new(
            PrimitiveArray::from(offsets).into_array(),
            PrimitiveArray::from(bytes).into_array(),
            dtype,
            values_validity,
        ),
    )
}

#[cfg(test)]
mod test {
    use std::str;

    use vortex::array::primitive::{PrimitiveArray, TypedPrimitiveTrait};
    use vortex::array::varbin::VarBinArray;
    use vortex::compute::scalar_at::scalar_at;
    use vortex::scalar::PrimitiveScalar;

    use crate::compress::{dict_encode_typed_primitive, dict_encode_varbin};

    #[test]
    fn encode_primitive() {
        let arr = PrimitiveArray::from(vec![1, 1, 3, 3, 3]);
        let (codes, values) = dict_encode_typed_primitive::<i32>(&arr);
        assert_eq!(codes.typed_data::<u64>(), &[0, 0, 1, 1, 1]);
        assert_eq!(values.typed_data::<i32>(), &[1, 3]);
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
        let (codes, values) = dict_encode_typed_primitive::<i32>(&arr);
        assert_eq!(codes.typed_data::<u64>(), &[1, 1, 0, 2, 2, 0, 2, 0]);
        assert_eq!(
            scalar_at(&values, 0).unwrap(),
            PrimitiveScalar::nullable::<i32>(None).into()
        );
        assert_eq!(
            scalar_at(&values, 1).unwrap(),
            PrimitiveScalar::nullable(Some(1)).into()
        );
        assert_eq!(
            scalar_at(&values, 2).unwrap(),
            PrimitiveScalar::nullable(Some(3)).into()
        );
    }

    #[test]
    fn encode_varbin() {
        let arr = VarBinArray::from(vec!["hello", "world", "hello", "again", "world"]);
        let (codes, values) = dict_encode_varbin(&arr);
        assert_eq!(codes.typed_data::<u64>(), &[0, 1, 0, 2, 1]);
        assert_eq!(
            values
                .iter_primitive()
                .unwrap()
                .flatten()
                .map(|b| unsafe { str::from_utf8_unchecked(b) })
                .collect::<Vec<_>>(),
            vec!["hello", "world", "again"]
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
        assert_eq!(codes.typed_data::<u64>(), &[1, 0, 2, 1, 0, 3, 2, 0]);
        assert_eq!(
            values
                .iter_primitive()
                .unwrap()
                .map(|b| b.map(|bv| unsafe { str::from_utf8_unchecked(bv) }))
                .collect::<Vec<_>>(),
            vec![None, Some("hello"), Some("world"), Some("again")]
        );
    }

    #[test]
    fn repeated_values() {
        let arr = VarBinArray::from(vec!["a", "a", "b", "b", "a", "b", "a", "b"]);
        let (codes, values) = dict_encode_varbin(&arr);
        assert_eq!(
            values
                .iter_primitive()
                .unwrap()
                .flatten()
                .map(|b| unsafe { str::from_utf8_unchecked(b) })
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        assert_eq!(codes.typed_data::<u64>(), &[0u64, 0, 1, 1, 0, 1, 0, 1]);
    }
}
