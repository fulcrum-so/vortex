use std::hash::{Hash, Hasher};

use ahash::RandomState;
use hashbrown::hash_map::{Entry, RawEntryMut};
use hashbrown::HashMap;
use num_traits::AsPrimitive;
use vortex::accessor::ArrayAccessor;
use vortex::array::primitive::{Primitive, PrimitiveArray};
use vortex::array::varbin::{VarBin, VarBinArray};
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::stats::ArrayStatistics;
use vortex::validity::Validity;
use vortex::{Array, ArrayDType, ArrayDef, IntoArray, OwnedArray, ToArray};
use vortex_dtype::NativePType;
use vortex_dtype::{match_each_native_ptype, DType};
use vortex_error::VortexResult;
use vortex_scalar::AsBytes;

use crate::dict::{DictArray, DictEncoding};

impl EncodingCompression for DictEncoding {
    fn can_compress(
        &self,
        array: &Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        // TODO(robert): Add support for VarBinView
        if array.encoding().id() != Primitive::ID && array.encoding().id() != VarBin::ID {
            return None;
        };

        // No point dictionary coding if the array is unique.
        // We don't have a unique stat yet, but strict-sorted implies unique.
        if array
            .statistics()
            .compute_is_strict_sorted()
            .unwrap_or(false)
        {
            return None;
        }

        Some(self)
    }

    fn compress(
        &self,
        array: &Array,
        like: Option<&Array>,
        ctx: CompressCtx,
    ) -> VortexResult<OwnedArray> {
        let dict_like = like.map(|like_arr| DictArray::try_from(like_arr).unwrap());
        let dict_like_ref = dict_like.as_ref();

        let (codes, dict) = match array.encoding().id() {
            Primitive::ID => {
                let p = PrimitiveArray::try_from(array)?;
                let (codes, dict) = match_each_native_ptype!(p.ptype(), |$P| {
                    dict_encode_typed_primitive::<$P>(&p)
                });
                (
                    ctx.auxiliary("codes").excluding(&DictEncoding).compress(
                        &codes.to_array(),
                        dict_like_ref.map(|dict| dict.codes()).as_ref(),
                    )?,
                    ctx.named("values").excluding(&DictEncoding).compress(
                        &dict.to_array(),
                        dict_like_ref.map(|dict| dict.values()).as_ref(),
                    )?,
                )
            }
            VarBin::ID => {
                let vb = VarBinArray::try_from(array).unwrap();
                let (codes, dict) = dict_encode_varbin(&vb);
                (
                    ctx.auxiliary("codes").excluding(&DictEncoding).compress(
                        &codes.to_array(),
                        dict_like_ref.map(|dict| dict.codes()).as_ref(),
                    )?,
                    ctx.named("values").excluding(&DictEncoding).compress(
                        &dict.to_array(),
                        dict_like_ref.map(|dict| dict.values()).as_ref(),
                    )?,
                )
            }

            _ => unreachable!("This array kind should have been filtered out"),
        };

        DictArray::try_new(codes, dict).map(|a| a.into_array())
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
pub fn dict_encode_typed_primitive<'a, T: NativePType>(
    array: &PrimitiveArray<'a>,
) -> (PrimitiveArray<'a>, PrimitiveArray<'a>) {
    let mut lookup_dict: HashMap<Value<T>, u64> = HashMap::new();
    let mut codes: Vec<u64> = Vec::new();
    let mut values: Vec<T> = Vec::new();

    if array.dtype().is_nullable() {
        values.push(T::zero());
    }

    ArrayAccessor::<T>::with_iterator(array, |iter| {
        for ov in iter {
            match ov {
                None => codes.push(0),
                Some(&v) => {
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
    })
    .unwrap();

    let values_validity = if array.dtype().is_nullable() {
        let mut validity = vec![true; values.len()];
        validity[0] = false;

        validity.into()
    } else {
        Validity::NonNullable
    };

    (
        PrimitiveArray::from(codes),
        PrimitiveArray::from_vec(values, values_validity),
    )
}

/// Dictionary encode varbin array. Specializes for primitive byte arrays to avoid double copying
pub fn dict_encode_varbin<'a>(array: &'a VarBinArray) -> (PrimitiveArray<'a>, VarBinArray<'a>) {
    array
        .with_iterator(|iter| dict_encode_typed_varbin(array.dtype().clone(), iter))
        .unwrap()
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

fn dict_encode_typed_varbin<'a, I, U>(
    dtype: DType,
    values: I,
) -> (PrimitiveArray<'a>, VarBinArray<'a>)
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

        validity.into()
    } else {
        Validity::NonNullable
    };

    (
        PrimitiveArray::from(codes),
        VarBinArray::try_new(
            PrimitiveArray::from(offsets).into_array(),
            PrimitiveArray::from(bytes).into_array(),
            dtype,
            values_validity,
        )
        .unwrap(),
    )
}

#[cfg(test)]
mod test {
    use std::str;

    use vortex::accessor::ArrayAccessor;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::array::varbin::VarBinArray;
    use vortex::compute::scalar_at::scalar_at;
    use vortex_scalar::PrimitiveScalar;

    use crate::compress::{dict_encode_typed_primitive, dict_encode_varbin};

    #[test]
    fn encode_primitive() {
        let arr = PrimitiveArray::from(vec![1, 1, 3, 3, 3]);
        let (codes, values) = dict_encode_typed_primitive::<i32>(&arr);
        assert_eq!(codes.buffer().typed_data::<u64>(), &[0, 0, 1, 1, 1]);
        assert_eq!(values.buffer().typed_data::<i32>(), &[1, 3]);
    }

    #[test]
    fn encode_primitive_nulls() {
        let arr = PrimitiveArray::from_nullable_vec(vec![
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
        assert_eq!(
            codes.buffer().typed_data::<u64>(),
            &[1, 1, 0, 2, 2, 0, 2, 0]
        );
        assert_eq!(
            scalar_at(values.array(), 0).unwrap(),
            PrimitiveScalar::nullable::<i32>(None).into()
        );
        assert_eq!(
            scalar_at(values.array(), 1).unwrap(),
            PrimitiveScalar::nullable(Some(1)).into()
        );
        assert_eq!(
            scalar_at(values.array(), 2).unwrap(),
            PrimitiveScalar::nullable(Some(3)).into()
        );
    }

    #[test]
    fn encode_varbin() {
        let arr = VarBinArray::from(vec!["hello", "world", "hello", "again", "world"]);
        let (codes, values) = dict_encode_varbin(&arr);
        assert_eq!(codes.buffer().typed_data::<u64>(), &[0, 1, 0, 2, 1]);
        values
            .with_iterator(|iter| {
                assert_eq!(
                    iter.flatten()
                        .map(|b| unsafe { str::from_utf8_unchecked(b) })
                        .collect::<Vec<_>>(),
                    vec!["hello", "world", "again"]
                );
            })
            .unwrap();
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
        assert_eq!(
            codes.buffer().typed_data::<u64>(),
            &[1, 0, 2, 1, 0, 3, 2, 0]
        );
        assert_eq!(String::from_utf8(values.bytes_at(0).unwrap()).unwrap(), "");
        values
            .with_iterator(|iter| {
                assert_eq!(
                    iter.map(|b| b.map(|v| unsafe { str::from_utf8_unchecked(v) }))
                        .collect::<Vec<_>>(),
                    vec![None, Some("hello"), Some("world"), Some("again")]
                );
            })
            .unwrap();
    }

    #[test]
    fn repeated_values() {
        let arr = VarBinArray::from(vec!["a", "a", "b", "b", "a", "b", "a", "b"]);
        let (codes, values) = dict_encode_varbin(&arr);
        values
            .with_iterator(|iter| {
                assert_eq!(
                    iter.flatten()
                        .map(|b| unsafe { str::from_utf8_unchecked(b) })
                        .collect::<Vec<_>>(),
                    vec!["a", "b"]
                );
            })
            .unwrap();
        assert_eq!(codes.typed_data::<u64>(), &[0u64, 0, 1, 1, 0, 1, 0, 1]);
    }
}
