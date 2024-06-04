use std::ops::{BitAnd, BitOr};
use std::sync::Arc;

use arrow_array::{
    Array as ArrowArray, ArrayRef as ArrowArrayRef, StructArray as ArrowStructArray,
};
use arrow_buffer::BooleanBuffer;
use arrow_schema::{Field, Fields};
use itertools::Itertools;
use vortex_error::VortexResult;
use vortex_expr::expressions::{Conjunction, Disjunction, Predicate, Value};
use vortex_scalar::Scalar;

use crate::array::bool::BoolArray;
use crate::array::r#struct::StructArray;
use crate::compute::as_arrow::{as_arrow, AsArrowArray};
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::compare::compare;
use crate::compute::filter_indices::{filter_indices, FilterIndicesFn};
use crate::compute::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::slice::{slice, SliceFn};
use crate::compute::take::{take, TakeFn};
use crate::compute::ArrayCompute;
use crate::validity::Validity;
use crate::ArrayTrait;
use crate::{Array, ArrayDType, IntoArray};

impl ArrayCompute for StructArray {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        Some(self)
    }

    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }

    fn filter_indices(&self) -> Option<&dyn FilterIndicesFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl AsArrowArray for StructArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        let field_arrays: Vec<ArrowArrayRef> =
            self.children().map(|f| as_arrow(&f)).try_collect()?;

        let arrow_fields: Fields = self
            .names()
            .iter()
            .zip(field_arrays.iter())
            .zip(self.dtypes().iter())
            .map(|((name, arrow_field), vortex_field)| {
                Field::new(
                    &**name,
                    arrow_field.data_type().clone(),
                    vortex_field.is_nullable(),
                )
            })
            .map(Arc::new)
            .collect();

        Ok(Arc::new(ArrowStructArray::new(
            arrow_fields,
            field_arrays,
            None,
        )))
    }
}

impl AsContiguousFn for StructArray {
    fn as_contiguous(&self, arrays: &[Array]) -> VortexResult<Array> {
        let struct_arrays = arrays
            .iter()
            .map(Self::try_from)
            .collect::<VortexResult<Vec<_>>>()?;
        let mut fields = vec![Vec::new(); self.dtypes().len()];
        for array in struct_arrays.iter() {
            for (f, field) in fields.iter_mut().enumerate() {
                field.push(array.field(f).unwrap());
            }
        }

        let validity = if self.dtype().is_nullable() {
            Validity::from_iter(arrays.iter().map(|a| a.with_dyn(|a| a.logical_validity())))
        } else {
            Validity::NonNullable
        };

        Self::try_new(
            self.names().clone(),
            fields
                .iter()
                .map(|field_arrays| as_contiguous(field_arrays))
                .try_collect()?,
            self.len(),
            validity,
        )
        .map(|a| a.into_array())
    }
}

impl ScalarAtFn for StructArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        Ok(Scalar::r#struct(
            self.dtype().clone(),
            self.children()
                .map(|field| scalar_at(&field, index).map(|s| s.into_value()))
                .try_collect()?,
        ))
    }
}

impl TakeFn for StructArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        Self::try_new(
            self.names().clone(),
            self.children()
                .map(|field| take(&field, indices))
                .try_collect()?,
            indices.len(),
            self.validity().take(indices)?,
        )
        .map(|a| a.into_array())
    }
}

impl SliceFn for StructArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        let fields = self
            .children()
            .map(|field| slice(&field, start, stop))
            .try_collect()?;
        Self::try_new(
            self.names().clone(),
            fields,
            stop - start,
            self.validity().slice(start, stop)?,
        )
        .map(|a| a.into_array())
    }
}

impl FilterIndicesFn for StructArray {
    fn filter_indices(&self, predicate: &Disjunction) -> VortexResult<Array> {
        let conjunction_indices = predicate.conjunctions.iter().map(|conj| {
            conj.predicates
                .iter()
                .map(|pred| indices_matching_predicate(self, pred))
                .reduce(|a, b| Ok(a?.bitand(&b?)))
                .unwrap()
        });
        let present_buf = self
            .validity()
            .to_logical(self.len())
            .to_present_null_buffer()?
            .into_inner();

        let bitset: VortexResult<BooleanBuffer> = conjunction_indices
            .reduce(|a, b| Ok(a?.bitor(&b?)))
            .map(|bitset| Ok(bitset?.bitand(&present_buf)))
            .unwrap_or_else(|| Ok(BooleanBuffer::new_set(self.len())));

        Ok(BoolArray::from(bitset?).into_array())
    }
}

fn indices_matching_predicate(arr: &StructArray, pred: &Predicate) -> VortexResult<BooleanBuffer> {
    let inner = arr
        .clone()
        .into_array()
        .resolve_field(arr.dtype(), &pred.left)?;

    match &pred.right {
        Value::Field(rh_field) => {
            let rhs = arr
                .clone()
                .into_array()
                .resolve_field(arr.dtype(), rh_field)?;
            Ok(compare(&inner, &rhs, pred.op)?
                .flatten_bool()?
                .boolean_buffer())
        }
        Value::Literal(_) => {
            let conj = Conjunction {
                predicates: vec![pred.clone()],
            };
            let disj = Disjunction {
                conjunctions: vec![conj],
            };
            Ok(filter_indices(&inner, &disj)?
                .flatten_bool()?
                .boolean_buffer())
        }
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use vortex_dtype::field_paths::{field, FieldPath};
    use vortex_dtype::{DType, Nullability, PType, StructDType};
    use vortex_expr::operators::{field_comparison, Operator};

    use super::*;
    use crate::array::primitive::PrimitiveArray;
    use crate::array::r#struct::StructMetadata;
    use crate::stats::StatsSet;
    use crate::validity::ValidityMetadata;
    use crate::IntoArrayData;

    fn to_int_indices(indices_bits: BoolArray) -> Vec<u64> {
        let filtered = indices_bits
            .boolean_buffer()
            .iter()
            .enumerate()
            .flat_map(|(idx, v)| if v { Some(idx as u64) } else { None })
            .collect_vec();
        filtered
    }

    fn comparison(op: Operator) -> Disjunction {
        field_comparison(op, field("field_a"), field("field_b"))
    }

    #[test]
    fn test_basic_field_comparisons() -> VortexResult<()> {
        let struct_dtype = DType::Struct(
            StructDType::new(
                Arc::new([Arc::from("field_a"), Arc::from("field_b")]),
                vec![
                    DType::Primitive(PType::U64, Nullability::Nullable),
                    DType::Primitive(PType::U64, Nullability::Nullable),
                ],
            ),
            Nullability::NonNullable,
        );

        let ints_a =
            PrimitiveArray::from_nullable_vec(vec![Some(0u64), Some(1), None, Some(3), Some(4)]);
        let ints_b =
            PrimitiveArray::from_nullable_vec(vec![Some(0u64), Some(2), None, Some(5), Some(1)]);

        let structs = StructArray::try_from_parts(
            struct_dtype,
            StructMetadata {
                length: 5,
                validity: ValidityMetadata::AllValid,
            },
            Arc::new([
                ints_a.clone().into_array_data(),
                ints_b.clone().into_array_data(),
            ]),
            StatsSet::new(),
        )?;

        let matches = FilterIndicesFn::filter_indices(&structs, &comparison(Operator::EqualTo))?
            .flatten_bool()?;
        assert_eq!(to_int_indices(matches), [0]);

        let matches = FilterIndicesFn::filter_indices(&structs, &comparison(Operator::LessThan))?
            .flatten_bool()?;
        assert_eq!(to_int_indices(matches), [1, 3]);

        let matches =
            FilterIndicesFn::filter_indices(&structs, &comparison(Operator::LessThanOrEqualTo))?
                .flatten_bool()?;
        assert_eq!(to_int_indices(matches), [0, 1, 3]);

        let matches =
            FilterIndicesFn::filter_indices(&structs, &comparison(Operator::GreaterThan))?
                .flatten_bool()?;
        assert_eq!(to_int_indices(matches), [4]);

        let matches =
            FilterIndicesFn::filter_indices(&structs, &comparison(Operator::GreaterThanOrEqualTo))?
                .flatten_bool()?;
        assert_eq!(to_int_indices(matches), [0, 4]);
        Ok(())
    }

    #[test]
    fn test_nested_comparisons() -> VortexResult<()> {
        let struct_dtype = DType::Struct(
            StructDType::new(
                Arc::new([Arc::from("field_a"), Arc::from("field_b")]),
                vec![
                    DType::Primitive(PType::U64, Nullability::Nullable),
                    DType::Primitive(PType::U64, Nullability::Nullable),
                ],
            ),
            Nullability::NonNullable,
        );

        let top_level = DType::Struct(
            StructDType::new(
                Arc::new([Arc::from("struct"), Arc::from("flat")]),
                vec![
                    struct_dtype.clone(),
                    DType::Primitive(PType::U64, Nullability::Nullable),
                ],
            ),
            Nullability::NonNullable,
        );

        let ints_a =
            PrimitiveArray::from_nullable_vec(vec![Some(0u64), Some(1), None, Some(3), Some(4)]);
        let other_b =
            PrimitiveArray::from_nullable_vec(vec![Some(0u64), Some(2), None, Some(5), Some(1)]);

        let structs = StructArray::try_from_parts(
            struct_dtype,
            StructMetadata {
                length: 5,
                validity: ValidityMetadata::AllValid,
            },
            Arc::new([
                ints_a.clone().into_array_data(),
                other_b.clone().into_array_data(),
            ]),
            StatsSet::new(),
        )?;

        let top_level_structs = StructArray::try_from_parts(
            top_level,
            StructMetadata {
                length: 5,
                validity: ValidityMetadata::AllValid,
            },
            Arc::new([
                structs.clone().into_array_data(),
                other_b.clone().into_array_data(),
            ]),
            StatsSet::new(),
        )?;

        compare_nested_fields(&top_level_structs)?;
        compare_nested_to_top_level_field(&top_level_structs)?;

        Ok(())
    }

    fn compare_nested_to_top_level_field(top_level_structs: &StructArray) -> VortexResult<()> {
        let mixed_level_cmp = |op: Operator| -> VortexResult<BoolArray> {
            FilterIndicesFn::filter_indices(
                top_level_structs,
                &field_comparison(
                    op,
                    FieldPath::builder().join("struct").join("field_a").build(),
                    field("flat"),
                ),
            )?
            .flatten_bool()
        };
        let matches = mixed_level_cmp(Operator::EqualTo)?;
        assert_eq!(to_int_indices(matches), [0]);

        let matches = mixed_level_cmp(Operator::LessThan)?;
        assert_eq!(to_int_indices(matches), [1, 3]);

        let matches = mixed_level_cmp(Operator::LessThanOrEqualTo)?;
        assert_eq!(to_int_indices(matches), [0, 1, 3]);

        let matches = mixed_level_cmp(Operator::GreaterThan)?;
        assert_eq!(to_int_indices(matches), [4]);

        let matches = mixed_level_cmp(Operator::GreaterThanOrEqualTo)?;
        assert_eq!(to_int_indices(matches), [0, 4]);
        Ok(())
    }

    fn compare_nested_fields(top_level_structs: &StructArray) -> VortexResult<()> {
        let nested_cmp = |op: Operator| -> VortexResult<BoolArray> {
            FilterIndicesFn::filter_indices(
                top_level_structs,
                &field_comparison(
                    op,
                    FieldPath::builder().join("struct").join("field_a").build(),
                    FieldPath::builder().join("struct").join("field_b").build(),
                ),
            )?
            .flatten_bool()
        };

        let matches = nested_cmp(Operator::EqualTo)?;
        assert_eq!(to_int_indices(matches), [0]);

        let matches = nested_cmp(Operator::LessThan)?;
        assert_eq!(to_int_indices(matches), [1, 3]);

        let matches = nested_cmp(Operator::LessThanOrEqualTo)?;
        assert_eq!(to_int_indices(matches), [0, 1, 3]);

        let matches = nested_cmp(Operator::GreaterThan)?;
        assert_eq!(to_int_indices(matches), [4]);

        let matches = nested_cmp(Operator::GreaterThanOrEqualTo)?;
        assert_eq!(to_int_indices(matches), [0, 4]);
        Ok(())
    }
}
