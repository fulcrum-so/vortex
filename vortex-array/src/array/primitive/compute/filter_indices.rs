use std::ops::{BitAnd, BitOr};

use arrow_buffer::BooleanBuffer;
use vortex_error::{vortex_bail, VortexResult};
use vortex_expr::expressions::{Disjunction, Predicate, Value};

use crate::array::bool::BoolArray;
use crate::array::primitive::PrimitiveArray;
use crate::compute::compare::CompareFn;
use crate::compute::compare_scalar::CompareScalarFn;
use crate::compute::filter_indices::FilterIndicesFn;
use crate::{Array, ArrayDType, ArrayTrait, IntoArray};

impl FilterIndicesFn for PrimitiveArray {
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

fn indices_matching_predicate(
    arr: &PrimitiveArray,
    predicate: &Predicate,
) -> VortexResult<BooleanBuffer> {
    if predicate.left.head().is_some() {
        vortex_bail!("Invalid path for primitive array")
    }

    match &predicate.right {
        Value::Field(path) => {
            let rhs = arr.clone().into_array().resolve_field(arr.dtype(), path)?;
            arr.compare(&rhs, predicate.op)?
                .flatten_bool()
                .map(|arr| arr.boolean_buffer())
        }
        Value::Literal(scalar) => arr
            .compare_scalar(predicate.op, scalar)?
            .flatten_bool()
            .map(|arr| arr.boolean_buffer()),
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use itertools::Itertools;
    use vortex_dtype::field_paths::{field, FieldPathBuilder};
    use vortex_expr::expressions::{lit, Conjunction};
    use vortex_expr::field_paths::FieldPathOperations;
    use vortex_expr::operators::{field_comparison, Operator};

    use super::*;
    use crate::array::r#struct::StructArray;
    use crate::validity::Validity;

    fn apply_conjunctive_filter(arr: &PrimitiveArray, conj: Conjunction) -> VortexResult<Array> {
        arr.filter_indices(&Disjunction {
            conjunctions: vec![conj],
        })
    }

    fn to_int_indices(filtered_primitive: BoolArray) -> Vec<u64> {
        let filtered = filtered_primitive
            .boolean_buffer()
            .iter()
            .enumerate()
            .flat_map(|(idx, v)| if v { Some(idx as u64) } else { None })
            .collect_vec();
        filtered
    }

    #[test]
    fn test_basic_filter() {
        let arr = PrimitiveArray::from_nullable_vec(vec![
            Some(1u32),
            Some(2),
            Some(3),
            Some(4),
            None,
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            None,
            Some(9),
            None,
        ]);

        let field = FieldPathBuilder::new().build();
        let filtered_primitive = apply_conjunctive_filter(
            &arr,
            Conjunction {
                predicates: vec![field.clone().lt(lit(5u32))],
            },
        )
        .unwrap()
        .flatten_bool()
        .unwrap();
        let filtered = to_int_indices(filtered_primitive);
        assert_eq!(filtered, [0u64, 1, 2, 3]);

        let filtered_primitive = apply_conjunctive_filter(
            &arr,
            Conjunction {
                predicates: vec![field.clone().gt(lit(5u32))],
            },
        )
        .unwrap()
        .flatten_bool()
        .unwrap();
        let filtered = to_int_indices(filtered_primitive);
        assert_eq!(filtered, [6u64, 7, 8, 10]);

        let filtered_primitive = apply_conjunctive_filter(
            &arr,
            Conjunction {
                predicates: vec![field.clone().eq(lit(5u32))],
            },
        )
        .unwrap()
        .flatten_bool()
        .unwrap();
        let filtered = to_int_indices(filtered_primitive);
        assert_eq!(filtered, [5u64]);

        let filtered_primitive = apply_conjunctive_filter(
            &arr,
            Conjunction {
                predicates: vec![field.clone().gte(lit(5u32))],
            },
        )
        .unwrap()
        .flatten_bool()
        .unwrap();
        let filtered = to_int_indices(filtered_primitive);
        assert_eq!(filtered, [5u64, 6, 7, 8, 10]);

        let filtered_primitive = apply_conjunctive_filter(
            &arr,
            Conjunction {
                predicates: vec![field.clone().lte(lit(5u32))],
            },
        )
        .unwrap()
        .flatten_bool()
        .unwrap();
        let filtered = to_int_indices(filtered_primitive);
        assert_eq!(filtered, [0u64, 1, 2, 3, 5]);
    }

    #[test]
    fn test_multiple_predicates() {
        let arr =
            PrimitiveArray::from_vec(vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10], Validity::AllValid);
        let field = FieldPathBuilder::new().build();
        let filtered_primitive = apply_conjunctive_filter(
            &arr,
            Conjunction {
                predicates: vec![field.clone().lt(lit(5u32)), field.clone().gt(lit(2u32))],
            },
        )
        .unwrap()
        .flatten_bool()
        .unwrap();
        let filtered = to_int_indices(filtered_primitive);
        assert_eq!(filtered, [2u64, 3])
    }

    #[test]
    fn test_disjoint_predicates() {
        let arr =
            PrimitiveArray::from_vec(vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10], Validity::AllValid);
        let field = FieldPathBuilder::new().build();
        let filtered_primitive = apply_conjunctive_filter(
            &arr,
            Conjunction {
                predicates: vec![field.clone().lt(lit(5u32)), field.clone().gt(lit(5u32))],
            },
        )
        .unwrap()
        .flatten_bool()
        .unwrap();
        let filtered = to_int_indices(filtered_primitive);
        let expected: [u64; 0] = [];
        assert_eq!(filtered, expected)
    }

    #[test]
    fn test_disjunctive_predicate() {
        let arr =
            PrimitiveArray::from_vec(vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10], Validity::AllValid);
        let field = FieldPathBuilder::new().build();
        let c1 = Conjunction {
            predicates: vec![field.clone().lt(lit(5u32))],
        };
        let c2 = Conjunction {
            predicates: vec![field.clone().gt(lit(5u32))],
        };

        let disj = Disjunction {
            conjunctions: vec![c1, c2],
        };
        let filtered_primitive = arr.filter_indices(&disj).unwrap().flatten_bool().unwrap();
        let filtered = to_int_indices(filtered_primitive);
        assert_eq!(filtered, [0u64, 1, 2, 3, 5, 6, 7, 8, 9])
    }

    #[test]
    fn test_invalid_path_err() {
        let arr =
            PrimitiveArray::from_vec(vec![1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10], Validity::AllValid);
        let field = FieldPathBuilder::new().join("some_field").build();
        apply_conjunctive_filter(
            &arr,
            Conjunction {
                predicates: vec![field.clone().lt(lit(5u32)), field.clone().gt(lit(5u32))],
            },
        )
        .expect_err("Cannot apply field reference to primitive array");
    }

    #[test]
    fn test_basic_field_comparisons() -> VortexResult<()> {
        let ints =
            PrimitiveArray::from_nullable_vec(vec![Some(0u64), Some(1), None, Some(3), Some(4)]);
        let other =
            PrimitiveArray::from_nullable_vec(vec![Some(0u64), Some(2), None, Some(5), Some(1)]);

        let structs = StructArray::try_new(
            Arc::new([Arc::from("field_a"), Arc::from("field_b")]),
            vec![ints.into_array(), other.clone().into_array()],
            5,
            Validity::AllValid,
        )?;

        fn comparison(op: Operator) -> Disjunction {
            field_comparison(op, field("field_a"), field("field_b"))
        }

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
}
