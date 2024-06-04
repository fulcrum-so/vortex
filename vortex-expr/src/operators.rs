use std::ops;

use vortex_dtype::field_paths::FieldPath;
use vortex_dtype::NativePType;

use crate::expressions::{Conjunction, Disjunction, Predicate, Value};

#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Operator {
    // comparison
    EqualTo,
    NotEqualTo,
    GreaterThan,
    GreaterThanOrEqualTo,
    LessThan,
    LessThanOrEqualTo,
}

impl ops::Not for Predicate {
    type Output = Self;

    fn not(self) -> Self::Output {
        let inverse_op = match self.op {
            Operator::EqualTo => Operator::NotEqualTo,
            Operator::NotEqualTo => Operator::EqualTo,
            Operator::GreaterThan => Operator::LessThanOrEqualTo,
            Operator::GreaterThanOrEqualTo => Operator::LessThan,
            Operator::LessThan => Operator::GreaterThanOrEqualTo,
            Operator::LessThanOrEqualTo => Operator::GreaterThan,
        };
        Predicate {
            left: self.left,
            op: inverse_op,
            right: self.right,
        }
    }
}

impl Operator {
    pub fn inverse(self) -> Self {
        match self {
            Operator::EqualTo => Operator::NotEqualTo,
            Operator::NotEqualTo => Operator::EqualTo,
            Operator::GreaterThan => Operator::LessThanOrEqualTo,
            Operator::GreaterThanOrEqualTo => Operator::LessThan,
            Operator::LessThan => Operator::GreaterThanOrEqualTo,
            Operator::LessThanOrEqualTo => Operator::GreaterThan,
        }
    }

    pub fn to_predicate<T: NativePType>(&self) -> fn(&T, &T) -> bool {
        match self {
            Operator::EqualTo => PartialEq::eq,
            Operator::NotEqualTo => PartialEq::ne,
            Operator::GreaterThan => PartialOrd::gt,
            Operator::GreaterThanOrEqualTo => PartialOrd::ge,
            Operator::LessThan => PartialOrd::lt,
            Operator::LessThanOrEqualTo => PartialOrd::le,
        }
    }
}

pub fn field_comparison(op: Operator, left: FieldPath, right: FieldPath) -> Disjunction {
    Disjunction {
        conjunctions: vec![Conjunction {
            predicates: vec![Predicate {
                left,
                op,
                right: Value::Field(right),
            }],
        }],
    }
}
