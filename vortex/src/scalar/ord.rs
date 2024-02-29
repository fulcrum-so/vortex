// (c) Copyright 2024 Fulcrum Technologies, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::scalar::{
    BinaryScalar, BoolScalar, LocalTimeScalar, NullableScalar, PScalar, Scalar, StructScalar,
    Utf8Scalar,
};
use std::cmp::Ordering;
use std::sync::Arc;
macro_rules! dyn_ord {
    ($ty:ty, $lhs:expr, $rhs:expr) => {{
        let lhs = $lhs.as_any().downcast_ref::<$ty>().unwrap();
        let rhs = $rhs.as_any().downcast_ref::<$ty>().unwrap();
        if lhs < rhs {
            Ordering::Less
        } else if lhs == rhs {
            Ordering::Equal
        } else {
            Ordering::Greater
        }
    }};
}

fn cmp(lhs: &dyn Scalar, rhs: &dyn Scalar) -> Option<Ordering> {
    if lhs.dtype() != rhs.dtype() {
        return None;
    }

    // If the dtypes are the same then both of the scalars are either nullable or plain scalar
    if let Some(ls) = lhs.as_any().downcast_ref::<NullableScalar>() {
        if let Some(rs) = rhs.as_any().downcast_ref::<NullableScalar>() {
            return Some(dyn_ord!(NullableScalar, ls, rs));
        } else {
            unreachable!("DTypes were equal, but only one was nullable")
        }
    }

    use crate::dtype::DType::*;
    Some(match lhs.dtype() {
        Bool(_) => dyn_ord!(BoolScalar, lhs, rhs),
        Int(_, _, _) => dyn_ord!(PScalar, lhs, rhs),
        Float(_, _) => dyn_ord!(PScalar, lhs, rhs),
        Struct(..) => dyn_ord!(StructScalar, lhs, rhs),
        Utf8(_) => dyn_ord!(Utf8Scalar, lhs, rhs),
        Binary(_) => dyn_ord!(BinaryScalar, lhs, rhs),
        LocalTime(_, _) => dyn_ord!(LocalTimeScalar, lhs, rhs),
        _ => todo!("Cmp not yet implemented for {:?} {:?}", lhs, rhs),
    })
}

impl PartialOrd for dyn Scalar {
    fn partial_cmp(&self, that: &Self) -> Option<Ordering> {
        cmp(self, that)
    }
}

impl PartialOrd<dyn Scalar> for Box<dyn Scalar> {
    fn partial_cmp(&self, that: &dyn Scalar) -> Option<Ordering> {
        cmp(self.as_ref(), that)
    }
}

impl PartialOrd<dyn Scalar> for Arc<dyn Scalar> {
    fn partial_cmp(&self, that: &dyn Scalar) -> Option<Ordering> {
        cmp(&**self, that)
    }
}
