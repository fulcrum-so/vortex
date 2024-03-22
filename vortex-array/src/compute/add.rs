use crate::array::constant::ConstantArray;
use crate::array::{Array, ArrayKind, ArrayRef};
use crate::error::{VortexError, VortexResult};
use crate::scalar::Scalar;

// TODO(ngates): convert this to arithmetic operations with macro over the kernel.
pub fn add(lhs: &dyn Array, rhs: &dyn Array) -> VortexResult<ArrayRef> {
    // Check that the arrays are the same length.
    let length = lhs.len();
    if rhs.len() != length {
        return Err(VortexError::LengthMismatch);
    }

    match (ArrayKind::from(lhs), ArrayKind::from(rhs)) {
        (ArrayKind::Constant(lhs), ArrayKind::Constant(rhs)) => {
            Ok(ConstantArray::new(add_scalars(lhs.scalar(), rhs.scalar())?, length).into_array())
        }
        (ArrayKind::Constant(lhs), _) => add_scalar(rhs, lhs.scalar()),
        (_, ArrayKind::Constant(rhs)) => add_scalar(lhs, rhs.scalar()),
        _ => todo!("Implement default addition"),
    }
}

pub fn add_scalar(lhs: &dyn Array, rhs: &Scalar) -> VortexResult<ArrayRef> {
    match ArrayKind::from(lhs) {
        ArrayKind::Constant(lhs) => {
            Ok(ConstantArray::new(add_scalars(lhs.scalar(), rhs)?, lhs.len()).into_array())
        }
        _ => todo!("Implement default addition"),
    }
}

pub fn add_scalars(_lhs: &Scalar, _rhs: &Scalar) -> VortexResult<Scalar> {
    // Might need to improve this implementation...
    Ok(24.into())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_add() {
        let lhs = ConstantArray::new(47.into(), 100);
        let rhs = ConstantArray::new(47.into(), 100);
        let result = add(&lhs, &rhs).unwrap();
        assert_eq!(result.len(), 100);
        // assert_eq!(scalar_at(result, 0), 94);
    }
}
