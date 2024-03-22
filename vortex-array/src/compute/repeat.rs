use crate::array::constant::ConstantArray;
use crate::array::{Array, ArrayRef};
use crate::scalar::Scalar;

pub fn repeat(scalar: &Scalar, n: usize) -> ArrayRef {
    ConstantArray::new(scalar.clone(), n).into_array()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_repeat() {
        let scalar: Scalar = 47.into();
        let array = repeat(&scalar, 100);
        assert_eq!(array.len(), 100);
    }
}
