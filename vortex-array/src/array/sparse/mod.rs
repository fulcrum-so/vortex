use std::collections::HashMap;

use ::serde::{Deserialize, Serialize};
use vortex_error::{vortex_bail, VortexResult};

use crate::array::constant::ConstantArray;
use crate::compute::search_sorted::{search_sorted, SearchSortedSide};
use crate::stats::ArrayStatisticsCompute;
use crate::validity::{ArrayValidity, LogicalValidity};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::{impl_encoding, match_each_integer_ptype, ArrayDType, IntoArrayData, ToArrayData};

// mod compress;
mod compute;
mod flatten;

impl_encoding!("vortex.sparse", Sparse);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparseMetadata {
    indices_dtype: DType,
    // Offset value for patch indices as a result of slicing
    indices_offset: usize,
    len: usize,
    fill_value: Scalar,
}

impl<'a> SparseArray<'a> {
    pub fn new(indices: Array<'a>, values: Array<'a>, len: usize, fill_value: Scalar) -> Self {
        Self::try_new(indices, values, len, fill_value).unwrap()
    }

    pub fn try_new(
        indices: Array<'a>,
        values: Array<'a>,
        len: usize,
        fill_value: Scalar,
    ) -> VortexResult<Self> {
        Self::try_new_with_offset(indices, values, len, 0, fill_value)
    }

    pub(crate) fn try_new_with_offset(
        indices: Array<'a>,
        values: Array<'a>,
        len: usize,
        indices_offset: usize,
        fill_value: Scalar,
    ) -> VortexResult<Self> {
        if !matches!(indices.dtype(), &DType::IDX) {
            vortex_bail!("Cannot use {} as indices", indices.dtype());
        }

        Self::try_from_parts(
            values.dtype().clone(),
            SparseMetadata {
                indices_dtype: indices.dtype().clone(),
                indices_offset,
                len,
                fill_value,
            },
            vec![].into(),
            vec![indices.to_array_data(), values.to_array_data()].into(),
            HashMap::default(),
        )
    }
}

impl SparseArray<'_> {
    #[inline]
    pub fn indices_offset(&self) -> usize {
        self.metadata().indices_offset
    }

    #[inline]
    pub fn values(&self) -> Array {
        self.array()
            .child(1, self.dtype())
            .expect("missing child array")
    }

    #[inline]
    pub fn indices(&self) -> Array {
        self.array()
            .child(0, &self.metadata().indices_dtype)
            .expect("missing indices array")
    }

    #[inline]
    fn fill_value(&self) -> &Scalar {
        &self.metadata().fill_value
    }

    /// Returns the position of a given index in the indices array if it exists.
    pub fn find_index(&self, index: usize) -> VortexResult<Option<usize>> {
        search_sorted(
            &self.indices(),
            self.indices_offset() + index,
            SearchSortedSide::Left,
        )
        .map(|r| r.to_found())
    }

    /// Return indices as a vector of usize with the indices_offset applied.
    pub fn resolved_indices(&self) -> Vec<usize> {
        let flat_indices = self.indices().flatten_primitive().unwrap();
        match_each_integer_ptype!(flat_indices.ptype(), |$P| {
            flat_indices
                .typed_data::<$P>()
                .iter()
                .map(|v| (*v as usize) - self.indices_offset())
                .collect::<Vec<_>>()
        })
    }
}

impl ArrayTrait for SparseArray<'_> {
    fn len(&self) -> usize {
        self.metadata().len
    }
}

impl AcceptArrayVisitor for SparseArray<'_> {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("indices", &self.indices())?;
        visitor.visit_child("values", &self.values())
    }
}

impl ArrayStatisticsCompute for SparseArray<'_> {}

impl ArrayValidity for SparseArray<'_> {
    fn logical_validity(&self) -> LogicalValidity {
        let validity = if self.fill_value().is_null() {
            // If we have a null fill value, then the result is a Sparse array with a fill_value
            // of true, and patch values of false.
            SparseArray::try_new_with_offset(
                self.indices(),
                ConstantArray::new(false, self.len()).into_array(),
                self.len(),
                self.indices_offset(),
                true.into(),
            )
        } else {
            // If the fill_value is non-null, then the validity is based on the validity of the
            // existing values.
            SparseArray::try_new_with_offset(
                self.indices(),
                self.values()
                    .with_dyn(|a| a.logical_validity().into_array()),
                self.len(),
                self.indices_offset(),
                true.into(),
            )
        }
        .unwrap();

        LogicalValidity::Array(validity.into_array_data())
    }

    fn is_valid(&self, index: usize) -> bool {
        match self.find_index(index).unwrap() {
            None => !self.fill_value().is_null(),
            Some(idx) => self.values().with_dyn(|a| a.is_valid(idx)),
        }
    }
}

impl EncodingCompression for SparseEncoding {}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use vortex_error::VortexError;
    use vortex_schema::Nullability::Nullable;
    use vortex_schema::Signedness::Signed;
    use vortex_schema::{DType, IntWidth};

    use crate::accessor::ArrayAccessor;
    use crate::array::sparse::SparseArray;
    use crate::compute::scalar_at::scalar_at;
    use crate::compute::slice::slice;
    use crate::scalar::Scalar;
    use crate::{Array, IntoArray, OwnedArray};

    fn nullable_fill() -> Scalar {
        Scalar::null(&DType::Int(IntWidth::_32, Signed, Nullable))
    }

    #[allow(dead_code)]
    fn non_nullable_fill() -> Scalar {
        Scalar::from(42i32)
    }

    fn sparse_array(fill_value: Scalar) -> OwnedArray {
        // merged array: [null, null, 100, null, null, 200, null, null, 300, null]
        SparseArray::new(
            vec![2u64, 5, 8].into_array(),
            vec![100i32, 200, 300].into_array(),
            10,
            fill_value,
        )
        .into_array()
    }

    fn assert_sparse_array(sparse: &Array, values: &[Option<i32>]) {
        let sparse_arrow = ArrayAccessor::<i32>::with_iterator(
            &sparse.clone().flatten_primitive().unwrap(),
            |iter| iter.map(|v| v.cloned()).collect_vec(),
        )
        .unwrap();
        assert_eq!(&sparse_arrow, values);
    }

    #[test]
    pub fn iter() {
        assert_sparse_array(
            &sparse_array(nullable_fill()),
            &[
                None,
                None,
                Some(100),
                None,
                None,
                Some(200),
                None,
                None,
                Some(300),
                None,
            ],
        );
    }

    #[test]
    pub fn iter_sliced() {
        let p_fill_val = Some(non_nullable_fill().try_into().unwrap());
        assert_sparse_array(
            &slice(&sparse_array(non_nullable_fill()), 2, 7).unwrap(),
            &[Some(100), p_fill_val, p_fill_val, Some(200), p_fill_val],
        );
    }

    #[test]
    pub fn iter_sliced_nullable() {
        assert_sparse_array(
            &slice(&sparse_array(nullable_fill()), 2, 7).unwrap(),
            &[Some(100), None, None, Some(200), None],
        );
    }

    #[test]
    pub fn iter_sliced_twice() {
        let sliced_once = slice(&sparse_array(nullable_fill()), 1, 8).unwrap();
        assert_sparse_array(
            &sliced_once,
            &[None, Some(100), None, None, Some(200), None, None],
        );
        assert_sparse_array(
            &slice(&sliced_once, 1, 6).unwrap(),
            &[Some(100), None, None, Some(200), None],
        );
    }

    #[test]
    pub fn test_find_index() {
        let sparse = SparseArray::try_from(sparse_array(nullable_fill())).unwrap();
        assert_eq!(sparse.find_index(0).unwrap(), None);
        assert_eq!(sparse.find_index(2).unwrap(), Some(0));
        assert_eq!(sparse.find_index(5).unwrap(), Some(1));
    }

    #[test]
    pub fn test_scalar_at() {
        assert_eq!(
            usize::try_from(scalar_at(&sparse_array(nullable_fill()), 2).unwrap()).unwrap(),
            100
        );
        let error = scalar_at(&sparse_array(nullable_fill()), 10).err().unwrap();
        let VortexError::OutOfBounds(i, start, stop, _) = error else {
            unreachable!()
        };
        assert_eq!(i, 10);
        assert_eq!(start, 0);
        assert_eq!(stop, 10);
    }

    #[test]
    pub fn scalar_at_sliced() {
        let sliced = slice(&sparse_array(nullable_fill()), 2, 7).unwrap();
        assert_eq!(
            usize::try_from(scalar_at(&sliced, 0).unwrap()).unwrap(),
            100
        );
        let error = scalar_at(&sliced, 5).err().unwrap();
        let VortexError::OutOfBounds(i, start, stop, _) = error else {
            unreachable!()
        };
        assert_eq!(i, 5);
        assert_eq!(start, 0);
        assert_eq!(stop, 5);
    }

    #[test]
    pub fn scalar_at_sliced_twice() {
        let sliced_once = slice(&sparse_array(nullable_fill()), 1, 8).unwrap();
        assert_eq!(
            usize::try_from(scalar_at(&sliced_once, 1).unwrap()).unwrap(),
            100
        );
        let error = scalar_at(&sliced_once, 7).err().unwrap();
        let VortexError::OutOfBounds(i, start, stop, _) = error else {
            unreachable!()
        };
        assert_eq!(i, 7);
        assert_eq!(start, 0);
        assert_eq!(stop, 7);

        let sliced_twice = slice(&sliced_once, 1, 6).unwrap();
        assert_eq!(
            usize::try_from(scalar_at(&sliced_twice, 3).unwrap()).unwrap(),
            200
        );
        let error2 = scalar_at(&sliced_twice, 5).err().unwrap();
        let VortexError::OutOfBounds(i, start, stop, _) = error2 else {
            unreachable!()
        };
        assert_eq!(i, 5);
        assert_eq!(start, 0);
        assert_eq!(stop, 5);
    }
}
