use serde::{Deserialize, Serialize};
use vortex::array::primitive::{Primitive, PrimitiveArray};
use vortex::compute::scalar_at::scalar_at;
use vortex::compute::search_sorted::{search_sorted, SearchSortedSide};
use vortex::stats::{ArrayStatistics, ArrayStatisticsCompute, Stat};
use vortex::validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{impl_encoding, ArrayDType, ArrayFlatten, IntoArrayData};
use vortex_error::vortex_bail;

use crate::compress::{ree_decode, ree_encode};

impl_encoding!("vortex.ree", REE);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct REEMetadata {
    validity: ValidityMetadata,
    ends_dtype: DType,
    offset: usize,
    length: usize,
}

impl REEArray<'_> {
    pub fn try_new(ends: Array, values: Array, validity: Validity) -> VortexResult<Self> {
        let length: usize = scalar_at(&ends, ends.len() - 1)?.try_into()?;
        Self::with_offset_and_size(ends, values, validity, length, 0)
    }

    pub(crate) fn with_offset_and_size(
        ends: Array,
        values: Array,
        validity: Validity,
        length: usize,
        offset: usize,
    ) -> VortexResult<Self> {
        if values.dtype().is_nullable() == (validity == Validity::NonNullable) {
            vortex_bail!("incorrect validity {:?}", validity);
        }

        if !ends
            .statistics()
            .get_as(Stat::IsStrictSorted)
            .unwrap_or(true)
        {
            vortex_bail!("Ends array must be strictly sorted",);
        }
        let dtype = values.dtype().clone();
        let metadata = REEMetadata {
            validity: validity.to_metadata(length)?,
            ends_dtype: ends.dtype().clone(),
            offset,
            length,
        };

        let mut children = Vec::with_capacity(3);
        children.push(ends.into_array_data());
        children.push(values.into_array_data());
        if let Some(a) = validity.into_array_data() {
            children.push(a)
        }

        Self::try_from_parts(dtype, metadata, children.into(), StatsSet::new())
    }

    pub fn find_physical_index(&self, index: usize) -> VortexResult<usize> {
        search_sorted(&self.ends(), index + self.offset(), SearchSortedSide::Right)
            .map(|s| s.to_index())
    }

    pub fn encode(array: Array) -> VortexResult<Self> {
        if array.encoding().id() == Primitive::ID {
            let primitive = PrimitiveArray::try_from(array)?;
            let (ends, values) = ree_encode(&primitive);
            REEArray::try_new(ends.into_array(), values.into_array(), primitive.validity())
        } else {
            vortex_bail!("REE can only encode primitive arrays")
        }
    }

    pub fn validity(&self) -> Validity {
        self.metadata()
            .validity
            .to_validity(self.array().child(2, &Validity::DTYPE))
    }

    #[inline]
    pub fn offset(&self) -> usize {
        self.metadata().offset
    }

    #[inline]
    pub fn ends(&self) -> Array {
        self.array()
            .child(0, &self.metadata().ends_dtype)
            .expect("missing ends")
    }

    #[inline]
    pub fn values(&self) -> Array {
        self.array().child(1, self.dtype()).expect("missing values")
    }
}

impl ArrayValidity for REEArray<'_> {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
    }
}

impl ArrayFlatten for REEArray<'_> {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a,
    {
        let pends = self.ends().flatten_primitive()?;
        let pvalues = self.values().flatten_primitive()?;
        ree_decode(&pends, &pvalues, self.validity(), self.offset(), self.len())
            .map(Flattened::Primitive)
    }
}

impl AcceptArrayVisitor for REEArray<'_> {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("ends", &self.ends())?;
        visitor.visit_child("values", &self.values())?;
        visitor.visit_validity(&self.validity())
    }
}

impl ArrayStatisticsCompute for REEArray<'_> {}

impl ArrayTrait for REEArray<'_> {
    fn len(&self) -> usize {
        self.metadata().length
    }
}

#[cfg(test)]
mod test {
    use vortex::compute::scalar_at::scalar_at;
    use vortex::compute::slice::slice;
    use vortex::validity::Validity;
    use vortex::{ArrayDType, ArrayTrait, IntoArray};
    use vortex_dtype::{DType, Nullability, PType};

    use crate::REEArray;

    #[test]
    fn new() {
        let arr = REEArray::try_new(
            vec![2u32, 5, 10].into_array(),
            vec![1i32, 2, 3].into_array(),
            Validity::NonNullable,
        )
        .unwrap();
        assert_eq!(arr.len(), 10);
        assert_eq!(
            arr.dtype(),
            &DType::Primitive(PType::I32, Nullability::NonNullable)
        );

        // 0, 1 => 1
        // 2, 3, 4 => 2
        // 5, 6, 7, 8, 9 => 3
        assert_eq!(scalar_at(arr.array(), 0).unwrap(), 1.into());
        assert_eq!(scalar_at(arr.array(), 2).unwrap(), 2.into());
        assert_eq!(scalar_at(arr.array(), 5).unwrap(), 3.into());
        assert_eq!(scalar_at(arr.array(), 9).unwrap(), 3.into());
    }

    #[test]
    fn slice_array() {
        let arr = slice(
            REEArray::try_new(
                vec![2u32, 5, 10].into_array(),
                vec![1i32, 2, 3].into_array(),
                Validity::NonNullable,
            )
            .unwrap()
            .array(),
            3,
            8,
        )
        .unwrap();
        assert_eq!(
            arr.dtype(),
            &DType::Primitive(PType::I32, Nullability::NonNullable)
        );
        assert_eq!(arr.len(), 5);

        assert_eq!(
            arr.flatten_primitive().unwrap().typed_data::<i32>(),
            vec![2, 2, 3, 3, 3]
        );
    }

    #[test]
    fn flatten() {
        let arr = REEArray::try_new(
            vec![2u32, 5, 10].into_array(),
            vec![1i32, 2, 3].into_array(),
            Validity::NonNullable,
        )
        .unwrap();
        assert_eq!(
            arr.into_array()
                .flatten_primitive()
                .unwrap()
                .typed_data::<i32>(),
            vec![1, 1, 2, 2, 2, 3, 3, 3, 3, 3]
        );
    }
}
