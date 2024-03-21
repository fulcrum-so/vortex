use std::cmp::Ordering;
use std::collections::HashMap;
use vortex_schema::DType;

use crate::array::varbin::VarBinArray;
use crate::array::varbinview::VarBinViewArray;
use crate::array::Array;
use crate::error::VortexResult;
use crate::stats::{Stat, StatsCompute, StatsSet};

pub trait BinaryArray {
    fn bytes_at(&self, index: usize) -> VortexResult<Vec<u8>>;
}

impl<T> StatsCompute for T
where
    T: BinaryArray + Array,
{
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        let mut min = vec![0xFF];
        let mut max = vec![0x00];
        let mut is_constant = true;
        let mut is_sorted = true;
        let mut last_value = vec![0x00];
        let mut runs: usize = 0;
        for i in 0..self.len() {
            let next_val = self.bytes_at(i).unwrap();
            if next_val < min {
                min.clone_from(&next_val);
            }
            if next_val > max {
                max.clone_from(&next_val);
            }
            match next_val.cmp(&last_value) {
                Ordering::Less => is_sorted = false,
                Ordering::Equal => continue,
                Ordering::Greater => {}
            }
            is_constant = false;
            last_value = next_val;
            runs += 1;
        }

        Ok(StatsSet::from(HashMap::from([
            (
                Stat::Min,
                if matches!(self.dtype(), DType::Utf8(_)) {
                    unsafe { String::from_utf8_unchecked(min.to_vec()) }.into()
                } else {
                    min.into()
                },
            ),
            (
                Stat::Max,
                if matches!(self.dtype(), DType::Utf8(_)) {
                    unsafe { String::from_utf8_unchecked(max.to_vec()) }.into()
                } else {
                    max.into()
                },
            ),
            (Stat::RunCount, runs.into()),
            (Stat::IsSorted, is_sorted.into()),
            (Stat::IsConstant, is_constant.into()),
        ])))
    }
}

impl BinaryArray for VarBinArray {
    fn bytes_at(&self, index: usize) -> VortexResult<Vec<u8>> {
        VarBinArray::bytes_at(self, index)
    }
}

impl BinaryArray for VarBinViewArray {
    fn bytes_at(&self, index: usize) -> VortexResult<Vec<u8>> {
        VarBinViewArray::bytes_at(self, index)
    }
}

#[cfg(test)]
mod test {
    use crate::array::primitive::PrimitiveArray;
    use crate::array::varbin::VarBinArray;
    use crate::array::Array;
    use crate::stats::Stat;
    use vortex_schema::{DType, Nullability};

    fn array(dtype: DType) -> VarBinArray {
        let values = PrimitiveArray::from(
            "hello worldhello world this is a long string"
                .as_bytes()
                .to_vec(),
        );
        let offsets = PrimitiveArray::from(vec![0, 11, 44]);

        VarBinArray::new(offsets.boxed(), values.boxed(), dtype, None)
    }

    #[test]
    fn utf8_stats() {
        let arr = array(DType::Utf8(Nullability::NonNullable));
        assert_eq!(
            arr.stats().get_or_compute_as::<String>(&Stat::Min).unwrap(),
            String::from("hello world")
        );
        assert_eq!(
            arr.stats().get_or_compute_as::<String>(&Stat::Max).unwrap(),
            String::from("hello world this is a long string")
        );
        assert_eq!(
            arr.stats()
                .get_or_compute_as::<usize>(&Stat::RunCount)
                .unwrap(),
            2
        );
        assert!(!arr
            .stats()
            .get_or_compute_as::<bool>(&Stat::IsConstant)
            .unwrap());
        assert!(arr
            .stats()
            .get_or_compute_as::<bool>(&Stat::IsSorted)
            .unwrap());
    }

    #[test]
    fn binary_stats() {
        let arr = array(DType::Binary(Nullability::NonNullable));
        assert_eq!(
            arr.stats()
                .get_or_compute_as::<Vec<u8>>(&Stat::Min)
                .unwrap(),
            "hello world".as_bytes().to_vec()
        );
        assert_eq!(
            arr.stats()
                .get_or_compute_as::<Vec<u8>>(&Stat::Max)
                .unwrap(),
            "hello world this is a long string".as_bytes().to_vec()
        );
        assert_eq!(
            arr.stats()
                .get_or_compute_as::<usize>(&Stat::RunCount)
                .unwrap(),
            2
        );
        assert!(!arr
            .stats()
            .get_or_compute_as::<bool>(&Stat::IsConstant)
            .unwrap());
        assert!(arr
            .stats()
            .get_or_compute_as::<bool>(&Stat::IsSorted)
            .unwrap());
    }
}
