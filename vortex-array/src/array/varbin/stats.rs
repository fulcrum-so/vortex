use std::cmp::Ordering;
use std::collections::HashMap;

use vortex_dtype::DType;
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::accessor::ArrayAccessor;
use crate::array::varbin::{varbin_scalar, VarBinArray};
use crate::stats::{ArrayStatisticsCompute, Stat, StatsSet};
use crate::{ArrayDType, ArrayTrait};

impl ArrayStatisticsCompute for VarBinArray<'_> {
    fn compute_statistics(&self, _stat: Stat) -> VortexResult<StatsSet> {
        if self.is_empty() {
            return Ok(StatsSet::new());
        }
        self.with_iterator(|iter| compute_stats(iter, self.dtype()))
    }
}

pub fn compute_stats(iter: &mut dyn Iterator<Item = Option<&[u8]>>, dtype: &DType) -> StatsSet {
    let mut leading_nulls: usize = 0;
    let mut first_value: Option<&[u8]> = None;
    for v in &mut *iter {
        if v.is_none() {
            leading_nulls += 1;
        } else {
            first_value = v;
            break;
        }
    }

    if let Some(first_non_null) = first_value {
        let mut acc = VarBinAccumulator::new(first_non_null);
        iter.for_each(|n| acc.nullable_next(n));
        acc.n_nulls(leading_nulls);
        acc.finish(dtype)
    } else {
        all_null_stats(leading_nulls, dtype)
    }
}

fn all_null_stats(len: usize, dtype: &DType) -> StatsSet {
    StatsSet::from(HashMap::from([
        (Stat::Min, Scalar::null(dtype)),
        (Stat::Max, Scalar::null(dtype)),
        (Stat::IsConstant, true.into()),
        (Stat::IsSorted, true.into()),
        (Stat::IsStrictSorted, (len < 2).into()),
        (Stat::RunCount, 1.into()),
        (Stat::NullCount, len.into()),
    ]))
}

pub struct VarBinAccumulator<'a> {
    min: &'a [u8],
    max: &'a [u8],
    is_constant: bool,
    is_sorted: bool,
    is_strict_sorted: bool,
    last_value: &'a [u8],
    null_count: usize,
    runs: usize,
}

impl<'a> VarBinAccumulator<'a> {
    pub fn new(value: &'a [u8]) -> Self {
        Self {
            min: value,
            max: value,
            is_constant: true,
            is_sorted: true,
            is_strict_sorted: true,
            last_value: value,
            runs: 1,
            null_count: 0,
        }
    }

    pub fn nullable_next(&mut self, val: Option<&'a [u8]>) {
        match val {
            None => self.null_count += 1,
            Some(v) => self.next(v),
        }
    }

    pub fn n_nulls(&mut self, null_count: usize) {
        self.null_count += null_count;
    }

    pub fn next(&mut self, val: &'a [u8]) {
        if val < self.min {
            self.min.clone_from(&val);
        } else if val > self.max {
            self.max.clone_from(&val);
        }

        match val.cmp(self.last_value) {
            Ordering::Less => self.is_sorted = false,
            Ordering::Equal => {
                self.is_strict_sorted = false;
                return;
            }
            Ordering::Greater => {}
        }
        self.is_constant = false;
        self.last_value = val;
        self.runs += 1;
    }

    pub fn finish(&self, dtype: &DType) -> StatsSet {
        StatsSet::from(HashMap::from([
            (Stat::Min, varbin_scalar(self.min.to_vec(), dtype)),
            (Stat::Max, varbin_scalar(self.max.to_vec(), dtype)),
            (Stat::RunCount, self.runs.into()),
            (Stat::IsSorted, self.is_sorted.into()),
            (Stat::IsStrictSorted, self.is_strict_sorted.into()),
            (Stat::IsConstant, self.is_constant.into()),
            (Stat::NullCount, self.null_count.into()),
        ]))
    }
}

#[cfg(test)]
mod test {
    use vortex_dtype::{DType, Nullability};

    use crate::array::varbin::{OwnedVarBinArray, VarBinArray};
    use crate::stats::{ArrayStatistics, Stat};

    fn array(dtype: DType) -> OwnedVarBinArray {
        VarBinArray::from_vec(
            vec!["hello world", "hello world this is a long string"],
            dtype,
        )
    }

    #[test]
    fn utf8_stats() {
        let arr = array(DType::Utf8(Nullability::NonNullable));
        assert_eq!(
            arr.statistics().compute_min::<String>().unwrap(),
            "hello world".to_owned()
        );
        assert_eq!(
            arr.statistics().compute_max::<String>().unwrap(),
            "hello world this is a long string".to_owned()
        );
        assert_eq!(arr.statistics().compute_run_count().unwrap(), 2);
        assert!(!arr.statistics().compute_is_constant().unwrap());
        assert!(arr.statistics().compute_is_sorted().unwrap());
    }

    #[test]
    fn binary_stats() {
        let arr = array(DType::Binary(Nullability::NonNullable));
        assert_eq!(
            arr.statistics().compute_min::<Vec<u8>>().unwrap(),
            "hello world".as_bytes().to_vec()
        );
        assert_eq!(
            arr.statistics().compute_max::<Vec<u8>>().unwrap(),
            "hello world this is a long string".as_bytes().to_vec()
        );
        assert_eq!(arr.statistics().compute_run_count().unwrap(), 2);
        assert!(!arr.statistics().compute_is_constant().unwrap());
        assert!(arr.statistics().compute_is_sorted().unwrap());
    }

    #[test]
    fn some_nulls() {
        let array = VarBinArray::from_iter(
            vec![
                Some("hello world"),
                None,
                Some("hello world this is a long string"),
                None,
            ],
            DType::Utf8(Nullability::Nullable),
        );
        assert_eq!(
            array.statistics().compute_min::<String>().unwrap(),
            "hello world".to_owned()
        );
        assert_eq!(
            array.statistics().compute_max::<String>().unwrap(),
            "hello world this is a long string".to_owned()
        );
    }

    #[test]
    fn all_nulls() {
        let array = VarBinArray::from_iter(
            vec![Option::<&str>::None, None, None],
            DType::Utf8(Nullability::Nullable),
        );
        assert!(array.statistics().get(Stat::Min).is_none());
        assert!(array.statistics().get(Stat::Max).is_none());
    }
}
