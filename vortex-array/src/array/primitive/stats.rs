use std::collections::HashMap;
use std::mem::size_of;

use arrow_buffer::buffer::BooleanBuffer;

use vortex_error::VortexResult;

use crate::array::primitive::PrimitiveArray;
use crate::match_each_native_ptype;
use crate::ptype::NativePType;
use crate::scalar::{ListScalarVec, PScalar};
use crate::stats::{Stat, StatsCompute, StatsSet};
use crate::validity::ArrayValidity;

impl StatsCompute for PrimitiveArray {
    fn compute(&self, stat: &Stat) -> VortexResult<StatsSet> {
        match_each_native_ptype!(self.ptype(), |$P| {
            match self.logical_validity() {
                None => self.typed_data::<$P>().compute(stat),
                Some(validity_array) => {
                    NullableValues(self.typed_data::<$P>(), validity_array.to_bool_array().buffer()).compute(stat)
                }
            }
        })
    }
}

impl<T: NativePType> StatsCompute for &[T] {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        if self.is_empty() {
            return Ok(StatsSet::default());
        }
        let mut stats = StatsAccumulator::new(self[0]);
        self.iter().skip(1).for_each(|next| stats.next(*next));
        Ok(stats.into_set())
    }
}

struct NullableValues<'a, T: NativePType>(&'a [T], &'a BooleanBuffer);

impl<'a, T: NativePType> StatsCompute for NullableValues<'a, T> {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        let values = self.0;
        if values.is_empty() {
            return Ok(StatsSet::default());
        }

        let first_non_null = self
            .1
            .iter()
            .enumerate()
            .skip_while(|(_, valid)| !*valid)
            .map(|(idx, _)| values[idx])
            .next();

        if first_non_null.is_none() {
            return Ok(StatsSet::from(HashMap::from([
                (Stat::Min, Option::<T>::None.into()),
                (Stat::Max, Option::<T>::None.into()),
                (Stat::IsConstant, true.into()),
                (Stat::IsSorted, true.into()),
                (Stat::IsStrictSorted, true.into()),
                (Stat::RunCount, 1.into()),
                (Stat::NullCount, 1.into()),
                (
                    Stat::BitWidthFreq,
                    ListScalarVec(vec![0; size_of::<T>() * 8 + 1]).into(),
                ),
                (
                    Stat::TrailingZeroFreq,
                    ListScalarVec(vec![size_of::<T>() * 8; size_of::<T>() * 8 + 1]).into(),
                ),
            ])));
        }

        let mut stats = StatsAccumulator::new(first_non_null.unwrap());
        values
            .iter()
            .zip(self.1.iter())
            .skip(1)
            .map(|(next, valid)| valid.then_some(*next))
            .for_each(|next| stats.nullable_next(next));
        Ok(stats.into_set())
    }
}

trait BitWidth {
    fn bit_width(self) -> usize;
    fn trailing_zeros(self) -> usize;
}

impl<T: NativePType + Into<PScalar>> BitWidth for T {
    fn bit_width(self) -> usize {
        let bit_width = size_of::<T>() * 8;
        let scalar: PScalar = self.into();
        match scalar {
            PScalar::U8(i) => bit_width - i.leading_zeros() as usize,
            PScalar::U16(i) => bit_width - i.leading_zeros() as usize,
            PScalar::U32(i) => bit_width - i.leading_zeros() as usize,
            PScalar::U64(i) => bit_width - i.leading_zeros() as usize,
            PScalar::I8(i) => bit_width - i.leading_zeros() as usize,
            PScalar::I16(i) => bit_width - i.leading_zeros() as usize,
            PScalar::I32(i) => bit_width - i.leading_zeros() as usize,
            PScalar::I64(i) => bit_width - i.leading_zeros() as usize,
            PScalar::F16(_) => bit_width,
            PScalar::F32(_) => bit_width,
            PScalar::F64(_) => bit_width,
        }
    }

    fn trailing_zeros(self) -> usize {
        let scalar: PScalar = self.into();
        match scalar {
            PScalar::U8(i) => i.trailing_zeros() as usize,
            PScalar::U16(i) => i.trailing_zeros() as usize,
            PScalar::U32(i) => i.trailing_zeros() as usize,
            PScalar::U64(i) => i.trailing_zeros() as usize,
            PScalar::I8(i) => i.trailing_zeros() as usize,
            PScalar::I16(i) => i.trailing_zeros() as usize,
            PScalar::I32(i) => i.trailing_zeros() as usize,
            PScalar::I64(i) => i.trailing_zeros() as usize,
            PScalar::F16(_) => 0,
            PScalar::F32(_) => 0,
            PScalar::F64(_) => 0,
        }
    }
}

struct StatsAccumulator<T: NativePType> {
    prev: T,
    min: T,
    max: T,
    is_sorted: bool,
    is_strict_sorted: bool,
    run_count: usize,
    null_count: usize,
    bit_widths: Vec<usize>,
    trailing_zeros: Vec<usize>,
}

impl<T: NativePType> StatsAccumulator<T> {
    fn new(first_value: T) -> Self {
        let mut stats = Self {
            prev: first_value,
            min: first_value,
            max: first_value,
            is_sorted: true,
            is_strict_sorted: true,
            run_count: 1,
            null_count: 0,
            bit_widths: vec![0; size_of::<T>() * 8 + 1],
            trailing_zeros: vec![0; size_of::<T>() * 8 + 1],
        };
        stats.bit_widths[first_value.bit_width()] += 1;
        stats.trailing_zeros[first_value.trailing_zeros()] += 1;
        stats
    }

    pub fn nullable_next(&mut self, next: Option<T>) {
        match next {
            Some(n) => self.next(n),
            None => {
                self.bit_widths[0] += 1;
                self.trailing_zeros[T::PTYPE.bit_width()] += 1;
                self.null_count += 1;
            }
        }
    }

    pub fn next(&mut self, next: T) {
        self.bit_widths[next.bit_width()] += 1;
        self.trailing_zeros[next.trailing_zeros()] += 1;

        if self.prev == next {
            self.is_strict_sorted = false;
        } else {
            if next < self.prev {
                self.is_sorted = false;
            }
            self.run_count += 1;
        }
        if next < self.min {
            self.min = next;
        } else if next > self.max {
            self.max = next;
        }
        self.prev = next;
    }

    pub fn into_set(self) -> StatsSet {
        StatsSet::from(HashMap::from([
            (Stat::Min, self.min.into()),
            (Stat::Max, self.max.into()),
            (Stat::NullCount, self.null_count.into()),
            (Stat::IsConstant, (self.min == self.max).into()),
            (Stat::BitWidthFreq, ListScalarVec(self.bit_widths).into()),
            (
                Stat::TrailingZeroFreq,
                ListScalarVec(self.trailing_zeros).into(),
            ),
            (Stat::IsSorted, self.is_sorted.into()),
            (
                Stat::IsStrictSorted,
                (self.is_sorted && self.is_strict_sorted).into(),
            ),
            (Stat::RunCount, self.run_count.into()),
        ]))
    }
}

#[cfg(test)]
mod test {
    use crate::array::primitive::PrimitiveArray;
    use crate::array::Array;
    use crate::scalar::ListScalarVec;
    use crate::stats::Stat;

    #[test]
    fn stats() {
        let arr = PrimitiveArray::from(vec![1, 2, 3, 4, 5]);
        let min: i32 = arr.stats().get_or_compute_as(&Stat::Min).unwrap();
        let max: i32 = arr.stats().get_or_compute_as(&Stat::Max).unwrap();
        let is_sorted: bool = arr.stats().get_or_compute_as(&Stat::IsSorted).unwrap();
        let is_strict_sorted: bool = arr
            .stats()
            .get_or_compute_as(&Stat::IsStrictSorted)
            .unwrap();
        let is_constant: bool = arr.stats().get_or_compute_as(&Stat::IsConstant).unwrap();
        let bit_width_freq: Vec<u64> = arr
            .stats()
            .get_or_compute_as::<ListScalarVec<u64>>(&Stat::BitWidthFreq)
            .unwrap()
            .0;
        let trailing_zeros_freq: Vec<u64> = arr
            .stats()
            .get_or_compute_as::<ListScalarVec<u64>>(&Stat::TrailingZeroFreq)
            .unwrap()
            .0;
        let run_count: u64 = arr.stats().get_or_compute_as(&Stat::RunCount).unwrap();
        assert_eq!(min, 1);
        assert_eq!(max, 5);
        assert!(is_sorted);
        assert!(is_strict_sorted);
        assert!(!is_constant);
        assert_eq!(
            bit_width_freq,
            vec![
                0u64, 1, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0,
            ]
        );
        assert_eq!(
            trailing_zeros_freq,
            vec![
                // 1, 3, 5 have 0 trailing zeros
                // 2 has 1 trailing zero, 4 has 2 trailing zeros
                3u64, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0,
            ]
        );
        assert_eq!(run_count, 5);
    }

    #[test]
    fn stats_u8() {
        let arr = PrimitiveArray::from(vec![1u8, 2, 3, 4, 5]);
        let min: u8 = arr.stats().get_or_compute_as(&Stat::Min).unwrap();
        let max: u8 = arr.stats().get_or_compute_as(&Stat::Max).unwrap();
        assert_eq!(min, 1);
        assert_eq!(max, 5);
    }

    #[test]
    fn nullable_stats_u8() {
        let arr = PrimitiveArray::from_iter(vec![None, Some(1i32), None, Some(2)]);
        let min: Option<i32> = arr.stats().get_or_compute_as(&Stat::Min);
        let max: Option<i32> = arr.stats().get_or_compute_as(&Stat::Max);
        assert_eq!(min, Some(1));
        assert_eq!(max, Some(2));
    }

    #[test]
    fn all_null() {
        let arr = PrimitiveArray::from_iter(vec![Option::<i32>::None, None, None]);
        let min: Option<i32> = arr.stats().get_or_compute_as(&Stat::Min);
        let max: Option<i32> = arr.stats().get_or_compute_as(&Stat::Max);
        assert_eq!(min, None);
        assert_eq!(max, None);
    }
}
