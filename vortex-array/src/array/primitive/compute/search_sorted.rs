use vortex_error::VortexResult;

use crate::array::primitive::PrimitiveArray;
use crate::compute::search_sorted::{SearchSortedFn, SearchSortedSide};
use crate::match_each_native_ptype;
use crate::ptype::NativePType;
use crate::scalar::Scalar;

impl SearchSortedFn for PrimitiveArray {
    fn search_sorted(&self, value: &Scalar, side: SearchSortedSide) -> VortexResult<usize> {
        match_each_native_ptype!(self.ptype(), |$T| {
            let pvalue: $T = value.try_into()?;
            Ok(search_sorted(self.typed_data::<$T>(), pvalue, side))
        })
    }
}

fn search_sorted<T: NativePType>(arr: &[T], target: T, side: SearchSortedSide) -> usize {
    match side {
        SearchSortedSide::Left => search_sorted_cmp(arr, target, |a, b| a < b),
        SearchSortedSide::Right => search_sorted_cmp(arr, target, |a, b| a <= b),
    }
}

fn search_sorted_cmp<T: NativePType, Cmp>(arr: &[T], target: T, cmp: Cmp) -> usize
where
    Cmp: Fn(T, T) -> bool + 'static,
{
    let mut low = 0;
    let mut high = arr.len();

    while low < high {
        let mid = low + (high - low) / 2;

        if cmp(arr[mid], target) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    low
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_searchsorted_primitive() {
        let values = vec![1u16, 2, 3];

        assert_eq!(search_sorted(&values, 0, SearchSortedSide::Left), 0);
        assert_eq!(search_sorted(&values, 1, SearchSortedSide::Left), 0);
        assert_eq!(search_sorted(&values, 1, SearchSortedSide::Right), 1);
        assert_eq!(search_sorted(&values, 4, SearchSortedSide::Left), 3);
    }
}
