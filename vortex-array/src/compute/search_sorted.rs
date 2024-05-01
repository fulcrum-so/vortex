use std::cmp::Ordering;
use std::cmp::Ordering::{Equal, Greater, Less};

use vortex_error::{vortex_err, VortexResult};
use vortex_scalar::Scalar;

use crate::compute::scalar_at::scalar_at;
use crate::{Array, ArrayDType};

#[derive(Debug, Copy, Clone)]
pub enum SearchSortedSide {
    Left,
    Right,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SearchResult {
    Found(usize),
    NotFound(usize),
}

impl SearchResult {
    pub fn to_found(self) -> Option<usize> {
        match self {
            SearchResult::Found(i) => Some(i),
            SearchResult::NotFound(_) => None,
        }
    }

    pub fn to_index(self) -> usize {
        match self {
            SearchResult::Found(i) => i,
            SearchResult::NotFound(i) => i,
        }
    }
}

pub trait SearchSortedFn {
    fn search_sorted(&self, value: &Scalar, side: SearchSortedSide) -> VortexResult<SearchResult>;
}

pub fn search_sorted<T: Into<Scalar>>(
    array: &Array,
    target: T,
    side: SearchSortedSide,
) -> VortexResult<SearchResult> {
    let scalar = target.into().cast(array.dtype())?;
    array.with_dyn(|a| {
        if let Some(search_sorted) = a.search_sorted() {
            return search_sorted.search_sorted(&scalar, side);
        }

        if a.scalar_at().is_some() {
            return Ok(SearchSorted::search_sorted(array, &scalar, side));
        }

        Err(vortex_err!(
            NotImplemented: "search_sorted",
            array.encoding().id()
        ))
    })
}

pub trait IndexOrd<V> {
    fn index_cmp(&self, idx: usize, elem: &V) -> Option<Ordering>;

    fn index_lt(&self, idx: usize, elem: &V) -> bool {
        matches!(self.index_cmp(idx, elem), Some(Less))
    }

    fn index_le(&self, idx: usize, elem: &V) -> bool {
        matches!(self.index_cmp(idx, elem), Some(Less | Equal))
    }

    fn index_gt(&self, idx: usize, elem: &V) -> bool {
        matches!(self.index_cmp(idx, elem), Some(Greater))
    }

    fn index_ge(&self, idx: usize, elem: &V) -> bool {
        matches!(self.index_cmp(idx, elem), Some(Greater | Equal))
    }
}

#[allow(clippy::len_without_is_empty)]
pub trait Len {
    fn len(&self) -> usize;
}

pub trait SearchSorted<T> {
    fn search_sorted(&self, value: &T, side: SearchSortedSide) -> SearchResult
    where
        Self: IndexOrd<T>,
    {
        match side {
            SearchSortedSide::Left => self.search_sorted_by(
                |idx| self.index_cmp(idx, value).unwrap_or(Less),
                |idx| {
                    if self.index_lt(idx, value) {
                        Less
                    } else {
                        Greater
                    }
                },
                side,
            ),
            SearchSortedSide::Right => self.search_sorted_by(
                |idx| self.index_cmp(idx, value).unwrap_or(Less),
                |idx| {
                    if self.index_le(idx, value) {
                        Less
                    } else {
                        Greater
                    }
                },
                side,
            ),
        }
    }

    /// find function is used to find the element if it exists, if element exists side_find will be used to find desired index amongst equal values
    fn search_sorted_by<F: FnMut(usize) -> Ordering, N: FnMut(usize) -> Ordering>(
        &self,
        find: F,
        side_find: N,
        side: SearchSortedSide,
    ) -> SearchResult;
}

impl<S: IndexOrd<T> + Len + ?Sized, T> SearchSorted<T> for S {
    fn search_sorted_by<F: FnMut(usize) -> Ordering, N: FnMut(usize) -> Ordering>(
        &self,
        find: F,
        side_find: N,
        side: SearchSortedSide,
    ) -> SearchResult {
        match search_sorted_side_idx(find, 0, self.len()) {
            SearchResult::Found(found) => {
                let idx_search = match side {
                    SearchSortedSide::Left => search_sorted_side_idx(side_find, 0, found),
                    SearchSortedSide::Right => search_sorted_side_idx(side_find, found, self.len()),
                };
                match idx_search {
                    SearchResult::NotFound(i) => SearchResult::Found(i),
                    _ => unreachable!(
                        "searching amongst equal values should never return Found result"
                    ),
                }
            }
            s => s,
        }
    }
}

// Code adapted from Rust standard library slice::binary_search_by
fn search_sorted_side_idx<F: FnMut(usize) -> Ordering>(
    mut find: F,
    from: usize,
    to: usize,
) -> SearchResult {
    // INVARIANTS:
    // - from <= left <= left + size = right <= to
    // - f returns Less for everything in self[..left]
    // - f returns Greater for everything in self[right..]
    let mut size = to - from;
    let mut left = from;
    let mut right = to;
    while left < right {
        let mid = left + size / 2;
        let cmp = find(mid);

        left = if cmp == Less { mid + 1 } else { left };
        right = if cmp == Greater { mid } else { right };
        if cmp == Equal {
            return SearchResult::Found(mid);
        }

        size = right - left;
    }

    SearchResult::NotFound(left)
}

impl IndexOrd<Scalar> for Array<'_> {
    fn index_cmp(&self, idx: usize, elem: &Scalar) -> Option<Ordering> {
        let scalar_a = scalar_at(self, idx).ok()?;
        scalar_a.partial_cmp(elem)
    }
}

impl<T: PartialOrd> IndexOrd<T> for [T] {
    fn index_cmp(&self, idx: usize, elem: &T) -> Option<Ordering> {
        // SAFETY: Used in search_sorted_by same as the standard library. The search_sorted ensures idx is in bounds
        unsafe { self.get_unchecked(idx) }.partial_cmp(elem)
    }
}

impl Len for Array<'_> {
    fn len(&self) -> usize {
        Array::len(self)
    }
}

impl<T> Len for [T] {
    fn len(&self) -> usize {
        self.len()
    }
}

#[cfg(test)]
mod test {
    use crate::compute::search_sorted::{SearchResult, SearchSorted, SearchSortedSide};

    #[test]
    fn left_side_equal() {
        let arr = [0, 1, 2, 2, 2, 2, 3, 4, 5, 6, 7, 8, 9];
        let res = arr.search_sorted(&2, SearchSortedSide::Left);
        assert_eq!(arr[res.to_index()], 2);
        assert_eq!(res, SearchResult::Found(2));
    }

    #[test]
    fn right_side_equal() {
        let arr = [0, 1, 2, 2, 2, 2, 3, 4, 5, 6, 7, 8, 9];
        let res = arr.search_sorted(&2, SearchSortedSide::Right);
        assert_eq!(arr[res.to_index() - 1], 2);
        assert_eq!(res, SearchResult::Found(6));
    }

    #[test]
    fn left_side_equal_beginning() {
        let arr = [0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let res = arr.search_sorted(&0, SearchSortedSide::Left);
        assert_eq!(arr[res.to_index()], 0);
        assert_eq!(res, SearchResult::Found(0));
    }

    #[test]
    fn right_side_equal_beginning() {
        let arr = [0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let res = arr.search_sorted(&0, SearchSortedSide::Right);
        assert_eq!(arr[res.to_index() - 1], 0);
        assert_eq!(res, SearchResult::Found(4));
    }

    #[test]
    fn left_side_equal_end() {
        let arr = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 9, 9];
        let res = arr.search_sorted(&9, SearchSortedSide::Left);
        assert_eq!(arr[res.to_index()], 9);
        assert_eq!(res, SearchResult::Found(9));
    }

    #[test]
    fn right_side_equal_end() {
        let arr = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 9, 9];
        let res = arr.search_sorted(&9, SearchSortedSide::Right);
        assert_eq!(arr[res.to_index() - 1], 9);
        assert_eq!(res, SearchResult::Found(13));
    }
}
