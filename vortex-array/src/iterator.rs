use std::marker::PhantomData;

use crate::accessor::ArrayAccessor;

pub struct ArrayIter<A: ArrayAccessor<T>, T> {
    array: A,
    current: usize,
    end: usize,
    phantom: PhantomData<T>,
}

impl<A: ArrayAccessor<T>, T> ArrayIter<A, T> {
    pub fn new(array: A) -> Self {
        let len = array.len();
        ArrayIter {
            array,
            current: 0,
            end: len,
            phantom: PhantomData,
        }
    }
}

impl<A: ArrayAccessor<T>, T> Iterator for ArrayIter<A, T> {
    type Item = Option<T>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.end {
            None
        } else {
            let old = self.current;
            self.current += 1;
            Some(self.array.value(old))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.array.len() - self.current,
            Some(self.array.len() - self.current),
        )
    }
}

impl<A: ArrayAccessor<T>, T> DoubleEndedIterator for ArrayIter<A, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.end == self.current {
            None
        } else {
            self.end -= 1;
            Some(self.array.value(self.end))
        }
    }
}

impl<A: ArrayAccessor<T>, T> ExactSizeIterator for ArrayIter<A, T> {}
