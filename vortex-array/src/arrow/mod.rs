pub mod dtypes;
mod recordbatch;
pub mod wrappers;

pub trait FromArrowType<T>: Sized {
    fn from_arrow(value: T) -> Self;
}
