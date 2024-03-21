pub mod dtypes;
pub mod wrappers;

pub trait FromArrowType<T>: Sized {
    fn from_arrow(value: T) -> Self;
}
