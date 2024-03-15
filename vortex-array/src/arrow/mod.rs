use arrow_array::array::ArrayRef as ArrowArrayRef;
use arrow_schema::DataType;
use arrow_select::concat::concat;
use itertools::Itertools;

use crate::array::{Array, ArrowIterator};

pub mod aligned_iter;
pub mod convert;

pub trait TryIntoArrow: 'static {
    fn arrow_array(&self) -> VortexResult<ArrowArrayRef>;
    fn arrow_dtype(&self) -> VortexResult<DataType>;
}

struct Wrap<T>(T);

trait ViaImpl {
    fn foo(&self) -> Option<&dyn TryIntoArrow>;
}
impl<T: TryIntoArrow> ViaImpl for &Wrap<T> {
    fn foo(&self) -> Option<&dyn TryIntoArrow> {
        Some(&self.0)
    }
}

trait ViaAny {
    fn foo(&self) -> Option<&dyn TryIntoArrow>;
}
impl<T> ViaAny for Wrap<T> {
    fn foo(&self) -> Option<&dyn TryIntoArrow> {
        None
    }
}

macro_rules! _as_try_into_arrow {
    ($e:expr) => {
        (&&&Wrap($e)).foo()
    };
}

pub fn as_try_into_arrow(array: &dyn Array) -> Option<&dyn TryIntoArrow> {
    // TODO(ngates): match macro that downcasts to all discovered encodings.
    _as_try_into_arrow!(array)
}

pub trait CombineChunks {
    fn combine_chunks(self) -> ArrowArrayRef;
}

impl CombineChunks for Box<ArrowIterator> {
    fn combine_chunks(self) -> ArrowArrayRef {
        let chunks = self.collect_vec();
        let chunk_refs = chunks.iter().map(|a| a.as_ref()).collect_vec();
        concat(&chunk_refs).unwrap()
    }
}

#[macro_export]
macro_rules! match_arrow_numeric_type {
    ($self:expr, | $_:tt $enc:ident | $($body:tt)*) => ({
        macro_rules! __with__ {( $_ $enc:ident ) => ( $($body)* )}
        use $crate::dtype::DType::*;
        use $crate::dtype::IntWidth::*;
        use $crate::dtype::Signedness::*;
        use $crate::dtype::FloatWidth;
        use arrow_array::types::*;
        match $self {
            Int(_8, Unsigned, _) => __with__! {UInt8Type},
            Int(_16, Unsigned, _) => __with__!{UInt16Type},
            Int(_32, Unsigned, _) => __with__!{UInt32Type},
            Int(_64, Unsigned, _) => __with__!{UInt64Type},
            Int(_8, Signed, _) => __with__! {Int8Type},
            Int(_16, Signed, _) => __with__!{Int16Type},
            Int(_32, Signed, _) => __with__!{Int32Type},
            Int(_64, Signed, _) => __with__!{Int64Type},
            Float(FloatWidth::_16, _) => __with__!{Float16Type},
            Float(FloatWidth::_32, _) => __with__!{Float32Type},
            Float(FloatWidth::_64, _) => __with__!{Float64Type},
            _ => unimplemented!("Convert this DType to ArrowPrimitiveType")
        }
    })
}

use crate::error::VortexResult;
pub use match_arrow_numeric_type;
