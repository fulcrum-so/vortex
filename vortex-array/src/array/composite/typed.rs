use std::fmt::Debug;
use std::sync::Arc;

use vortex_schema::CompositeID;
use vortex_schema::DType;

use crate::array::composite::array::CompositeArray;
use crate::array::composite::CompositeMetadata;
use crate::array::{Array, ArrayRef};
use crate::compute::ArrayCompute;

pub trait CompositeExtension: Debug + Send + Sync + 'static {
    fn id(&self) -> CompositeID;

    fn as_typed_compute(&self, array: &CompositeArray) -> Box<dyn ArrayCompute>;
}

pub type CompositeExtensionRef = &'static dyn CompositeExtension;

#[derive(Debug, Clone)]
pub struct TypedCompositeArray<M: CompositeMetadata> {
    metadata: M,
    underlying: ArrayRef,
    dtype: DType,
}

impl<M: CompositeMetadata> TypedCompositeArray<M> {
    pub fn new(metadata: M, underlying: ArrayRef) -> Self {
        let dtype = DType::Composite(metadata.id(), underlying.dtype().is_nullable().into());
        Self {
            metadata,
            underlying,
            dtype,
        }
    }

    #[inline]
    pub fn metadata(&self) -> &M {
        &self.metadata
    }

    #[inline]
    pub fn underlying(&self) -> &ArrayRef {
        &self.underlying
    }

    #[inline]
    pub fn dtype(&self) -> &DType {
        &self.dtype
    }

    pub fn as_composite(&self) -> CompositeArray {
        CompositeArray::new(
            self.metadata().id(),
            Arc::new(self.metadata().serialize()),
            self.underlying().clone(),
        )
    }
}

macro_rules! composite_impl {
    ($id:expr, $T:ty) => {
        use crate::array::composite::{
            CompositeArray, CompositeExtension, CompositeMetadata, COMPOSITE_EXTENSIONS,
        };
        use crate::compute::ArrayCompute;
        use linkme::distributed_slice;
        use paste::paste;
        use vortex_schema::{DType, Nullability};

        paste! {
            #[derive(Debug)]
            pub struct [<$T Extension>];

            impl [<$T Extension>] {
                pub const ID: CompositeID = CompositeID($id);

                pub fn dtype(nullability: Nullability) -> DType {
                    DType::Composite(Self::ID, nullability)
                }
            }

            impl CompositeExtension for [<$T Extension>] {
                fn id(&self) -> CompositeID {
                    Self::ID
                }

                fn as_typed_compute(&self, array: &CompositeArray) -> Box<dyn ArrayCompute> {
                    if array.id() != Self::ID {
                        panic!("Incorrect CompositeID");
                    }
                    Box::new(array.as_typed::<$T>())
                }
            }

            impl CompositeMetadata for $T {
                fn id(&self) -> CompositeID {
                    [<$T Extension>]::ID
                }
            }

            #[distributed_slice(COMPOSITE_EXTENSIONS)]
            static ENCODINGS_COMPOSITE_EXT: &'static dyn CompositeExtension = &[<$T Extension>];
        }
    };
}

pub(crate) use composite_impl;
