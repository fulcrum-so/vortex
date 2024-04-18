use vortex_error::{VortexError, VortexResult};
use vortex_schema::DType;

use crate::buffer::{Buffer, OwnedBuffer};
use crate::encoding::{ArrayEncoding, ArrayEncodingRef, EncodingRef};
use crate::encoding::{ArrayEncodingExt, EncodingId};
use crate::stats::{ArrayStatistics, Statistics};
use crate::visitor::ArrayVisitor;
use crate::{
    Array, ArrayDType, ArrayData, ArrayMetadata, AsArray, GetArrayMetadata, IntoArray,
    IntoArrayData, ToArrayData, ToStatic,
};
use crate::{ArrayTrait, TryDeserializeArrayMetadata};

/// Trait the defines the set of types relating to an array.
/// Because it has associated types it can't be used as a trait object.
pub trait ArrayDef {
    const ID: EncodingId;
    const ENCODING: EncodingRef;

    type Array<'a>: ArrayTrait + TryFrom<Array<'a>, Error = VortexError> + 'a;
    type Metadata: ArrayMetadata + Clone + for<'m> TryDeserializeArrayMetadata<'m>;
    type Encoding: ArrayEncoding + ArrayEncodingExt<D = Self>;
}

#[macro_export]
macro_rules! impl_encoding {
    ($id:literal, $Name:ident) => {
        use paste::paste;

        paste! {
            use $crate::{
                Array,
                ArrayData,
                ArrayDef,
                ArrayMetadata,
                ArrayTrait,
                AsArray,
                Flattened,
                GetArrayMetadata,
                IntoArray,
                ToArray,
                TypedArray,
            };
            use $crate::compress::EncodingCompression;
            use $crate::encoding::{
                ArrayEncoding,
                ArrayEncodingExt,
                EncodingId,
                EncodingRef,
                VORTEX_ENCODINGS,
            };
            use $crate::buffer::OwnedBuffer;
            use $crate::stats::Stat;
            use $crate::scalar::Scalar;
            use std::any::Any;
            use std::collections::HashMap;
            use std::fmt::Debug;
            use std::marker::{Send, Sync};
            use std::sync::Arc;
            use vortex_error::VortexError;
            use vortex_schema::DType;

            /// The array definition trait
            #[derive(Debug, Clone)]
            pub struct $Name;
            impl ArrayDef for $Name {
                const ID: EncodingId = EncodingId::new($id);
                const ENCODING: EncodingRef = &[<$Name Encoding>];
                type Array<'a> = [<$Name Array>]<'a>;
                type Metadata = [<$Name Metadata>];
                type Encoding = [<$Name Encoding>];
            }

            #[derive(Debug, Clone)]
            pub struct [<$Name Array>]<'a> {
                typed: TypedArray<'a, $Name>
            }
            pub type [<Owned $Name Array>] = [<$Name Array>]<'static>;
            impl<'a> [<$Name Array>]<'a> {
                pub fn array(&'a self) -> &'a Array<'a> {
                    self.typed.array()
                }
                pub fn metadata(&'a self) -> &'a [<$Name Metadata>] {
                    self.typed.metadata()
                }

                pub fn try_from_parts(
                    dtype: DType,
                    metadata: [<$Name Metadata>],
                    buffers: Box<[OwnedBuffer]>,
                    children: Box<[ArrayData]>,
                    stats: HashMap<Stat, Scalar>) -> VortexResult<Self> {
                    Ok(Self { typed: TypedArray::try_from_parts(dtype, metadata, buffers, children, stats)? })
                }
            }
            impl<'a> GetArrayMetadata for [<$Name Array>]<'a> {
                fn metadata(&self) -> Arc<dyn ArrayMetadata> {
                    Arc::new(self.metadata().clone())
                }
            }
            impl<'a> AsArray for [<$Name Array>]<'a> {
                fn as_array_ref(&self) -> &Array {
                    self.typed.array()
                }
            }
            impl<'a> ToArray for [<$Name Array>]<'a> {
                fn to_array(&self) -> Array {
                    self.typed.to_array()
                }
            }
            impl<'a> IntoArray<'a> for [<$Name Array>]<'a> {
                fn into_array(self) -> Array<'a> {
                    self.typed.into_array()
                }
            }
            impl<'a> From<TypedArray<'a, $Name>> for [<$Name Array>]<'a> {
                fn from(typed: TypedArray<'a, $Name>) -> Self {
                    Self { typed }
                }
            }
            impl<'a> TryFrom<Array<'a>> for [<$Name Array>]<'a> {
                type Error = VortexError;

                fn try_from(array: Array<'a>) -> Result<Self, Self::Error> {
                    TypedArray::<$Name>::try_from(array).map(Self::from)
                }
            }
            impl<'a> TryFrom<&'a Array<'a>> for [<$Name Array>]<'a> {
                type Error = VortexError;

                fn try_from(array: &'a Array<'a>) -> Result<Self, Self::Error> {
                    TypedArray::<$Name>::try_from(array).map(Self::from)
                }
            }

            /// The array encoding
            #[derive(Debug)]
            pub struct [<$Name Encoding>];
            #[$crate::linkme::distributed_slice(VORTEX_ENCODINGS)]
            #[allow(non_upper_case_globals)]
            static [<ENCODINGS_ $Name>]: EncodingRef = &[<$Name Encoding>];
            impl ArrayEncoding for [<$Name Encoding>] {
                fn as_any(&self) -> &dyn Any {
                    self
                }

                fn id(&self) -> EncodingId {
                    $Name::ID
                }

                fn flatten<'a>(&self, array: Array<'a>) -> VortexResult<Flattened<'a>> {
                    <Self as ArrayEncodingExt>::flatten(array)
                }

                #[inline]
                fn with_dyn<'a>(
                    &self,
                    array: &'a Array<'a>,
                    f: &mut dyn for<'b> FnMut(&'b (dyn ArrayTrait + 'a)) -> VortexResult<()>,
                ) -> VortexResult<()> {
                    <Self as ArrayEncodingExt>::with_dyn(array, f)
                }

                fn compression(&self) -> &dyn EncodingCompression {
                    self
                }
            }
            impl ArrayEncodingExt for [<$Name Encoding>] {
                type D = $Name;
            }

            /// Implement ArrayMetadata
            impl ArrayMetadata for [<$Name Metadata>] {
                fn as_any(&self) -> &dyn Any {
                    self
                }

                fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
                    self
                }
            }
        }
    };
}

impl<'a> AsArray for Array<'a> {
    fn as_array_ref(&self) -> &Array {
        self
    }
}

impl<T: AsArray> ArrayEncodingRef for T {
    fn encoding(&self) -> EncodingRef {
        self.as_array_ref().encoding()
    }
}

impl<T: AsArray> ArrayDType for T {
    fn dtype(&self) -> &DType {
        match self.as_array_ref() {
            Array::Data(d) => d.dtype(),
            Array::DataRef(d) => d.dtype(),
            Array::View(v) => v.dtype(),
        }
    }
}

impl<T: AsArray> ArrayStatistics for T {
    fn statistics(&self) -> &(dyn Statistics + '_) {
        match self.as_array_ref() {
            Array::Data(d) => d.statistics(),
            Array::DataRef(d) => d.statistics(),
            Array::View(v) => v.statistics(),
        }
    }
}

impl<'a, T: IntoArray<'a> + ArrayEncodingRef + ArrayStatistics + GetArrayMetadata> IntoArrayData
    for T
{
    fn into_array_data(self) -> ArrayData {
        let encoding = self.encoding();
        let metadata = self.metadata();
        let stats = self.statistics().to_map();
        let array = self.into_array();
        match array {
            Array::Data(d) => d,
            Array::DataRef(d) => d.clone(),
            Array::View(_) => {
                struct Visitor {
                    buffers: Vec<OwnedBuffer>,
                    children: Vec<ArrayData>,
                }
                impl ArrayVisitor for Visitor {
                    fn visit_child(&mut self, _name: &str, array: &Array) -> VortexResult<()> {
                        self.children.push(array.to_array_data());
                        Ok(())
                    }

                    fn visit_buffer(&mut self, buffer: &Buffer) -> VortexResult<()> {
                        self.buffers.push(buffer.to_static());
                        Ok(())
                    }
                }
                let mut visitor = Visitor {
                    buffers: vec![],
                    children: vec![],
                };
                array.with_dyn(|a| a.accept(&mut visitor).unwrap());
                ArrayData::try_new(
                    encoding,
                    array.dtype().clone(),
                    metadata,
                    visitor.buffers.into(),
                    visitor.children.into(),
                    stats,
                )
                .unwrap()
            }
        }
    }
}

impl<T: IntoArrayData + Clone> ToArrayData for T {
    fn to_array_data(&self) -> ArrayData {
        self.clone().into_array_data()
    }
}
