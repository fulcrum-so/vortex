use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexError, VortexResult};

use crate::encoding::{ArrayEncoding, ArrayEncodingRef, EncodingRef};
use crate::encoding::{ArrayEncodingExt, EncodingId};
use crate::stats::{ArrayStatistics, Statistics};
use crate::visitor::ArrayVisitor;
use crate::{
    Array, ArrayDType, ArrayData, ArrayMetadata, AsArray, GetArrayMetadata, IntoArray,
    IntoArrayData, ToArrayData,
};
use crate::{ArrayTrait, TryDeserializeArrayMetadata};

/// Trait the defines the set of types relating to an array.
/// Because it has associated types it can't be used as a trait object.
pub trait ArrayDef {
    const ID: EncodingId;
    const ENCODING: EncodingRef;

    type Array: ArrayTrait + TryFrom<Array, Error = VortexError>;
    type Metadata: ArrayMetadata + Clone + for<'m> TryDeserializeArrayMetadata<'m>;
    type Encoding: ArrayEncoding + ArrayEncodingExt<D = Self>;
}

#[macro_export]
macro_rules! impl_encoding {
    ($id:literal, $Name:ident) => {
        use $crate::vendored::paste::paste;

        paste! {
            use $crate::{
                Array,
                ArrayDef,
                ArrayMetadata,
                ArrayTrait,
                AsArray,
                GetArrayMetadata,
                IntoArray,
                ToArray,
                TypedArray,
            };
            use $crate::encoding::{
                ArrayEncoding,
                ArrayEncodingExt,
                EncodingId,
                EncodingRef,
            };
            use $crate::stats::StatsSet;
            use std::any::Any;
            use std::fmt::Debug;
            use std::marker::{Send, Sync};
            use std::sync::Arc;
            use vortex_error::{VortexError, VortexResult};
            use vortex_dtype::DType;

            /// The array definition trait
            #[derive(Debug, Clone)]
            pub struct $Name;
            impl ArrayDef for $Name {
                const ID: EncodingId = EncodingId::new($id);
                const ENCODING: EncodingRef = &[<$Name Encoding>];
                type Array = [<$Name Array>];
                type Metadata = [<$Name Metadata>];
                type Encoding = [<$Name Encoding>];
            }

            #[derive(Debug, Clone)]
            pub struct [<$Name Array>] {
                typed: TypedArray<$Name>
            }
            impl [<$Name Array>] {
                pub fn array(&self) -> &Array {
                    self.typed.array()
                }
                fn metadata(&self) -> &[<$Name Metadata>] {
                    self.typed.metadata()
                }

                #[allow(dead_code)]
                fn try_from_parts(
                    dtype: DType,
                    metadata: [<$Name Metadata>],
                    children: Arc<[Array]>,
                    stats: StatsSet,
                ) -> VortexResult<Self> {
                    Ok(Self { typed: TypedArray::try_from_parts(dtype, metadata, None, children, stats)? })
                }
            }
            impl GetArrayMetadata for [<$Name Array>] {
                fn metadata(&self) -> Arc<dyn ArrayMetadata> {
                    Arc::new(self.metadata().clone())
                }
            }
            impl AsArray for [<$Name Array>] {
                fn as_array_ref(&self) -> &Array {
                    self.typed.array()
                }
            }
            impl ToArray for [<$Name Array>] {
                fn to_array(&self) -> Array {
                    self.typed.to_array()
                }
            }
            impl<'a> IntoArray for [<$Name Array>] {
                fn into_array(self) -> Array {
                    self.typed.into_array()
                }
            }
            impl From<TypedArray<$Name>> for [<$Name Array>] {
                fn from(typed: TypedArray<$Name>) -> Self {
                    Self { typed }
                }
            }
            impl TryFrom<Array> for [<$Name Array>] {
                type Error = VortexError;

                #[inline]
                fn try_from(array: Array) -> Result<Self, Self::Error> {
                    TypedArray::<$Name>::try_from(array).map(Self::from)
                }
            }
            impl TryFrom<&Array> for [<$Name Array>] {
                type Error = VortexError;

                #[inline]
                fn try_from(array: &Array) -> Result<Self, Self::Error> {
                    TypedArray::<$Name>::try_from(array).map(Self::from)
                }
            }

            /// The array encoding
            #[derive(Debug)]
            pub struct [<$Name Encoding>];
            impl ArrayEncoding for [<$Name Encoding>] {
                #[inline]
                fn id(&self) -> EncodingId {
                    $Name::ID
                }

                #[inline]
                fn canonicalize(&self, array: Array) -> VortexResult<$crate::Canonical> {
                    <Self as ArrayEncodingExt>::into_canonical(array)
                }

                #[inline]
                fn with_dyn(
                    &self,
                    array: &Array,
                    f: &mut dyn for<'b> FnMut(&'b (dyn ArrayTrait + 'b)) -> VortexResult<()>,
                ) -> VortexResult<()> {
                    <Self as ArrayEncodingExt>::with_dyn(array, f)
                }
            }
            impl ArrayEncodingExt for [<$Name Encoding>] {
                type D = $Name;
            }

            /// Implement ArrayMetadata
            impl ArrayMetadata for [<$Name Metadata>] {
                #[inline]
                fn as_any(&self) -> &dyn Any {
                    self
                }

                #[inline]
                fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
                    self
                }
            }
        }
    };
}

impl AsArray for Array {
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
            Array::View(v) => v.dtype(),
        }
    }
}

impl<T: AsArray> ArrayStatistics for T {
    fn statistics(&self) -> &(dyn Statistics + '_) {
        match self.as_array_ref() {
            Array::Data(d) => d.statistics(),
            Array::View(v) => v.statistics(),
        }
    }
}

impl<T: IntoArray + ArrayEncodingRef + ArrayStatistics + GetArrayMetadata> IntoArrayData for T {
    fn into_array_data(self) -> ArrayData {
        let encoding = self.encoding();
        let metadata = self.metadata();
        let stats = self.statistics().to_set();
        let array = self.into_array();
        match array {
            Array::Data(d) => d,
            Array::View(_) => {
                struct Visitor {
                    buffer: Option<Buffer>,
                    children: Vec<Array>,
                }
                impl ArrayVisitor for Visitor {
                    fn visit_child(&mut self, _name: &str, array: &Array) -> VortexResult<()> {
                        self.children.push(array.clone());
                        Ok(())
                    }

                    fn visit_buffer(&mut self, buffer: &Buffer) -> VortexResult<()> {
                        if self.buffer.is_some() {
                            vortex_bail!("Multiple buffers found in view")
                        }
                        self.buffer = Some(buffer.clone());
                        Ok(())
                    }
                }
                let mut visitor = Visitor {
                    buffer: None,
                    children: vec![],
                };
                array.with_dyn(|a| a.accept(&mut visitor).unwrap());
                ArrayData::try_new(
                    encoding,
                    array.dtype().clone(),
                    metadata,
                    visitor.buffer,
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
