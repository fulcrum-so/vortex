use vortex::array::bool::BoolEncoding;
use vortex_error::{vortex_err, VortexResult};

use crate::array::bool::{bool_try_from_parts, BoolArray, BoolMetadata};
use crate::encoding::{ArrayEncoding, EncodingRef};
use crate::encoding::{EncodingId, WithEncodedArray};
use crate::validity::Validity;
use crate::{ArrayData, ArrayMetadata, ArrayParts, ArrayView, TryFromArrayParts};
use crate::{ArrayTrait, TryDeserializeArrayMetadata, TrySerializeArrayMetadata};

/// Trait the defines the set of types relating to an array.
/// Because it has associated types it can't be used as a trait object.
pub trait ArrayDef {
    const ID: EncodingId;
    const ENCODING: EncodingRef;

    type Array<'a>: ArrayTrait + TryFromArrayParts<'a, Self::Metadata> + 'a;
    type Metadata: ArrayMetadata
        + TrySerializeArrayMetadata
        + for<'a> TryDeserializeArrayMetadata<'a>;
    type Encoding: ArrayEncoding; //  + for<'a> WithEncodedArray<'a, Self::Array<'a>>;
}

#[macro_export]
macro_rules! impl_encoding {
    ($id:literal, $Name:ident) => {
        use paste::paste;

        paste! {
            use $crate::{
                ArrayDef,
                ArrayEncodingRef,
                ArrayParts,
                ArrayTrait,
                TryFromArrayParts,
                TryDeserializeArrayMetadata,
            };
            use $crate::encoding::{ArrayEncoding, EncodingId, EncodingRef, VORTEX_ENCODINGS};
            use vortex_error::vortex_err;
            use std::any::Any;
            use std::fmt::Debug;
            use std::sync::Arc;
            use std::marker::{Send, Sync};

            /// The array definition trait
            #[derive(Debug)]
            pub struct [<$Name Def>];
            impl ArrayDef for [<$Name Def>] {
                const ID: EncodingId = EncodingId::new($id);
                const ENCODING: EncodingRef = &[<$Name Encoding>];
                type Array<'a> = [<$Name Array>]<'a>;
                type Metadata = [<$Name Metadata>];
                type Encoding = [<$Name Encoding>];
            }

            pub type [<$Name Data>] = TypedArrayData<[<$Name Def>]>;

            /// The array encoding
            pub struct [<$Name Encoding>];
            #[$crate::linkme::distributed_slice(VORTEX_ENCODINGS)]
            #[allow(non_upper_case_globals)]
            static [<ENCODINGS_ $Name>]: EncodingRef = &[<$Name Encoding>];
            impl ArrayEncoding for [<$Name Encoding>] {
                fn as_any(&self) -> &dyn Any {
                    self
                }

                fn id(&self) -> EncodingId {
                    [<$Name Def>]::ID
                }

                fn with_view_mut<'v>(
                    &self,
                    view: &'v ArrayView<'v>,
                    f: &mut dyn FnMut(&dyn ArrayTrait) -> VortexResult<()>,
                ) -> VortexResult<()> {
                    // Convert ArrayView -> PrimitiveArray, then call compute.
                    let metadata = [<$Name Metadata>]::try_deserialize_metadata(view.metadata())?;
                    let array = [<$Name Array>]::try_from_parts(view as &dyn ArrayParts, &metadata)?;
                    f(&array)
                }

                fn with_data_mut(
                    &self,
                    data: &ArrayData,
                    f: &mut dyn FnMut(&dyn ArrayTrait) -> VortexResult<()>,
                ) -> VortexResult<()> {
                    let metadata = data.metadata()
                        .as_any()
                        .downcast_ref::<[<$Name Metadata>]>()
                        .ok_or_else(|| vortex_err!("Failed to downcast metadata"))?
                        .clone();
                    let array = [<$Name Array>]::try_from_parts(data as &dyn ArrayParts, &metadata)?;
                    f(&array)
                }
            }
            //
            // impl<'v> WithEncodedArray<'v, [<$Name Array>]<'v>> for [<$Name Encoding>] {
            //     fn with_view_mut(
            //         &self,
            //         view: &'v ArrayView<'v>,
            //         f: &mut dyn FnMut(&[<$Name Array>]<'v>) -> VortexResult<()>,
            //     ) -> VortexResult<()> {
            //         // Convert ArrayView -> PrimitiveArray, then call compute.
            //         let metadata = [<$Name Metadata>]::try_deserialize_metadata(view.metadata())?;
            //         let array = [<$Name Array>]::try_from_parts(view as &dyn ArrayParts, &metadata)?;
            //         f(&array)
            //     }
            //
            //     fn with_data_mut(
            //         &self,
            //         data: &ArrayData,
            //         f: &mut dyn FnMut(&[<$Name Array>]<'v>) -> VortexResult<()>,
            //     ) -> VortexResult<()> {
            //         let metadata = data.metadata()
            //             .as_any()
            //             .downcast_ref::<[<$Name Metadata>]>()
            //             .ok_or_else(|| vortex_err!("Failed to downcast metadata"))?
            //             .clone();
            //         let array = [<$Name Array>]::try_from_parts(data as &dyn ArrayParts, &metadata)?;
            //         f(&array)
            //     }
            // }

            /// Implement ArrayMetadata
            impl ArrayMetadata for [<$Name Metadata>] {
                fn as_any(&self) -> &dyn Any {
                    self
                }

                fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
                    self
                }
            }

            impl ArrayEncodingRef for [<$Name Array>]<'_> {
                fn encoding(&self) -> EncodingRef {
                    [<$Name Def>]::ENCODING
                }
            }
        }
    };
}

impl<'v: 'a, 'a> WithEncodedArray<'v, BoolArray<'a>> for BoolEncoding {
    fn with_view_mut(
        &'v self,
        view: &'v ArrayView<'v>,
        f: &mut dyn for<'m> FnMut(&BoolArray<'m>) -> VortexResult<()>,
    ) -> VortexResult<()> {
        let metadata = BoolMetadata::try_deserialize_metadata(view.metadata())?;
        let array = bool_try_from_parts(view as &dyn ArrayParts, &metadata)?;
        f(&array)
    }

    fn with_data_mut(
        &self,
        data: &'v ArrayData,
        f: &mut dyn for<'m> FnMut(&BoolArray<'m>) -> VortexResult<()>,
    ) -> VortexResult<()> {
        let metadata = data
            .metadata()
            .as_any()
            .downcast_ref::<BoolMetadata>()
            .ok_or_else(|| vortex_err!("Failed to downcast metadata"))?
            .clone();
        let array = bool_try_from_parts(data as &dyn ArrayParts, &metadata)?;
        f(&array)
    }
}
