use flatbuffers::root;
use vortex_dtype::flatbuffers as fb;
use vortex_dtype::CompositeID;
use vortex_error::vortex_err;
use vortex_flatbuffers::{FlatBufferToBytes, ReadFlatBuffer};

use crate::array::composite::{find_extension, CompositeExtensionRef, TypedCompositeArray};
use crate::compute::ArrayCompute;
use crate::stats::ArrayStatisticsCompute;
use crate::validity::{ArrayValidity, LogicalValidity};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::{
    impl_encoding, ArrayDType, ArrayFlatten, IntoArrayData, TryDeserializeArrayMetadata,
    TrySerializeArrayMetadata,
};

pub trait UnderlyingMetadata:
    'static + Send + Sync + Debug + TrySerializeArrayMetadata + for<'m> TryDeserializeArrayMetadata<'m>
{
    fn id(&self) -> CompositeID;
}

impl_encoding!("vortex.composite", Composite);

#[derive(Debug, Clone)]
pub struct CompositeMetadata {
    ext: CompositeExtensionRef,
    underlying_dtype: DType,
    underlying_metadata: Arc<[u8]>,
}

impl TrySerializeArrayMetadata for CompositeMetadata {
    fn try_serialize_metadata(&self) -> VortexResult<Arc<[u8]>> {
        let mut fb = flexbuffers::Builder::default();
        {
            let mut elems = fb.start_vector();
            elems.push(self.ext.id().0);
            self.underlying_dtype
                .with_flatbuffer_bytes(|b| elems.push(flexbuffers::Blob(b)));
            elems.push(flexbuffers::Blob(self.underlying_metadata.as_ref()));
        }
        Ok(fb.take_buffer().into())
    }
}

impl TryDeserializeArrayMetadata<'_> for CompositeMetadata {
    fn try_deserialize_metadata(metadata: Option<&[u8]>) -> VortexResult<Self> {
        let reader = flexbuffers::Reader::get_root(metadata.expect("missing metadata"))?;
        let elems = reader.as_vector();

        let ext_id = elems.index(0).expect("missing composite id").as_str();
        let ext = find_extension(ext_id)
            .ok_or_else(|| vortex_err!("Unrecognized composite extension: {}", ext_id))?;

        let dtype_blob = elems.index(1).expect("missing dtype").as_blob();
        let underlying_dtype =
            DType::read_flatbuffer(&root::<fb::DType>(dtype_blob.0).expect("invalid dtype"))?;

        let underlying_metadata: Arc<[u8]> = elems
            .index(2)
            .expect("missing underlying metadata")
            .as_blob()
            .0
            .to_vec()
            .into();

        Ok(CompositeMetadata {
            ext,
            underlying_dtype,
            underlying_metadata,
        })
    }
}

impl<'a> CompositeArray<'a> {
    pub fn new(id: CompositeID, metadata: Arc<[u8]>, underlying: Array<'a>) -> Self {
        let dtype = DType::Composite(id, underlying.dtype().is_nullable().into());
        let ext = find_extension(id.0).expect("Unrecognized composite extension");
        Self::try_from_parts(
            dtype,
            CompositeMetadata {
                ext,
                underlying_dtype: underlying.dtype().clone(),
                underlying_metadata: metadata,
            },
            [underlying.into_array_data()].into(),
            StatsSet::new(),
        )
        .unwrap()
    }
}

impl CompositeArray<'_> {
    #[inline]
    pub fn id(&self) -> CompositeID {
        self.metadata().ext.id()
    }

    #[inline]
    pub fn extension(&self) -> CompositeExtensionRef {
        find_extension(self.id().0).expect("Unrecognized composite extension")
    }

    pub fn underlying_metadata(&self) -> &Arc<[u8]> {
        &self.metadata().underlying_metadata
    }

    pub fn underlying_dtype(&self) -> &DType {
        &self.metadata().underlying_dtype
    }

    #[inline]
    pub fn underlying(&self) -> Array {
        self.array()
            .child(0, self.underlying_dtype())
            .expect("CompositeArray must have an underlying array")
    }

    pub fn with_compute<R, F>(&self, mut f: F) -> R
    where
        F: FnMut(&dyn ArrayCompute) -> R,
    {
        let mut result = None;

        self.extension()
            .with_compute(self, &mut |c| {
                result = Some(f(c));
                Ok(())
            })
            .unwrap();

        // Now we unwrap the optional, which we know to be populated by the closure.
        result.unwrap()
    }
}

impl<'a> CompositeArray<'a> {
    pub fn as_typed<M: UnderlyingMetadata>(&'a self) -> VortexResult<TypedCompositeArray<'a, M>> {
        Ok(TypedCompositeArray::new(
            M::try_deserialize_metadata(Some(self.underlying_metadata()))?,
            self.underlying().clone(),
        ))
    }
}

impl ArrayTrait for CompositeArray<'_> {
    fn len(&self) -> usize {
        self.underlying().len()
    }
}

impl ArrayFlatten for CompositeArray<'_> {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a,
    {
        Ok(Flattened::Composite(self))
    }
}

impl ArrayValidity for CompositeArray<'_> {
    fn is_valid(&self, index: usize) -> bool {
        self.underlying().with_dyn(|a| a.is_valid(index))
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.underlying().with_dyn(|a| a.logical_validity())
    }
}

impl AcceptArrayVisitor for CompositeArray<'_> {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("underlying", &self.underlying())
    }
}

impl ArrayStatisticsCompute for CompositeArray<'_> {}

impl EncodingCompression for CompositeEncoding {}
