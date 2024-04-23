use compress::roaring_encode;
use croaring::{Bitmap, Portable};
use serde::{Deserialize, Serialize};
use vortex::array::primitive::{Primitive, PrimitiveArray};
use vortex::buffer::Buffer;
use vortex::ptype::PType;
use vortex::stats::ArrayStatisticsCompute;
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{impl_encoding, ArrayFlatten, OwnedArray};
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_schema::Nullability::NonNullable;

mod compress;
mod compute;

impl_encoding!("vortex.roaring_int", RoaringInt);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoaringIntMetadata {
    ptype: PType,
    length: usize,
}

impl RoaringIntArray<'_> {
    pub fn new(bitmap: Bitmap, ptype: PType) -> Self {
        Self::try_new(bitmap, ptype).unwrap()
    }

    pub fn try_new(bitmap: Bitmap, ptype: PType) -> VortexResult<Self> {
        if !ptype.is_unsigned_int() {
            vortex_bail!("RoaringInt expected unsigned int");
        }
        Ok(Self {
            typed: TypedArray::try_from_parts(
                DType::Bool(NonNullable),
                RoaringIntMetadata {
                    ptype,
                    length: bitmap.statistics().cardinality as usize,
                },
                Some(Buffer::Owned(bitmap.serialize::<Portable>().into())),
                vec![].into(),
                HashMap::default(),
            )?,
        })
    }

    pub fn bitmap(&self) -> Bitmap {
        //TODO(@jdcasale): figure out a way to avoid this deserialization per-call
        Bitmap::deserialize::<Portable>(
            self.array()
                .buffer()
                .expect("RoaringBoolArray buffer is missing")
                .as_slice(),
        )
    }

    pub fn ptype(&self) -> PType {
        self.metadata().ptype
    }

    pub fn encode(array: OwnedArray) -> VortexResult<OwnedArray> {
        if array.encoding().id() == Primitive::ID {
            Ok(roaring_encode(PrimitiveArray::try_from(array)?).into_array())
        } else {
            Err(vortex_err!("RoaringInt can only encode primitive arrays"))
        }
    }
}

impl ArrayValidity for RoaringIntArray<'_> {
    fn logical_validity(&self) -> LogicalValidity {
        LogicalValidity::AllValid(self.bitmap().iter().count())
    }

    fn is_valid(&self, _index: usize) -> bool {
        true
    }
}

impl ArrayFlatten for RoaringIntArray<'_> {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a,
    {
        todo!()
    }
}

impl AcceptArrayVisitor for RoaringIntArray<'_> {
    fn accept(&self, _visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        todo!()
    }
}

impl ArrayStatisticsCompute for RoaringIntArray<'_> {}

impl ArrayTrait for RoaringIntArray<'_> {
    fn len(&self) -> usize {
        self.metadata().length
    }
}

#[cfg(test)]
mod test {
    use vortex::array::primitive::PrimitiveArray;
    use vortex::compute::scalar_at::scalar_at;
    use vortex::IntoArray;
    use vortex_error::VortexResult;

    use crate::RoaringIntArray;

    #[test]
    pub fn test_scalar_at() -> VortexResult<()> {
        let ints = PrimitiveArray::from(vec![2u32, 12, 22, 32]).into_array();
        let array = RoaringIntArray::encode(ints)?;

        assert_eq!(scalar_at(&array, 0).unwrap(), 2u32.into());
        assert_eq!(scalar_at(&array, 1).unwrap(), 12u32.into());

        Ok(())
    }
}
