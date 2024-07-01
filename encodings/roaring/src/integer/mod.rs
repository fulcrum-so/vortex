pub use compress::*;
use croaring::{Bitmap, Portable};
use serde::{Deserialize, Serialize};
use vortex::array::primitive::{Primitive, PrimitiveArray};
use vortex::stats::ArrayStatisticsCompute;
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{impl_encoding, Canonical, IntoCanonical};
use vortex_buffer::Buffer;
use vortex_dtype::Nullability::NonNullable;
use vortex_dtype::PType;
use vortex_error::{vortex_bail, vortex_err};

mod compress;
mod compute;

impl_encoding!("vortex.roaring_int", RoaringInt);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoaringIntMetadata {
    ptype: PType,
    // NB: this is stored because we want to avoid the overhead of deserializing the bitmap
    // on every len() call. It's CRITICAL that this is kept up-to date.
    length: usize,
}

impl RoaringIntArray {
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
                Some(Buffer::from(bitmap.serialize::<Portable>())),
                vec![].into(),
                StatsSet::new(),
            )?,
        })
    }

    pub fn bitmap(&self) -> Bitmap {
        //TODO(@jdcasale): figure out a way to avoid this deserialization per-call
        Bitmap::deserialize::<Portable>(
            self.array()
                .buffer()
                .expect("RoaringBoolArray buffer is missing")
                .as_ref(),
        )
    }

    pub fn ptype(&self) -> PType {
        self.metadata().ptype
    }

    pub fn encode(array: Array) -> VortexResult<Array> {
        if array.encoding().id() == Primitive::ID {
            Ok(roaring_int_encode(PrimitiveArray::try_from(array)?)?.into_array())
        } else {
            Err(vortex_err!("RoaringInt can only encode primitive arrays"))
        }
    }
}

impl ArrayValidity for RoaringIntArray {
    fn is_valid(&self, _index: usize) -> bool {
        true
    }

    fn logical_validity(&self) -> LogicalValidity {
        LogicalValidity::AllValid(self.bitmap().iter().count())
    }
}

impl IntoCanonical for RoaringIntArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        todo!()
    }
}

impl AcceptArrayVisitor for RoaringIntArray {
    fn accept(&self, _visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        todo!()
    }
}

impl ArrayStatisticsCompute for RoaringIntArray {}

impl ArrayTrait for RoaringIntArray {
    fn len(&self) -> usize {
        self.metadata().length
    }
}

#[cfg(test)]
mod test {
    use vortex::array::primitive::PrimitiveArray;
    use vortex::compute::unary::scalar_at::scalar_at;
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
