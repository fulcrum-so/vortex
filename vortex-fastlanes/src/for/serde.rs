use vortex::array::{Array, ArrayRef};
use vortex::error::VortexResult;
use vortex::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

use crate::{FoRArray, FoREncoding};

impl ArraySerde for FoRArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.scalar(self.reference())?;
        ctx.write_usize(self.shift() as usize)?;
        ctx.write(self.encoded())
    }
}

impl EncodingSerde for FoREncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let reference = ctx.scalar()?;
        let shift = ctx.read_usize()? as u8;
        let child = ctx.read()?;
        Ok(FoRArray::try_new(child, reference, shift)
            .unwrap()
            .into_array())
    }
}

#[cfg(test)]
mod test {

    use vortex::array::IntoArray;
    use vortex::array::{Array, ArrayRef};
    use vortex::error::VortexResult;
    use vortex::scalar::Scalar;
    use vortex::serde::{ReadCtx, WriteCtx};

    use crate::FoRArray;

    fn roundtrip_array(array: &dyn Array) -> VortexResult<ArrayRef> {
        let mut buf = Vec::<u8>::new();
        let mut write_ctx = WriteCtx::new(&mut buf);
        write_ctx.write(array)?;
        let mut read = buf.as_slice();
        let mut read_ctx = ReadCtx::new(array.dtype(), &mut read);
        read_ctx.read()
    }

    #[test]
    fn roundtrip() {
        let arr = FoRArray::try_new(
            vec![-7i64, -13, 17, 23].into_array(),
            <i64 as Into<Scalar>>::into(-7i64),
            2,
        )
        .unwrap();
        roundtrip_array(&arr).unwrap();
    }
}
