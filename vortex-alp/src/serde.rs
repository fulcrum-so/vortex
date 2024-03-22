use crate::alp::Exponents;
use vortex::array::{Array, ArrayRef};
use vortex::error::{VortexError, VortexResult};
use vortex::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};
use vortex_schema::{DType, FloatWidth, Signedness};

use crate::ALPArray;
use crate::ALPEncoding;

impl ArraySerde for ALPArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.write_option_tag(self.patches().is_some())?;
        if let Some(p) = self.patches() {
            ctx.write(p)?;
        }
        ctx.write_fixed_slice([self.exponents().e, self.exponents().f])?;
        ctx.write(self.encoded())
    }
}

impl EncodingSerde for ALPEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let patches_tag = ctx.read_nbytes::<1>()?[0];
        let patches = if patches_tag == 0x01 {
            Some(ctx.read()?)
        } else {
            None
        };
        let exponents = ctx.read_nbytes::<2>()?;
        let encoded_dtype = match ctx.schema() {
            DType::Float(width, nullability) => match width {
                FloatWidth::_32 => DType::Int(32.into(), Signedness::Signed, *nullability),
                FloatWidth::_64 => DType::Int(64.into(), Signedness::Signed, *nullability),
                _ => return Err(VortexError::InvalidDType(ctx.schema().clone())),
            },
            _ => return Err(VortexError::InvalidDType(ctx.schema().clone())),
        };
        let encoded = ctx.with_schema(&encoded_dtype).read()?;
        Ok(ALPArray::new(
            encoded,
            Exponents {
                e: exponents[0],
                f: exponents[1],
            },
            patches,
        )
        .into_array())
    }
}

#[cfg(test)]
mod test {
    use vortex::array::downcast::DowncastArrayBuiltin;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::array::{Array, ArrayRef};
    use vortex::error::VortexResult;
    use vortex::serde::{ReadCtx, WriteCtx};

    use crate::compress::alp_encode;
    use crate::downcast::DowncastALP;

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
        let arr = alp_encode(&PrimitiveArray::from(vec![
            0.00001f64,
            0.0004f64,
            1000000.0f64,
            0.33f64,
        ]))
        .unwrap();
        let read_arr = roundtrip_array(&arr).unwrap();

        let read_alp = read_arr.as_alp();
        assert_eq!(
            arr.encoded().as_primitive().buffer().typed_data::<i8>(),
            read_alp
                .encoded()
                .as_primitive()
                .buffer()
                .typed_data::<i8>()
        );

        assert_eq!(arr.exponents().e, read_alp.exponents().e);
        assert_eq!(arr.exponents().f, read_alp.exponents().f);
    }
}
