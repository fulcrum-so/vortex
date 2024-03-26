#[cfg(test)]
pub mod test {
    use vortex::array::{Array, ArrayRef};
    use vortex::serde::{ReadCtx, WriteCtx};
    use vortex_error::VortexResult;

    pub fn roundtrip_array(array: &dyn Array) -> VortexResult<ArrayRef> {
        let mut buf = Vec::<u8>::new();
        let mut write_ctx = WriteCtx::new(&mut buf);
        write_ctx.write(array)?;
        let mut read = buf.as_slice();
        let mut read_ctx = ReadCtx::new(array.dtype(), &mut read);
        read_ctx.read()
    }
}
