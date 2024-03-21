use std::io;
use std::io::{ErrorKind, Read, Write};

use arrow_buffer::buffer::{Buffer, MutableBuffer};

use vortex_schema::{
    DType, DTypeReader, DTypeWriter, IntWidth, Nullability, SchemaError, Signedness,
};

use crate::array::composite::find_extension_id;
use crate::array::{Array, ArrayRef, EncodingId, ENCODINGS};
use crate::error::{VortexError, VortexResult};
use crate::ptype::PType;
use crate::scalar::{Scalar, ScalarReader, ScalarWriter};
use crate::serde::ptype::PTypeTag;

mod ptype;

pub trait ArraySerde {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()>;
}

pub trait EncodingSerde {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef>;
}

pub trait BytesSerde
where
    Self: Sized,
{
    fn serialize(&self) -> Vec<u8>;

    fn deserialize(data: &[u8]) -> VortexResult<Self>;
}

pub struct ReadCtx<'a> {
    schema: &'a DType,
    encodings: Vec<&'static EncodingId>,
    r: &'a mut dyn Read,
}

impl<'a> ReadCtx<'a> {
    pub fn new(schema: &'a DType, r: &'a mut dyn Read) -> Self {
        let encodings = ENCODINGS.iter().map(|e| e.id()).collect::<Vec<_>>();
        Self {
            schema,
            encodings,
            r,
        }
    }

    #[inline]
    pub fn schema(&self) -> &DType {
        self.schema
    }

    pub fn subfield(&mut self, idx: usize) -> ReadCtx {
        let DType::Struct(_, fs) = self.schema else {
            panic!("Schema was not a struct")
        };
        self.with_schema(&fs[idx])
    }

    #[inline]
    pub fn with_schema<'b>(&'b mut self, schema: &'b DType) -> ReadCtx {
        ReadCtx::new(schema, self.r)
    }

    #[inline]
    pub fn bytes(&mut self) -> ReadCtx {
        self.with_schema(&DType::Int(
            IntWidth::_8,
            Signedness::Unsigned,
            Nullability::NonNullable,
        ))
    }

    #[inline]
    pub fn validity(&mut self) -> ReadCtx {
        self.with_schema(&DType::Bool(Nullability::NonNullable))
    }

    #[inline]
    pub fn dtype(&mut self) -> VortexResult<DType> {
        DTypeReader::new(self.r)
            .read(find_extension_id)
            .map_err(|e| match e {
                SchemaError::InvalidArgument(s) => VortexError::InvalidArgument(s),
                SchemaError::IOError(io_err) => io_err.0.into(),
            })
    }

    pub fn ptype(&mut self) -> VortexResult<PType> {
        let typetag = PTypeTag::try_from(self.read_nbytes::<1>()?[0])
            .map_err(|e| io::Error::new(ErrorKind::InvalidInput, e))?;
        Ok(typetag.into())
    }

    #[inline]
    pub fn scalar(&mut self) -> VortexResult<Scalar> {
        ScalarReader::new(self).read()
    }

    pub fn read_optional_slice(&mut self) -> VortexResult<Option<Vec<u8>>> {
        let is_present = self.read_option_tag()?;
        is_present.then(|| self.read_slice()).transpose()
    }

    pub fn read_slice(&mut self) -> VortexResult<Vec<u8>> {
        let len = self.read_usize()?;
        let mut data = Vec::<u8>::with_capacity(len);
        self.r.take(len as u64).read_to_end(&mut data)?;
        Ok(data)
    }

    pub fn read_buffer<F: Fn(usize) -> usize>(
        &mut self,
        byte_len: F,
    ) -> VortexResult<(usize, Buffer)> {
        let logical_len = self.read_usize()?;
        let buffer_len = byte_len(logical_len);
        let mut buffer = MutableBuffer::from_len_zeroed(buffer_len);
        self.r.read_exact(&mut buffer)?;
        Ok((logical_len, buffer.into()))
    }

    pub fn read_nbytes<const N: usize>(&mut self) -> VortexResult<[u8; N]> {
        let mut bytes: [u8; N] = [0; N];
        self.r.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    pub fn read_usize(&mut self) -> VortexResult<usize> {
        leb128::read::unsigned(self.r)
            .map_err(|_| VortexError::InvalidArgument("Failed to parse leb128 usize".into()))
            .map(|u| u as usize)
    }

    pub fn read_option_tag(&mut self) -> VortexResult<bool> {
        let mut tag = [0; 1];
        self.r.read_exact(&mut tag)?;
        Ok(tag[0] == 0x01)
    }

    pub fn read_optional_array(&mut self) -> VortexResult<Option<ArrayRef>> {
        if self.read_option_tag()? {
            self.read().map(Some)
        } else {
            Ok(None)
        }
    }

    pub fn read(&mut self) -> VortexResult<ArrayRef> {
        let encoding_id = self.read_usize()?;
        if let Some(serde) = ENCODINGS
            .iter()
            .filter(|e| e.id().name() == self.encodings[encoding_id].name())
            .flat_map(|e| e.serde())
            .next()
        {
            serde.read(self)
        } else {
            Err(VortexError::InvalidArgument(
                "Failed to recognize encoding ID".into(),
            ))
        }
    }
}

pub struct WriteCtx<'a> {
    w: &'a mut dyn Write,
    available_encodings: Vec<&'static EncodingId>,
}

impl<'a> WriteCtx<'a> {
    pub fn new(w: &'a mut dyn Write) -> Self {
        let available_encodings = ENCODINGS.iter().map(|e| e.id()).collect::<Vec<_>>();
        Self {
            w,
            available_encodings,
        }
    }

    pub fn dtype(&mut self, dtype: &DType) -> VortexResult<()> {
        DTypeWriter::new(self.w).write(dtype).map_err(|e| match e {
            SchemaError::InvalidArgument(s) => VortexError::InvalidArgument(s),
            SchemaError::IOError(io_err) => io_err.0.into(),
        })
    }

    pub fn ptype(&mut self, ptype: PType) -> VortexResult<()> {
        self.write_fixed_slice([PTypeTag::from(ptype).into()])
    }

    pub fn scalar(&mut self, scalar: &Scalar) -> VortexResult<()> {
        ScalarWriter::new(self).write(scalar)
    }

    pub fn write_usize(&mut self, u: usize) -> VortexResult<()> {
        leb128::write::unsigned(self.w, u as u64)
            .map_err(|_| VortexError::InvalidArgument("Failed to write leb128 usize".into()))
            .map(|_| ())
    }

    pub fn write_fixed_slice<const N: usize>(&mut self, slice: [u8; N]) -> VortexResult<()> {
        self.w.write_all(&slice).map_err(|e| e.into())
    }

    pub fn write_slice(&mut self, slice: &[u8]) -> VortexResult<()> {
        self.write_usize(slice.len())?;
        self.w.write_all(slice).map_err(|e| e.into())
    }

    pub fn write_optional_slice(&mut self, slice: Option<&[u8]>) -> VortexResult<()> {
        self.write_option_tag(slice.is_some())?;
        if let Some(s) = slice {
            self.write_slice(s)
        } else {
            Ok(())
        }
    }

    pub fn write_buffer(&mut self, logical_len: usize, buf: &Buffer) -> VortexResult<()> {
        self.write_usize(logical_len)?;
        self.w.write_all(buf.as_slice()).map_err(|e| e.into())
    }

    pub fn write_option_tag(&mut self, present: bool) -> VortexResult<()> {
        self.w
            .write_all(&[if present { 0x01 } else { 0x00 }])
            .map_err(|e| e.into())
    }

    pub fn write_optional_array(&mut self, array: Option<&dyn Array>) -> VortexResult<()> {
        self.write_option_tag(array.is_some())?;
        if let Some(array) = array {
            self.write(array)
        } else {
            Ok(())
        }
    }

    pub fn write(&mut self, array: &dyn Array) -> VortexResult<()> {
        let encoding_id = self
            .available_encodings
            .iter()
            .position(|e| e.name() == array.encoding().id().name())
            .ok_or(io::Error::new(ErrorKind::InvalidInput, "unknown encoding"))?;
        self.write_usize(encoding_id)?;
        array.serde().map(|s| s.write(self)).unwrap_or_else(|| {
            Err(VortexError::InvalidArgument(
                format!("Serialization not supported for {}", array.encoding().id()).into(),
            ))
        })
    }
}

#[cfg(test)]
pub mod test {
    use crate::array::{Array, ArrayRef};
    use crate::error::VortexResult;
    use crate::serde::{ReadCtx, WriteCtx};

    pub fn roundtrip_array(array: &dyn Array) -> VortexResult<ArrayRef> {
        let mut buf = Vec::<u8>::new();
        let mut write_ctx = WriteCtx::new(&mut buf);
        write_ctx.write(array)?;
        let mut read = buf.as_slice();
        let mut read_ctx = ReadCtx::new(array.dtype(), &mut read);
        read_ctx.read()
    }
}
