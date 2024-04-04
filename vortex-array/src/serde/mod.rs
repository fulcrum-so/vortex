use std::io;
use std::io::{Cursor, ErrorKind, Read, Write};

use arrow_buffer::buffer::{Buffer, MutableBuffer};
use flatbuffers::root;
use itertools::Itertools;
use vortex_error::{vortex_err, VortexResult};
use vortex_schema::DTypeSerdeContext;
use vortex_schema::{DType, IntWidth, Nullability, Signedness};

use crate::array::composite::COMPOSITE_EXTENSIONS;
use crate::array::validity::Validity;
use crate::array::{Array, ArrayRef};
use crate::encoding::{find_encoding, EncodingId, ENCODINGS};
use crate::ptype::PType;
use crate::scalar::{Scalar, ScalarReader, ScalarWriter};
use crate::serde::ptype::PTypeTag;

pub mod context;
pub mod data;
mod ptype;
pub mod view;

pub use view::*;
use vortex_flatbuffers::{FlatBufferToBytes, ReadFlatBuffer};

pub trait ArraySerde {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()>;

    fn metadata(&self) -> VortexResult<Option<Vec<u8>>>;
}

pub trait EncodingSerde {
    fn validate(&self, _view: &ArrayView) -> VortexResult<()> {
        Ok(())
        // todo!("Validate not implemented for {}", _view.encoding().id());
    }
    //
    // fn to_array(&self, view: &ArrayView) -> ArrayRef {
    //     BoolArray::new(
    //         BooleanBuffer::new(view.buffers().first().unwrap().clone(), 0, view.len()),
    //         view.child(0, &Validity::DTYPE)
    //             .map(|c| Validity::Array(c.into_array())),
    //     )
    //     .into_array()
    // }

    fn with_array_view<'view>(
        &self,
        view: &'view ArrayView<'view>,
        _f: &mut dyn FnMut(&dyn Array) -> VortexResult<()>,
    ) -> VortexResult<()> {
        Err(vortex_err!(
            "with_array_view not implemented for {}",
            view.encoding().id()
        ))
    }

    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef>;
}

pub trait WithArray {
    fn with_array<'view, R, F: Fn(&dyn Array) -> VortexResult<R>>(
        &self,
        view: &'view ArrayView<'view>,
        f: F,
    ) -> VortexResult<R>;
}

impl WithArray for dyn EncodingSerde + '_ {
    fn with_array<'view, R, F: Fn(&dyn Array) -> VortexResult<R>>(
        &self,
        view: &'view ArrayView<'view>,
        f: F,
    ) -> VortexResult<R> {
        let mut result: Option<R> = None;
        self.with_array_view(view, &mut |compute| {
            result = Some(f(compute)?);
            Ok(())
        })?;
        Ok(result.unwrap())
    }
}

pub trait BytesSerde
where
    Self: Sized,
{
    fn serialize(&self) -> Vec<u8>;

    fn deserialize(data: &[u8]) -> VortexResult<Self>;
}

impl BytesSerde for usize {
    fn serialize(&self) -> Vec<u8> {
        let mut vec = Vec::new();
        // IOError only happens on EOF.
        leb128::write::unsigned(&mut vec, *self as u64).unwrap();
        vec
    }

    fn deserialize(data: &[u8]) -> VortexResult<Self> {
        let mut cursor = Cursor::new(data);
        leb128::read::unsigned(&mut cursor)
            .map(|v| v as usize)
            .map_err(|e| vortex_err!(InvalidSerde: "Failed to parse leb128 {}", e))
    }
}

pub struct ReadCtx<'a> {
    schema: &'a DType,
    encodings: Vec<EncodingId>,
    r: &'a mut dyn Read,
}

pub trait Serde: Sized {
    fn read(ctx: &mut ReadCtx) -> VortexResult<Self>;
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()>;
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
    pub fn dtype(&mut self) -> VortexResult<DType> {
        let dtype_bytes = self.read_slice()?;
        let ctx = DTypeSerdeContext::new(COMPOSITE_EXTENSIONS.iter().map(|e| e.id()).collect_vec());
        DType::read_flatbuffer(
            &ctx,
            &(root::<vortex_schema::flatbuffers::DType>(&dtype_bytes)?),
        )
    }

    pub fn ptype(&mut self) -> VortexResult<PType> {
        let typetag = PTypeTag::try_from(self.read_nbytes::<1>()?[0])
            .map_err(|e| io::Error::new(ErrorKind::InvalidInput, e))?;
        Ok(typetag.into())
    }

    pub fn nullability(&mut self) -> VortexResult<Nullability> {
        match self.read_nbytes::<1>()? {
            [0] => Ok(Nullability::NonNullable),
            [1] => Ok(Nullability::Nullable),
            _ => Err(vortex_err!("Invalid nullability tag")),
        }
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
            .map_err(|_| vortex_err!("Failed to parse leb128 usize"))
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

    pub fn read_validity(&mut self) -> VortexResult<Option<Validity>> {
        if self.read_option_tag()? {
            match self.read_nbytes::<1>()? {
                [0u8] => Ok(Some(Validity::Valid(self.read_usize()?))),
                [1u8] => Ok(Some(Validity::Invalid(self.read_usize()?))),
                [2u8] => Ok(Some(Validity::array(
                    self.with_schema(&Validity::DTYPE).read()?,
                ))),
                _ => panic!("Invalid validity tag"),
            }
        } else {
            Ok(None)
        }
    }

    pub fn read(&mut self) -> VortexResult<ArrayRef> {
        let encoding_id = self.read_usize()?;
        if let Some(serde) =
            find_encoding(self.encodings[encoding_id].name()).and_then(|e| e.serde())
        {
            serde.read(self)
        } else {
            Err(vortex_err!("Failed to recognize encoding ID",))
        }
    }
}

pub struct WriteCtx<'a> {
    w: &'a mut dyn Write,
    available_encodings: Vec<EncodingId>,
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
        let (bytes, offset) = dtype.flatbuffer_to_bytes();
        self.write_slice(&bytes[offset..])
    }

    pub fn ptype(&mut self, ptype: PType) -> VortexResult<()> {
        self.write_fixed_slice([PTypeTag::from(ptype).into()])
    }

    pub fn nullability(&mut self, nullability: Nullability) -> VortexResult<()> {
        match nullability {
            Nullability::NonNullable => self.write_fixed_slice([0u8]),
            Nullability::Nullable => self.write_fixed_slice([1u8]),
        }
    }

    pub fn scalar(&mut self, scalar: &Scalar) -> VortexResult<()> {
        ScalarWriter::new(self).write(scalar)
    }

    pub fn write_usize(&mut self, u: usize) -> VortexResult<()> {
        leb128::write::unsigned(self.w, u as u64)
            .map_err(|_| vortex_err!("Failed to write leb128 usize"))
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

    pub fn write_optional_array(&mut self, array: Option<&ArrayRef>) -> VortexResult<()> {
        self.write_option_tag(array.is_some())?;
        if let Some(array) = array {
            self.write(array)
        } else {
            Ok(())
        }
    }

    pub fn write_validity(&mut self, validity: Option<Validity>) -> VortexResult<()> {
        match validity {
            None => self.write_option_tag(false),
            Some(v) => {
                self.write_option_tag(true)?;
                match v {
                    Validity::Valid(len) => {
                        self.write_fixed_slice([0u8])?;
                        self.write_usize(len)
                    }
                    Validity::Invalid(len) => {
                        self.write_fixed_slice([1u8])?;
                        self.write_usize(len)
                    }
                    Validity::Array(a) => {
                        self.write_fixed_slice([2u8])?;
                        self.write(&a)
                    }
                }
            }
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
            Err(vortex_err!(
                "Serialization not supported for {}",
                array.encoding().id()
            ))
        })
    }
}

#[cfg(test)]
pub mod test {
    use vortex_error::VortexResult;

    use crate::array::{Array, ArrayRef};
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
