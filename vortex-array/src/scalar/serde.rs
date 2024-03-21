use std::io;
use std::sync::Arc;

use half::f16;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use vortex_schema::DType;

use crate::error::VortexResult;
use crate::ptype::PType;
use crate::scalar::composite::CompositeScalar;
use crate::scalar::{
    BinaryScalar, BoolScalar, ListScalar, NullScalar, PScalar, PrimitiveScalar, Scalar,
    StructScalar, Utf8Scalar,
};
use crate::serde::{ReadCtx, WriteCtx};

pub struct ScalarReader<'a, 'b> {
    reader: &'b mut ReadCtx<'a>,
}

impl<'a, 'b> ScalarReader<'a, 'b> {
    pub fn new(reader: &'b mut ReadCtx<'a>) -> Self {
        Self { reader }
    }

    pub fn read(&mut self) -> VortexResult<Scalar> {
        let tag = ScalarTag::try_from(self.reader.read_nbytes::<1>()?[0])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        match tag {
            ScalarTag::Binary => {
                let slice = self.reader.read_optional_slice()?;
                Ok(BinaryScalar::new(slice).into())
            }
            ScalarTag::Bool => {
                let is_present = self.reader.read_option_tag()?;
                if is_present {
                    Ok(BoolScalar::some(self.reader.read_nbytes::<1>()?[0] != 0).into())
                } else {
                    Ok(BoolScalar::none().into())
                }
            }
            ScalarTag::PrimitiveS => self.read_primitive_scalar().map(|p| p.into()),
            ScalarTag::List => {
                let is_present = self.reader.read_option_tag()?;
                if is_present {
                    let elems = self.reader.read_usize()?;
                    let mut values = Vec::with_capacity(elems);
                    for _ in 0..elems {
                        values.push(self.read()?);
                    }
                    Ok(ListScalar::new(values[0].dtype().clone(), Some(values)).into())
                } else {
                    Ok(ListScalar::new(self.reader.dtype()?, None).into())
                }
            }
            ScalarTag::Null => Ok(NullScalar::new().into()),
            ScalarTag::Struct => {
                let field_num = self.reader.read_usize()?;
                let mut names = Vec::with_capacity(field_num);
                for _ in 0..field_num {
                    names.push(Arc::new(
                        self.reader
                            .read_slice()
                            .map(|v| unsafe { String::from_utf8_unchecked(v) })?,
                    ));
                }
                let mut values = Vec::with_capacity(field_num);
                for _ in 0..field_num {
                    values.push(self.read()?);
                }
                let dtypes = values.iter().map(|s| s.dtype().clone()).collect::<Vec<_>>();
                Ok(StructScalar::new(DType::Struct(names, dtypes), values).into())
            }
            ScalarTag::Utf8 => {
                let value = self.reader.read_optional_slice()?;
                Ok(
                    Utf8Scalar::new(value.map(|v| unsafe { String::from_utf8_unchecked(v) }))
                        .into(),
                )
            }
            ScalarTag::Composite => {
                let dtype = self.reader.dtype()?;
                let scalar = self.read()?;
                Ok(CompositeScalar::new(dtype, Box::new(scalar)).into())
            }
        }
    }

    fn read_primitive_scalar(&mut self) -> VortexResult<PrimitiveScalar> {
        let ptype = self.reader.ptype()?;
        let is_present = self.reader.read_option_tag()?;
        if is_present {
            let pscalar = match ptype {
                PType::U8 => PrimitiveScalar::some(PScalar::U8(u8::from_le_bytes(
                    self.reader.read_nbytes()?,
                ))),
                PType::U16 => PrimitiveScalar::some(PScalar::U16(u16::from_le_bytes(
                    self.reader.read_nbytes()?,
                ))),
                PType::U32 => PrimitiveScalar::some(PScalar::U32(u32::from_le_bytes(
                    self.reader.read_nbytes()?,
                ))),
                PType::U64 => PrimitiveScalar::some(PScalar::U64(u64::from_le_bytes(
                    self.reader.read_nbytes()?,
                ))),
                PType::I8 => PrimitiveScalar::some(PScalar::I8(i8::from_le_bytes(
                    self.reader.read_nbytes()?,
                ))),
                PType::I16 => PrimitiveScalar::some(PScalar::I16(i16::from_le_bytes(
                    self.reader.read_nbytes()?,
                ))),
                PType::I32 => PrimitiveScalar::some(PScalar::I32(i32::from_le_bytes(
                    self.reader.read_nbytes()?,
                ))),
                PType::I64 => PrimitiveScalar::some(PScalar::I64(i64::from_le_bytes(
                    self.reader.read_nbytes()?,
                ))),
                PType::F16 => PrimitiveScalar::some(PScalar::F16(f16::from_le_bytes(
                    self.reader.read_nbytes()?,
                ))),
                PType::F32 => PrimitiveScalar::some(PScalar::F32(f32::from_le_bytes(
                    self.reader.read_nbytes()?,
                ))),
                PType::F64 => PrimitiveScalar::some(PScalar::F64(f64::from_le_bytes(
                    self.reader.read_nbytes()?,
                ))),
            };
            Ok(pscalar)
        } else {
            Ok(PrimitiveScalar::none(ptype))
        }
    }
}

pub struct ScalarWriter<'a, 'b> {
    writer: &'b mut WriteCtx<'a>,
}

impl<'a, 'b> ScalarWriter<'a, 'b> {
    pub fn new(writer: &'b mut WriteCtx<'a>) -> Self {
        Self { writer }
    }

    pub fn write(&mut self, scalar: &Scalar) -> VortexResult<()> {
        self.writer
            .write_fixed_slice([ScalarTag::from(scalar).into()])?;
        match scalar {
            Scalar::Binary(b) => self.writer.write_optional_slice(b.value()),
            Scalar::Bool(b) => {
                self.writer.write_option_tag(b.value().is_some())?;
                if let Some(v) = b.value() {
                    self.writer.write_fixed_slice([v as u8])?;
                }
                Ok(())
            }
            Scalar::List(ls) => {
                self.writer.write_option_tag(ls.values().is_some())?;
                if let Some(vs) = ls.values() {
                    self.writer.write_usize(vs.len())?;
                    for elem in vs {
                        self.write(elem)?;
                    }
                } else {
                    self.writer.dtype(ls.dtype())?;
                }
                Ok(())
            }
            Scalar::Null(_) => Ok(()),
            Scalar::Primitive(p) => self.write_primitive_scalar(p),
            Scalar::Struct(s) => {
                let names = s.names();
                self.writer.write_usize(names.len())?;
                for n in names {
                    self.writer.write_slice(n.as_bytes())?;
                }
                for field in s.values() {
                    self.write(field)?;
                }
                Ok(())
            }
            Scalar::Utf8(u) => self
                .writer
                .write_optional_slice(u.value().map(|s| s.as_bytes())),
            Scalar::Composite(c) => {
                self.writer.dtype(c.dtype())?;
                self.write(c.scalar())
            }
        }
    }

    fn write_primitive_scalar(&mut self, scalar: &PrimitiveScalar) -> VortexResult<()> {
        self.writer.ptype(scalar.ptype())?;
        self.writer.write_option_tag(scalar.value().is_some())?;
        if let Some(ps) = scalar.value() {
            match ps {
                PScalar::F16(f) => self.writer.write_fixed_slice(f.to_le_bytes())?,
                PScalar::F32(f) => self.writer.write_fixed_slice(f.to_le_bytes())?,
                PScalar::F64(f) => self.writer.write_fixed_slice(f.to_le_bytes())?,
                PScalar::I16(i) => self.writer.write_fixed_slice(i.to_le_bytes())?,
                PScalar::I32(i) => self.writer.write_fixed_slice(i.to_le_bytes())?,
                PScalar::I64(i) => self.writer.write_fixed_slice(i.to_le_bytes())?,
                PScalar::I8(i) => self.writer.write_fixed_slice(i.to_le_bytes())?,
                PScalar::U16(u) => self.writer.write_fixed_slice(u.to_le_bytes())?,
                PScalar::U32(u) => self.writer.write_fixed_slice(u.to_le_bytes())?,
                PScalar::U64(u) => self.writer.write_fixed_slice(u.to_le_bytes())?,
                PScalar::U8(u) => self.writer.write_fixed_slice(u.to_le_bytes())?,
            }
        }
        Ok(())
    }
}

#[derive(Copy, Clone, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
enum ScalarTag {
    Binary,
    Bool,
    List,
    Null,
    // TODO(robert): rename to primitive once we stop using enum for serialization
    PrimitiveS,
    Struct,
    Utf8,
    Composite,
}

impl From<&Scalar> for ScalarTag {
    fn from(value: &Scalar) -> Self {
        match value {
            Scalar::Binary(_) => ScalarTag::Binary,
            Scalar::Bool(_) => ScalarTag::Bool,
            Scalar::List(_) => ScalarTag::List,
            Scalar::Null(_) => ScalarTag::Null,
            Scalar::Primitive(_) => ScalarTag::PrimitiveS,
            Scalar::Struct(_) => ScalarTag::Struct,
            Scalar::Utf8(_) => ScalarTag::Utf8,
            Scalar::Composite(_) => ScalarTag::Composite,
        }
    }
}
