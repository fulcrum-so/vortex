#![cfg(feature = "serde")]

use flatbuffers::{root, FlatBufferBuilder, WIPOffset};
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use vortex_dtype::match_each_native_ptype;
use vortex_dtype::Nullability;
use vortex_error::{vortex_bail, vortex_err, VortexError};
use vortex_flatbuffers::{FlatBufferRoot, FlatBufferToBytes, ReadFlatBuffer, WriteFlatBuffer};

use crate::flatbuffers::scalar as fb;
use crate::{PScalar, PrimitiveScalar, Scalar, Utf8Scalar};

impl FlatBufferRoot for Scalar {}

impl WriteFlatBuffer for Scalar {
    type Target<'a> = fb::Scalar<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let union = match self {
            Scalar::Binary(b) => {
                let bytes = b.value().map(|bytes| fbb.create_vector(bytes));
                fb::ScalarArgs {
                    type_type: fb::Type::Binary,
                    type_: Some(
                        fb::Binary::create(fbb, &fb::BinaryArgs { value: bytes }).as_union_value(),
                    ),
                    nullability: self.nullability().into(),
                }
            }
            Scalar::Bool(b) => fb::ScalarArgs {
                type_type: fb::Type::Bool,
                // TODO(ngates): I think this optional is in the wrong place and should be inside BoolArgs.
                //  However I think Rust Flatbuffers has incorrectly generated non-optional BoolArgs.
                type_: b
                    .value()
                    .map(|&value| fb::Bool::create(fbb, &fb::BoolArgs { value }).as_union_value()),
                nullability: self.nullability().into(),
            },
            Scalar::List(_) => todo!("List not supported in scalar serde"),
            Scalar::Null(_) => fb::ScalarArgs {
                type_type: fb::Type::Null,
                type_: Some(fb::Null::create(fbb, &fb::NullArgs {}).as_union_value()),
                nullability: self.nullability().into(),
            },
            Scalar::Primitive(p) => {
                let bytes = p.value().map(|pscalar| match pscalar {
                    PScalar::U8(v) => fbb.create_vector(&v.to_le_bytes()),
                    PScalar::U16(v) => fbb.create_vector(&v.to_le_bytes()),
                    PScalar::U32(v) => fbb.create_vector(&v.to_le_bytes()),
                    PScalar::U64(v) => fbb.create_vector(&v.to_le_bytes()),
                    PScalar::I8(v) => fbb.create_vector(&v.to_le_bytes()),
                    PScalar::I16(v) => fbb.create_vector(&v.to_le_bytes()),
                    PScalar::I32(v) => fbb.create_vector(&v.to_le_bytes()),
                    PScalar::I64(v) => fbb.create_vector(&v.to_le_bytes()),
                    PScalar::F16(v) => fbb.create_vector(&v.to_le_bytes()),
                    PScalar::F32(v) => fbb.create_vector(&v.to_le_bytes()),
                    PScalar::F64(v) => fbb.create_vector(&v.to_le_bytes()),
                });
                let primitive = fb::Primitive::create(
                    fbb,
                    &fb::PrimitiveArgs {
                        ptype: p.ptype().into(),
                        bytes,
                    },
                );
                fb::ScalarArgs {
                    type_type: fb::Type::Primitive,
                    type_: Some(primitive.as_union_value()),
                    nullability: self.nullability().into(),
                }
            }
            Scalar::Struct(_) => todo!(),
            Scalar::Utf8(utf) => {
                let value = utf.value().map(|utf| fbb.create_string(utf));
                let value = fb::UTF8::create(fbb, &fb::UTF8Args { value }).as_union_value();
                fb::ScalarArgs {
                    type_type: fb::Type::UTF8,
                    type_: Some(value),
                    nullability: self.nullability().into(),
                }
            }
            Scalar::Extension(ext) => {
                let id = Some(fbb.create_string(ext.id().as_ref()));
                let metadata = ext.metadata().map(|m| fbb.create_vector(m.as_ref()));
                let value = ext.value().map(|s| s.write_flatbuffer(fbb));
                fb::ScalarArgs {
                    type_type: fb::Type::Extension,
                    type_: Some(
                        fb::Extension::create(
                            fbb,
                            &fb::ExtensionArgs {
                                id,
                                metadata,
                                value,
                            },
                        )
                        .as_union_value(),
                    ),
                    nullability: self.nullability().into(),
                }
            }
            Scalar::Composite(_) => panic!(),
        };

        fb::Scalar::create(fbb, &union)
    }
}

impl ReadFlatBuffer for Scalar {
    type Source<'a> = fb::Scalar<'a>;
    type Error = VortexError;

    fn read_flatbuffer(fb: &Self::Source<'_>) -> Result<Self, Self::Error> {
        let nullability = Nullability::from(fb.nullability());
        match fb.type_type() {
            fb::Type::Binary => {
                todo!()
            }
            fb::Type::Bool => {
                todo!()
            }
            fb::Type::List => {
                todo!()
            }
            fb::Type::Null => {
                todo!()
            }
            fb::Type::Primitive => {
                let primitive = fb
                    .type__as_primitive()
                    .ok_or_else(|| vortex_err!("missing primitive value"))?;
                let ptype = primitive.ptype().try_into()?;
                Ok(match_each_native_ptype!(ptype, |$T| {
                    Scalar::Primitive(PrimitiveScalar::try_new(
                        if let Some(bytes) = primitive.bytes() {
                            Some($T::from_le_bytes(bytes.bytes().try_into()?))
                        } else {
                            None
                        },
                        nullability,
                    )?)
                }))
            }
            fb::Type::Struct_ => {
                todo!()
            }
            fb::Type::UTF8 => Ok(Scalar::Utf8(Utf8Scalar::try_new(
                fb.type__as_utf8()
                    .ok_or_else(|| vortex_err!("missing UTF8 value"))?
                    .value()
                    .map(|s| s.to_string()),
                nullability,
            )?)),
            _ => vortex_bail!(InvalidSerde: "Unrecognized scalar type"),
        }
    }
}

impl Serialize for Scalar {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.with_flatbuffer_bytes(|bytes| serializer.serialize_bytes(bytes))
    }
}

struct ScalarDeserializer;

impl<'de> Visitor<'de> for ScalarDeserializer {
    type Value = Scalar;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a vortex dtype")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let fb = root::<fb::Scalar>(v).map_err(E::custom)?;
        Scalar::read_flatbuffer(&fb).map_err(E::custom)
    }
}

// TODO(ngates): Should we just inline composites in scalars?
impl<'de> Deserialize<'de> for Scalar {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(ScalarDeserializer)
    }
}

// impl<'a, 'b> ScalarReader<'a, 'b> {
//     pub fn read(&mut self) -> VortexResult<Scalar> {
//         let bytes = self.reader.read_slice()?;
//         let scalar = root::<scalar::Scalar>(&bytes)
//             .map_err(|_e| VortexError::InvalidArgument("Invalid FlatBuffer".into()))
//             .unwrap();

//     }
//
//     fn read_primitive_scalar(&mut self) -> VortexResult<PrimitiveScalar> {
//         let ptype = self.reader.ptype()?;
//         let is_present = self.reader.read_option_tag()?;
//         if is_present {
//             let pscalar = match ptype {
//                 PType::U8 => PrimitiveScalar::some(PScalar::U8(u8::from_le_bytes(
//                     self.reader.read_nbytes()?,
//                 ))),
//                 PType::U16 => PrimitiveScalar::some(PScalar::U16(u16::from_le_bytes(
//                     self.reader.read_nbytes()?,
//                 ))),
//                 PType::U32 => PrimitiveScalar::some(PScalar::U32(u32::from_le_bytes(
//                     self.reader.read_nbytes()?,
//                 ))),
//                 PType::U64 => PrimitiveScalar::some(PScalar::U64(u64::from_le_bytes(
//                     self.reader.read_nbytes()?,
//                 ))),
//                 PType::I8 => PrimitiveScalar::some(PScalar::I8(i8::from_le_bytes(
//                     self.reader.read_nbytes()?,
//                 ))),
//                 PType::I16 => PrimitiveScalar::some(PScalar::I16(i16::from_le_bytes(
//                     self.reader.read_nbytes()?,
//                 ))),
//                 PType::I32 => PrimitiveScalar::some(PScalar::I32(i32::from_le_bytes(
//                     self.reader.read_nbytes()?,
//                 ))),
//                 PType::I64 => PrimitiveScalar::some(PScalar::I64(i64::from_le_bytes(
//                     self.reader.read_nbytes()?,
//                 ))),
//                 PType::F16 => PrimitiveScalar::some(PScalar::F16(f16::from_le_bytes(
//                     self.reader.read_nbytes()?,
//                 ))),
//                 PType::F32 => PrimitiveScalar::some(PScalar::F32(f32::from_le_bytes(
//                     self.reader.read_nbytes()?,
//                 ))),
//                 PType::F64 => PrimitiveScalar::some(PScalar::F64(f64::from_le_bytes(
//                     self.reader.read_nbytes()?,
//                 ))),
//             };
//             Ok(pscalar)
//         } else {
//             Ok(PrimitiveScalar::none(ptype))
//         }
//     }
// }
