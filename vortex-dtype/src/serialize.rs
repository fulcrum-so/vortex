use flatbuffers::{FlatBufferBuilder, WIPOffset};
use itertools::Itertools;
use vortex_error::{vortex_bail, VortexError};
use vortex_flatbuffers::{FlatBufferRoot, WriteFlatBuffer};

use crate::{flatbuffers as fb, PType};
use crate::{DType, Nullability};

impl FlatBufferRoot for DType {}
impl WriteFlatBuffer for DType {
    type Target<'a> = fb::DType<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let dtype_union = match self {
            DType::Null => fb::Null::create(fbb, &fb::NullArgs {}).as_union_value(),
            DType::Bool(n) => fb::Bool::create(
                fbb,
                &fb::BoolArgs {
                    nullability: n.into(),
                },
            )
            .as_union_value(),
            DType::Primitive(ptype, n) => fb::Primitive::create(
                fbb,
                &fb::PrimitiveArgs {
                    ptype: (*ptype).into(),
                    nullability: n.into(),
                },
            )
            .as_union_value(),
            DType::Decimal(p, s, n) => fb::Decimal::create(
                fbb,
                &fb::DecimalArgs {
                    precision: *p,
                    scale: *s,
                    nullability: n.into(),
                },
            )
            .as_union_value(),
            DType::Utf8(n) => fb::Utf8::create(
                fbb,
                &fb::Utf8Args {
                    nullability: n.into(),
                },
            )
            .as_union_value(),
            DType::Binary(n) => fb::Binary::create(
                fbb,
                &fb::BinaryArgs {
                    nullability: n.into(),
                },
            )
            .as_union_value(),
            DType::Struct(names, dtypes) => {
                let names = names
                    .iter()
                    .map(|n| fbb.create_string(n.as_str()))
                    .collect_vec();
                let names = Some(fbb.create_vector(&names));

                let dtypes = dtypes
                    .iter()
                    .map(|dtype| dtype.write_flatbuffer(fbb))
                    .collect_vec();
                let fields = Some(fbb.create_vector(&dtypes));

                fb::Struct_::create(fbb, &fb::Struct_Args { names, fields }).as_union_value()
            }
            DType::List(e, n) => {
                let element_type = Some(e.as_ref().write_flatbuffer(fbb));
                fb::List::create(
                    fbb,
                    &fb::ListArgs {
                        element_type,
                        nullability: n.into(),
                    },
                )
                .as_union_value()
            }
            DType::Extension(ext, n) => {
                let id = Some(fbb.create_string(ext.id().as_ref()));
                let metadata = ext.metadata().map(|m| fbb.create_vector(m.as_ref()));
                fb::Extension::create(
                    fbb,
                    &fb::ExtensionArgs {
                        id,
                        metadata,
                        nullability: n.into(),
                    },
                )
                .as_union_value()
            }
            DType::Composite(..) => todo!(),
        };

        let dtype_type = match self {
            DType::Null => fb::Type::Null,
            DType::Bool(_) => fb::Type::Bool,
            DType::Primitive(..) => fb::Type::Primitive,
            DType::Decimal(..) => fb::Type::Decimal,
            DType::Utf8(_) => fb::Type::Utf8,
            DType::Binary(_) => fb::Type::Binary,
            DType::Struct(..) => fb::Type::Struct_,
            DType::List(..) => fb::Type::List,
            DType::Extension { .. } => fb::Type::Extension,
            DType::Composite(..) => unreachable!(),
        };

        fb::DType::create(
            fbb,
            &fb::DTypeArgs {
                type_type: dtype_type,
                type_: Some(dtype_union),
            },
        )
    }
}

impl From<Nullability> for fb::Nullability {
    fn from(value: Nullability) -> Self {
        match value {
            Nullability::NonNullable => fb::Nullability::NonNullable,
            Nullability::Nullable => fb::Nullability::Nullable,
        }
    }
}

impl From<&Nullability> for fb::Nullability {
    fn from(value: &Nullability) -> Self {
        match value {
            Nullability::NonNullable => fb::Nullability::NonNullable,
            Nullability::Nullable => fb::Nullability::Nullable,
        }
    }
}

impl From<PType> for fb::PType {
    fn from(value: PType) -> Self {
        match value {
            PType::U8 => fb::PType::U8,
            PType::U16 => fb::PType::U16,
            PType::U32 => fb::PType::U32,
            PType::U64 => fb::PType::U64,
            PType::I8 => fb::PType::I8,
            PType::I16 => fb::PType::I16,
            PType::I32 => fb::PType::I32,
            PType::I64 => fb::PType::I64,
            PType::F16 => fb::PType::F16,
            PType::F32 => fb::PType::F32,
            PType::F64 => fb::PType::F64,
        }
    }
}

impl TryFrom<fb::PType> for PType {
    type Error = VortexError;

    fn try_from(value: fb::PType) -> Result<Self, Self::Error> {
        Ok(match value {
            fb::PType::U8 => PType::U8,
            fb::PType::U16 => PType::U16,
            fb::PType::U32 => PType::U32,
            fb::PType::U64 => PType::U64,
            fb::PType::I8 => PType::I8,
            fb::PType::I16 => PType::I16,
            fb::PType::I32 => PType::I32,
            fb::PType::I64 => PType::I64,
            fb::PType::F16 => PType::F16,
            fb::PType::F32 => PType::F32,
            fb::PType::F64 => PType::F64,
            _ => vortex_bail!(InvalidSerde: "Unknown PType variant"),
        })
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use flatbuffers::root;
    use vortex_flatbuffers::{FlatBufferToBytes, ReadFlatBuffer};

    use crate::{flatbuffers as fb, PType};
    use crate::{DType, Nullability};

    fn roundtrip_dtype(dtype: DType) {
        let bytes = dtype.with_flatbuffer_bytes(|bytes| bytes.to_vec());
        let deserialized = DType::read_flatbuffer(&root::<fb::DType>(&bytes).unwrap()).unwrap();
        assert_eq!(dtype, deserialized);
    }

    #[test]
    fn roundtrip() {
        roundtrip_dtype(DType::Null);
        roundtrip_dtype(DType::Bool(Nullability::NonNullable));
        roundtrip_dtype(DType::Primitive(PType::U64, Nullability::NonNullable));
        roundtrip_dtype(DType::Decimal(18, 9, Nullability::NonNullable));
        roundtrip_dtype(DType::Binary(Nullability::NonNullable));
        roundtrip_dtype(DType::Utf8(Nullability::NonNullable));
        roundtrip_dtype(DType::List(
            Box::new(DType::Primitive(PType::F32, Nullability::Nullable)),
            Nullability::NonNullable,
        ));
        roundtrip_dtype(DType::Struct(
            vec![Arc::new("strings".into()), Arc::new("ints".into())],
            vec![
                DType::Utf8(Nullability::NonNullable),
                DType::Primitive(PType::U16, Nullability::Nullable),
            ],
        ))
    }
}
