use std::sync::Arc;

use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::generated::{
    root_as_dtype, Bool, BoolArgs, Composite, CompositeArgs, Int, IntArgs, List, ListArgs, Null,
    NullArgs, Struct_, Struct_Args, Type,
};
use crate::generated::{Binary, BinaryArgs, Signedness as FbSignedness};
use crate::generated::{DType as FbDType, DTypeArgs};
use crate::generated::{Decimal, DecimalArgs, FloatWidth as FbFloatWidth};
use crate::generated::{Float, FloatArgs, IntWidth as FbIntWidth};
use crate::generated::{Nullability as FbNullability, Utf8, Utf8Args};
use crate::{
    CompositeID, DType, FloatWidth, IntWidth, Nullability, SchemaError, SchemaResult, Signedness,
};

pub trait FbSerialize<'a> {
    type OffsetType;

    // Convert self to flatbuffer representation, returns written bytes and index of valid data
    // If you want to serialize multiple objects you should prefer serialize_to_builder to reuse the allocated memory
    fn serialize(&self) -> (Vec<u8>, usize) {
        let mut fbb = FlatBufferBuilder::new();
        let wip_dtype = self.serialize_to_builder(&mut fbb);
        fbb.finish_minimal(wip_dtype);
        fbb.collapse()
    }

    fn serialize_to_builder(&self, fbb: &mut FlatBufferBuilder<'a>) -> WIPOffset<Self::OffsetType>;
}

pub trait FbDeserialize<'a>: Sized {
    type OffsetType;

    fn deserialize(bytes: &[u8], find_id: fn(&str) -> Option<CompositeID>) -> SchemaResult<Self>;

    fn convert_from_fb(
        fb_type: Self::OffsetType,
        find_id: fn(&str) -> Option<CompositeID>,
    ) -> SchemaResult<Self>;
}

impl<'a> FbSerialize<'a> for DType {
    type OffsetType = FbDType<'a>;

    fn serialize_to_builder(&self, fbb: &mut FlatBufferBuilder<'a>) -> WIPOffset<Self::OffsetType> {
        let (dtype_union, dtype_union_variant) = match self {
            DType::Null => (Null::create(fbb, &NullArgs {}).as_union_value(), Type::Null),
            DType::Bool(n) => (
                Bool::create(
                    fbb,
                    &BoolArgs {
                        nullability: n.into(),
                    },
                )
                .as_union_value(),
                Type::Bool,
            ),
            DType::Int(w, s, n) => (
                Int::create(
                    fbb,
                    &IntArgs {
                        width: w.into(),
                        signedness: s.into(),
                        nullability: n.into(),
                    },
                )
                .as_union_value(),
                Type::Int,
            ),
            DType::Decimal(p, s, n) => (
                Decimal::create(
                    fbb,
                    &DecimalArgs {
                        precision: *p,
                        scale: *s,
                        nullability: n.into(),
                    },
                )
                .as_union_value(),
                Type::Decimal,
            ),
            DType::Float(w, n) => (
                Float::create(
                    fbb,
                    &FloatArgs {
                        width: w.into(),
                        nullability: n.into(),
                    },
                )
                .as_union_value(),
                Type::Float,
            ),
            DType::Utf8(n) => (
                Utf8::create(
                    fbb,
                    &Utf8Args {
                        nullability: n.into(),
                    },
                )
                .as_union_value(),
                Type::Utf8,
            ),
            DType::Binary(n) => (
                Binary::create(
                    fbb,
                    &BinaryArgs {
                        nullability: n.into(),
                    },
                )
                .as_union_value(),
                Type::Binary,
            ),
            DType::Struct(ns, fs) => {
                let name_offsets = ns
                    .iter()
                    .map(|n| fbb.create_string(n.as_ref()))
                    .collect::<Vec<_>>();
                fbb.start_vector::<WIPOffset<&str>>(ns.len());
                for name in name_offsets.iter().rev() {
                    fbb.push(name);
                }
                let names_vector = fbb.end_vector(ns.len());

                let dtype_offsets = fs
                    .iter()
                    .map(|f| f.serialize_to_builder(fbb))
                    .collect::<Vec<_>>();
                fbb.start_vector::<WIPOffset<FbDType>>(fs.len());
                for doff in dtype_offsets.iter().rev() {
                    fbb.push(doff);
                }
                let fields_vector = fbb.end_vector(fs.len());

                (
                    Struct_::create(
                        fbb,
                        &Struct_Args {
                            names: Some(names_vector),
                            fields: Some(fields_vector),
                        },
                    )
                    .as_union_value(),
                    Type::Struct_,
                )
            }
            DType::List(e, n) => {
                let fb_dtype = e.as_ref().serialize_to_builder(fbb);
                (
                    List::create(
                        fbb,
                        &ListArgs {
                            element_type: Some(fb_dtype),
                            nullability: n.into(),
                        },
                    )
                    .as_union_value(),
                    Type::List,
                )
            }
            DType::Composite(id, n) => {
                let id = fbb.create_string(id.0);
                (
                    Composite::create(
                        fbb,
                        &CompositeArgs {
                            id: Some(id),
                            nullability: n.into(),
                        },
                    )
                    .as_union_value(),
                    Type::Composite,
                )
            }
        };

        FbDType::create(
            fbb,
            &DTypeArgs {
                type_type: dtype_union_variant,
                type_: Some(dtype_union),
            },
        )
    }
}

impl<'a> FbDeserialize<'a> for DType {
    type OffsetType = FbDType<'a>;

    fn deserialize(bytes: &[u8], find_id: fn(&str) -> Option<CompositeID>) -> SchemaResult<Self> {
        root_as_dtype(bytes)
            .map_err(|e| {
                SchemaError::InvalidArgument(format!("Unable to read bytes as DType: {}", e).into())
            })
            .and_then(|d| Self::convert_from_fb(d, find_id))
    }

    fn convert_from_fb(
        fb_type: Self::OffsetType,
        find_id: fn(&str) -> Option<CompositeID>,
    ) -> SchemaResult<Self> {
        match fb_type.type_type() {
            Type::Null => Ok(DType::Null),
            Type::Bool => Ok(DType::Bool(
                fb_type.type__as_bool().unwrap().nullability().try_into()?,
            )),
            Type::Int => {
                let fb_int = fb_type.type__as_int().unwrap();
                Ok(DType::Int(
                    fb_int.width().try_into()?,
                    fb_int.signedness().try_into()?,
                    fb_int.nullability().try_into()?,
                ))
            }
            Type::Float => {
                let fb_float = fb_type.type__as_float().unwrap();
                Ok(DType::Float(
                    fb_float.width().try_into()?,
                    fb_float.nullability().try_into()?,
                ))
            }
            Type::Decimal => {
                let fb_decimal = fb_type.type__as_decimal().unwrap();
                Ok(DType::Decimal(
                    fb_decimal.precision(),
                    fb_decimal.scale(),
                    fb_decimal.nullability().try_into()?,
                ))
            }
            Type::Binary => Ok(DType::Binary(
                fb_type
                    .type__as_binary()
                    .unwrap()
                    .nullability()
                    .try_into()?,
            )),
            Type::Utf8 => Ok(DType::Utf8(
                fb_type.type__as_utf_8().unwrap().nullability().try_into()?,
            )),
            Type::List => {
                let fb_list = fb_type.type__as_list().unwrap();
                let element_dtype =
                    DType::convert_from_fb(fb_list.element_type().unwrap(), find_id)?;
                Ok(DType::List(
                    Box::new(element_dtype),
                    fb_list.nullability().try_into()?,
                ))
            }
            Type::Struct_ => {
                let fb_struct = fb_type.type__as_struct_().unwrap();
                let names = fb_struct
                    .names()
                    .unwrap()
                    .iter()
                    .map(|n| Arc::new(n.to_string()))
                    .collect::<Vec<_>>();
                let fields: Vec<DType> = fb_struct
                    .fields()
                    .unwrap()
                    .iter()
                    .map(|f| DType::convert_from_fb(f, find_id))
                    .collect::<SchemaResult<Vec<_>>>()?;
                Ok(DType::Struct(names, fields))
            }
            Type::Composite => {
                let fb_composite = fb_type.type__as_composite().unwrap();
                let id = find_id(fb_composite.id().unwrap()).ok_or_else(|| {
                    SchemaError::InvalidArgument("Couldn't find composite id".into())
                })?;
                Ok(DType::Composite(id, fb_composite.nullability().try_into()?))
            }
            _ => Err(SchemaError::InvalidArgument("Unknown DType variant".into())),
        }
    }
}

impl From<&Nullability> for FbNullability {
    fn from(value: &Nullability) -> Self {
        match value {
            Nullability::NonNullable => FbNullability::NonNullable,
            Nullability::Nullable => FbNullability::Nullable,
        }
    }
}

impl TryFrom<FbNullability> for Nullability {
    type Error = SchemaError;

    fn try_from(value: FbNullability) -> SchemaResult<Self> {
        match value {
            FbNullability::NonNullable => Ok(Nullability::NonNullable),
            FbNullability::Nullable => Ok(Nullability::Nullable),
            _ => Err(SchemaError::InvalidArgument(
                "Unknown nullability value".into(),
            )),
        }
    }
}

impl From<&IntWidth> for FbIntWidth {
    fn from(value: &IntWidth) -> Self {
        match value {
            IntWidth::Unknown => FbIntWidth::Unknown,
            IntWidth::_8 => FbIntWidth::_8,
            IntWidth::_16 => FbIntWidth::_16,
            IntWidth::_32 => FbIntWidth::_32,
            IntWidth::_64 => FbIntWidth::_64,
        }
    }
}

impl TryFrom<FbIntWidth> for IntWidth {
    type Error = SchemaError;

    fn try_from(value: FbIntWidth) -> SchemaResult<Self> {
        match value {
            FbIntWidth::Unknown => Ok(IntWidth::Unknown),
            FbIntWidth::_8 => Ok(IntWidth::_8),
            FbIntWidth::_16 => Ok(IntWidth::_16),
            FbIntWidth::_32 => Ok(IntWidth::_32),
            FbIntWidth::_64 => Ok(IntWidth::_64),
            _ => Err(SchemaError::InvalidArgument(
                "Unknown IntWidth value".into(),
            )),
        }
    }
}

impl From<&Signedness> for FbSignedness {
    fn from(value: &Signedness) -> Self {
        match value {
            Signedness::Unknown => FbSignedness::Unknown,
            Signedness::Unsigned => FbSignedness::Unsigned,
            Signedness::Signed => FbSignedness::Signed,
        }
    }
}

impl TryFrom<FbSignedness> for Signedness {
    type Error = SchemaError;

    fn try_from(value: FbSignedness) -> SchemaResult<Self> {
        match value {
            FbSignedness::Unknown => Ok(Signedness::Unknown),
            FbSignedness::Unsigned => Ok(Signedness::Unsigned),
            FbSignedness::Signed => Ok(Signedness::Signed),
            _ => Err(SchemaError::InvalidArgument(
                "Unknown Signedness value".into(),
            )),
        }
    }
}

impl From<&FloatWidth> for FbFloatWidth {
    fn from(value: &FloatWidth) -> Self {
        match value {
            FloatWidth::Unknown => FbFloatWidth::Unknown,
            FloatWidth::_16 => FbFloatWidth::_16,
            FloatWidth::_32 => FbFloatWidth::_32,
            FloatWidth::_64 => FbFloatWidth::_64,
        }
    }
}

impl TryFrom<FbFloatWidth> for FloatWidth {
    type Error = SchemaError;

    fn try_from(value: FbFloatWidth) -> SchemaResult<Self> {
        match value {
            FbFloatWidth::Unknown => Ok(FloatWidth::Unknown),
            FbFloatWidth::_16 => Ok(FloatWidth::_16),
            FbFloatWidth::_32 => Ok(FloatWidth::_32),
            FbFloatWidth::_64 => Ok(FloatWidth::_64),
            _ => Err(SchemaError::InvalidArgument(
                "Unknown IntWidth value".into(),
            )),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::{DType, FbDeserialize, FbSerialize, FloatWidth, IntWidth, Nullability, Signedness};

    fn roundtrip_dtype(dtype: DType) {
        let (bytes, head) = dtype.serialize();
        let deserialized =
            DType::deserialize(&bytes[head..], |_| panic!("no composite ids")).unwrap();
        assert_eq!(dtype, deserialized);
    }

    #[test]
    fn roundtrip() {
        roundtrip_dtype(DType::Null);
        roundtrip_dtype(DType::Bool(Nullability::NonNullable));
        roundtrip_dtype(DType::Int(
            IntWidth::_64,
            Signedness::Unsigned,
            Nullability::NonNullable,
        ));
        roundtrip_dtype(DType::Decimal(18, 9, Nullability::NonNullable));
        roundtrip_dtype(DType::Float(FloatWidth::_64, Nullability::NonNullable));
        roundtrip_dtype(DType::Binary(Nullability::NonNullable));
        roundtrip_dtype(DType::Utf8(Nullability::NonNullable));
        roundtrip_dtype(DType::List(
            Box::new(DType::Float(FloatWidth::_32, Nullability::Nullable)),
            Nullability::NonNullable,
        ));
        roundtrip_dtype(DType::Struct(
            vec![Arc::new("strings".into()), Arc::new("ints".into())],
            vec![
                DType::Utf8(Nullability::NonNullable),
                DType::Int(IntWidth::_16, Signedness::Unsigned, Nullability::Nullable),
            ],
        ))
    }
}
