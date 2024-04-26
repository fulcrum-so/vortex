use std::sync::Arc;

use vortex_error::{vortex_err, VortexError, VortexResult};
use vortex_flatbuffers::ReadFlatBuffer;

use crate::{flatbuffers as fb, Nullability};
use crate::{CompositeID, DType};

#[allow(dead_code)]
pub struct DTypeSerdeContext {
    composite_ids: Arc<[CompositeID]>,
}

impl DTypeSerdeContext {
    pub fn new(composite_ids: Vec<CompositeID>) -> Self {
        Self {
            composite_ids: composite_ids.into(),
        }
    }

    pub fn find_composite_id(&self, id: &str) -> Option<CompositeID> {
        self.composite_ids.iter().find(|c| c.0 == id).copied()
    }
}

impl ReadFlatBuffer<DTypeSerdeContext> for DType {
    type Source<'a> = fb::DType<'a>;
    type Error = VortexError;

    fn read_flatbuffer(
        ctx: &DTypeSerdeContext,
        fb: &Self::Source<'_>,
    ) -> Result<Self, Self::Error> {
        match fb.type_type() {
            fb::Type::Null => Ok(DType::Null),
            fb::Type::Bool => Ok(DType::Bool(
                fb.type__as_bool()
                    .ok_or_else(|| vortex_err!("type__as_bool returned None"))?
                    .nullability()
                    .try_into()?,
            )),
            fb::Type::Primitive => {
                let fb_primitive = fb
                    .type__as_primitive()
                    .ok_or_else(|| vortex_err!("type__as_primitive returned None"))?;
                Ok(DType::Primitive(
                    fb_primitive.ptype().try_into()?,
                    fb_primitive.nullability().try_into()?,
                ))
            }
            fb::Type::Decimal => {
                let fb_decimal = fb
                    .type__as_decimal()
                    .ok_or_else(|| vortex_err!("type__as_decimal returned None"))?;
                Ok(DType::Decimal(
                    fb_decimal.precision(),
                    fb_decimal.scale(),
                    fb_decimal.nullability().try_into()?,
                ))
            }
            fb::Type::Binary => Ok(DType::Binary(
                fb.type__as_binary()
                    .ok_or_else(|| vortex_err!("type__as_binary returned None"))?
                    .nullability()
                    .try_into()?,
            )),
            fb::Type::Utf8 => Ok(DType::Utf8(
                fb.type__as_utf_8()
                    .ok_or_else(|| vortex_err!("type__as_utf_8 returned None"))?
                    .nullability()
                    .try_into()?,
            )),
            fb::Type::List => {
                let fb_list = fb
                    .type__as_list()
                    .ok_or_else(|| vortex_err!("type__as_list returned None"))?;
                let element_dtype = DType::read_flatbuffer(
                    ctx,
                    &fb_list
                        .element_type()
                        .ok_or_else(|| vortex_err!("list element_type returned None"))?,
                )?;
                Ok(DType::List(
                    Box::new(element_dtype),
                    fb_list.nullability().try_into()?,
                ))
            }
            fb::Type::Struct_ => {
                let fb_struct = fb
                    .type__as_struct_()
                    .ok_or_else(|| vortex_err!("type__as_struct_ returned None"))?;
                let names = fb_struct
                    .names()
                    .ok_or_else(|| vortex_err!("struct names returned None"))?
                    .iter()
                    .map(|n| Arc::new(n.to_string()))
                    .collect::<Vec<_>>();
                let fields: Vec<DType> = fb_struct
                    .fields()
                    .ok_or_else(|| vortex_err!("struct fields returned None"))?
                    .iter()
                    .map(|f| DType::read_flatbuffer(ctx, &f))
                    .collect::<VortexResult<Vec<_>>>()?;
                Ok(DType::Struct(names, fields))
            }
            fb::Type::Composite => {
                let fb_composite = fb
                    .type__as_composite()
                    .ok_or_else(|| vortex_err!("type__as_composite returned None"))?;
                let id = ctx
                    .find_composite_id(
                        fb_composite
                            .id()
                            .ok_or_else(|| vortex_err!("composite id returned None"))?,
                    )
                    .ok_or_else(|| vortex_err!("Couldn't find composite id"))?;
                Ok(DType::Composite(id, fb_composite.nullability().try_into()?))
            }
            _ => Err(vortex_err!("Unknown DType variant")),
        }
    }
}

impl TryFrom<fb::Nullability> for Nullability {
    type Error = VortexError;

    fn try_from(value: fb::Nullability) -> VortexResult<Self> {
        match value {
            fb::Nullability::NonNullable => Ok(Nullability::NonNullable),
            fb::Nullability::Nullable => Ok(Nullability::Nullable),
            _ => Err(vortex_err!("Unknown nullability value")),
        }
    }
}
