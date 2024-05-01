use flatbuffers::{FlatBufferBuilder, WIPOffset};
use itertools::Itertools;
use vortex::encoding::find_encoding;
use vortex::flatbuffers::array as fba;
use vortex::{ArrayData, SerdeContext};
use vortex_dtype::DType;
use vortex_error::{vortex_err, VortexError};
use vortex_flatbuffers::{FlatBufferRoot, WriteFlatBuffer};

use crate::flatbuffers::ipc as fb;
use crate::flatbuffers::ipc::Compression;
use crate::{missing, ALIGNMENT};

pub(crate) enum IPCMessage<'a> {
    Context(IPCContext<'a>),
    Schema(IPCSchema<'a>),
    Chunk(IPCChunk<'a>),
}

pub(crate) struct IPCContext<'a>(pub &'a SerdeContext);
pub(crate) struct IPCSchema<'a>(pub &'a DType);
pub(crate) struct IPCChunk<'a>(pub &'a SerdeContext, pub &'a ArrayData);
pub(crate) struct IPCArray<'a>(pub &'a SerdeContext, pub &'a ArrayData);

impl FlatBufferRoot for IPCMessage<'_> {}
impl WriteFlatBuffer for IPCMessage<'_> {
    type Target<'a> = fb::Message<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let header = match self {
            Self::Context(f) => f.write_flatbuffer(fbb).as_union_value(),
            Self::Schema(f) => f.write_flatbuffer(fbb).as_union_value(),
            Self::Chunk(f) => f.write_flatbuffer(fbb).as_union_value(),
        };

        let mut msg = fb::MessageBuilder::new(fbb);
        msg.add_version(Default::default());
        msg.add_header_type(match self {
            Self::Context(_) => fb::MessageHeader::Context,
            Self::Schema(_) => fb::MessageHeader::Schema,
            Self::Chunk(_) => fb::MessageHeader::Chunk,
        });
        msg.add_header(header);
        msg.finish()
    }
}

impl<'a> WriteFlatBuffer for IPCContext<'a> {
    type Target<'t> = fb::Context<'t>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let fb_encodings = self
            .0
            .encodings()
            .iter()
            .map(|e| e.id())
            .map(|id| {
                let encoding_id = fbb.create_string(id.as_ref());
                fb::Encoding::create(
                    fbb,
                    &fb::EncodingArgs {
                        id: Some(encoding_id),
                    },
                )
            })
            .collect_vec();
        let fb_encodings = fbb.create_vector(fb_encodings.as_slice());

        fb::Context::create(
            fbb,
            &fb::ContextArgs {
                encodings: Some(fb_encodings),
            },
        )
    }
}

impl<'a> TryFrom<fb::Context<'a>> for SerdeContext {
    type Error = VortexError;

    fn try_from(value: fb::Context<'a>) -> Result<Self, Self::Error> {
        let fb_encodings = value.encodings().ok_or_else(missing("encodings"))?;
        let mut encodings = Vec::with_capacity(fb_encodings.len());
        for fb_encoding in fb_encodings {
            let encoding_id = fb_encoding.id().ok_or_else(missing("encoding.id"))?;
            encodings.push(
                find_encoding(encoding_id)
                    .ok_or_else(|| vortex_err!("Stream uses unknown encoding {}", encoding_id))?,
            );
        }
        Ok(Self::new(encodings.into()))
    }
}

impl<'a> WriteFlatBuffer for IPCSchema<'a> {
    type Target<'t> = fb::Schema<'t>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let dtype = Some(self.0.write_flatbuffer(fbb));
        fb::Schema::create(fbb, &fb::SchemaArgs { dtype })
    }
}

impl<'a> WriteFlatBuffer for IPCChunk<'a> {
    type Target<'t> = fb::Chunk<'t>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let array_data = self.1;
        let array = Some(IPCArray(self.0, array_data).write_flatbuffer(fbb));

        // Walk the ColumnData depth-first to compute the buffer offsets.
        let mut buffers = vec![];
        let mut offset = 0;
        for array_data in array_data.depth_first_traversal() {
            if let Some(buffer) = array_data.buffer() {
                buffers.push(fb::Buffer::new(
                    offset as u64,
                    buffer.len() as u64,
                    Compression::None,
                ));
                let aligned_size = (buffer.len() + (ALIGNMENT - 1)) & !(ALIGNMENT - 1);
                offset += aligned_size;
            }
        }
        let buffers = Some(fbb.create_vector(&buffers));

        fb::Chunk::create(
            fbb,
            &fb::ChunkArgs {
                array,
                buffers,
                buffer_size: offset as u64,
            },
        )
    }
}

impl<'a> WriteFlatBuffer for IPCArray<'a> {
    type Target<'t> = fba::Array<'t>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let ctx = self.0;
        let column_data = self.1;

        let encoding = ctx
            .encoding_idx(column_data.encoding().id())
            // TODO(ngates): return result from this writer?
            .unwrap_or_else(|| panic!("Encoding not found: {:?}", column_data.encoding()));

        let metadata = Some(
            fbb.create_vector(
                column_data
                    .metadata()
                    .try_serialize_metadata()
                    // TODO(ngates): should we serialize externally to here?
                    .unwrap()
                    .as_ref(),
            ),
        );

        let children = column_data
            .children()
            .iter()
            .map(|child| IPCArray(self.0, child).write_flatbuffer(fbb))
            .collect_vec();
        let children = Some(fbb.create_vector(&children));

        fba::Array::create(
            fbb,
            &fba::ArrayArgs {
                version: Default::default(),
                has_buffer: column_data.buffer().is_some(),
                encoding,
                metadata,
                children,
            },
        )
    }
}
