use std::io;
use std::sync::Arc;

use bytes::{Buf, BytesMut};
use flatbuffers::{root, root_unchecked};
use futures_util::stream::try_unfold;
use itertools::Itertools;
use vortex::stream::{ArrayStream, ArrayStreamAdapter};
use vortex::{Array, ArrayView, Context, IntoArray, ToArray, ViewContext};
use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};

use crate::flatbuffers::ipc as fb;
use crate::io::VortexRead;
use crate::messages::SerdeContextDeserializer;

pub struct MessageReader<R> {
    read: R,
    message: BytesMut,
    prev_message: BytesMut,
    finished: bool,
}

impl<R: VortexRead> MessageReader<R> {
    pub async fn try_new(read: R) -> VortexResult<Self> {
        let mut reader = Self {
            read,
            message: BytesMut::new(),
            prev_message: BytesMut::new(),
            finished: false,
        };
        reader.load_next_message().await?;
        Ok(reader)
    }

    async fn load_next_message(&mut self) -> VortexResult<bool> {
        let mut buffer = std::mem::take(&mut self.message);
        buffer.resize(4, 0);
        let mut buffer = match self.read.read_into(buffer).await {
            Ok(b) => b,
            Err(e) => {
                return match e.kind() {
                    io::ErrorKind::UnexpectedEof => Ok(false),
                    _ => Err(e.into()),
                };
            }
        };

        let len = buffer.get_u32_le();
        if len == u32::MAX {
            // Marker for no more messages.
            return Ok(false);
        } else if len == 0 {
            vortex_bail!(InvalidSerde: "Invalid IPC stream")
        }

        buffer.reserve(len as usize);
        unsafe { buffer.set_len(len as usize) };
        self.message = self.read.read_into(buffer).await?;

        // Validate that the message is valid a flatbuffer.
        root::<fb::Message>(&self.message).map_err(
            |e| vortex_err!(InvalidSerde: "Failed to parse flatbuffer message: {:?}", e),
        )?;

        Ok(true)
    }

    fn peek(&self) -> Option<fb::Message> {
        if self.finished {
            return None;
        }
        // The message has been validated by the next() call.
        Some(unsafe { root_unchecked::<fb::Message>(&self.message) })
    }

    async fn next(&mut self) -> VortexResult<fb::Message> {
        if self.finished {
            panic!("StreamMessageReader is finished - should've checked peek!");
        }
        self.prev_message = self.message.split();
        if !self.load_next_message().await? {
            self.finished = true;
        }
        Ok(unsafe { root_unchecked::<fb::Message>(&self.prev_message) })
    }

    async fn next_raw(&mut self) -> VortexResult<Buffer> {
        if self.finished {
            panic!("StreamMessageReader is finished - should've checked peek!");
        }
        self.prev_message = self.message.split();
        if !self.load_next_message().await? {
            self.finished = true;
        }
        Ok(Buffer::from(self.prev_message.clone().freeze()))
    }

    /// Fetch the buffers associated with this message.
    async fn read_buffers(&mut self) -> VortexResult<Vec<Buffer>> {
        let Some(chunk_msg) = self.peek().and_then(|m| m.header_as_chunk()) else {
            // We could return an error here?
            return Ok(Vec::new());
        };

        // Issue a single read to grab all buffers
        let mut all_buffers = BytesMut::with_capacity(chunk_msg.buffer_size() as usize);
        unsafe { all_buffers.set_len(chunk_msg.buffer_size() as usize) };
        let mut all_buffers = self.read.read_into(all_buffers).await?;

        // Split out into individual buffers
        // Initialize the column's buffers for a vectored read.
        // To start with, we include the padding and then truncate the buffers after.
        // TODO(ngates): improve the flatbuffer format instead of storing offset/len per buffer.
        let buffers = self
            .peek()
            .expect("Checked above in peek")
            .header_as_chunk()
            .expect("Checked above in peek")
            .buffers()
            .unwrap_or_default()
            .iter()
            .scan(0, |offset, buffer| {
                let len = buffer.length() as usize;
                let padding_len = buffer.offset() as usize - *offset;

                // Strip off any padding from the previous buffer
                all_buffers.advance(padding_len);
                // Grab the buffer
                let buffer = all_buffers.split_to(len);

                *offset += padding_len + len;
                Some(Buffer::from(buffer.freeze()))
            })
            .collect_vec();

        Ok(buffers)
    }

    pub async fn read_view_context<'a>(
        &'a mut self,
        ctx: &'a Context,
    ) -> VortexResult<Arc<ViewContext>> {
        if self.peek().and_then(|m| m.header_as_context()).is_none() {
            vortex_bail!("Expected context message");
        }

        let view_ctx: ViewContext = SerdeContextDeserializer {
            fb: self.next().await?.header_as_context().unwrap(),
            ctx,
        }
        .try_into()?;

        Ok(view_ctx.into())
    }

    pub async fn read_dtype(&mut self) -> VortexResult<DType> {
        if self.peek().and_then(|m| m.header_as_schema()).is_none() {
            vortex_bail!("Expected schema message")
        }

        let schema_msg = self.next().await?.header_as_schema().unwrap();

        let dtype = DType::try_from(
            schema_msg
                .dtype()
                .ok_or_else(|| vortex_err!(InvalidSerde: "Schema missing DType"))?,
        )
        .map_err(|e| vortex_err!(InvalidSerde: "Failed to parse DType: {}", e))?;

        Ok(dtype)
    }

    pub async fn maybe_read_chunk(
        &mut self,
        view_ctx: Arc<ViewContext>,
        dtype: DType,
    ) -> VortexResult<Option<Array>> {
        if self.peek().and_then(|m| m.header_as_chunk()).is_none() {
            return Ok(None);
        }

        let buffers = self.read_buffers().await?;
        let flatbuffer = self.next_raw().await?;

        let view = ArrayView::try_new(
            view_ctx,
            dtype,
            flatbuffer,
            |flatbuffer| {
                unsafe { root_unchecked::<fb::Message>(flatbuffer) }
                    .header_as_chunk()
                    .unwrap()
                    .array()
                    .ok_or_else(|| vortex_err!("Chunk missing Array"))
            },
            buffers,
        )?;

        // Validate it
        view.to_array().with_dyn(|_| Ok::<(), VortexError>(()))?;

        Ok(Some(view.into_array()))
    }

    /// Construct an ArrayStream pulling the ViewContext and DType from the stream.
    pub async fn array_stream_from_messages(
        &mut self,
        ctx: &Context,
    ) -> VortexResult<impl ArrayStream + '_> {
        let view_context = self.read_view_context(ctx).await?;
        let dtype = self.read_dtype().await?;
        Ok(self.array_stream(view_context, dtype))
    }

    pub fn array_stream(
        &mut self,
        view_context: Arc<ViewContext>,
        dtype: DType,
    ) -> impl ArrayStream + '_ {
        struct State<'a, R: VortexRead> {
            msgs: &'a mut MessageReader<R>,
            view_context: Arc<ViewContext>,
            dtype: DType,
        }

        let init = State {
            msgs: self,
            view_context,
            dtype: dtype.clone(),
        };

        ArrayStreamAdapter::new(
            dtype,
            try_unfold(init, |state| async move {
                match state
                    .msgs
                    .maybe_read_chunk(state.view_context.clone(), state.dtype.clone())
                    .await?
                {
                    None => Ok(None),
                    Some(array) => Ok(Some((array, state))),
                }
            }),
        )
    }

    pub fn into_array_stream(
        self,
        view_context: Arc<ViewContext>,
        dtype: DType,
    ) -> impl ArrayStream {
        struct State<R: VortexRead> {
            msgs: MessageReader<R>,
            view_context: Arc<ViewContext>,
            dtype: DType,
        }

        let init = State {
            msgs: self,
            view_context,
            dtype: dtype.clone(),
        };

        ArrayStreamAdapter::new(
            dtype,
            try_unfold(init, |mut state| async move {
                match state
                    .msgs
                    .maybe_read_chunk(state.view_context.clone(), state.dtype.clone())
                    .await?
                {
                    None => Ok(None),
                    Some(array) => Ok(Some((array, state))),
                }
            }),
        )
    }

    pub async fn maybe_read_page(&mut self) -> VortexResult<Option<Buffer>> {
        let Some(page_msg) = self.peek().and_then(|m| m.header_as_page()) else {
            return Ok(None);
        };

        let buffer_len = page_msg.buffer_size() as usize;
        let total_len = buffer_len + (page_msg.padding() as usize);

        let mut buffer = BytesMut::with_capacity(total_len);
        unsafe { buffer.set_len(total_len) }
        buffer = self.read.read_into(buffer).await?;
        buffer.truncate(buffer_len);
        let page_buffer = Ok(Some(Buffer::from(buffer.freeze())));
        let _ = self.next().await?;
        page_buffer
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use bytes::Bytes;
    use vortex_buffer::Buffer;

    use crate::{MessageReader, MessageWriter};

    #[tokio::test]
    async fn read_write_page() {
        let write = Vec::new();
        let mut writer = MessageWriter::new(write);
        writer
            .write_page(Buffer::Bytes(Bytes::from("somevalue")))
            .await
            .unwrap();
        let written = writer.into_inner();
        let mut reader = MessageReader::try_new(Cursor::new(written.as_slice()))
            .await
            .unwrap();
        let read_page = reader.maybe_read_page().await.unwrap().unwrap();
        assert_eq!(read_page, Buffer::Bytes(Bytes::from("somevalue")));
    }
}
