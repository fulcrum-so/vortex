use std::io::{BufWriter, Write};

use itertools::Itertools;
use vortex::array::chunked::ChunkedArray;
use vortex::{Array, ArrayDType, SerdeContext, ToArrayData};
use vortex_error::VortexResult;
use vortex_flatbuffers::FlatBufferWriter;
use vortex_schema::DType;

use crate::messages::{IPCChunk, IPCContext, IPCMessage, IPCSchema};
use crate::ALIGNMENT;

#[allow(dead_code)]
pub struct StreamWriter<W: Write> {
    write: W,
    ctx: SerdeContext,
}

impl<W: Write> StreamWriter<BufWriter<W>> {
    pub fn try_new(write: W, ctx: SerdeContext) -> VortexResult<Self> {
        Self::try_new_unbuffered(BufWriter::new(write), ctx)
    }
}

impl<W: Write> StreamWriter<W> {
    pub fn try_new_unbuffered(mut write: W, ctx: SerdeContext) -> VortexResult<Self> {
        // Write the IPC context to the stream
        write.write_message(&IPCMessage::Context(IPCContext(&ctx)), ALIGNMENT)?;
        Ok(Self { write, ctx })
    }

    pub fn write_array(&mut self, array: &Array) -> VortexResult<()> {
        self.write_schema(array.dtype())?;
        match ChunkedArray::try_from(array) {
            Ok(chunked) => {
                for chunk in chunked.chunks() {
                    self.write_batch(&chunk)?;
                }
                Ok(())
            }
            Err(_) => self.write_batch(array),
        }
    }

    pub fn write_schema(&mut self, dtype: &DType) -> VortexResult<()> {
        Ok(self
            .write
            .write_message(&IPCMessage::Schema(IPCSchema(dtype)), ALIGNMENT)?)
    }

    pub fn write_batch(&mut self, array: &Array) -> VortexResult<()> {
        // TODO(ngates): support writing from an ArrayView.
        let data = array.to_array_data();
        let buffer_offsets = data.all_buffer_offsets(ALIGNMENT);

        // Serialize the Chunk message.
        self.write
            .write_message(&IPCMessage::Chunk(IPCChunk(&self.ctx, &data)), ALIGNMENT)?;

        // Keep track of the offset to add padding after each buffer.
        let mut current_offset = 0;
        for (buffer, &buffer_end) in data
            .depth_first_traversal()
            .flat_map(|data| data.buffer().into_iter())
            .zip_eq(buffer_offsets.iter().skip(1))
        {
            let buffer_len = buffer.len();
            self.write.write_all(buffer.as_slice())?;
            let padding = (buffer_end as usize) - current_offset - buffer_len;
            self.write.write_all(&vec![0; padding])?;
            current_offset = buffer_end as usize;
        }

        Ok(())
    }
}

impl<W: Write> Drop for StreamWriter<W> {
    fn drop(&mut self) {
        // Terminate the stream
        let _ = self.write.write_all(&[u8::MAX; 4]);
    }
}
