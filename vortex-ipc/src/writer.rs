use flatbuffers::root_unchecked;
use std::io::{BufWriter, Write};

use vortex::array::Array;
use vortex::serde::context::SerdeContext;
use vortex::serde::data::{ArrayData, ColumnData};

use crate::ALIGNMENT;
use vortex_error::VortexResult;
use vortex_flatbuffers::FlatBufferWriter;

use crate::flatbuffers::ipc as fb;
use crate::messages::{IPCChunk, IPCChunkColumn, IPCContext, IPCMessage, IPCSchema};

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
        write.write_flatbuffer(&IPCMessage::Context(IPCContext(&ctx)), ALIGNMENT)?;
        Ok(Self { write, ctx })
    }

    pub fn write(&mut self, array: &dyn Array) -> VortexResult<()> {
        // First, write a schema message indicating the start of an array.
        self.write
            .write_flatbuffer(&IPCMessage::Schema(IPCSchema(array.dtype())), ALIGNMENT)?;

        // Then we write the array in chunks.
        // TODO(ngates): should we do any chunking ourselves?
        // TODO(ngates): If it's a chunked array, use those chunks. Else write the whole thing.

        // For now, we write a single chunk.
        self.write_chunk(array)
    }

    fn write_chunk(&mut self, array: &dyn Array) -> VortexResult<()> {
        // A chunk contains the forward byte offsets to each of the columns in the chunk.
        let col_data = ColumnData::try_from_array(array)?;

        // TODO(ngates): somehow get the flattened columns as ArrayData.
        let data = ArrayData::new(vec![col_data]);

        // In order to generate chunk metadata, we need to know the forward offsets for each
        // column. To compute this, we need to know how big the metadata messages are for each
        // column, as well as how long their buffers are.
        let mut offset = 0;
        let mut chunk_column_msgs = Vec::with_capacity(data.columns().len());
        let mut chunk_column_offsets = Vec::with_capacity(data.columns().len());
        for column_data in data.columns() {
            chunk_column_offsets.push(offset);

            // Serialize the ChunkColumn message and add its offset.
            let mut vec = Vec::new();
            vec.write_flatbuffer(
                &IPCMessage::ChunkColumn(IPCChunkColumn(&self.ctx, &column_data)),
                ALIGNMENT,
            )?;

            // Parse our message to extract the total size used by all buffers of the column.
            let chunk_col = unsafe { root_unchecked::<fb::Message>(&vec[4..]) }
                .header_as_chunk_column()
                .unwrap();
            let total_buffer_size = chunk_col.buffer_offsets().unwrap().iter().last().unwrap();
            offset += total_buffer_size;

            chunk_column_msgs.push(vec);
        }

        // Now we can construct a Chunk message with the offsets to each column.
        self.write.write_flatbuffer(
            &IPCMessage::Chunk(IPCChunk(&chunk_column_offsets)),
            ALIGNMENT,
        )?;

        // Then write each chunk column chunk message, followed by its buffers.
        for (msg, column_data) in chunk_column_msgs.iter().zip(data.columns()) {
            self.write.write_all(&msg)?;

            // FIXME(ngates): not implemented yet.
            let buffer_offsets = column_data.buffer_offsets(ALIGNMENT);
            let mut current_offset = 0;
            for (buffer_end, buffer) in buffer_offsets.iter().skip(1).zip(column_data.buffers()) {
                self.write.write_all(&buffer.as_slice())?;
                current_offset += buffer.len();
                let padding = buffer_end - current_offset;
                self.write.write_all(&vec![0; padding])?;
            }
        }

        // First, convert the array to ArrayData.
        Ok(())
    }
}
