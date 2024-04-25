use std::io;
use std::io::{BufReader, Read};

use arrow_buffer::Buffer as ArrowBuffer;
use flatbuffers::{root, root_unchecked};
use itertools::Itertools;
use nougat::gat;
use vortex::array::chunked::ChunkedArray;
use vortex::array::composite::VORTEX_COMPOSITE_EXTENSIONS;
use vortex::array::primitive::PrimitiveArray;
use vortex::buffer::Buffer;
use vortex::compute::search_sorted::{search_sorted, SearchSortedSide};
use vortex::compute::slice::slice;
use vortex::compute::take::take;
use vortex::stats::{ArrayStatistics, Stat};
use vortex::{
    match_each_unsigned_integer_ptype, Array, ArrayDType, ArrayView, IntoArray, OwnedArray,
    SerdeContext, ToArray, ToStatic,
};
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};
use vortex_flatbuffers::ReadFlatBuffer;
use vortex_schema::{DType, DTypeSerdeContext};

use crate::flatbuffers::ipc::Message;
use crate::iter::{FallibleLendingIterator, FallibleLendingIteratorඞItem};

#[allow(dead_code)]
pub struct StreamReader<R: Read> {
    read: R,
    messages: StreamMessageReader,
    ctx: SerdeContext,
}

impl<R: Read> StreamReader<BufReader<R>> {
    pub fn try_new(read: R) -> VortexResult<Self> {
        Self::try_new_unbuffered(BufReader::new(read))
    }
}

impl<R: Read> StreamReader<R> {
    pub fn try_new_unbuffered(mut read: R) -> VortexResult<Self> {
        let mut messages = StreamMessageReader::try_new(&mut read)?;
        match messages.peek() {
            None => vortex_bail!("IPC stream is empty"),
            Some(msg) => {
                if msg.header_as_context().is_none() {
                    vortex_bail!(InvalidSerde: "Expected IPC Context as first message in stream")
                }
            }
        }

        let ctx: SerdeContext = messages
            .next(&mut read)?
            .header_as_context()
            .unwrap()
            .try_into()?;

        Ok(Self {
            read,
            messages,
            ctx,
        })
    }

    /// Read a single array from the IPC stream.
    pub fn read_array(&mut self) -> VortexResult<Array> {
        let mut array_reader = self
            .next()?
            .ok_or_else(|| vortex_err!(InvalidSerde: "Unexpected EOF"))?;

        let mut chunks = vec![];
        while let Some(chunk) = array_reader.next()? {
            chunks.push(chunk.to_static());
        }

        if chunks.len() == 1 {
            Ok(chunks[0].clone())
        } else {
            ChunkedArray::try_new(chunks.into_iter().collect(), array_reader.dtype().clone())
                .map(|chunked| chunked.into_array())
        }
    }
}

#[gat]
impl<R: Read> FallibleLendingIterator for StreamReader<R> {
    type Error = VortexError;
    type Item<'next> =  StreamArrayReader<'next, R> where Self: 'next;

    fn next(&mut self) -> Result<Option<StreamArrayReader<'_, R>>, Self::Error> {
        if self
            .messages
            .peek()
            .and_then(|msg| msg.header_as_schema())
            .is_none()
        {
            return Ok(None);
        }

        let schema_msg = self
            .messages
            .next(&mut self.read)?
            .header_as_schema()
            .unwrap();

        // TODO(ngates): construct this from the SerdeContext.
        let dtype_ctx =
            DTypeSerdeContext::new(VORTEX_COMPOSITE_EXTENSIONS.iter().map(|e| e.id()).collect());
        let dtype = DType::read_flatbuffer(
            &dtype_ctx,
            &schema_msg
                .dtype()
                .ok_or_else(|| vortex_err!(InvalidSerde: "Schema missing DType"))?,
        )
        .map_err(|e| vortex_err!(InvalidSerde: "Failed to parse DType: {}", e))?;

        Ok(Some(StreamArrayReader {
            ctx: &self.ctx,
            read: &mut self.read,
            messages: &mut self.messages,
            dtype,
            buffers: vec![],
            byte_offset: 0,
        }))
    }
}

#[allow(dead_code)]
pub struct StreamArrayReader<'a, R: Read> {
    ctx: &'a SerdeContext,
    read: &'a mut R,
    messages: &'a mut StreamMessageReader,
    dtype: DType,
    buffers: Vec<Buffer<'a>>,
    byte_offset: usize,
}

impl<'a, R: Read> StreamArrayReader<'a, R> {
    pub fn dtype(&self) -> &DType {
        &self.dtype
    }

    pub fn take(&mut self, indices: &Array<'_>) -> VortexResult<OwnedArray> {
        if !indices
            .statistics()
            .compute_as::<bool>(Stat::IsSorted)
            .unwrap_or_default()
        {
            vortex_bail!("Indices must be sorted to take from IPC stream")
        }

        if self.byte_offset != 0 {
            vortex_bail!("Stream has already been (at least partially) consumed")
        }

        let mut chunks = Vec::new();

        let mut offset = self.byte_offset;
        let idx_len = indices.len();
        let mut found_so_far = 0;

        // Continue reading batches from the stream until we either run out or find all indices
        while let Some(batch) = self.next()? {
            let left =
                search_sorted::<u64>(indices, offset as u64, SearchSortedSide::Left)?.to_index();
            let right = search_sorted::<u64>(
                indices,
                offset as u64 + batch.len() as u64,
                SearchSortedSide::Left,
            )?
            .to_index();

            // TODO(@jdcasale): replace this with compute scalar_sum when we've added it
            let indices_for_batch = slice(indices, left, right)?.flatten_primitive()?;
            let ptype = indices.dtype().try_into()?;
            let shifted = match_each_unsigned_integer_ptype!(ptype, |$P| {
                indices_for_batch
                    .typed_data::<$P>()
                    .iter()
                    .map(|&idx| {
                        idx  as u64 - offset as u64
                    })
                    .collect_vec()
            });

            let shifted_arr = PrimitiveArray::from(shifted).to_array().to_static();
            let from_current_batch = take(&batch, &shifted_arr)?;
            found_so_far += from_current_batch.len();
            chunks.push(from_current_batch);
            offset += batch.len();
            if found_so_far >= indices.len() {
                break;
            }
        }
        self.byte_offset = offset;
        if found_so_far < idx_len {
            vortex_bail!("EOF encountered before finding all indices")
        }

        let result = if chunks.len() == 1 {
            chunks.first().unwrap().to_static()
        } else {
            ChunkedArray::try_new(chunks, self.dtype.clone())?
                .into_array()
                .to_static()
        };
        Ok(OwnedArray::from(result))
    }
}

#[gat]
impl<'iter, R: Read> FallibleLendingIterator for StreamArrayReader<'iter, R> {
    type Error = VortexError;
    type Item<'next> = Array<'next> where Self: 'next;

    fn next(&mut self) -> Result<Option<Array<'_>>, Self::Error> {
        let Some(chunk_msg) = self.messages.peek().and_then(|msg| msg.header_as_chunk()) else {
            return Ok(None);
        };

        // Read all the column's buffers
        self.buffers.clear();
        let mut offset = 0;
        for buffer in chunk_msg.buffers().unwrap_or_default().iter() {
            let _skip = buffer.offset() - offset;
            self.read.skip(buffer.offset() - offset)?;

            // TODO(ngates): read into a single buffer, then Arc::clone and slice
            let mut bytes = Vec::with_capacity(buffer.length() as usize);
            self.read.read_into(buffer.length(), &mut bytes)?;
            let arrow_buffer = ArrowBuffer::from_vec(bytes);
            self.buffers.push(Buffer::Owned(arrow_buffer));

            offset = buffer.offset() + buffer.length();
        }

        // Consume any remaining padding after the final buffer.
        self.read.skip(chunk_msg.buffer_size() - offset)?;

        // After reading the buffers we're now able to load the next message.
        let col_array = self
            .messages
            .next(&mut self.read)?
            .header_as_chunk()
            .unwrap()
            .array()
            .unwrap();
        let view = ArrayView::try_new(self.ctx, &self.dtype, col_array, self.buffers.as_slice())?;

        // Validate it
        view.to_array().with_dyn(|_| Ok::<(), VortexError>(()))?;

        Ok(Some(view.into_array()))
    }
}

pub trait ReadExtensions: Read {
    /// Skip n bytes in the stream.
    fn skip(&mut self, nbytes: u64) -> io::Result<()> {
        io::copy(&mut self.take(nbytes), &mut io::sink())?;
        Ok(())
    }

    /// Read exactly nbytes into the buffer.
    fn read_into(&mut self, nbytes: u64, buffer: &mut Vec<u8>) -> VortexResult<()> {
        buffer.reserve_exact(nbytes as usize);
        if self.take(nbytes).read_to_end(buffer)? != nbytes as usize {
            vortex_bail!(InvalidSerde: "Failed to read all bytes")
        }
        Ok(())
    }
}

impl<R: Read> ReadExtensions for R {}

struct StreamMessageReader {
    message: Vec<u8>,
    prev_message: Vec<u8>,
    finished: bool,
}

impl StreamMessageReader {
    pub fn try_new<R: Read>(read: &mut R) -> VortexResult<Self> {
        let mut reader = Self {
            message: Vec::new(),
            prev_message: Vec::new(),
            finished: false,
        };
        reader.load_next_message(read)?;
        Ok(reader)
    }

    pub fn peek(&self) -> Option<Message> {
        if self.finished {
            return None;
        }
        // The message has been validated by the next() call.
        Some(unsafe { root_unchecked::<Message>(&self.message) })
    }

    pub fn next<R: Read>(&mut self, read: &mut R) -> VortexResult<Message> {
        if self.finished {
            panic!("StreamMessageReader is finished - should've checked peek!");
        }
        std::mem::swap(&mut self.prev_message, &mut self.message);
        if !self.load_next_message(read)? {
            self.finished = true;
        }
        Ok(unsafe { root_unchecked::<Message>(&self.prev_message) })
    }

    fn load_next_message<R: Read>(&mut self, read: &mut R) -> VortexResult<bool> {
        let mut len_buf = [0u8; 4];
        match read.read_exact(&mut len_buf) {
            Ok(_) => {}
            Err(e) => {
                return match e.kind() {
                    io::ErrorKind::UnexpectedEof => Ok(false),
                    _ => Err(e.into()),
                }
            }
        }

        let len = u32::from_le_bytes(len_buf);
        if len == u32::MAX {
            // Marker for no more messages.
            return Ok(false);
        }

        self.message.clear();
        self.message.reserve(len as usize);
        if read.take(len as u64).read_to_end(&mut self.message)? != len as usize {
            vortex_bail!(InvalidSerde: "Failed to read all bytes")
        }

        std::hint::black_box(root::<Message>(&self.message)?);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Write};
    use std::sync::Arc;

    use itertools::Itertools;
    use vortex::array::chunked::{Chunked, ChunkedArray, ChunkedEncoding};
    use vortex::array::primitive::{Primitive, PrimitiveArray, PrimitiveEncoding};
    use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
    use vortex::encoding::{ArrayEncoding, EncodingId, EncodingRef};
    use vortex::ptype::NativePType;
    use vortex::{Array, ArrayDType, ArrayDef, IntoArray, SerdeContext};
    use vortex_alp::{ALPArray, ALPEncoding};
    use vortex_fastlanes::BitPackedEncoding;
    use vortex_schema::{DType, IntWidth, Nullability, Signedness};

    use crate::iter::FallibleLendingIterator;
    use crate::reader::StreamReader;
    use crate::writer::StreamWriter;

    #[test]
    fn test_read_write() {
        let array = PrimitiveArray::from(vec![0, 1, 2]).into_array();
        let chunked_array =
            ChunkedArray::try_new(vec![array.clone(), array.clone()], array.dtype().clone())
                .unwrap()
                .into_array();

        let mut buffer = vec![];
        let mut cursor = Cursor::new(&mut buffer);
        {
            let mut writer = StreamWriter::try_new(&mut cursor, SerdeContext::default()).unwrap();
            writer.write_array(&array).unwrap();
            writer.write_array(&chunked_array).unwrap();
        }
        // Push some extra bytes to test that the reader is well-behaved and doesn't read past the
        // end of the stream.
        let _ = cursor.write(b"hello").unwrap();

        cursor.set_position(0);
        {
            let mut reader = StreamReader::try_new_unbuffered(&mut cursor).unwrap();
            let first = reader.read_array().unwrap();
            assert_eq!(first.encoding().id(), Primitive::ID);
            let second = reader.read_array().unwrap();
            assert_eq!(second.encoding().id(), Chunked::ID);
        }
        let _pos = cursor.position();
        // Test our termination bytes exist
        let mut terminator = [0u8; 5];
        cursor.read_exact(&mut terminator).unwrap();
        assert_eq!(&terminator, b"hello");
    }

    #[test]
    fn test_write_read_primitive() {
        let data = PrimitiveArray::from((0i32..3_000_000).rev().collect_vec()).into_array();
        test_write_read_inner(
            data,
            vec![2999989i32, 2999988, 2999987, 2999986, 2899999, 0],
            PrimitiveEncoding.id(),
        );
    }

    #[test]
    fn test_write_read_alp() {
        let pdata = PrimitiveArray::from(
            (0i32..3_000_000)
                .rev()
                .map(|v| v as f64 + 0.5)
                .collect_vec(),
        )
        .into_array();
        let apl_encoded = ALPArray::encode(pdata).unwrap();
        test_write_read_inner(
            apl_encoded,
            vec![
                2999989.5f64,
                2999988.5,
                2999987.5,
                2999986.5,
                2899999.5,
                0.5,
            ],
            ALPEncoding.id(),
        );
    }

    #[test]
    fn test_write_read_bitpacked() {
        let uncompressed = PrimitiveArray::from((0i64..3_000_000).rev().collect_vec());

        let cfg = CompressConfig::new().with_enabled([&BitPackedEncoding as EncodingRef]);
        let ctx = CompressCtx::new(Arc::new(cfg));

        let packed = BitPackedEncoding {}
            .compress(uncompressed.array(), None, ctx)
            .unwrap();
        assert_eq!(packed.encoding().id(), BitPackedEncoding.id());
        test_write_read_inner(
            packed,
            vec![2999989i64, 2999988, 2999987, 2999986, 2899999, 0],
            PrimitiveEncoding.id(),
        );
    }

    #[test]
    fn test_write_read_chunked() {
        let indices = PrimitiveArray::from(vec![10u32, 11, 12, 13, 100_000, 2_999_999, 3_000_000])
            .into_array();
        let data = PrimitiveArray::from((0i32..3_000_000).rev().collect_vec()).into_array();
        let data2 =
            PrimitiveArray::from((3_000_000i32..6_000_000).rev().collect_vec()).into_array();
        let chunked = ChunkedArray::try_new(
            vec![data, data2],
            DType::Int(IntWidth::_32, Signedness::Signed, Nullability::NonNullable),
        )
        .unwrap()
        .into_array();
        let mut buffer = vec![];
        {
            let mut cursor = Cursor::new(&mut buffer);
            {
                let mut writer =
                    StreamWriter::try_new(&mut cursor, SerdeContext::default()).unwrap();
                writer.write_array(&chunked).unwrap();
                writer.write_array(&chunked).unwrap();
            }
        }

        let mut cursor = Cursor::new(&buffer);
        let mut reader = StreamReader::try_new(&mut cursor).unwrap();
        let mut array_reader = reader.next().unwrap().unwrap();
        let array = array_reader.take(&indices).unwrap();
        assert_eq!(array.encoding().id(), ChunkedEncoding.id());
        let bind = ChunkedArray::try_from(array).unwrap();
        let mut chunked_out = bind.chunks();
        assert_eq!(
            chunked_out
                .next()
                .unwrap()
                .into_primitive()
                .typed_data::<u32>()
                .to_vec(),
            vec![2999989, 2999988, 2999987, 2999986, 2899999, 0]
        );
        assert_eq!(
            chunked_out
                .next()
                .unwrap()
                .into_primitive()
                .typed_data::<u32>()
                .to_vec(),
            vec![5999999]
        );
    }
    fn test_write_read_inner<T: NativePType>(
        data: Array,
        expected: Vec<T>,
        expected_encoding_id: EncodingId,
    ) {
        let indices =
            PrimitiveArray::from(vec![10u32, 11, 12, 13, 100_000, 2_999_999]).into_array();
        let mut buffer = vec![];
        {
            let mut cursor = Cursor::new(&mut buffer);
            {
                let mut writer =
                    StreamWriter::try_new(&mut cursor, SerdeContext::default()).unwrap();
                writer.write_array(&data).unwrap();
                writer.write_array(&data).unwrap();
            }
        }

        let mut cursor = Cursor::new(&buffer);
        let mut reader = StreamReader::try_new(&mut cursor).unwrap();
        let mut array_reader = reader.next().unwrap().unwrap();
        let array = array_reader.take(&indices).unwrap();
        assert_eq!(array.encoding().id(), expected_encoding_id);

        let results = array
            .flatten_primitive()
            .unwrap()
            .typed_data::<T>()
            .to_vec();
        assert_eq!(results, expected);
    }
}
