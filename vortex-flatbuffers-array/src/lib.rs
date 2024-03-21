use crate::fb::Flat;
use arrow_buffer::Buffer;
use flatbuffers::{Follow, Verifiable};
use std::io::Read;
use std::sync::Arc;
use vortex::array::{EncodingId, ENCODINGS};
use vortex_flatbuffers::column::Array;

mod fb;

// Serialized Form
//
// Split into batches (similar to RecordBatches)
// For the batch, we assume we have:
//   * Schema - a DType with flattened structs / nested columns.
//   * Capabilities - the set of encodings used in the stream of batches.
//

// BatchMessage:
//   * length - long
//   * columns - list<Column{byte_offset}>

// MessageColumn:
//   * stats: list<Statistic> // Non-pruning statistics, e.g. IsSorted.
//   * array: Array
//   * buffer_lens: [long] // Byte lengths (or offsets) of each of the buffers of the column.
//
// Array:
//   * encoding_id: short (the encoding ID as defined in the capabilities)
//   * metadata: [byte]
//   * nbuffers: short // The number of buffers consumed by this encoding (and all its children)

// Maybe some buffers are lazy? Variable sized buffers perhaps?

// MessageColumn Buffers (written immediately after the MessageColumn):

struct ColumnReader {}

// From the wire, we read an Array message (which holds the encodings for the entire column) as
// well as the correct number of buffers.

trait ArrayTrait {
    fn encoding_id(&self) -> EncodingId;

    fn len(&self) -> usize;
}

struct ReaderCtx {
    dtype: DType,
}

struct ArrayData<'a> {
    ctx: Arc<ArrayDataCtx>,
    encoding: Flat<'a, Array<'a>>,
    buffers: Arc<[Buffer]>,
}

// Is this an array? I guess so?

impl<'a> ArrayTrait for ArrayData<'a> {
    fn encoding_id(&self) -> EncodingId {
        ENCODINGS.iter()
            .find(|e| e.id().name() == self.encoding.as_typed().encoding_id())
        todo!()
    }

    fn len(&self) -> usize {
        todo!()
    }
}

// Then we want to run a scan over this thing...

// Say we have some DictArray and we want to scan it.
struct DictArray {
    keys: ArrayData,
    values: ArrayData,
}

impl<'a> ArrayData<'a> {
    fn child(&self, i: usize) -> Option<ArrayData<'a>> {
        if let Some(children) = self.encoding.as_typed().children() {
            let num_buffers_before: i16 =
                children.iter().map(|child| child.nbuffers()).take(i).sum();
            let child = children.get(i);
            let child_buffers = self.buffers[num_buffers_before as usize..][..child.nbuffers()];

            Some(ArrayData {
                encoding: child.bytes(),
                buffers: Arc::from(child_buffers),
            })
        }
        self.encoding.as_typed().children().map(|c| c.get(i))
    }
}

#[cfg(test)]
mod test {
    use flatbuffers::FlatBufferBuilder;

    use vortex_flatbuffers::column::{Column, ColumnArgs};

    use crate::fb::Flat;

    fn write_column<'a>() -> Flat<'a, Column<'a>> {
        let mut fb = FlatBufferBuilder::new();
        let col = Column::create(
            &mut fb,
            &ColumnArgs {
                array: None,
                buffer_lens: None,
            },
        );
        Flat::<Column>::from_root(fb, col)
    }

    #[test]
    pub fn test_something() {
        let col_buf = write_column();
        println!("Buffer {:?}", col_buf);
        let col: Flat<Column> = col_buf.try_into().unwrap();
        println!("Col {:?}", col);

        let c = col.as_typed();
        println!("C {:?}", c);
    }
}
