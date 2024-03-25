use std::borrow::{Borrow, Cow};
use std::sync::Arc;

use arrow_buffer::Buffer;
use flatbuffers::{root, Follow, Verifiable};
use vortex::error::VortexResult;
use vortex::ptype::PType;
use vortex_flatbuffers::encoding::{Buffer, Encoding};

use crate::fb::FlatBuffer;
use vortex_schema::DType;

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

// From the wire, we read an Array message (which holds the encodings for the entire column) as
// well as the correct number of buffers.

trait ArrayTrait {
    fn len(&self) -> usize;
}

impl ToOwned for dyn ArrayTrait {
    type Owned = Arc<dyn ArrayTrait>;

    fn to_owned(&self) -> Arc<dyn ArrayTrait> {
        todo!()
    }
}

impl<'a> Borrow<dyn ArrayTrait + 'a> for PrimitiveArray {
    fn borrow(&self) -> &(dyn ArrayTrait + 'a) {
        self
    }
}

trait ArrayCompute {}

trait WithCompute {
    fn with_compute<T, F>(&self, closure: F) -> T
    where
        F: FnOnce(&dyn ArrayCompute) -> T;
}

trait EncodingPlugin: Sync {
    fn as_array<'a>(
        &self,
        children: Vec<Cow<'a, dyn ArrayTrait>>,
    ) -> VortexResult<Cow<'a, dyn ArrayTrait>>;
}

type EncodingRef = &'static dyn EncodingPlugin;

#[derive(Clone)]
struct ArrayData {
    dtype: DType,
    encoding: Buffer,
    buffers: Arc<[Buffer]>,
}

impl ArrayData {
    fn encoding(&self) -> Encoding {
        FlatBuffer::<Encoding>::try_from_slice(&self.encoding)
    }

    fn as_array(&self) -> &dyn ArrayTrait {
        todo!()
    }
}

fn as_array<'a>(buffers: &[Buffer], encoding: Encoding<'a>) -> &'a dyn ArrayTrait {
    // Switch on ID encoding.id()
    let buffer_meta = encoding.buffers().unwrap();

    // How many children?
    let mut buffer_idx = 0;
    let mut children = Vec::with_capacity(encoding.children().unwrap().len());
    for child in encoding.children().unwrap() {
        let nbuffers = child.buffers().unwrap().len();
        children.push(as_array(&buffers[buffer_idx..][0..nbuffers], child));
        buffer_idx += nbuffers;
    }

    DictEncodingPlugin::as_array(children)
}

struct PrimitiveEncodingPlugin;
struct DictEncodingPlugin;

impl EncodingPlugin for PrimitiveEncodingPlugin {
    fn as_array(&self, children: Vec<Cow<dyn ArrayTrait>>) -> VortexResult<Cow<dyn ArrayTrait>> {
        todo!()
    }
}

impl EncodingPlugin for DictEncodingPlugin {
    fn as_array(&self, children: Vec<Cow<dyn ArrayTrait>>) -> VortexResult<Cow<dyn ArrayTrait>> {
        todo!()
    }
}

trait HasArrayData {
    fn data(&self) -> &ArrayData;
}

trait FlatBufferArray<F: Verifiable>: HasArrayData {
    fn flatbuffer_metadata<'a>(&'a self) -> F
    where
        F: 'a,
        F: Default,
        F: Follow<'a, Inner = F>,
    {
        self.data()
            .encoding()
            .metadata()
            .map(|buffer| root::<F>(buffer.bytes()).unwrap())
            .unwrap_or_else(move || F::default())
    }
}

impl<A, F> FlatBufferArray<F> for A
where
    A: HasArrayData,
    F: Verifiable,
{
}

struct PrimitiveArray {
    ptype: PType,
    buffer: Buffer,
    validity: Option<Buffer>,
}

impl HasArrayData for PrimitiveArray {
    fn data(&self) -> &ArrayData {
        &self.0
    }
}

//
// struct PrimitiveArray<'a> {
//     encoding: FlatBuffer<PrimitiveEncoding<'a>>,
//     buffer: Buffer,
//     validity: Option<Buffer>,
// }

// Say we have some DictArray and we want to scan it.
struct DictEncoding {}

#[cfg(test)]
mod test {
    use flatbuffers::FlatBufferBuilder;
    use vortex_flatbuffers::flat::{
        FlatEncoding, FlatEncodingArgs, FlatUnion, PType, PrimitiveEncoding, PrimitiveEncodingArgs,
    };

    use crate::fb::FlatBuffer;

    fn write_encoding<'a>() -> FlatBuffer<FlatEncoding<'a>> {
        let mut fb = FlatBufferBuilder::new();
        let primitive =
            PrimitiveEncoding::create(&mut fb, &PrimitiveEncodingArgs { ptype: PType::I64 });

        let col = FlatEncoding::create(
            &mut fb,
            &FlatEncodingArgs {
                flat_type: FlatUnion::PrimitiveEncoding,
                flat: Some(primitive.as_union_value()),
            },
        );

        FlatBuffer::<FlatEncoding>::from_root(fb, col)
    }

    #[test]
    pub fn test_something() {
        let col_buf = write_encoding();
        println!("Buffer {:?}", col_buf);
        let col: FlatBuffer<FlatEncoding> = col_buf.try_into().unwrap();
        println!("Col {:?}", col);

        let c = col.as_typed();
        println!("C {:?}", c);
    }
}
