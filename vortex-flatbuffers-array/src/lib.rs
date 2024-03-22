use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::io::Read;
use std::sync::Arc;

use arrow_buffer::Buffer;
use flatbuffers::{root, Follow, Verifiable};

use vortex::array::ArrayRef;
use vortex::compute::patch::PatchFn;
use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::take::TakeFn;
use vortex::error::VortexResult;
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::scalar::Scalar;
use vortex::stats::Stats;
use vortex_flatbuffers::column::Array;
use vortex_schema::DType;

use crate::fb::Flat;

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
    fn len(&self) -> usize;
}

trait ArrayCompute {}

trait WithCompute {
    fn with_compute<T, F>(&self, closure: F) -> T
    where
        F: FnOnce(&dyn ArrayCompute) -> T;
}

trait Encoding: Sync {
    fn compute(&self, array_data: &ArrayData) -> Box<dyn ArrayCompute>;
}

type EncodingRef = &'static dyn Encoding;

#[derive(Clone)]
struct ReaderCtx {
    dtype: DType,
    encodings: Vec<EncodingRef>,
}

struct VortexArray<M> {
    metadata: M,
}

#[derive(Clone)]
struct ArrayData<'a> {
    ctx: Arc<ReaderCtx>,
    metadata: Array<'a>,
    buffers: Arc<[Buffer]>,
    encoding: EncodingRef,
}

impl<'a> ArrayData<'a> {
    fn new(ctx: Arc<ReaderCtx>, metadata: Array<'a>, buffers: Arc<[Buffer]>) -> Self {
        Self {
            ctx,
            metadata,
            buffers,
            encoding: ctx.encodings[0],
        }
    }

    fn with_child<T, F>(&self, _child_idx: usize, closure: F) -> T
    where
        F: FnOnce(ArrayData<'a>) -> T,
    {
        // TODO(ngates): construct the child
        let child = self.clone();
        closure(child)
    }

    fn as_array(&self) -> ArrayRef {
        todo!()
    }
}

impl<'a> ArrayDisplay for ArrayData<'a> {
    fn fmt(&self, fmt: &'_ mut ArrayFormatter) -> std::fmt::Result {
        todo!()
    }
}

impl<'a> WithCompute for ArrayData<'a> {
    fn with_compute<T, F>(&self, closure: F) -> T
    where
        F: FnOnce(&dyn ArrayCompute) -> T,
    {
        let array_compute = self.encoding.compute(self);
        closure(array_compute.as_ref())
    }
}

impl<'a> ArrayCompute for ArrayData<'a> {}

// Say we have some DictArray and we want to scan it.
struct DictArrayData<'a>(ArrayData<'a>);

impl<'a> DictArrayData<'a> {
    fn codes(&self) -> ArrayData {
        ArrayData::new(
            self.0.ctx.clone(),
            self.0.metadata.codes().unwrap(),
            self.0.buffers.clone(),
        )
    }

    fn with_codes<T>(&self, closure: impl FnOnce(ArrayData<'a>) -> T) -> T {
        self.0.with_child(0, closure)
    }

    fn with_values<T>(&self, closure: impl FnOnce(ArrayData<'a>) -> T) -> T {
        self.0.with_child(1, closure)
    }
}

impl<'a> ScalarAtFn for DictArrayData<'a> {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let code: usize = self
            .with_codes(|codes| scalar_at(&codes, index))?
            .try_into()?;
        self.with_values(|values| scalar_at(&values, code))
    }
}

#[cfg(test)]
mod test {
    use flatbuffers::FlatBufferBuilder;

    use vortex_flatbuffers::column::{Column, ColumnArgs};

    use crate::fb::Flat;

    fn write_column<'a>() -> Flat<Column<'a>> {
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
