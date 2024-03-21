use std::io::Read;

use flatbuffers::Follow;

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

struct Reader<R: Read> {
    read: R,
}

impl<R: Read> Reader<R> {
    pub fn new(read: R) -> Self {
        Self { read }
    }
}

#[cfg(test)]
mod test {
    #[test]
    pub fn test_something() {}
}
