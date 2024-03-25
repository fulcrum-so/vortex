use std::marker::PhantomData;
use std::sync::Arc;

use arrow_buffer::Buffer;
use flatbuffers::{Follow, Verifiable};

use vortex_flatbuffers::encoding::FBArray;
use vortex_schema::DType;

mod fb;

type ArrayRef = Arc<dyn Array>;

trait Array {
    fn len(&self) -> usize;
    fn to_array(&self) -> ArrayRef;
}

struct ArrayCtx {
    encodings: Vec<EncodingRef>,
}

struct ArrayData<'a> {
    fb: FBArray<'a>,
    ctx: &'a ArrayCtx,
}

struct TypedArray<'a, T> {
    fb: &'a FBArray<'a>,
    ctx: &'a ArrayCtx,
    phantom: PhantomData<T>,
}

impl<'a, T> TypedArray<'a, T> {
    fn child(&self, idx: usize) -> ArrayData {
        ArrayData {
            fb: self.fb.children().unwrap().get(idx),
            ctx: self.ctx,
        }
    }
}

impl<'a> TypedArray<'a, DictArray> {
    fn codes(&self) -> ArrayData {
        self.child(0)
    }

    fn values(&self) -> ArrayData {
        self.child(1)
    }
}

impl<'a> Array for TypedArray<'a, DictArray> {
    fn len(&self) -> usize {
        self.codes().len()
    }

    fn to_array(&self) -> ArrayRef {
        todo!()
    }
}

impl EncodingPlugin for DictEncodingPlugin {
    fn array<'a>(&self, data: &'a ArrayData, out: &mut dyn Array) {
        *out = &TypedArray {
            fb: &data.fb,
            ctx: data.ctx,
            phantom: Default::default(),
        };
    }
}

impl<'a> Array for ArrayData<'a> {
    fn len(&self) -> usize {
        let encoding = self.ctx.encodings[self.fb.id() as usize];
        encoding.array(self).len()
        // match self.fb.id() {
        //     0 => TypedArray::<DictArray> {
        //         fb: &self.fb,
        //         ctx: self.ctx,
        //         phantom: Default::default(),
        //     }
        //     .len(),
        //     _ => panic!("unsupported"),
        // }
    }

    fn to_array(&self) -> ArrayRef {
        todo!()
    }
}

trait EncodingPlugin {
    fn array<'a>(&self, data: &'a ArrayData, out: &mut dyn Array);
}

type EncodingRef = &'static dyn EncodingPlugin;

struct DictEncodingPlugin;

struct PrimitiveArray<'a> {
    buffer: &'a Buffer,
}

struct DictArray {
    codes: ArrayRef,
    values: ArrayRef,
}

struct BorrowedDictArray<'a> {
    codes: &'a dyn Array,
    values: &'a dyn Array,
}

struct SerializedArray {
    dtype: DType,
    encoding_specs: Vec<EncodingRef>,
    encoding_idx: u16,
    metadata: Buffer,
    children: Vec<SerializedArray>,
    buffers: Vec<Buffer>,
}

impl SerializedArray {
    fn with_children<T, F: Fn(Vec<&SerializedArray>) -> T>(&self, f: F) -> T {
        let refs = self.children.iter().map(|c| c).collect();
        f(refs)
    }
}

impl Array for &SerializedArray {
    fn len(&self) -> usize {
        todo!()
    }

    fn to_array(&self) -> ArrayRef {
        todo!()
    }
}

struct FlatBufferArray<T> {
    data: SerializedArray,
    metadata: T,
}

trait DictEncodedArray {
    fn with_children<T, F: Fn((&dyn Array, &dyn Array)) -> T>(&self, f: F) -> T;
}

struct DictFlatBuffer<'a> {
    codes: &'a [u8],
    values: &'a [u8],
}

trait ArrayCompute {
    fn take(&self, indices: &dyn Array) -> ArrayRef;
}

trait ArrayComputeDispatch<T> {
    fn take(&self, array: T, indices: &dyn Array) -> ArrayRef;
}

//
// impl<'a> ArrayCompute for DictArray<'a> {
//     fn take(&self, indices: &dyn Array) -> ArrayRef {
//         Arc::new(DictArray {
//             codes: self.codes.take(indices),
//             values: self.values.to_array(),
//         })
//     }
// }

#[cfg(test)]
mod test {
    use vortex_schema::DType;
    use vortex_schema::IntWidth::_32;
    use vortex_schema::Nullability::Nullable;
    use vortex_schema::Signedness::Signed;

    use crate::SerializedArray;

    #[test]
    pub fn test_something() {
        let data = SerializedArray {
            dtype: DType::Int(_32, Signed, Nullable),
            encoding_specs: vec![],
            encoding_idx: 0,
            children: vec![
                SerializedArray {
                    dtype: DType::Int(_32, Signed, Nullable),
                    encoding_specs: vec![],
                    encoding_idx: 0,
                    children: vec![],
                    metadata: vec![].into(),
                    buffers: vec![].into(),
                },
                SerializedArray {
                    dtype: DType::Int(_32, Signed, Nullable),
                    encoding_specs: vec![],
                    encoding_idx: 0,
                    children: vec![],
                    metadata: vec![].into(),
                    buffers: vec![].into(),
                },
            ],
            metadata: vec![].into(),
            buffers: vec![].into(),
        };
    }
}
