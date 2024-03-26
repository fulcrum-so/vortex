use std::marker::PhantomData;
use std::sync::Arc;

use arrow_buffer::{Buffer, ToByteSlice};
use flatbuffers::root;

use vortex_flatbuffers::encoding::FBArray;

type ArrayRef = Arc<dyn Array>;

trait Array {
    fn len(&self) -> usize;
}

trait ArrayVTable<'a> {
    fn len<'data: 'a>(&self, data: &'data ArrayView) -> usize;
}

struct ArrayCtx {
    encodings: Vec<EncodingRef>,
}

struct ArrayData {
    data: Buffer,
    ctx: Arc<ArrayCtx>,
}

impl ArrayData {
    // Take an immutable view over the ArrayData.
    fn as_view(&self) -> Result<ArrayView, ()> {
        let fbarray = root::<FBArray>(self.data.to_byte_slice()).map_err(|_| ())?;
        Ok(ArrayView {
            fb: fbarray,
            ctx: self.ctx.as_ref(),
        })
    }
}

#[derive(Clone)]
struct ArrayView<'a> {
    fb: FBArray<'a>,
    data: &'a ArrayData,
}

impl ToOwned for ArrayView {
    type Owned = ArrayData;

    fn to_owned(&self) -> Self::Owned {
        ArrayData {
            data: ,
            ctx: Arc::new(ArrayCtx {}),
        }
    }
}

impl<'a> ArrayView<'a> {
    fn as_typed<T>(&'a self) -> TypedArray<'a, T> {
        TypedArray {
            data: self.clone(),
            phantom: Default::default(),
        }
    }

    fn vtable(&self) -> Option<&dyn ArrayVTable> {
        self.ctx
            .encodings
            .get(self.fb.id() as usize)
            .map(|e| e.vtable())
    }

    fn child(&self, idx: usize) -> Option<ArrayView> {
        self.fb.children().map(|c| ArrayView {
            fb: c.get(idx),
            ctx: self.ctx,
        })
    }

    fn buffer(&self, idx: usize) -> Option<&[u8]> {
        self.fb
            .buffers()
            .and_then(|b| b.get(idx).bytes())
            .map(|b| b.bytes())
    }
}

struct TypedArray<'a, T> {
    data: ArrayView<'a>,
    phantom: PhantomData<T>,
}

/// Dictionary Encoding
struct DictEncodingPlugin;
impl EncodingPlugin for DictEncodingPlugin {
    fn vtable(&self) -> &dyn ArrayVTable {
        self
    }
}
struct DictArray<'a> {
    codes: &'a dyn Array,
    values: &'a dyn Array,
}
impl<'a> DictArray<'a> {
    fn new(codes: &'a dyn Array, values: &'a dyn Array) -> Self {
        Self { codes, values }
    }
}
type DictArrayData<'a> = TypedArray<'a, DictEncodingPlugin>;
impl<'a> DictArrayData<'a> {
    fn codes(&self) -> ArrayView {
        self.data.child(0).expect("DictArray missing codes child")
    }

    fn values(&self) -> ArrayView {
        self.data.child(1).expect("DictArray missing values child")
    }
}
impl<'a> Array for DictArrayData<'a> {
    fn len(&self) -> usize {
        self.codes().len()
    }
}

/// Primitive Encoding
struct PrimitiveEncodingPlugin;
impl EncodingPlugin for PrimitiveEncodingPlugin {
    fn vtable(&self) -> &dyn ArrayVTable {
        self
    }
}
struct PrimitiveArray {
    buffer: Buffer,
}
impl<'a> TypedArray<'a, PrimitiveEncodingPlugin> {
    fn buffer(&self) -> &[u8] {
        self.data.buffer(0).expect("PrimitiveArray missing buffer")
    }
}
impl<'a> Array for TypedArray<'a, PrimitiveEncodingPlugin> {
    fn len(&self) -> usize {
        self.buffer().len() / 4
    }
}

impl<'a, T> ArrayVTable<'a> for T
where
    TypedArray<'a, T>: Array,
{
    fn len<'data: 'a>(&self, data: &'data ArrayView) -> usize {
        data.as_typed::<T>().len()
    }
}

impl<'a> Array for ArrayView<'a> {
    fn len(&self) -> usize {
        self.vtable().expect("Invalid encoding").len(self)
    }
}

trait EncodingPlugin {
    fn vtable(&self) -> &dyn ArrayVTable;
}

type EncodingRef = &'static dyn EncodingPlugin;

#[cfg(test)]
mod test {
    use flatbuffers::{FlatBufferBuilder, root};

    use vortex_flatbuffers::encoding::{Buffer, BufferArgs, FBArray, FBArrayArgs};

    use crate::{Array, ArrayCtx, ArrayData, DictEncodingPlugin, PrimitiveEncodingPlugin};

    #[test]
    pub fn test_something() {
        let mut fbb = FlatBufferBuilder::new();

        let codes_bytes = fbb.create_vector(&[0i8, 0, 0, 0, 0i8, 0, 0, 0]);
        let codes_buffer = Buffer::create(
            &mut fbb,
            &BufferArgs {
                bytes: Some(codes_bytes),
            },
        );
        let codes_buffers = fbb.create_vector(&[codes_buffer]);
        let codes = FBArray::create(
            &mut fbb,
            &FBArrayArgs {
                id: 0,
                metadata: None,
                children: None,
                buffers: Some(codes_buffers),
            },
        );

        let values_bytes = fbb.create_vector(&[0i8, 0, 0, 5]);
        let values_buffer = Buffer::create(
            &mut fbb,
            &BufferArgs {
                bytes: Some(values_bytes),
            },
        );
        let values_buffers = fbb.create_vector(&[values_buffer]);
        let values = FBArray::create(
            &mut fbb,
            &FBArrayArgs {
                id: 0,
                metadata: None,
                children: None,
                buffers: Some(values_buffers),
            },
        );

        let children = fbb.create_vector(&[codes, values]);

        let dict_array = FBArray::create(
            &mut fbb,
            &FBArrayArgs {
                id: 1,
                metadata: None,
                children: Some(children),
                buffers: None,
            },
        );

        fbb.finish_minimal(dict_array);
        let root = root::<FBArray>(fbb.finished_data()).unwrap();

        let array_data = ArrayData {
            fb: root,
            ctx: &ArrayCtx {
                encodings: vec![&PrimitiveEncodingPlugin, &DictEncodingPlugin],
            },
        };

        assert_eq!(array_data.len(), 2);
    }
}
