use std::fmt::Formatter;
use std::ops::Deref;
use std::{mem, slice};

use ::serde::{Deserialize, Serialize};
use arrow_array::{ArrayRef, BinaryViewArray, StringViewArray};
use arrow_buffer::{Buffer, ScalarBuffer};
use arrow_schema::DataType;
use itertools::Itertools;
use vortex_dtype::{Nullability, PType};
use vortex_error::vortex_bail;

use crate::array::primitive::PrimitiveArray;
use crate::array::varbin::VarBinArray;
use crate::array::varbinview::builder::VarBinViewBuilder;
use crate::arrow::FromArrowArray;
use crate::compute::slice::slice;
use crate::validity::Validity;
use crate::validity::{ArrayValidity, LogicalValidity, ValidityMetadata};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::{impl_encoding, ArrayDType, ArrayData, Canonical, IntoCanonical};

mod accessor;
mod builder;
mod compute;
mod stats;

#[derive(Clone, Copy, Debug)]
#[repr(C, align(8))]
struct Inlined {
    size: u32,
    data: [u8; BinaryView::MAX_INLINED_SIZE],
}

impl Inlined {
    #[allow(dead_code)]
    pub fn new(value: &[u8]) -> Self {
        assert!(
            value.len() <= BinaryView::MAX_INLINED_SIZE,
            "Inlined strings must be shorter than 13 characters, {} given",
            value.len()
        );
        let mut inlined = Self {
            size: value.len() as u32,
            data: [0u8; BinaryView::MAX_INLINED_SIZE],
        };
        inlined.data[..value.len()].copy_from_slice(value);
        inlined
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(C, align(8))]
struct Ref {
    size: u32,
    prefix: [u8; 4],
    buffer_index: u32,
    offset: u32,
}

impl Ref {
    pub fn new(size: u32, prefix: [u8; 4], buffer_index: u32, offset: u32) -> Self {
        Self {
            size,
            prefix,
            buffer_index,
            offset,
        }
    }
}

#[derive(Clone, Copy)]
#[repr(C, align(8))]
pub union BinaryView {
    inlined: Inlined,
    _ref: Ref,
}

impl BinaryView {
    pub const MAX_INLINED_SIZE: usize = 12;

    #[inline]
    pub fn size(&self) -> usize {
        unsafe { self.inlined.size as usize }
    }

    pub fn is_inlined(&self) -> bool {
        unsafe { self.inlined.size <= Self::MAX_INLINED_SIZE as u32 }
    }
}

impl Debug for BinaryView {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("BinaryView");
        if self.is_inlined() {
            s.field("inline", unsafe { &self.inlined });
        } else {
            s.field("ref", unsafe { &self._ref });
        }
        s.finish()
    }
}

pub const VIEW_SIZE: usize = mem::size_of::<BinaryView>();

impl_encoding!("vortex.varbinview", VarBinView);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VarBinViewMetadata {
    validity: ValidityMetadata,
    n_children: usize,
}

impl VarBinViewArray {
    pub fn try_new(
        views: Array,
        data: Vec<Array>,
        dtype: DType,
        validity: Validity,
    ) -> VortexResult<Self> {
        if !matches!(views.dtype(), &DType::BYTES) {
            vortex_bail!(MismatchedTypes: "u8", views.dtype());
        }

        for d in data.iter() {
            if !matches!(d.dtype(), &DType::BYTES) {
                vortex_bail!(MismatchedTypes: "u8", d.dtype());
            }
        }

        if !matches!(dtype, DType::Binary(_) | DType::Utf8(_)) {
            vortex_bail!(MismatchedTypes: "utf8 or binary", dtype);
        }

        if dtype.is_nullable() == (validity == Validity::NonNullable) {
            vortex_bail!("incorrect validity {:?}", validity);
        }

        let metadata = VarBinViewMetadata {
            validity: validity.to_metadata(views.len() / VIEW_SIZE)?,
            n_children: data.len(),
        };

        let mut children = Vec::with_capacity(data.len() + 2);
        children.push(views);
        children.extend(data);
        if let Some(a) = validity.into_array() {
            children.push(a)
        }

        Self::try_from_parts(dtype, metadata, children.into(), StatsSet::new())
    }

    fn view_slice(&self) -> &[BinaryView] {
        unsafe {
            slice::from_raw_parts(
                PrimitiveArray::try_from(self.views())
                    .expect("Views must be a primitive array")
                    .maybe_null_slice::<u8>()
                    .as_ptr() as _,
                self.views().len() / VIEW_SIZE,
            )
        }
    }

    fn view_at(&self, index: usize) -> BinaryView {
        self.view_slice()[index]
    }

    #[inline]
    pub fn views(&self) -> Array {
        self.array().child(0, &DType::BYTES).expect("missing views")
    }

    #[inline]
    pub fn bytes(&self, idx: usize) -> Array {
        self.array()
            .child(idx + 1, &DType::BYTES)
            .expect("Missing data buffer")
    }

    pub fn validity(&self) -> Validity {
        self.metadata().validity.to_validity(
            self.array()
                .child(self.metadata().n_children + 1, &Validity::DTYPE),
        )
    }

    pub fn from_vec<T: AsRef<[u8]>>(vec: Vec<T>, dtype: DType) -> Self {
        let mut builder = VarBinViewBuilder::with_capacity(vec.len());
        for v in vec {
            builder.push_value(v)
        }
        builder.finish(dtype)
    }

    pub fn from_iter<T: AsRef<[u8]>, I: IntoIterator<Item = Option<T>>>(
        iter: I,
        dtype: DType,
    ) -> Self {
        let iter = iter.into_iter();
        let mut builder = VarBinViewBuilder::with_capacity(iter.size_hint().0);
        for v in iter {
            builder.push(v)
        }
        builder.finish(dtype)
    }

    pub fn bytes_at(&self, index: usize) -> VortexResult<Vec<u8>> {
        let view = self.view_at(index);
        unsafe {
            if view.inlined.size > 12 {
                let data_buf = slice(
                    &self.bytes(view._ref.buffer_index as usize),
                    view._ref.offset as usize,
                    (view._ref.size + view._ref.offset) as usize,
                )?
                .into_canonical()?
                .into_primitive()?;
                Ok(data_buf.maybe_null_slice::<u8>().to_vec())
            } else {
                Ok(view.inlined.data[..view.inlined.size as usize].to_vec())
            }
        }
    }
}

impl IntoCanonical for VarBinViewArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        let nullable = self.dtype().is_nullable();
        let arrow_self = as_arrow(self);
        let arrow_varbin = arrow_cast::cast(arrow_self.deref(), &DataType::Utf8)
            .expect("Utf8View must cast to Ut8f");
        let vortex_array = ArrayData::from_arrow(arrow_varbin, nullable).into_array();

        Ok(Canonical::VarBin(VarBinArray::try_from(&vortex_array)?))
    }
}

fn as_arrow(var_bin_view: VarBinViewArray) -> ArrayRef {
    // Views should be buffer of u8
    let views = var_bin_view
        .views()
        .into_canonical()
        .expect("into_canonical")
        .into_primitive()
        .expect("views must be primitive");
    assert_eq!(views.ptype(), PType::U8);
    let nulls = var_bin_view
        .logical_validity()
        .to_null_buffer()
        .expect("null buffer");

    let data = (0..var_bin_view.metadata().n_children)
        .map(|i| {
            var_bin_view
                .bytes(i)
                .into_canonical()
                .and_then(Canonical::into_primitive)
        })
        .collect::<VortexResult<Vec<_>>>()
        .expect("bytes arrays must be primitive");
    if !data.is_empty() {
        assert_eq!(data[0].ptype(), PType::U8);
        assert!(data.iter().map(|d| d.ptype()).all_equal());
    }

    let data = data
        .iter()
        .map(|p| Buffer::from(p.buffer()))
        .collect::<Vec<_>>();

    // Switch on Arrow DType.
    match var_bin_view.dtype() {
        DType::Binary(_) => Arc::new(BinaryViewArray::new(
            ScalarBuffer::<u128>::from(Buffer::from(views.buffer())),
            data,
            nulls,
        )),
        DType::Utf8(_) => Arc::new(StringViewArray::new(
            ScalarBuffer::<u128>::from(Buffer::from(views.buffer())),
            data,
            nulls,
        )),
        _ => panic!("expected utf8 or binary, got {}", var_bin_view.dtype()),
    }
}

impl ArrayValidity for VarBinViewArray {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
    }
}

impl AcceptArrayVisitor for VarBinViewArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("views", &self.views())?;
        for i in 0..self.metadata().n_children {
            visitor.visit_child(format!("bytes_{i}").as_str(), &self.bytes(i))?;
        }
        visitor.visit_validity(&self.validity())
    }
}

impl ArrayTrait for VarBinViewArray {
    fn len(&self) -> usize {
        self.view_slice().len()
    }
}

impl From<Vec<&[u8]>> for VarBinViewArray {
    fn from(value: Vec<&[u8]>) -> Self {
        Self::from_vec(value, DType::Binary(Nullability::NonNullable))
    }
}

impl From<Vec<Vec<u8>>> for VarBinViewArray {
    fn from(value: Vec<Vec<u8>>) -> Self {
        Self::from_vec(value, DType::Binary(Nullability::NonNullable))
    }
}

impl From<Vec<String>> for VarBinViewArray {
    fn from(value: Vec<String>) -> Self {
        Self::from_vec(value, DType::Utf8(Nullability::NonNullable))
    }
}

impl From<Vec<&str>> for VarBinViewArray {
    fn from(value: Vec<&str>) -> Self {
        Self::from_vec(value, DType::Utf8(Nullability::NonNullable))
    }
}

impl<'a> FromIterator<Option<&'a [u8]>> for VarBinViewArray {
    fn from_iter<T: IntoIterator<Item = Option<&'a [u8]>>>(iter: T) -> Self {
        Self::from_iter(iter, DType::Binary(Nullability::NonNullable))
    }
}

impl FromIterator<Option<Vec<u8>>> for VarBinViewArray {
    fn from_iter<T: IntoIterator<Item = Option<Vec<u8>>>>(iter: T) -> Self {
        Self::from_iter(iter, DType::Binary(Nullability::NonNullable))
    }
}

impl FromIterator<Option<String>> for VarBinViewArray {
    fn from_iter<T: IntoIterator<Item = Option<String>>>(iter: T) -> Self {
        Self::from_iter(iter, DType::Utf8(Nullability::NonNullable))
    }
}

impl<'a> FromIterator<Option<&'a str>> for VarBinViewArray {
    fn from_iter<T: IntoIterator<Item = Option<&'a str>>>(iter: T) -> Self {
        Self::from_iter(iter, DType::Utf8(Nullability::NonNullable))
    }
}

#[cfg(test)]
mod test {
    use vortex_scalar::Scalar;

    use crate::array::varbinview::VarBinViewArray;
    use crate::compute::slice::slice;
    use crate::compute::unary::scalar_at::scalar_at;
    use crate::{ArrayTrait, Canonical, IntoArray, IntoCanonical};

    #[test]
    pub fn varbin_view() {
        let binary_arr =
            VarBinViewArray::from(vec!["hello world", "hello world this is a long string"]);
        assert_eq!(binary_arr.len(), 2);
        assert_eq!(
            scalar_at(binary_arr.array(), 0).unwrap(),
            Scalar::from("hello world")
        );
        assert_eq!(
            scalar_at(binary_arr.array(), 1).unwrap(),
            Scalar::from("hello world this is a long string")
        );
    }

    #[test]
    pub fn slice_array() {
        let binary_arr = slice(
            &VarBinViewArray::from(vec!["hello world", "hello world this is a long string"])
                .into_array(),
            1,
            2,
        )
        .unwrap();
        assert_eq!(
            scalar_at(&binary_arr, 0).unwrap(),
            Scalar::from("hello world this is a long string")
        );
    }

    #[test]
    pub fn flatten_array() {
        let binary_arr = VarBinViewArray::from(vec!["string1", "string2"]);

        let flattened = binary_arr.into_canonical().unwrap();
        assert!(matches!(flattened, Canonical::VarBin(_)));

        let var_bin = flattened.into_array();
        assert_eq!(scalar_at(&var_bin, 0).unwrap(), Scalar::from("string1"));
        assert_eq!(scalar_at(&var_bin, 1).unwrap(), Scalar::from("string2"));
    }
}
