use std::mem;
use std::sync::{Arc, RwLock};

use linkme::distributed_slice;
use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::{DType, IntWidth, Nullability, Signedness};

use crate::array::validity::Validity;
use crate::array::{check_slice_bounds, Array, ArrayRef};
use crate::compute::flatten::flatten_primitive;
use crate::encoding::{Encoding, EncodingId, EncodingRef, ENCODINGS};
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{Stats, StatsSet};
use crate::{impl_array, impl_array_compute, ArrayWalker};

mod compute;
mod serde;

#[derive(Clone, Copy)]
#[repr(C, align(8))]
struct Inlined {
    size: u32,
    data: [u8; 12],
}

impl Inlined {
    #[allow(dead_code)]
    pub fn new(value: &str) -> Self {
        assert!(
            value.len() < 13,
            "Inlined strings must be shorter than 13 characters, {} given",
            value.len()
        );
        let mut inlined = Inlined {
            size: value.len() as u32,
            data: [0u8; 12],
        };
        inlined.data[..value.len()].copy_from_slice(value.as_bytes());
        inlined
    }
}

#[derive(Clone, Copy)]
#[repr(C, align(8))]
struct Ref {
    size: u32,
    prefix: [u8; 4],
    buffer_index: u32,
    offset: u32,
}

#[derive(Clone, Copy)]
#[repr(C, align(8))]
union BinaryView {
    inlined: Inlined,
    _ref: Ref,
}

impl BinaryView {
    #[inline]
    pub fn from_le_bytes(bytes: [u8; 16]) -> BinaryView {
        unsafe { mem::transmute(bytes) }
    }

    #[inline]
    #[allow(dead_code)]
    pub fn to_le_bytes(self) -> [u8; 16] {
        unsafe { mem::transmute(self) }
    }
}

pub const VIEW_SIZE: usize = std::mem::size_of::<BinaryView>();

#[derive(Debug, Clone)]
pub struct VarBinViewArray {
    views: ArrayRef,
    data: Vec<ArrayRef>,
    dtype: DType,
    validity: Option<Validity>,
    stats: Arc<RwLock<StatsSet>>,
}

impl VarBinViewArray {
    pub fn new(
        views: ArrayRef,
        data: Vec<ArrayRef>,
        dtype: DType,
        validity: Option<Validity>,
    ) -> Self {
        Self::try_new(views, data, dtype, validity).unwrap()
    }

    pub fn try_new(
        views: ArrayRef,
        data: Vec<ArrayRef>,
        dtype: DType,
        validity: Option<Validity>,
    ) -> VortexResult<Self> {
        if !matches!(
            views.dtype(),
            DType::Int(IntWidth::_8, Signedness::Unsigned, Nullability::NonNullable)
        ) {
            vortex_bail!(MismatchedTypes: "u8", views.dtype());
        }

        for d in data.iter() {
            if !matches!(
                d.dtype(),
                DType::Int(IntWidth::_8, Signedness::Unsigned, Nullability::NonNullable)
            ) {
                vortex_bail!(MismatchedTypes: "u8", d.dtype());
            }
        }

        if !matches!(dtype, DType::Binary(_) | DType::Utf8(_)) {
            vortex_bail!(MismatchedTypes: "utf8 or binary", dtype);
        }

        let dtype = if validity.is_some() && !dtype.is_nullable() {
            dtype.as_nullable()
        } else {
            dtype
        };

        Ok(Self {
            views,
            data,
            dtype,
            validity,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    pub fn plain_size(&self) -> usize {
        (0..self.views.len() / VIEW_SIZE).fold(0usize, |acc, i| {
            let view = self.view_at(i);
            unsafe { acc + view.inlined.size as usize }
        })
    }

    pub(self) fn view_at(&self, index: usize) -> BinaryView {
        let view_vec = flatten_primitive(
            self.views
                .slice(index * VIEW_SIZE, (index + 1) * VIEW_SIZE)
                .unwrap()
                .as_ref(),
        )
        .unwrap();
        BinaryView::from_le_bytes(view_vec.typed_data::<u8>().try_into().unwrap())
    }

    #[inline]
    pub fn views(&self) -> &ArrayRef {
        &self.views
    }

    #[inline]
    pub fn data(&self) -> &[ArrayRef] {
        &self.data
    }

    pub fn bytes_at(&self, index: usize) -> VortexResult<Vec<u8>> {
        let view = self.view_at(index);
        unsafe {
            if view.inlined.size > 12 {
                let arrow_data_buffer = flatten_primitive(
                    self.data
                        .get(view._ref.buffer_index as usize)
                        .unwrap()
                        .slice(
                            view._ref.offset as usize,
                            (view._ref.size + view._ref.offset) as usize,
                        )?
                        .as_ref(),
                )?;
                // TODO(ngates): can we avoid returning a copy?
                Ok(arrow_data_buffer.typed_data::<u8>().to_vec())
            } else {
                Ok(view.inlined.data[..view.inlined.size as usize].to_vec())
            }
        }
    }
}

impl Array for VarBinViewArray {
    impl_array!();
    impl_array_compute!();

    #[inline]
    fn len(&self) -> usize {
        self.views.len() / std::mem::size_of::<BinaryView>()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.views.is_empty()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;

        Ok(Self {
            views: self.views.slice(start * VIEW_SIZE, stop * VIEW_SIZE)?,
            data: self.data.clone(),
            dtype: self.dtype.clone(),
            validity: self.validity.as_ref().map(|v| v.slice(start, stop)),
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
        .into_array())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &VarBinViewEncoding
    }

    fn nbytes(&self) -> usize {
        self.views.nbytes() + self.data.iter().map(|arr| arr.nbytes()).sum::<usize>()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }

    fn validity(&self) -> Option<Validity> {
        self.validity.clone()
    }

    fn walk(&self, walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        walker.visit_child(self.views())?;
        for data in self.data() {
            walker.visit_child(data)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct VarBinViewEncoding;

impl VarBinViewEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.varbinview");
}

#[distributed_slice(ENCODINGS)]
static ENCODINGS_VARBINVIEW: EncodingRef = &VarBinViewEncoding;

impl Encoding for VarBinViewEncoding {
    fn id(&self) -> EncodingId {
        Self::ID
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}

impl ArrayDisplay for VarBinViewArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.child("views", self.views())?;
        for (i, d) in self.data().iter().enumerate() {
            f.child(&format!("data_{}", i), d.as_ref())?;
        }
        f.validity(self.validity())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::array::primitive::PrimitiveArray;
    use crate::compute::scalar_at::scalar_at;
    use crate::scalar::Scalar;

    fn binary_array() -> VarBinViewArray {
        let values = PrimitiveArray::from("hello world this is a long string".as_bytes().to_vec());
        let view1 = BinaryView {
            inlined: Inlined::new("hello world"),
        };
        let view2 = BinaryView {
            _ref: Ref {
                size: 33,
                prefix: "hell".as_bytes().try_into().unwrap(),
                buffer_index: 0,
                offset: 0,
            },
        };
        let view_arr = PrimitiveArray::from(
            vec![view1.to_le_bytes(), view2.to_le_bytes()]
                .into_iter()
                .flatten()
                .collect::<Vec<u8>>(),
        );

        VarBinViewArray::new(
            view_arr.into_array(),
            vec![values.into_array()],
            DType::Utf8(Nullability::NonNullable),
            None,
        )
    }

    #[test]
    pub fn varbin_view() {
        let binary_arr = binary_array();
        assert_eq!(binary_arr.len(), 2);
        assert_eq!(
            scalar_at(&binary_arr, 0).unwrap(),
            Scalar::from("hello world")
        );
        assert_eq!(
            scalar_at(&binary_arr, 1).unwrap(),
            Scalar::from("hello world this is a long string")
        );
    }

    #[test]
    pub fn slice() {
        let binary_arr = binary_array().slice(1, 2).unwrap();
        assert_eq!(
            scalar_at(&binary_arr, 0).unwrap(),
            Scalar::from("hello world this is a long string")
        );
    }
}
