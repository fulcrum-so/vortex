use std::any::Any;
use std::sync::Arc;

use arrow_buffer::Buffer;

use vortex_error::{vortex_err, VortexResult};
use vortex_schema::DType;

use crate::array::{Array, ArrayRef, PrimitiveArray};
use crate::array::primitive::compute::PrimitiveTrait;
use crate::array::validity::{Validity, ValidityView};
use crate::ArrayWalker;
use crate::compute::ArrayCompute;
use crate::encoding::EncodingRef;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::ptype::PType;
use crate::serde::ArrayView;
use crate::stats::Stats;

#[derive(Debug)]
pub struct PrimitiveView<'a> {
    ptype: PType,
    buffer: &'a Buffer,
    validity: Option<ValidityView<'a>>,
}

impl<'a> PrimitiveView<'a> {
    pub fn try_new(view: &'a ArrayView<'a>) -> VortexResult<Self> {
        // TODO(ngates): validate the number of buffers / children. We could even extract them?
        let ptype = PType::try_from(view.dtype())?;
        let buffer = view
            .buffers()
            .first()
            .ok_or_else(|| vortex_err!(InvalidSerde: "Missing primitive buffer"))?;
        let validity = view.child(0, &Validity::DTYPE).map(ValidityView::from);

        Ok(Self {
            ptype,
            buffer,
            validity,
        })
    }

    pub fn ptype(&self) -> PType {
        self.ptype
    }
}

impl Array for PrimitiveView<'_> {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        todo!()
    }

    fn to_array(&self) -> ArrayRef {
        self.to_primitive().into_array()
    }

    fn into_array(self) -> ArrayRef {
        todo!()
    }

    fn len(&self) -> usize {
        todo!()
    }

    fn is_empty(&self) -> bool {
        todo!()
    }

    fn dtype(&self) -> &DType {
        todo!()
    }

    fn stats(&self) -> Stats {
        todo!()
    }

    fn validity(&self) -> Option<Validity> {
        todo!()
    }

    fn slice(&self, _start: usize, _stop: usize) -> VortexResult<ArrayRef> {
        todo!()
    }

    fn encoding(&self) -> EncodingRef {
        todo!()
    }

    fn nbytes(&self) -> usize {
        todo!()
    }

    fn with_compute_mut(
        &self,
        _f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()> {
        todo!()
    }

    fn walk(&self, _walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        todo!()
    }
}

impl ArrayDisplay for PrimitiveView<'_> {
    fn fmt(&self, _fmt: &'_ mut ArrayFormatter) -> std::fmt::Result {
        todo!()
    }
}

// Do I want to implement Array for &dyn PrimitiveTrait?
// Or enforce PrimitiveTrait: Array?
impl PrimitiveTrait for PrimitiveView<'_> {
    fn ptype(&self) -> PType {
        self.ptype
    }

    fn validity_view(&self) -> Option<ValidityView> {
        self.validity.clone()
    }

    fn buffer(&self) -> &Buffer {
        self.buffer
    }

    fn to_primitive(&self) -> PrimitiveArray {
        PrimitiveArray::new(
            self.ptype(),
            self.buffer.clone(),
            self.validity.as_ref().map(|v| v.to_validity()),
        )
    }
}
