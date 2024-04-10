mod compute;
mod data;
mod stats;

use arrow_buffer::{BooleanBuffer, Buffer};
use serde::{Deserialize, Serialize};
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::impl_encoding;
use crate::stats::Statistics;
use crate::validity::{ArrayValidity, ValidityMetadata};
use crate::validity::{LogicalValidity, Validity};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::ArrayMetadata;
use crate::ArrayView;
use crate::{ArrayData, TypedArrayData};

impl_encoding!("vortex.bool", Bool);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BoolMetadata {
    validity: ValidityMetadata,
    length: usize,
}

pub struct BoolArray<'a> {
    dtype: &'a DType,
    buffer: &'a Buffer,
    validity: Validity<'a>,
    length: usize,
    stats: &'a (dyn Statistics + 'a),
}

impl BoolArray<'_> {
    pub fn buffer(&self) -> BooleanBuffer {
        // TODO(ngates): look into whether we should store this on BoolArray
        BooleanBuffer::new(self.buffer.clone(), 0, self.length)
    }

    pub fn validity(&self) -> &Validity {
        &self.validity
    }
}

impl<'v> TryFromArrayParts<'v, BoolMetadata> for BoolArray<'v> {
    fn try_from_parts(parts: &'v dyn ArrayParts, metadata: &'v BoolMetadata) -> VortexResult<Self> {
        Ok(BoolArray {
            dtype: parts.dtype(),
            buffer: parts
                .buffer(0)
                .ok_or(vortex_err!("BoolArray requires a buffer"))?,
            validity: metadata
                .validity
                .to_validity(parts.child(0, &Validity::DTYPE)),
            length: metadata.length,
            stats: parts.statistics(),
        })
    }
}

pub fn bool_try_from_parts<'v: 'm, 'm>(
    parts: &'v dyn ArrayParts,
    metadata: &'m BoolMetadata,
) -> VortexResult<BoolArray<'m>> {
    Ok(BoolArray {
        dtype: parts.dtype(),
        buffer: parts
            .buffer(0)
            .ok_or(vortex_err!("BoolArray requires a buffer"))?,
        validity: metadata
            .validity
            .to_validity(parts.child(0, &Validity::DTYPE)),
        length: metadata.length,
        stats: parts.statistics(),
    })
}

impl ArrayTrait for BoolArray<'_> {
    fn dtype(&self) -> &DType {
        self.dtype
    }

    fn len(&self) -> usize {
        self.length
    }

    fn metadata(&self) -> Arc<dyn ArrayMetadata> {
        Arc::new(BoolMetadata {
            validity: self.validity.to_metadata(self.length).unwrap(),
            length: self.length,
        })
    }
}

impl ArrayValidity for BoolArray<'_> {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity.to_logical(self.len())
    }
}

impl AcceptArrayVisitor for BoolArray<'_> {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_buffer(self.buffer().inner())?;
        visitor.visit_validity(self.validity())
    }
}

#[cfg(test)]
mod tests {
    use crate::array::bool::BoolData;
    use crate::compute::scalar_at::scalar_at;
    use crate::IntoArray;

    #[test]
    fn bool_array() {
        let arr = BoolData::from(vec![true, false, true]).into_array();
        let scalar: bool = scalar_at(&arr, 0).unwrap().try_into().unwrap();
        assert!(scalar);
    }
}
