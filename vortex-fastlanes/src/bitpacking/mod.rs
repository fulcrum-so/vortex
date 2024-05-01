use ::serde::{Deserialize, Serialize};
pub use compress::*;
use vortex::array::primitive::PrimitiveArray;
use vortex::stats::ArrayStatisticsCompute;
use vortex::validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{impl_encoding, ArrayDType, ArrayFlatten, IntoArrayData};
use vortex_error::{vortex_bail, vortex_err};

mod compress;
mod compute;

impl_encoding!("fastlanes.bitpacked", BitPacked);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitPackedMetadata {
    validity: ValidityMetadata,
    patches_dtype: Option<DType>,
    bit_width: usize,
    offset: usize,
    length: usize,
}

/// NB: All non-null values in the patches array are considered patches
impl BitPackedArray<'_> {
    pub fn try_new(
        packed: Array,
        validity: Validity,
        patches: Option<Array>,
        bit_width: usize,
        dtype: DType,
        len: usize,
    ) -> VortexResult<Self> {
        Self::try_new_from_offset(packed, validity, patches, bit_width, dtype, len, 0)
    }

    pub(crate) fn try_new_from_offset(
        packed: Array,
        validity: Validity,
        patches: Option<Array>,
        bit_width: usize,
        dtype: DType,
        length: usize,
        offset: usize,
    ) -> VortexResult<Self> {
        if packed.dtype() != &DType::BYTES {
            vortex_bail!(MismatchedTypes: DType::BYTES, packed.dtype());
        }
        if bit_width > 64 {
            vortex_bail!("Unsupported bit width {}", bit_width);
        }
        if !dtype.is_int() {
            vortex_bail!(MismatchedTypes: "int", dtype);
        }

        let expected_packed_size = ((length + 1023) / 1024) * 128 * bit_width;
        if packed.len() != expected_packed_size {
            return Err(vortex_err!(
                "Expected {} packed bytes, got {}",
                expected_packed_size,
                packed.len()
            ));
        }

        let metadata = BitPackedMetadata {
            validity: validity.to_metadata(length)?,
            patches_dtype: patches.as_ref().map(|p| p.dtype().as_nullable()),
            offset,
            length,
            bit_width,
        };

        let mut children = Vec::with_capacity(3);
        children.push(packed.into_array_data());
        if let Some(p) = patches {
            children.push(p.into_array_data());
        }
        if let Some(a) = validity.into_array_data() {
            children.push(a)
        }

        Self::try_from_parts(dtype, metadata, children.into(), StatsSet::new())
    }

    #[inline]
    pub fn packed(&self) -> Array {
        self.array()
            .child(0, &DType::BYTES)
            .expect("Missing packed array")
    }

    #[inline]
    pub fn bit_width(&self) -> usize {
        self.metadata().bit_width
    }

    #[inline]
    pub fn patches(&self) -> Option<Array> {
        self.metadata().patches_dtype.as_ref().map(|pd| {
            self.array()
                .child(1, pd)
                .expect("Missing patches with present metadata flag")
        })
    }

    #[inline]
    pub fn offset(&self) -> usize {
        self.metadata().offset
    }

    pub fn validity(&self) -> Validity {
        self.metadata().validity.to_validity(self.array().child(
            if self.metadata().patches_dtype.is_some() {
                2
            } else {
                1
            },
            &Validity::DTYPE,
        ))
    }

    pub fn encode(array: Array<'_>, bit_width: usize) -> VortexResult<BitPackedArray> {
        if let Ok(parray) = PrimitiveArray::try_from(array) {
            Ok(bitpack_encode(parray, bit_width)?)
        } else {
            vortex_bail!("Bitpacking can only encode primitive arrays");
        }
    }
}

impl ArrayFlatten for BitPackedArray<'_> {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a,
    {
        unpack(self).map(Flattened::Primitive)
    }
}

impl ArrayValidity for BitPackedArray<'_> {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
    }
}

impl AcceptArrayVisitor for BitPackedArray<'_> {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("packed", &self.packed())?;
        if self.metadata().patches_dtype.is_some() {
            visitor.visit_child(
                "patches",
                &self.patches().expect("Expected patches to be present "),
            )?;
        }
        visitor.visit_validity(&self.validity())
    }
}

impl ArrayStatisticsCompute for BitPackedArray<'_> {}

impl ArrayTrait for BitPackedArray<'_> {
    fn len(&self) -> usize {
        self.metadata().length
    }

    fn nbytes(&self) -> usize {
        // Ignore any overheads like padding or the bit-width flag.
        let packed_size = ((self.bit_width() * self.len()) + 7) / 8;
        packed_size + self.patches().map(|p| p.nbytes()).unwrap_or(0)
    }
}

#[macro_export]
macro_rules! match_integers_by_width {
    ($self:expr, | $_:tt $enc:ident | $($body:tt)*) => ({
        macro_rules! __with__ {( $_ $enc:ident ) => ( $($body)* )}
        use vortex_dtype::PType;
        use vortex_error::vortex_bail;
        match $self {
            PType::I8 | PType::U8 => __with__! { u8 },
            PType::I16 | PType::U16 => __with__! { u16 },
            PType::I32 | PType::U32 => __with__! { u32 },
            PType::I64 | PType::U64 => __with__! { u64 },
            _ => vortex_bail!("Unsupported ptype {}", $self),
        }
    })
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use vortex::array::primitive::PrimitiveArray;
    use vortex::compress::{CompressConfig, CompressCtx};
    use vortex::compute::scalar_at::scalar_at;
    use vortex::compute::slice::slice;
    use vortex::encoding::EncodingRef;
    use vortex::IntoArray;

    use crate::{BitPackedArray, BitPackedEncoding};

    #[test]
    fn slice_within_block() {
        let cfg = CompressConfig::new().with_enabled([&BitPackedEncoding as EncodingRef]);
        let ctx = CompressCtx::new(Arc::new(cfg));

        let compressed = slice(
            &ctx.compress(
                PrimitiveArray::from((0..10_000).map(|i| (i % 63) as u8).collect::<Vec<_>>())
                    .array(),
                None,
            )
            .unwrap(),
            768,
            9999,
        )
        .unwrap();
        assert_eq!(
            scalar_at(&compressed, 0).unwrap(),
            ((768 % 63) as u8).into()
        );
        assert_eq!(
            scalar_at(&compressed, compressed.len() - 1).unwrap(),
            ((9998 % 63) as u8).into()
        );
    }

    #[test]
    fn slice_block_boundary() {
        let cfg = CompressConfig::new().with_enabled([&BitPackedEncoding as EncodingRef]);
        let ctx = CompressCtx::new(Arc::new(cfg));

        let compressed = slice(
            &ctx.compress(
                PrimitiveArray::from((0..10_000).map(|i| (i % 63) as u8).collect::<Vec<_>>())
                    .array(),
                None,
            )
            .unwrap(),
            7168,
            9216,
        )
        .unwrap();
        assert_eq!(
            scalar_at(&compressed, 0).unwrap(),
            ((7168 % 63) as u8).into()
        );
        assert_eq!(
            scalar_at(&compressed, compressed.len() - 1).unwrap(),
            ((9215 % 63) as u8).into()
        );
    }

    #[test]
    fn test_encode() {
        let values = vec![Some(1), None, Some(1), None, Some(1), None, Some(u64::MAX)];
        let uncompressed = PrimitiveArray::from_nullable_vec(values);
        let packed = BitPackedArray::encode(uncompressed.into_array(), 1).unwrap();
        let expected = &[1, 0, 1, 0, 1, 0, u64::MAX];
        let results = packed
            .into_array()
            .flatten_primitive()
            .unwrap()
            .typed_data::<u64>()
            .to_vec();
        assert_eq!(results, expected);
    }

    #[test]
    fn test_encode_too_wide() {
        let values = vec![Some(1u8), None, Some(1), None, Some(1), None];
        let uncompressed = PrimitiveArray::from_nullable_vec(values);
        let _packed = BitPackedArray::encode(uncompressed.clone().into_array(), 8)
            .expect_err("Cannot pack value into the same width");
        let _packed = BitPackedArray::encode(uncompressed.into_array(), 9)
            .expect_err("Cannot pack value into larger width");
    }
}
