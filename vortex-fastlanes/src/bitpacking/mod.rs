use std::cmp::min;
use std::sync::{Arc, RwLock};

pub use compress::*;
use vortex::{ArrayWalker, impl_array, impl_array_compute};
use vortex::array::{Array, ArrayRef};
use vortex::array::validity::Validity;
use vortex::compress::EncodingCompression;
use vortex::compute::flatten::flatten_primitive;
use vortex::encoding::{Encoding, EncodingId, EncodingRef};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stat, Stats, StatsCompute, StatsSet};
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_schema::{DType, IntWidth, Nullability, Signedness};

mod compress;
mod compute;
mod serde;

#[derive(Debug, Clone)]
pub struct BitPackedArray {
    encoded: ArrayRef,
    validity: Option<Validity>,
    patches: Option<ArrayRef>,
    len: usize,
    bit_width: usize,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl BitPackedArray {
    const ENCODED_DTYPE: DType =
        DType::Int(IntWidth::_8, Signedness::Unsigned, Nullability::NonNullable);

    pub fn try_new(
        encoded: ArrayRef,
        validity: Option<Validity>,
        patches: Option<ArrayRef>,
        bit_width: usize,
        dtype: DType,
        len: usize,
    ) -> VortexResult<Self> {
        if encoded.dtype() != &Self::ENCODED_DTYPE {
            vortex_bail!(MismatchedTypes: Self::ENCODED_DTYPE, encoded.dtype());
        }
        if let Some(v) = &validity {
            assert_eq!(v.len(), len);
        }
        if bit_width > 64 {
            return Err(vortex_err!("Unsupported bit width {}", bit_width));
        }
        if !matches!(dtype, DType::Int(_, _, _)) {
            return Err(vortex_err!(MismatchedTypes: "int", dtype));
        }

        let expected_packed_size = ((len + 1023) / 1024) * 128 * bit_width;
        if encoded.len() != expected_packed_size {
            return Err(vortex_err!(
                "Expected {} packed bytes, got {}",
                expected_packed_size,
                encoded.len()
            ));
        }

        Ok(Self {
            encoded,
            validity,
            patches,
            bit_width,
            len,
            dtype,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    #[inline]
    pub fn encoded(&self) -> &ArrayRef {
        &self.encoded
    }

    #[inline]
    pub fn bit_width(&self) -> usize {
        self.bit_width
    }

    #[inline]
    pub fn patches(&self) -> Option<&ArrayRef> {
        self.patches.as_ref()
    }
}

impl Array for BitPackedArray {
    impl_array!();
    impl_array_compute!();

    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len == 0
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
        if start % 1024 != 0 || stop % 1024 != 0 {
            return flatten_primitive(self)?.slice(start, stop);
        }

        if start > self.len() {
            vortex_bail!(OutOfBounds: start, 0, self.len());
        }
        // If we are slicing more than one 1024 element chunk beyond end, we consider this out of bounds
        if stop / 1024 > ((self.len() + 1023) / 1024) {
            vortex_bail!(OutOfBounds: stop, 0, self.len());
        }

        let encoded_start = (start / 8) * self.bit_width;
        let encoded_stop = (stop / 8) * self.bit_width;
        Self::try_new(
            self.encoded().slice(encoded_start, encoded_stop)?,
            self.validity()
                .map(|v| v.slice(start, min(stop, self.len()))),
            self.patches()
                .map(|p| p.slice(start, min(stop, self.len())))
                .transpose()?,
            self.bit_width(),
            self.dtype().clone(),
            min(stop - start, self.len()),
        )
        .map(|a| a.into_array())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &BitPackedEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        // Ignore any overheads like padding or the bit-width flag.
        let packed_size = ((self.bit_width * self.len()) + 7) / 8;
        packed_size
            + self.patches().map(|p| p.nbytes()).unwrap_or(0)
            + self.validity().map(|v| v.nbytes()).unwrap_or(0)
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }

    fn validity(&self) -> Option<Validity> {
        self.validity.clone()
    }

    fn walk(&self, walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        walker.visit_child(self.encoded())
    }
}

impl ArrayDisplay for BitPackedArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.property("packed", format!("u{}", self.bit_width()))?;
        f.child("encoded", self.encoded())?;
        f.maybe_child("patches", self.patches())?;
        f.validity(self.validity())
    }
}

impl StatsCompute for BitPackedArray {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        Ok(StatsSet::default())
    }
}

#[derive(Debug)]
pub struct BitPackedEncoding;

impl BitPackedEncoding {
    pub const ID: EncodingId = EncodingId::new("fastlanes.bitpacked");
}

impl Encoding for BitPackedEncoding {
    fn id(&self) -> EncodingId {
        Self::ID
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}
