use vortex_error::VortexResult;

use crate::accessor::ArrayAccessor;
use crate::array::varbin::VarBinArray;
use crate::match_each_integer_ptype;
use crate::validity::ArrayValidity;

impl ArrayAccessor for VarBinArray<'_> {
    type Item<'a> = Option<&'a [u8]>;

    fn with_iterator<F: for<'a> FnOnce(&mut dyn Iterator<Item = Self::Item<'a>>) -> R, R>(
        &self,
        f: F,
    ) -> VortexResult<R> {
        // TODO(ngates): what happens if bytes is much larger than sliced_bytes?
        let primitive = self.bytes().flatten_primitive()?;
        let offsets = self.offsets().flatten_primitive()?;
        let validity = self.logical_validity().to_null_buffer()?;

        match_each_integer_ptype!(offsets.ptype(), |$T| {
            let offsets = offsets.typed_data::<$T>();
            let bytes = primitive.typed_data::<u8>();

            match validity {
                None => {
                    let mut iter = offsets
                        .iter()
                        .zip(offsets.iter().skip(1))
                        .map(|(start, end)| Some(&bytes[*start as usize..*end as usize]));
                    Ok(f(&mut iter))
                }
                Some(validity) => {
                    let mut iter = offsets
                        .iter()
                        .zip(offsets.iter().skip(1))
                        .zip(validity.iter())
                        .map(|((start, end), valid)| {
                            if valid {
                                Some(&bytes[*start as usize..*end as usize])
                            } else {
                                None
                            }
                        });
                    Ok(f(&mut iter))
                }
            }
        })
    }
}
