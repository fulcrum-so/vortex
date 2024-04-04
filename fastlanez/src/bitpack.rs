use std::mem::{size_of, MaybeUninit};

use arrayref::{array_mut_ref, array_ref};
use fastlanez_sys::*;
use num_traits::{PrimInt, Unsigned};
use seq_macro::seq;
use uninit::prelude::VecCapacity;

use crate::{Pred, Satisfied};

/// BitPack into a compile-time known bit-width.
pub trait BitPack<const W: usize>
where
    Self: Sized + Unsigned + PrimInt,
    Pred<{ W > 0 }>: Satisfied,
    Pred<{ W < 8 * size_of::<Self>() }>: Satisfied,
{
    // fastlanez processes 1024 elements in chunks of 1024 bits at a time
    const NUM_LANES: usize;
    const MASK: Self;

    /// Packs 1024 elements into W bits each -> (1024 * W / 8) -> 128 * W bytes
    fn pack<'a>(
        input: &[Self; 1024],
        output: &'a mut [MaybeUninit<u8>; 128 * W],
    ) -> &'a [u8; 128 * W];

    /// Unpacks 1024 elements that have been packed into W bits each
    fn unpack<'a>(
        input: &[u8; 128 * W],
        output: &'a mut [MaybeUninit<Self>; 1024],
    ) -> &'a [Self; 1024];

    /// Unpacks a single element (at provided index) that has been packed into W bits
    fn unpack_single(input: &[u8; 128 * W], index: usize) -> Self;
}

#[derive(Debug)]
pub struct UnsupportedBitWidth;

/// Try to bitpack into a runtime-known bit width.
pub trait TryBitPack
where
    Self: Sized + Unsigned + PrimInt,
{
    fn try_pack<'a>(
        input: &[Self; 1024],
        width: usize,
        output: &'a mut [MaybeUninit<u8>],
    ) -> Result<&'a [u8], UnsupportedBitWidth>;

    fn try_pack_into(
        input: &[Self; 1024],
        width: usize,
        output: &mut Vec<u8>,
    ) -> Result<(), UnsupportedBitWidth> {
        Self::try_pack(input, width, output.reserve_uninit(width * 128))?;
        unsafe { output.set_len(output.len() + (width * 128)) }
        Ok(())
    }

    fn try_unpack<'a>(
        input: &[u8],
        width: usize,
        output: &'a mut [MaybeUninit<Self>; 1024],
    ) -> Result<&'a [Self; 1024], UnsupportedBitWidth>;

    fn try_unpack_into(
        input: &[u8],
        width: usize,
        output: &mut Vec<Self>,
    ) -> Result<(), UnsupportedBitWidth> {
        Self::try_unpack(
            input,
            width,
            array_mut_ref![output.reserve_uninit(1024), 0, 1024],
        )?;
        unsafe { output.set_len(output.len() + 1024) }
        Ok(())
    }

    fn try_unpack_single(
        input: &[u8],
        width: usize,
        index: usize,
    ) -> Result<Self, UnsupportedBitWidth>;
}

macro_rules! bitpack_impl {
    ($T:ty, $BITS:literal) => {
        paste::item! {
            seq!(W in 1..$BITS {
                impl BitPack<W> for $T {
                    const NUM_LANES: usize = 128 / size_of::<$T>();
                    const MASK: $T = ((1 as $T) << W) - 1;

                    #[inline]
                    fn pack<'a>(
                        input: &[Self; 1024],
                        output: &'a mut [MaybeUninit<u8>; 128 * W],
                    ) -> &'a [u8; 128 * W] {
                            unsafe {
                                let output_array: &mut [u8; 128 * W] = std::mem::transmute(output);
                                [<fl_bitpack_ $T _u >]~W(input, output_array);
                                output_array
                            }
                    }

                    #[inline]
                    fn unpack<'a>(
                        input: &[u8; 128 * W],
                        output: &'a mut [MaybeUninit<Self>; 1024],
                    ) -> &'a [Self; 1024] {
                        unsafe {
                            let output_array: &mut [Self; 1024] = std::mem::transmute(output);
                            [<fl_bitunpack_ $T _u >]~W(input, output_array);
                            output_array
                        }
                    }

                    #[inline]
                    fn unpack_single(
                        input: &[u8; 128 * W],
                        index: usize
                    ) -> Self {
                        // lane_index is the index of the row
                        let lane_index = index % <$T as BitPack<W>>::NUM_LANES;
                        // lane_start_bit is the bit offset in the combined columns of the row
                        let lane_start_bit = (index / <$T as BitPack<W>>::NUM_LANES) * W;

                        let words: [Self; 2] = {
                            // each tranche is laid out as a column-major 2D array of words
                            // there are `num_lanes` rows (lanes), each of which contains `packed_bit_width` columns (words) of type T
                            let tranche_words = unsafe {
                                std::slice::from_raw_parts(
                                    input.as_ptr() as *const Self,
                                    input.len() / std::mem::size_of::<Self>(),
                                )
                            };

                            // the value may be split across two words
                            let lane_start_word = lane_start_bit / ($T::BITS as usize);
                            let lane_end_word_inclusive = (lane_start_bit + W - 1) / ($T::BITS as usize);

                            [
                                tranche_words[lane_start_word * <$T as BitPack<W>>::NUM_LANES + lane_index],
                                tranche_words[lane_end_word_inclusive * <$T as BitPack<W>>::NUM_LANES + lane_index], // this may be a duplicate
                            ]
                        };

                        let start_bit = lane_start_bit % ($T::BITS as usize);
                        let bits_left_in_first_word = ($T::BITS as usize) - start_bit;
                        if bits_left_in_first_word >= W {
                            // all the bits we need are in the same word
                            (words[0] >> start_bit) & <$T as BitPack<W>>::MASK
                        } else {
                            // we need to use two words
                            let lo = words[0] >> start_bit;
                            let hi = words[1] << bits_left_in_first_word;
                            (lo | hi) & <$T as BitPack<W>>::MASK
                        }
                    }
                }
            });
        }

        impl TryBitPack for $T {
            fn try_pack<'a>(
                input: &[Self; 1024],
                width: usize,
                output: &'a mut [MaybeUninit<u8>],
            ) -> Result<&'a [u8], UnsupportedBitWidth> {
                seq!(W in 1..$BITS {
                    match width {
                        #(W => Ok(BitPack::<W>::pack(input, array_mut_ref![output, 0, W * 128]).as_slice()),)*
                        _ => Err(UnsupportedBitWidth),
                    }
                })
            }

            fn try_unpack<'a>(
                input: &[u8],
                width: usize,
                output: &'a mut [MaybeUninit<Self>; 1024],
            ) -> Result<&'a [Self; 1024], UnsupportedBitWidth> {
                seq!(W in 1..$BITS {
                    match width {
                        #(W => Ok(BitPack::<W>::unpack(array_ref![input, 0, W * 128], output)),)*
                        _ => Err(UnsupportedBitWidth),
                    }
                })
            }

            fn try_unpack_single(input: &[u8], width: usize, index: usize) -> Result<Self, UnsupportedBitWidth> {
                seq!(W in 1..$BITS {
                    match width {
                        #(W => Ok(BitPack::<W>::unpack_single(array_ref![input, 0, W * 128], index)),)*
                        _ => Err(UnsupportedBitWidth),
                    }
                })
            }
        }
    };
}

bitpack_impl!(u8, 8);
bitpack_impl!(u16, 16);
bitpack_impl!(u32, 32);
bitpack_impl!(u64, 64);

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_bitpack_roundtrip() {
        let input = (0u32..1024).collect::<Vec<_>>();
        let mut output = Vec::new();
        TryBitPack::try_pack_into(array_ref![input, 0, 1024], 10, &mut output).unwrap();
        assert_eq!(output.len(), 1280);

        let mut decoded: Vec<u32> = Vec::new();
        TryBitPack::try_unpack_into(&output, 10, &mut decoded).unwrap();
        assert_eq!(input, decoded);
    }

    #[test]
    fn test_unpack_single() {
        let input = (0u32..1024).collect::<Vec<_>>();
        let mut output = Vec::new();
        TryBitPack::try_pack_into(array_ref![input, 0, 1024], 10, &mut output).unwrap();
        assert_eq!(output.len(), 1280);

        input.iter().enumerate().for_each(|(i, v)| {
            let decoded = <u32 as TryBitPack>::try_unpack_single(&output, 10, i).unwrap();
            assert_eq!(decoded, *v);
        });
    }
}
