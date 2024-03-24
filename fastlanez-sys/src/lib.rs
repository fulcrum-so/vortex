#![allow(incomplete_features)]
#![feature(generic_const_exprs)]
#![feature(maybe_uninit_uninit_array)]
#![feature(maybe_uninit_array_assume_init)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use std::mem::{size_of, MaybeUninit};

use arrayref::{array_mut_ref, array_ref};
use seq_macro::seq;
use uninit::prelude::VecCapacity;

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

pub fn transpose<T: Sized>(input: &[T; 1024], output: &mut [T; 1024]) {
    unsafe {
        match size_of::<T>() {
            1 => fl_transpose_u8(
                input.as_ptr() as *const [u8; 1024],
                output.as_ptr() as *mut [u8; 1024],
            ),
            2 => fl_transpose_u16(
                input.as_ptr() as *const [u16; 1024],
                output.as_ptr() as *mut [u16; 1024],
            ),
            4 => fl_transpose_u32(
                input.as_ptr() as *const [u32; 1024],
                output.as_ptr() as *mut [u32; 1024],
            ),
            8 => fl_transpose_u64(
                input.as_ptr() as *const [u64; 1024],
                output.as_ptr() as *mut [u64; 1024],
            ),
            _ => unreachable!(),
        }
    }
}

pub fn untranspose<T: Sized>(input: &[T; 1024], output: &mut Vec<T>) {
    unsafe {
        match size_of::<T>() {
            1 => fl_untranspose_u8(
                input.as_ptr() as *const [u8; 1024],
                array_mut_ref![output.reserve_uninit(1024), 0, 1024]
                    as *mut [std::mem::MaybeUninit<T>; 1024] as *mut [u8; 1024],
            ),
            2 => fl_untranspose_u16(
                input.as_ptr() as *const [u16; 1024],
                array_mut_ref![output.reserve_uninit(1024), 0, 1024]
                    as *mut [std::mem::MaybeUninit<T>; 1024] as *mut [u16; 1024],
            ),
            4 => fl_untranspose_u32(
                input.as_ptr() as *const [u32; 1024],
                array_mut_ref![output.reserve_uninit(1024), 0, 1024]
                    as *mut [std::mem::MaybeUninit<T>; 1024] as *mut [u32; 1024],
            ),
            8 => fl_untranspose_u64(
                input.as_ptr() as *const [u64; 1024],
                array_mut_ref![output.reserve_uninit(1024), 0, 1024]
                    as *mut [std::mem::MaybeUninit<T>; 1024] as *mut [u64; 1024],
            ),
            _ => unreachable!(),
        }
        output.set_len(output.len() + input.len());
    }
}

pub struct Pred<const B: bool>;
pub trait Satisfied {}
impl Satisfied for Pred<true> {}

/// BitPack into a compile-time known bit-width.
pub trait BitPack<const W: usize>
where
    Self: Sized,
    Pred<{ W > 0 }>: Satisfied,
    Pred<{ W < 8 * size_of::<Self>() }>: Satisfied,
{
    fn bitpack<'a>(
        input: &[Self; 1024],
        output: &'a mut [MaybeUninit<u8>; 128 * W],
    ) -> &'a [u8; 128 * W];

    fn bitunpack<'a>(
        input: &[u8; 128 * W],
        output: &'a mut [MaybeUninit<Self>; 1024],
    ) -> &'a [Self; 1024];
}

#[derive(Debug)]
pub struct UnsupportedBitWidth;

/// Try to bitpack into a runtime-known bit width.
pub trait TryBitPack
where
    Self: Sized,
{
    fn try_bitpack<'a>(
        input: &[Self; 1024],
        width: usize,
        output: &'a mut [MaybeUninit<u8>],
    ) -> Result<&'a [u8], UnsupportedBitWidth>;

    fn try_bitpack_into(
        input: &[Self; 1024],
        width: usize,
        output: &mut Vec<u8>,
    ) -> Result<(), UnsupportedBitWidth> {
        Self::try_bitpack(input, width, output.reserve_uninit(width * 128))?;
        unsafe { output.set_len(output.len() + (width * 128)) }
        Ok(())
    }

    fn try_bitunpack<'a>(
        input: &[u8],
        width: usize,
        output: &'a mut [MaybeUninit<Self>; 1024],
    ) -> Result<&'a [Self; 1024], UnsupportedBitWidth>;

    fn try_bitunpack_into(
        input: &[u8],
        width: usize,
        output: &mut Vec<Self>,
    ) -> Result<(), UnsupportedBitWidth> {
        Self::try_bitunpack(
            input,
            width,
            array_mut_ref![output.reserve_uninit(1024), 0, 1024],
        )?;
        unsafe { output.set_len(output.len() + 1024) }
        Ok(())
    }
}

macro_rules! bitpack_impl {
    ($T:ty, $W:literal) => {
        paste::item! {
            seq!(N in 1..$W {
                impl BitPack<N> for $T {
                    #[inline]
                    fn bitpack<'a>(
                        input: &[Self; 1024],
                        output: &'a mut [MaybeUninit<u8>; 128 * N],
                    ) -> &'a [u8; 128 * N] {
                            unsafe {
                                let output_array: &mut [u8; 128 * N] = std::mem::transmute(output);
                                [<fl_bitpack_ $T _u >]~N(input, output_array);
                                output_array
                            }
                    }

                    #[inline]
                    fn bitunpack<'a>(
                        input: &[u8; 128 * N],
                        output: &'a mut [MaybeUninit<Self>; 1024],
                    ) -> &'a [Self; 1024] {
                        unsafe {
                            let output_array: &mut [Self; 1024] = std::mem::transmute(output);
                            [<fl_bitunpack_ $T _u >]~N(input, output_array);
                            output_array
                        }
                    }
                }
            });
        }

        impl TryBitPack for $T {
            fn try_bitpack<'a>(
                input: &[Self; 1024],
                width: usize,
                output: &'a mut [MaybeUninit<u8>],
            ) -> Result<&'a [u8], UnsupportedBitWidth> {
                seq!(N in 1..$W {
                    match width {
                        #(N => Ok(BitPack::<N>::bitpack(input, array_mut_ref![output, 0, N * 128]).as_slice()),)*
                        _ => Err(UnsupportedBitWidth),
                    }
                })
            }

            fn try_bitunpack<'a>(
                input: &[u8],
                width: usize,
                output: &'a mut [MaybeUninit<Self>; 1024],
            ) -> Result<&'a [Self; 1024], UnsupportedBitWidth> {
                seq!(N in 1..$W {
                    match width {
                        #(N => Ok(BitPack::<N>::bitunpack(array_ref![input, 0, N * 128], output)),)*
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

pub trait Delta
where
    Self: Sized + Copy + Default,
{
    /// input is assumed to already be in the transposed layout
    /// call transpose() to convert from the original layout
    fn encode_transposed(
        input: &[Self; 1024],
        base: &mut [Self; 128 / size_of::<Self>()],
        output: &mut Vec<Self>,
    );

    /// output is still in the transposed layout
    /// call untranspose() to put it back in the original layout
    fn decode_transposed(
        input: &[Self; 1024],
        base: &mut [Self; 128 / size_of::<Self>()],
        output: &mut [Self; 1024],
    );

    fn lanes() -> usize {
        // fastlanez processes 1024 bits (128 bytes) at a time
        128 / std::mem::size_of::<Self>()
    }
}

macro_rules! delta_impl {
    ($T:ty) => {
        paste::item! {
            impl Delta for $T {
                fn encode_transposed(
                    input: &[Self; 1024],
                    base: &mut [Self; 128 / size_of::<Self>()],
                    output: &mut Vec<Self>,
                ) {
                    unsafe {
                        [<fl_delta_encode_ $T>](
                            input,
                            base,
                            array_mut_ref![output.reserve_uninit(1024), 0, 1024] as *mut [std::mem::MaybeUninit<Self>; 1024] as *mut [Self; 1024],
                        );
                        output.set_len(output.len() + 1024);
                    }
                }

                fn decode_transposed(
                    input: &[Self; 1024],
                    base: &mut [Self; 128 / size_of::<Self>()],
                    output: &mut [Self; 1024],
                ) {
                    unsafe { [<fl_delta_decode_ $T>](input, base, output); }
                }
            }
        }
    };
}

delta_impl!(i8);
delta_impl!(i16);
delta_impl!(i32);
delta_impl!(i64);
delta_impl!(u8);
delta_impl!(u16);
delta_impl!(u32);
delta_impl!(u64);
