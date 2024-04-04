use std::mem::{size_of, MaybeUninit};

use arrayref::array_mut_ref;
use fastlanez_sys::{
    fl_transpose_u16, fl_transpose_u32, fl_transpose_u64, fl_transpose_u8, fl_untranspose_u16,
    fl_untranspose_u32, fl_untranspose_u64, fl_untranspose_u8,
};
use uninit::prelude::VecCapacity;

pub fn transpose<T: Sized, U: Transposable<T>>(input: &[T; 1024], output: &mut [U; 1024]) {
    unsafe {
        // referencing U::SIZE forces a compile time size check; it is equal to size_of::<T>()
        match U::SIZE {
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

pub fn transpose_into<T: Sized>(input: &[T; 1024], output: &mut Vec<T>) {
    let out_slice = array_mut_ref![output.reserve_uninit(1024), 0, 1024];
    transpose(input, out_slice);
    unsafe {
        output.set_len(output.len() + input.len());
    }
}

pub fn untranspose<T: Sized, U: Transposable<T>>(input: &[T; 1024], output: &mut [U; 1024]) {
    unsafe {
        // referencing U::SIZE forces a compile time size check; it is equal to size_of::<T>()
        match U::SIZE {
            1 => fl_untranspose_u8(
                input.as_ptr() as *const [u8; 1024],
                output.as_mut_ptr() as *mut [u8; 1024],
            ),
            2 => fl_untranspose_u16(
                input.as_ptr() as *const [u16; 1024],
                output.as_mut_ptr() as *mut [u16; 1024],
            ),
            4 => fl_untranspose_u32(
                input.as_ptr() as *const [u32; 1024],
                output.as_mut_ptr() as *mut [u32; 1024],
            ),
            8 => fl_untranspose_u64(
                input.as_ptr() as *const [u64; 1024],
                output.as_mut_ptr() as *mut [u64; 1024],
            ),
            _ => unreachable!(),
        }
    }
}

pub fn untranspose_into<T: Sized>(input: &[T; 1024], output: &mut Vec<T>) {
    untranspose(input, array_mut_ref![output.reserve_uninit(1024), 0, 1024]);
    unsafe {
        output.set_len(output.len() + input.len());
    }
}

pub trait Transposable<T: Sized> {
    // must be referenced to force compile-time size checking
    const SIZE: usize = {
        assert!(
            size_of::<T>() == 1
                || size_of::<T>() == 2
                || size_of::<T>() == 4
                || size_of::<T>() == 8,
            "T must be 1, 2, 4 or 8 bytes in size"
        );
        size_of::<T>()
    };
}

impl<T: Sized> Transposable<T> for T {}
impl<T: Sized> Transposable<T> for MaybeUninit<T> {}

#[cfg(test)]
mod test {
    use arrayref::array_ref;

    use super::*;

    #[test]
    fn test_transpose() {
        let input: [u16; 1024] = (0u16..1024).collect::<Vec<_>>().try_into().unwrap();
        let mut output: Vec<u16> = Vec::new();
        transpose_into(&input, &mut output);
        assert_eq!(
            output[0..16],
            [0, 64, 128, 192, 256, 320, 384, 448, 512, 576, 640, 704, 768, 832, 896, 960]
        );
        assert_eq!(output[128], 1);
        assert_eq!(output[256], 2);
        assert_eq!(output[384], 3);
        assert_eq!(output[512], 4);
        assert_eq!(output[640], 5);
        assert_eq!(output[768], 6);
        assert_eq!(output[896], 7);

        let mut rt: Vec<u16> = Vec::new();
        untranspose_into(array_ref![output.as_slice(), 0, 1024], &mut rt);
        assert_eq!(input, rt.as_slice());
    }
}
