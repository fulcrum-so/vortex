use arrow_buffer::Buffer;
use flatbuffers::{
    root_unchecked, FlatBufferBuilder, Follow, ForwardsUOffset, InvalidFlatbuffer, Verifiable,
    Verifier, VerifierOptions, WIPOffset,
};
use std::convert::TryFrom;
use std::marker::PhantomData;

#[derive(Debug, Clone)]
pub struct FlatBuffer<F> {
    buffer: Buffer,
    phantom: PhantomData<F>,
}

impl<F> FlatBuffer<F>
where
    F: Verifiable,
{
    pub fn from_root(fbb: FlatBufferBuilder, root: WIPOffset<F>) -> Self {
        let mut fbb = fbb;
        (&mut fbb).finish_minimal(root);
        let (vec, offset) = fbb.collapse();
        Self {
            buffer: Buffer::from_vec(vec).slice(offset),
            phantom: PhantomData,
        }
    }

    pub fn try_from_slice<'a>(buffer: &'a [u8]) -> F
    where
        F: 'a,
        F: Follow<'a, Inner = F>,
    {
        // We verify once now and then use unchecked access later.
        let opts = VerifierOptions::default();
        let mut verifier = Verifier::new(&opts, buffer);
        <ForwardsUOffset<F>>::run_verifier(&mut verifier, 0).unwrap();
        unsafe { root_unchecked::<F>(buffer) }
    }

    pub fn try_from_buffer(buffer: &Buffer) -> Result<Self, InvalidFlatbuffer> {
        // We verify once now and then use unchecked access later.
        let opts = VerifierOptions::default();
        let mut verifier = Verifier::new(&opts, buffer.as_slice());
        <ForwardsUOffset<F>>::run_verifier(&mut verifier, 0)?;
        Ok(Self {
            buffer: buffer.clone(),
            phantom: PhantomData,
        })
    }

    pub fn as_typed<'a>(&'a self) -> F::Inner
    where
        F: Follow<'a>,
    {
        unsafe { root_unchecked::<F>(self.buffer.as_slice()) }
    }
}

impl<'a, F> TryFrom<&Buffer> for FlatBuffer<F>
where
    F: Follow<'a>,
    F: Verifiable,
{
    type Error = InvalidFlatbuffer;

    fn try_from(buffer: &Buffer) -> Result<Self, Self::Error> {
        Self::try_from_buffer(buffer)
    }
}
