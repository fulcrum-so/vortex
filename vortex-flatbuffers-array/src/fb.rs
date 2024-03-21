use arrow_buffer::Buffer;
use flatbuffers::{
    root_unchecked, FlatBufferBuilder, Follow, ForwardsUOffset, InvalidFlatbuffer, Verifiable,
    Verifier, VerifierOptions, WIPOffset,
};
use std::convert::TryFrom;
use std::marker::PhantomData;

#[derive(Debug, Clone)]
pub struct Flat<'a, F> {
    buffer: Buffer,
    phantom: PhantomData<&'a F>,
}

impl<'a, F> Flat<'a, F>
where
    F: Follow<'a>,
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

    pub fn try_from_buffer(buffer: Buffer) -> Result<Self, InvalidFlatbuffer> {
        // We verify once now and then use unchecked access later.
        let opts = VerifierOptions::default();
        let mut verifier = Verifier::new(&opts, buffer.as_slice());
        <ForwardsUOffset<F>>::run_verifier(&mut verifier, 0)?;
        Ok(Self {
            buffer,
            phantom: PhantomData,
        })
    }

    pub fn as_typed(&'a self) -> F::Inner {
        unsafe { root_unchecked::<F>(self.buffer.as_slice()) }
    }

    pub fn follow<T>(&'a self, follower: F) -> Flat<'a, T>
    where
        F: FnOnce() -> T,
    {
        Flat {
            buffer: self.buffer.clone(),
            phantom: PhantomData,
        }
    }
}

impl<'a, F> TryFrom<Buffer> for Flat<'a, F>
where
    F: Follow<'a>,
    F: Verifiable,
{
    type Error = InvalidFlatbuffer;

    fn try_from(buffer: Buffer) -> Result<Self, Self::Error> {
        Self::try_from_buffer(buffer)
    }
}
