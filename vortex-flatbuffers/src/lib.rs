use std::io;
use std::io::Write;

use flatbuffers::{FlatBufferBuilder, WIPOffset};

pub trait FlatBufferRoot {}

pub trait ReadFlatBuffer: Sized {
    type Source<'a>;
    type Error;

    fn read_flatbuffer(fb: &Self::Source<'_>) -> Result<Self, Self::Error>;
}

pub trait WriteFlatBuffer {
    type Target<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>>;
}

pub trait FlatBufferToBytes {
    fn with_flatbuffer_bytes<R, Fn: FnOnce(&[u8]) -> R>(&self, f: Fn) -> R;
}

impl<F: WriteFlatBuffer + FlatBufferRoot> FlatBufferToBytes for F {
    fn with_flatbuffer_bytes<R, Fn: FnOnce(&[u8]) -> R>(&self, f: Fn) -> R {
        let mut fbb = FlatBufferBuilder::new();
        let root_offset = self.write_flatbuffer(&mut fbb);
        fbb.finish_minimal(root_offset);
        f(fbb.finished_data())
    }
}

pub trait FlatBufferWriter {
    // Write the given FlatBuffer message, appending padding until the total bytes written
    // are a multiple of `alignment`.
    fn write_message<F: WriteFlatBuffer + FlatBufferRoot>(
        &mut self,
        msg: &F,
        alignment: usize,
    ) -> io::Result<()>;
}

impl<W: Write> FlatBufferWriter for W {
    fn write_message<F: WriteFlatBuffer + FlatBufferRoot>(
        &mut self,
        msg: &F,
        alignment: usize,
    ) -> io::Result<()> {
        let mut fbb = FlatBufferBuilder::new();
        let root = msg.write_flatbuffer(&mut fbb);
        fbb.finish_minimal(root);
        let fb_data = fbb.finished_data();
        let fb_size = fb_data.len();

        let aligned_size = (fb_size + (alignment - 1)) & !(alignment - 1);
        let padding_bytes = aligned_size - fb_size;

        self.write_all(&(aligned_size as u32).to_le_bytes())?;
        self.write_all(fb_data)?;
        self.write_all(&vec![0; padding_bytes])
    }
}
