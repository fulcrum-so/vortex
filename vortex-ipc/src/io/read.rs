use std::future::Future;
use std::io;
use std::io::{Cursor, Read};

use bytes::BytesMut;
use vortex_buffer::Buffer;
use vortex_error::vortex_err;

pub trait VortexRead {
    fn read_into(&mut self, buffer: BytesMut) -> impl Future<Output = io::Result<BytesMut>>;
}

pub trait VortexReadAt {
    fn read_at_into(
        &self,
        pos: u64,
        buffer: BytesMut,
    ) -> impl Future<Output = io::Result<BytesMut>>;

    // TODO(ngates): the read implementation should be able to hint at its latency/throughput
    //  allowing the caller to make better decisions about how to coalesce reads.
    fn performance_hint(&self) -> usize {
        0
    }
}

impl VortexRead for BytesMut {
    async fn read_into(&mut self, buffer: BytesMut) -> io::Result<BytesMut> {
        if buffer.len() > self.len() {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                vortex_err!("unexpected eof"),
            ))
        } else {
            Ok(self.split_to(buffer.len()))
        }
    }
}

impl VortexRead for Cursor<Vec<u8>> {
    async fn read_into(&mut self, mut buffer: BytesMut) -> io::Result<BytesMut> {
        Read::read_exact(self, buffer.as_mut())?;
        Ok(buffer)
    }
}

impl VortexRead for Cursor<&[u8]> {
    async fn read_into(&mut self, mut buffer: BytesMut) -> io::Result<BytesMut> {
        Read::read_exact(self, buffer.as_mut())?;
        Ok(buffer)
    }
}

impl VortexRead for Cursor<Buffer> {
    async fn read_into(&mut self, mut buffer: BytesMut) -> io::Result<BytesMut> {
        Read::read_exact(self, buffer.as_mut())?;
        Ok(buffer)
    }
}

impl VortexReadAt for Vec<u8> {
    fn read_at_into(
        &self,
        pos: u64,
        buffer: BytesMut,
    ) -> impl Future<Output = io::Result<BytesMut>> {
        VortexReadAt::read_at_into(self.as_slice(), pos, buffer)
    }
}

impl VortexReadAt for [u8] {
    async fn read_at_into(&self, pos: u64, mut buffer: BytesMut) -> io::Result<BytesMut> {
        let buffer_len = buffer.len();
        buffer.copy_from_slice(&self[pos as usize..][..buffer_len]);
        Ok(buffer)
    }
}

impl VortexReadAt for Buffer {
    async fn read_at_into(&self, pos: u64, mut buffer: BytesMut) -> io::Result<BytesMut> {
        let buffer_len = buffer.len();
        buffer.copy_from_slice(
            self.slice(pos as usize..pos as usize + buffer_len)
                .as_slice(),
        );
        Ok(buffer)
    }
}
