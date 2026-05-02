// Copyright (c) 2026 Michael J. Grier

use std::io::{self, Read, Seek, SeekFrom};

use crate::error::BranchError;

/// Positional read interface shared by all Branch types.
///
/// Both `byte_len` and `read_at` are already provided by `BaseBranch` and
/// `DerivedBranch` with identical signatures; this trait is a thin wrapper
/// that lets `BranchReader` be generic over either type.
///
/// Changing this trait (adding or removing methods, changing signatures) is a
/// breaking change to any caller that names or implements `ByteSource`.
pub trait ByteSource {
    /// Total byte length of the visible byte stream.
    fn byte_len(&self) -> u64;

    /// Read up to `buf.len()` bytes starting at `offset`.
    ///
    /// Returns the number of bytes placed in `buf`.  Returns `Ok(0)` when
    /// `offset >= byte_len()` or `buf` is empty.  Never returns more bytes
    /// than are available from `offset` to the end of the stream.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;

    /// Read the single byte at `offset`.
    ///
    /// Returns `Err(InvalidInput)` when `offset >= byte_len()`.  This avoids
    /// the boilerplate of allocating a 1-byte buffer for the common peek
    /// pattern.
    #[allow(dead_code)]
    fn read_byte(&self, offset: u64) -> io::Result<u8> {
        if offset >= self.byte_len() {
            return Err(BranchError::OutOfBounds.into());
        }
        let mut buf = [0u8; 1];
        self.read_at(offset, &mut buf)?;
        Ok(buf[0])
    }
}

/// A lazy `Read + Seek` adapter over any type implementing [`ByteSource`].
///
/// Created via `BaseBranch::as_reader()` or `DerivedBranch::as_reader()`.
/// Borrows the Branch for its lifetime; no bytes are copied until `read` is
/// called.
pub struct BranchReader<'a, S: ByteSource> {
    source: &'a S,
    pos: u64,
}

impl<'a, S: ByteSource> BranchReader<'a, S> {
    /// Wrap `source` at position 0.
    pub(crate) fn new(source: &'a S) -> Self {
        Self { source, pos: 0 }
    }
}

impl<S: ByteSource> Read for BranchReader<'_, S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.source.read_at(self.pos, buf)?;
        self.pos += n as u64;
        Ok(n)
    }
}

impl<S: ByteSource> Seek for BranchReader<'_, S> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        // Compute the new position in i128 so that overflow in either direction
        // is caught before we touch self.pos.
        let new_pos: i128 = match pos {
            SeekFrom::Start(n) => n as i128,
            SeekFrom::End(n) => self.source.byte_len() as i128 + n as i128,
            SeekFrom::Current(n) => self.pos as i128 + n as i128,
        };
        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek before beginning of Branch",
            ));
        }
        if new_pos > u64::MAX as i128 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek beyond maximum Branch position",
            ));
        }
        self.pos = new_pos as u64;
        Ok(self.pos)
    }
}

#[cfg(test)]
#[path = "tests/branch_reader.rs"]
mod tests;
