// Copyright (c) 2026 Michael J. Grier

use std::{io, sync::Arc};

use crate::branch::Branch;

/// Read the entire byte stream of `src` into a newly-allocated [`Vec<u8>`].
///
/// This is an explicit, named action: calling `materialize` makes it clear at
/// the call site that a full allocation is occurring.  The function is
/// intentionally not called internally by the Branch machinery — it exists
/// solely as a convenience for callers that need a flat, contiguous buffer.
///
/// The returned `Vec` has exactly `src.byte_len()` bytes when successful.
///
/// # Errors
///
/// - `InvalidInput` if `src.byte_len()` exceeds `usize::MAX` (only possible
///   on 32-bit targets with a Branch larger than 4 GiB).
/// - Any error returned by `src.read_at`.
/// - `UnexpectedEof` if `read_at` returns `Ok(0)` before the stream is fully
///   read (indicates a contract violation in the `ByteSource` impl).
pub fn materialize(src: &dyn Branch) -> io::Result<Vec<u8>> {
    let len = src.byte_len();
    let size = usize::try_from(len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "branch too large to materialize on this platform",
        )
    })?;
    let mut buf = vec![0u8; size];
    let mut off: u64 = 0;
    while off < len {
        let n = src.read_at(off, &mut buf[off as usize..])?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "read_at returned 0 before end of branch",
            ));
        }
        off += n as u64;
    }
    Ok(buf)
}

/// Read bytes `[offset, offset + len)` from `src` into a newly-allocated
/// [`Vec<u8>`].
///
/// Like [`materialize`], this is an explicit allocation and not used
/// internally.  It is useful when the caller needs only a portion of the
/// byte stream and wants to avoid materializing the whole Branch.
///
/// The returned `Vec` has exactly `len` bytes when successful.
///
/// # Errors
///
/// - `InvalidInput` if `offset + len` overflows `u64`.
/// - `InvalidInput` if `offset + len > src.byte_len()` (range out of bounds).
/// - `InvalidInput` if `len` exceeds `usize::MAX`.
/// - Any error returned by `src.read_at`.
/// - `UnexpectedEof` if `read_at` returns `Ok(0)` before `len` bytes are read.
pub fn materialize_range(src: &dyn Branch, offset: u64, len: u64) -> io::Result<Vec<u8>> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "offset + len overflows u64"))?;
    if end > src.byte_len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "range exceeds branch length",
        ));
    }
    let size = usize::try_from(len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "range too large to materialize on this platform",
        )
    })?;
    let mut buf = vec![0u8; size];
    let mut filled: usize = 0;
    while filled < size {
        let n = src.read_at(offset + filled as u64, &mut buf[filled..])?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "read_at returned 0 before range was fully read",
            ));
        }
        filled += n;
    }
    Ok(buf)
}

/// Collapse `src` into a new [`BaseBranch`] by full materialization.
///
/// Equivalent to [`materialize`] followed by [`BaseBranch::from_bytes`], but
/// named and co-located to make the intent clear at the call site: the caller
/// is converting a (possibly deep) derived chain into a single self-contained
/// Branch with no parent references.
///
/// Prefer this over calling `materialize` + `BaseBranch::from_bytes`
/// separately when the goal is to "flatten" a branch chain.
///
/// # Errors
///
/// Same error conditions as [`materialize`].
pub fn flatten(src: &dyn Branch) -> io::Result<Arc<dyn Branch>> {
    let bytes = materialize(src)?;
    let base = crate::base_branch::BaseBranch::from_bytes(bytes).into_arc();
    Ok(crate::derived_branch::DerivedBranch::derive_from_base(base))
}

/// Compare two [`ByteSource`] implementations for content equality without
/// full materialization.
///
/// Returns `Ok(false)` immediately when the two sources have different
/// `byte_len` values — no byte reads are performed in that case.  Otherwise
/// reads both sources in equal-sized chunks and compares them.  Returns
/// `Ok(true)` only when every byte matches.
///
/// The chunk size is an implementation detail and must not be relied upon.
pub fn bytes_equal(a: &dyn Branch, b: &dyn Branch) -> io::Result<bool> {
    if a.byte_len() != b.byte_len() {
        return Ok(false);
    }
    const CHUNK: usize = 4096;
    let mut buf_a = [0u8; CHUNK];
    let mut buf_b = [0u8; CHUNK];
    let mut offset: u64 = 0;
    let len = a.byte_len();
    while offset < len {
        let remaining = (len - offset).min(CHUNK as u64) as usize;
        let mut filled_a = 0usize;
        while filled_a < remaining {
            let n = a.read_at(offset + filled_a as u64, &mut buf_a[filled_a..remaining])?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "read_at returned 0 before end of Branch",
                ));
            }
            filled_a += n;
        }
        let mut filled_b = 0usize;
        while filled_b < remaining {
            let n = b.read_at(offset + filled_b as u64, &mut buf_b[filled_b..remaining])?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "read_at returned 0 before end of branch",
                ));
            }
            filled_b += n;
        }
        if buf_a[..remaining] != buf_b[..remaining] {
            return Ok(false);
        }
        offset += remaining as u64;
    }
    Ok(true)
}

#[cfg(test)]
#[path = "tests/materialize.rs"]
mod tests;
