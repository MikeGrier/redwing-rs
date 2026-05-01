// Copyright (c) 2026 Michael J. Grier

use std::io::{self, Read, Seek};

/// A combined `Read + Seek` trait object used as the return type of
/// [`branch::as_reader`].
///
/// This exists solely because Rust does not permit `dyn Read + Seek` —
/// only one non-auto trait may appear in a trait object bound.  A blanket
/// `impl` covers every type that already implements both.
///
/// Changing or removing this trait is a breaking change.
pub trait ReadSeek: Read + Seek {}
impl<T: Read + Seek> ReadSeek for T {}

/// The unified public interface for all Branch types.
///
/// A `Branch` represents a view of a byte stream with an optional delta log
/// of mutations layered on top of an immutable base.  The overall cluster of a
/// base byte stream and all derived branches that build on top of it is called
/// a **thicket**.
///
/// All methods take `&self`.  Mutable operations (`overwrite`, `insert_before`,
/// `delete`, `append`, `truncate`) are applied through interior mutability
/// inside the concrete implementation.
///
/// Use [`crate::make_thicket_from_bytes`] (or the related entry-point
/// functions) to create the first Branch in a thicket.  Use
/// [`branch::derive`] to branch off a new writable Branch from any
/// existing one.
///
/// Changing this trait (adding or removing methods, or changing any signature)
/// is a breaking change to every caller that names or implements `Branch`.
pub trait Branch {
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
    fn read_byte(&self, offset: u64) -> io::Result<u8> {
        if offset >= self.byte_len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "read_byte: offset out of bounds",
            ));
        }
        let mut buf = [0u8; 1];
        self.read_at(offset, &mut buf)?;
        Ok(buf[0])
    }

    /// Return a lazy `Read + Seek` adapter over this Branch at position 0.
    /// No bytes are copied until `read` is called on the returned reader.
    fn as_reader(&self) -> Box<dyn ReadSeek + '_>;

    /// Replace bytes at `[offset, offset + bytes.len())` with `bytes`.
    ///
    /// Returns `Err(InvalidInput)` when `offset + bytes.len() > byte_len()`.
    /// A zero-length `bytes` slice is a no-op (returns `Ok(())`).
    fn overwrite(&self, offset: u64, bytes: &[u8]) -> io::Result<()>;

    /// Insert `bytes` before the byte currently at `offset`.
    ///
    /// An insert at `offset == byte_len()` is equivalent to `append`.
    /// Returns `Err(InvalidInput)` when `offset > byte_len()`.
    /// A zero-length `bytes` slice is a no-op (returns `Ok(())`).
    fn insert_before(&self, offset: u64, bytes: &[u8]) -> io::Result<()>;

    /// Remove `len` bytes starting at `offset`.
    ///
    /// Returns `Err(InvalidInput)` when `offset + len > byte_len()`.
    /// A zero `len` is a no-op (returns `Ok(())`).
    fn delete(&self, offset: u64, len: u64) -> io::Result<()>;

    /// Replace `len` bytes starting at `offset` with `bytes`.
    ///
    /// Equivalent to `delete(offset, len)` followed by
    /// `insert_before(offset, bytes)`, expressed as a single call.
    /// The length of the replacement need not match `len`.
    ///
    /// Returns `Err(InvalidInput)` when `offset + len > byte_len()`.
    /// When `len == 0` and `bytes` is empty this is a no-op.
    fn splice(&self, offset: u64, len: u64, bytes: &[u8]) -> io::Result<()> {
        self.delete(offset, len)?;
        self.insert_before(offset, bytes)
    }

    /// Append `bytes` to the end of this Branch.
    ///
    /// Equivalent to `insert_before(byte_len(), bytes)`.
    /// A zero-length `bytes` slice is a no-op (returns `Ok(())`).
    fn append(&self, bytes: &[u8]) -> io::Result<()>;

    /// Remove all bytes from `new_len` onward, shortening the Branch.
    ///
    /// `new_len == byte_len()` is a no-op (returns `Ok(())`).
    /// Returns `Err(InvalidInput)` when `new_len > byte_len()`.
    fn truncate(&self, new_len: u64) -> io::Result<()>;

    /// Map a byte offset expressed in the **parent** branch's coordinate space
    /// to the equivalent offset in **this** (fork) branch's coordinate space.
    ///
    /// Returns `Some(fork_offset)` when the parent byte still exists in this
    /// branch, or `None` when it has been deleted or overwritten.
    ///
    /// For a base branch (no parent) this is an identity mapping: any
    /// in-bounds offset maps to itself.
    ///
    /// # Default implementation
    ///
    /// The default body returns `None` for every offset.  This is the safe
    /// fallback for external `Branch` implementations that the crate cannot
    /// introspect.  The concrete implementations inside this crate override
    /// it correctly.
    fn map_offset_to_fork(&self, _parent_offset: u64) -> Option<u64> {
        None
    }

    /// Map a byte range expressed in the **parent** branch's coordinate space
    /// to the equivalent range in **this** (fork) branch's coordinate space.
    ///
    /// Returns `Some(fork_range)` when the entire range maps contiguously —
    /// that is, all bytes in `parent_range` survive into this branch and no
    /// insert has split the run.  Returns `None` if any byte in the range was
    /// deleted, overwritten, or if an insert has broken the run's contiguity.
    ///
    /// An empty range (`start == end`) maps to itself as long as `start` is
    /// in bounds; it returns `None` if `start > byte_len()` of the parent.
    ///
    /// # Default implementation
    ///
    /// The default body returns `None`.  This is the safe fallback for
    /// external `Branch` implementations that the crate cannot introspect.
    fn map_range_to_fork(
        &self,
        _parent_range: std::ops::Range<u64>,
    ) -> Option<std::ops::Range<u64>> {
        None
    }

    /// Create a new, empty branch forked from this one.
    ///
    /// The returned branch accumulates its own independent delta log, but it
    /// remains derived from `self` rather than capturing an immutable snapshot
    /// of `self` at fork time.  As a result, mutations to `self` after the
    /// fork may be visible in the child unless they are shadowed by the
    /// child's own edits, and child mutations are not applied back to `self`.
    ///
    /// # Panics
    ///
    /// The default implementation panics unconditionally.  All concrete
    /// branch types inside this crate override this method via
    /// `Arc::new_cyclic`; external implementations that do not override
    /// it will panic if called.
    fn fork(&self) -> std::sync::Arc<dyn Branch> {
        unimplemented!("fork requires Arc::new_cyclic construction; see redwing docs")
    }
}
