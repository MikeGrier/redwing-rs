// Copyright (c) 2026 Michael J. Grier

//! Typed error values for `redwing` operations.
//!
//! All fallible Branch operations return `io::Result<...>` so that they can be
//! plugged into existing `std::io` infrastructure.  When an operation fails for
//! a domain-specific reason, the returned [`io::Error`] carries a
//! [`BranchError`] as its inner error so that callers can inspect the cause
//! programmatically without parsing a message string.
//!
//! ```no_run
//! use redwing::{branch_error, BranchError, make_thicket_from_bytes};
//!
//! let thicket = make_thicket_from_bytes(b"hello".to_vec());
//! let branch = thicket.main();
//! let branch = branch.fork();
//! if let Err(e) = branch.overwrite(100, b"x") {
//!     match branch_error(&e) {
//!         Some(BranchError::OutOfBounds) => { /* handle out-of-range */ }
//!         _ => return Err(e),
//!     }
//! }
//! # Ok::<_, std::io::Error>(())
//! ```

use std::{error::Error, fmt, io};

/// Domain-specific failure reasons returned by Branch operations.
///
/// Wrapped inside an [`io::Error`] (via the [`From`] impl) so that operations
/// can return `io::Result<...>` while still letting callers recover the
/// structured cause via [`branch_error`].
///
/// Adding new variants is a non-breaking change as long as exhaustive matches
/// outside the crate are discouraged; the enum is marked `#[non_exhaustive]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BranchError {
    /// An offset or range argument extended past `byte_len()`.
    OutOfBounds,
    /// `offset + len` overflowed `u64` (or `usize` where applicable).
    OffsetOverflow,
    /// A write was attempted on a read-only branch (e.g. a [`crate::Branch`]
    /// backed directly by a `BaseBranch`).
    ReadOnly,
    /// The underlying byte source returned `Ok(0)` from `read_at` before the
    /// requested range was satisfied — a contract violation by the source.
    UnexpectedEof,
    /// The branch's `byte_len()` exceeds the addressable size of this
    /// platform (only possible on 32-bit targets).
    BranchTooLarge,
}

impl BranchError {
    /// The default `io::ErrorKind` associated with this variant.
    pub fn kind(self) -> io::ErrorKind {
        match self {
            BranchError::OutOfBounds
            | BranchError::OffsetOverflow
            | BranchError::BranchTooLarge => io::ErrorKind::InvalidInput,
            BranchError::ReadOnly => io::ErrorKind::PermissionDenied,
            BranchError::UnexpectedEof => io::ErrorKind::UnexpectedEof,
        }
    }

    fn message(self) -> &'static str {
        match self {
            BranchError::OutOfBounds => "offset or range exceeds branch length",
            BranchError::OffsetOverflow => "offset arithmetic overflowed u64",
            BranchError::ReadOnly => "branch is read-only",
            BranchError::UnexpectedEof => "byte source returned 0 before end of range",
            BranchError::BranchTooLarge => "branch too large for this platform",
        }
    }
}

impl fmt::Display for BranchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl Error for BranchError {}

impl From<BranchError> for io::Error {
    fn from(e: BranchError) -> io::Error {
        io::Error::new(e.kind(), e)
    }
}

/// Recover a [`BranchError`] from an [`io::Error`] previously produced by a
/// `redwing` operation.  Returns `None` when the error did not originate
/// from this crate.
#[must_use]
pub fn branch_error(err: &io::Error) -> Option<BranchError> {
    err.get_ref()
        .and_then(|e| e.downcast_ref::<BranchError>())
        .copied()
}
