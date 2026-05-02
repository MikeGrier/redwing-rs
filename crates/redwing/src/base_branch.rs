// Copyright (c) 2026 Michael J. Grier

use std::{
    io::{self, Read, Seek, SeekFrom},
    sync::{Arc, Weak},
};

use memmap2::Mmap;

use crate::{
    branch::{Branch, ReadSeek},
    branch_reader::{BranchReader, ByteSource},
    error::BranchError,
};

/// Storage backing for a `BaseBranch`.
/// Changing the variant layout is a breaking change to any serialised form.
enum BaseData {
    Owned(Arc<[u8]>),
    Mapped(Arc<Mmap>),
}

impl BaseData {
    fn len(&self) -> u64 {
        match self {
            BaseData::Owned(b) => b.len() as u64,
            BaseData::Mapped(m) => m.len() as u64,
        }
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            BaseData::Owned(b) => b,
            BaseData::Mapped(m) => m,
        }
    }
}

/// An immutable base Branch whose byte content is held entirely in memory
/// (either as an owned buffer or as a demand-paged memory map).
pub(crate) struct BaseBranch {
    /// Weak back-reference to the `Arc` that owns this value, set the first
    /// time the branch is wrapped via [`BaseBranch::into_arc`].  Starts as
    /// a dangling `Weak::new()` for stack-allocated values used in tests.
    pub(crate) this: Weak<BaseBranch>,
    data: BaseData,
}

impl BaseBranch {
    /// Read the full content of `r` into an owned in-memory buffer.
    pub fn from_reader(mut r: impl Read + Seek) -> io::Result<Self> {
        r.seek(SeekFrom::Start(0))?;
        let mut buf = Vec::new();
        r.read_to_end(&mut buf)?;
        Ok(Self {
            this: Weak::new(),
            data: BaseData::Owned(buf.into()),
        })
    }

    /// Wrap a caller-provided memory map. The map is held via `Arc` so that
    /// clones of this Branch share the same mapping without re-mapping.
    pub fn from_mmap(mmap: Mmap) -> Self {
        Self {
            this: Weak::new(),
            data: BaseData::Mapped(Arc::new(mmap)),
        }
    }

    /// A zero-length Branch with no backing storage.
    ///
    /// Used in tests and as a convenience constructor for callers that build
    /// content incrementally via mutations.
    #[allow(dead_code)]
    pub fn empty() -> Self {
        Self {
            this: Weak::new(),
            data: BaseData::Owned(Arc::from([])),
        }
    }

    /// Wrap caller-supplied bytes as an immutable Branch.
    ///
    /// The argument is any type that converts into `Arc<[u8]>`: a `Vec<u8>`,
    /// a `&[u8]`, an `Arc<[u8]>`, or a `Box<[u8]>`.  When the caller already
    /// holds an `Arc<[u8]>`, this is a zero-copy operation — no new heap
    /// allocation occurs.
    ///
    /// Prefer this over `from_reader(Cursor::new(vec))` whenever the data is
    /// already in memory; `from_reader` always copies the bytes into a fresh
    /// `Vec` even when given a `Cursor`.
    pub fn from_bytes(data: impl Into<Arc<[u8]>>) -> Self {
        Self {
            this: Weak::new(),
            data: BaseData::Owned(data.into()),
        }
    }

    /// Wrap `self` in an `Arc`, wiring the internal `Weak` back-reference so
    /// that `Branch::fork` can later recover the owning `Arc` from `&self`.
    ///
    /// Call this instead of `Arc::new(base)` whenever the resulting `Arc`
    /// will be used as the root of a thicket.
    pub(crate) fn into_arc(self) -> Arc<Self> {
        Arc::new_cyclic(|weak| Self {
            this: weak.clone(),
            ..self
        })
    }

    /// Total number of bytes in this Branch.
    pub fn byte_len(&self) -> u64 {
        self.data.len()
    }

    /// Return a lazy `Read + Seek` adapter over this Branch at position 0.
    /// No bytes are copied until `read` is called on the returned reader.
    ///
    /// Returns the concrete `BranchReader` type rather than an erased
    /// `Box<dyn ReadSeek>`, which is useful in test and internal code that
    /// has a concrete `&BaseBranch` rather than a `&dyn Branch`.
    #[allow(dead_code)]
    pub fn as_reader(&self) -> BranchReader<'_, Self> {
        BranchReader::new(self)
    }

    /// Read up to `buf.len()` bytes starting at `offset`.
    ///
    /// Returns the number of bytes placed into `buf`. Returns `Ok(0)` when
    /// `offset >= self.byte_len()`. Never returns more bytes than are
    /// available from `offset` to the end of the Branch.
    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let data = self.data.as_slice();
        let len = data.len() as u64;
        if offset >= len || buf.is_empty() {
            return Ok(0);
        }
        let start = offset as usize;
        let available = (len - offset) as usize;
        let n = buf.len().min(available);
        buf[..n].copy_from_slice(&data[start..start + n]);
        Ok(n)
    }
}

impl ByteSource for BaseBranch {
    fn byte_len(&self) -> u64 {
        self.byte_len()
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.read_at(offset, buf)
    }
}

/// `BaseBranch` implements `Branch` for the read side.  Write methods
/// return `PermissionDenied` because the base layer is immutable — all
/// mutations must go through a `DerivedBranch` layered on top via
/// [`crate::derive`].  No-op cases (empty slice, `truncate` to current length)
/// still return `Ok(())` as required by the `Branch` contract.
impl Branch for BaseBranch {
    fn byte_len(&self) -> u64 {
        self.byte_len()
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.read_at(offset, buf)
    }

    fn as_reader(&self) -> Box<dyn ReadSeek + '_> {
        Box::new(BranchReader::new(self))
    }

    fn overwrite(&self, _offset: u64, bytes: &[u8]) -> io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        Err(BranchError::ReadOnly.into())
    }

    fn insert_before(&self, _offset: u64, bytes: &[u8]) -> io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        Err(BranchError::ReadOnly.into())
    }

    fn delete(&self, _offset: u64, len: u64) -> io::Result<()> {
        if len == 0 {
            return Ok(());
        }
        Err(BranchError::ReadOnly.into())
    }

    fn append(&self, bytes: &[u8]) -> io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        Err(BranchError::ReadOnly.into())
    }

    fn truncate(&self, new_len: u64) -> io::Result<()> {
        if new_len == self.byte_len() {
            return Ok(());
        }
        Err(BranchError::ReadOnly.into())
    }

    /// Identity mapping: a `BaseBranch` has no parent, so any in-bounds
    /// offset maps to itself.
    fn map_offset_to_fork(&self, parent_offset: u64) -> Option<u64> {
        if parent_offset < self.byte_len() {
            Some(parent_offset)
        } else {
            None
        }
    }

    /// Identity range mapping: returns `Some(parent_range)` when the entire
    /// range is within bounds, `None` otherwise.
    fn map_range_to_fork(
        &self,
        parent_range: std::ops::Range<u64>,
    ) -> Option<std::ops::Range<u64>> {
        if parent_range.end <= self.byte_len() {
            Some(parent_range)
        } else {
            None
        }
    }

    fn fork(&self) -> std::sync::Arc<dyn crate::branch::Branch> {
        let arc = self.this.upgrade().expect(
            "BaseBranch::fork called on a stack-allocated value; use into_arc() at construction time",
        );
        crate::derived_branch::DerivedBranch::derive_from_base(arc)
    }
}

#[cfg(test)]
#[path = "tests/base_branch.rs"]
mod tests;
