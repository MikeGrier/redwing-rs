// Copyright (c) 2026 Michael J. Grier

use std::{
    cell::RefCell,
    io,
    sync::Arc,
};

use crate::{
    base_branch::BaseBranch,
    branch::{Branch, ReadSeek},
    branch_reader::BranchReader,
    delta::Delta,
    error::BranchError,
    piece_table::PieceTable,
};

/// The parent of a `DerivedBranch`: either a base Branch or another
/// derived Branch (held as `Arc<dyn Branch>` so the free `derive`
/// function can accept any Branch without knowing its concrete type).
///
/// This enum is a closed set — the crate controls every variant — so there
/// is no need for additional virtual dispatch beyond what `dyn Branch`
/// already provides.  Changing the variant set or repr is a breaking change
/// to any serialised Branch chain.
pub(crate) enum BranchParent {
    Base(Arc<BaseBranch>),
    /// Used by [`DerivedBranch::derive_from_derived`] in tests; kept to cover
    /// the `Derived` path in `byte_len`/`read_at`.
    #[allow(dead_code)]
    Derived(Arc<dyn Branch>),
}

impl BranchParent {
    /// Total byte length of the parent's content.
    pub(crate) fn byte_len(&self) -> u64 {
        match self {
            BranchParent::Base(b) => b.byte_len(),
            BranchParent::Derived(d) => d.byte_len(),
        }
    }

    /// Read up to `buf.len()` bytes from the parent starting at `offset`.
    /// Returns `Ok(0)` when `offset >= byte_len()` or `buf` is empty.
    pub(crate) fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            BranchParent::Base(b) => b.read_at(offset, buf),
            BranchParent::Derived(d) => d.read_at(offset, buf),
        }
    }
}

/// A Branch that layers a delta log on top of a parent Branch.
///
/// This type is `pub(crate)`; external callers interact with it exclusively
/// through the `Arc<dyn Branch>` handle produced by `fork()` and the
/// `make_thicket_from_*` entry-point functions.
pub(crate) struct DerivedBranch {
    /// The Branch this one was derived from.
    pub(crate) parent: Arc<BranchParent>,
    /// Delta log: mutations applied on top of `parent`, in order.
    pub(crate) log: RefCell<Vec<Delta>>,
    /// Lazily-built piece table. Invalidated (set to `None`) whenever `log`
    /// is mutated; rebuilt on the next call to `byte_len` or `read_at`.
    ///
    /// Parent-change tracking is not required: `derive_from_base` (the only
    /// production constructor, called by `Branch::fork`) always receives an
    /// immutable `BaseBranch` snapshot, so `parent.byte_len()` is fixed for
    /// the lifetime of this branch.
    table: RefCell<Option<PieceTable>>,
}

impl DerivedBranch {
    /// Compute `offset + len` as a `u64`, returning [`BranchError::OffsetOverflow`]
    /// when the addition wraps.
    ///
    /// `len` is taken as `u64` so the same helper serves callers that begin
    /// with `bytes.len() as u64` and callers that already hold a `u64` length.
    fn checked_end(offset: u64, len: u64) -> io::Result<u64> {
        offset
            .checked_add(len)
            .ok_or_else(|| BranchError::OffsetOverflow.into())
    }

    /// Index of the first entry in the trailing overwrite-only suffix of the
    /// delta log.
    ///
    /// Returns `log.len()` when the log is empty or its last entry is not an
    /// `Overwrite`. Returns `0` when every entry is an `Overwrite`. Otherwise
    /// returns one past the index of the most recent non-`Overwrite` entry,
    /// since `Insert` and `Delete` entries shift the coordinate space and
    /// therefore form a hard boundary that overwrite merging cannot cross.
    fn find_overwrite_suffix_start(&self) -> usize {
        let log = self.log.borrow();
        log.iter()
            .rposition(|d| !matches!(d, Delta::Overwrite { .. }))
            .map(|i| i + 1)
            .unwrap_or(0)
    }

    /// Grow `[start, end)` to a fixed point by unioning every overlapping or
    /// adjacent `Overwrite` entry in `self.log[suffix_start..]`.
    ///
    /// The loop terminates when a full pass produces no change.  A single pass
    /// is insufficient because absorbing one entry may extend the range so that
    /// it now touches a previously disjoint entry.
    ///
    /// # Complexity
    ///
    /// Worst-case `O(k²)` in `k = log.len() - suffix_start`: each pass scans
    /// the suffix, and the range may grow by only one entry per pass.  In
    /// practice `k` is small because `DerivedBranch::overwrite` collapses the
    /// suffix back into a single entry on every call.
    fn grow_merge_range(&self, suffix_start: usize, start: u64, end: u64) -> (u64, u64) {
        let mut m_start = start;
        let mut m_end = end;
        loop {
            let prev_start = m_start;
            let prev_end = m_end;
            {
                let log = self.log.borrow();
                for d in &(*log)[suffix_start..] {
                    if let Delta::Overwrite {
                        offset: o,
                        bytes: b,
                    } = d
                    {
                        let o_end = o + b.len() as u64;
                        if *o <= m_end && m_start <= o_end {
                            m_start = m_start.min(*o);
                            m_end = m_end.max(o_end);
                        }
                    }
                }
            }
            if m_start == prev_start && m_end == prev_end {
                return (m_start, m_end);
            }
        }
    }

    /// Derive a new, empty Branch from a `BaseBranch`.
    #[allow(clippy::arc_with_non_send_sync)]
    pub(crate) fn derive_from_base(parent: Arc<BaseBranch>) -> Arc<Self> {
        Arc::new(Self {
            parent: Arc::new(BranchParent::Base(parent)),
            log: RefCell::new(Vec::new()),
            table: RefCell::new(None),
        })
    }

    /// Build a derived branch layered on top of any existing Branch.
    ///
    /// Not called by production code — `Branch::fork` now snapshots the parent
    /// into a `BaseBranch`.  Kept as a `pub(crate)` constructor so that unit
    /// tests can exercise the `BranchParent::Derived` path in `byte_len` and
    /// `read_at` directly.
    #[allow(dead_code)]
    #[allow(clippy::arc_with_non_send_sync)]
    pub(crate) fn derive_from_derived(parent: Arc<dyn Branch>) -> Arc<Self> {
        Arc::new(Self {
            parent: Arc::new(BranchParent::Derived(parent)),
            log: RefCell::new(Vec::new()),
            table: RefCell::new(None),
        })
    }

    /// Ensure the piece table is built and return a `Ref` to it.
    fn ensure_table(&self) {
        let mut slot = self.table.borrow_mut();
        if slot.is_none() {
            *slot = Some(PieceTable::build(
                &self.log.borrow(),
                self.parent.byte_len(),
            ));
        }
    }

    /// Total byte length of this Branch's content.
    pub fn byte_len(&self) -> u64 {
        self.ensure_table();
        self.table.borrow().as_ref().unwrap().byte_len
    }

    /// Read up to `buf.len()` bytes starting at `offset`.
    /// Returns `Ok(0)` when `offset >= byte_len()` or `buf` is empty.
    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.ensure_table();
        let tbl = self.table.borrow();
        let tbl = tbl.as_ref().unwrap();
        tbl.read_at(offset, buf, |off, b| self.parent.read_at(off, b))
    }

    /// Return a lazy `Read + Seek` adapter over this Branch at position 0.
    /// No bytes are copied until `read` is called on the returned reader.
    ///
    /// Returns the concrete `BranchReader` type; used in tests and internal
    /// code that has a concrete `&DerivedBranch` reference.
    #[allow(dead_code)]
    pub fn as_reader(&self) -> BranchReader<'_, Self> {
        BranchReader::new(self)
    }

    /// Replace bytes at `[offset, offset + bytes.len())` with `bytes`.
    ///
    /// Returns `Err(InvalidInput)` when `offset + bytes.len() > byte_len()`.
    /// A zero-length `bytes` slice is a no-op (returns `Ok(())`).
    ///
    /// Merge: all `Overwrite` entries in the trailing overwrite-only suffix of
    /// the log whose byte ranges overlap or are adjacent to the accumulating
    /// merged range are collapsed into one entry. The fixed-point scan repeats
    /// until the merged range no longer grows, so a new write that bridges two
    /// previously disjoint entries will absorb all three into one. The newest
    /// write wins wherever ranges overlap; older writes fill the gaps.
    ///
    /// Only `Overwrite` entries are eligible — an `Insert` or `Delete` anywhere
    /// in the log creates a suffix boundary because it shifts the coordinate
    /// space used by all subsequent deltas.
    ///
    /// # Invariants preserved by the helpers
    ///
    /// 1. **Suffix boundary.** [`find_overwrite_suffix_start`] returns an index
    ///    strictly past the most recent coordinate-shifting delta (`Insert` or
    ///    `Delete`).  Nothing the merge does crosses that boundary, so deltas
    ///    before it remain untouched and in their original relative order.
    /// 2. **Suffix homogeneity.** Every entry in `self.log[suffix_start..]`
    ///    is — and remains — a `Delta::Overwrite`.  Both `grow_merge_range`
    ///    and [`rebuild_merged_overwrite`] rely on this; the merged entry that
    ///    `rebuild_merged_overwrite` pushes is itself an `Overwrite`, so the
    ///    invariant holds for subsequent calls.
    /// 3. **Stable partition.** Surviving (disjoint) entries keep their
    ///    original relative order via `Iterator::partition` on the suffix
    ///    iterator, and the single merged entry is appended at the end of the
    ///    suffix. Replays of the log therefore observe overlapping writes in
    ///    their original chronological order, which is what makes "newest
    ///    write wins" correct.
    pub fn overwrite(&self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        let end = Self::checked_end(offset, bytes.len() as u64)?;
        if end > self.byte_len() {
            return Err(BranchError::OutOfBounds.into());
        }

        // Find the start of the trailing overwrite-only suffix.  Entries before
        // this index involve coordinate-space shifts and cannot be merged.
        let suffix_start = self.find_overwrite_suffix_start();

        // Fixed-point: grow the merged range until it no longer absorbs any new
        // suffix entry.  A single pass may miss entries that only become
        // adjacent after an earlier absorption expands the range.
        let (m_start, m_end) = self.grow_merge_range(suffix_start, offset, end);

        self.rebuild_merged_overwrite(suffix_start, offset, bytes, m_start, m_end);
        *self.table.borrow_mut() = None;
        Ok(())
    }

    /// Replace `self.log[suffix_start..]` with the surviving (disjoint)
    /// entries followed by a single merged `Overwrite` covering
    /// `[m_start, m_end)`.
    ///
    /// The merged buffer is built by applying every absorbed entry in original
    /// log order (so chronologically later writes overwrite earlier ones in
    /// the overlap region), then applying the new `(new_offset, new_bytes)`
    /// write last so that it wins over everything.  Surviving entries — those
    /// whose ranges lie entirely outside `[m_start, m_end)` — are preserved in
    /// their original relative order via `Vec::partition`.
    ///
    /// Preconditions (upheld by the caller):
    ///
    /// * `suffix_start <= self.log.borrow().len()`
    /// * Every entry in `self.log[suffix_start..]` is a `Delta::Overwrite`.
    /// * `[new_offset, new_offset + new_bytes.len()) ⊆ [m_start, m_end)`.
    /// * `(m_start, m_end)` is the fixed point of `grow_merge_range` for the
    ///   same suffix and request.
    fn rebuild_merged_overwrite(
        &self,
        suffix_start: usize,
        new_offset: u64,
        new_bytes: &[u8],
        m_start: u64,
        m_end: u64,
    ) {
        // Drain the suffix and split: absorbed entries fall within the merged
        // range; surviving entries lie entirely outside it.
        let suffix: Vec<Delta> = self.log.borrow_mut().drain(suffix_start..).collect();
        let (absorbed, surviving): (Vec<_>, Vec<_>) = suffix.into_iter().partition(|d| {
            if let Delta::Overwrite {
                offset: o,
                bytes: b,
            } = d
            {
                let o_end = o + b.len() as u64;
                *o <= m_end && m_start <= o_end
            } else {
                false
            }
        });

        // Build the merged buffer.  Apply absorbed entries in log order so that
        // chronologically later entries overwrite earlier ones in the overlap
        // region.  The new write is applied last so it wins over everything.
        let mut merged = vec![0u8; (m_end - m_start) as usize];
        for d in &absorbed {
            if let Delta::Overwrite {
                offset: o,
                bytes: b,
            } = d
            {
                let buf_off = (*o - m_start) as usize;
                merged[buf_off..buf_off + b.len()].copy_from_slice(b);
            }
        }
        let n_off = (new_offset - m_start) as usize;
        merged[n_off..n_off + new_bytes.len()].copy_from_slice(new_bytes);

        // Re-append surviving entries (they are disjoint from the merged range)
        // followed by the single merged entry.
        let mut log = self.log.borrow_mut();
        log.extend(surviving);
        log.push(Delta::Overwrite {
            offset: m_start,
            bytes: merged,
        });
    }

    /// Insert `bytes` before the byte currently at `offset`.
    ///
    /// An insert at `offset == byte_len()` is an append.
    /// Returns `Err(InvalidInput)` when `offset > byte_len()`.
    /// A zero-length `bytes` slice is a no-op (returns `Ok(())`).
    ///
    /// If the previous log entry is also an `Insert` and the new `offset` (in
    /// post-previous-insert coordinate space) falls within the range
    /// `[prev_off, prev_off + prev_len]`, the two are merged into a single
    /// entry. This covers prepend (`offset == prev_off`), append
    /// (`offset == prev_off + prev_len`), and insertion into the middle of the
    /// previously inserted run.
    pub fn insert_before(&self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        if offset > self.byte_len() {
            return Err(BranchError::OutOfBounds.into());
        }
        // Attempt to merge with the previous log entry.
        let merge_info: Option<(u64, Vec<u8>)> = {
            let log = self.log.borrow();
            if let Some(Delta::Insert {
                offset: prev_off,
                bytes: prev_bytes,
            }) = log.last()
            {
                let prev_off = *prev_off;
                let prev_end = prev_off + prev_bytes.len() as u64;
                // offset is in post-prev-insert space; merge if it lands within
                // or immediately adjacent to the previously inserted region.
                if offset >= prev_off && offset <= prev_end {
                    Some((prev_off, prev_bytes.clone()))
                } else {
                    None
                }
            } else {
                None
            }
        };
        if let Some((prev_off, prev_bytes)) = merge_info {
            let k = (offset - prev_off) as usize;
            let mut merged = Vec::with_capacity(prev_bytes.len() + bytes.len());
            merged.extend_from_slice(&prev_bytes[..k]);
            merged.extend_from_slice(bytes);
            merged.extend_from_slice(&prev_bytes[k..]);
            *self.log.borrow_mut().last_mut().unwrap() = Delta::Insert {
                offset: prev_off,
                bytes: merged,
            };
        } else {
            self.log.borrow_mut().push(Delta::Insert {
                offset,
                bytes: bytes.to_vec(),
            });
        }
        *self.table.borrow_mut() = None;
        Ok(())
    }

    /// Append `bytes` to the end of this Branch.
    ///
    /// Equivalent to `insert_before(byte_len(), bytes)`. A zero-length
    /// `bytes` slice is a no-op (no new log entry). Consecutive appends
    /// (with no intervening non-`Insert` mutation) merge into a single log
    /// entry via the same merge logic as `insert_before`.
    pub fn append(&self, bytes: &[u8]) -> io::Result<()> {
        let end = self.byte_len();
        self.insert_before(end, bytes)
    }

    /// Remove `len` bytes starting at `offset`.
    ///
    /// Returns `Err(InvalidInput)` when `offset + len > byte_len()`.
    /// A zero `len` is a no-op (returns `Ok(())`).
    pub fn delete(&self, offset: u64, len: u64) -> io::Result<()> {
        if len == 0 {
            return Ok(());
        }
        let end = Self::checked_end(offset, len)?;
        if end > self.byte_len() {
            return Err(BranchError::OutOfBounds.into());
        }
        self.log.borrow_mut().push(Delta::Delete { offset, len });
        *self.table.borrow_mut() = None;
        Ok(())
    }

    /// Remove all bytes from `new_len` onward, shortening the Branch.
    ///
    /// `new_len == byte_len()` is a no-op (no log entry added).
    /// Returns `Err(InvalidInput)` when `new_len > byte_len()`.
    /// Internally synthesised as `delete(new_len, byte_len() - new_len)`.
    pub fn truncate(&self, new_len: u64) -> io::Result<()> {
        let current = self.byte_len();
        if new_len > current {
            return Err(BranchError::OutOfBounds.into());
        }
        self.delete(new_len, current - new_len)
    }
}

impl crate::branch_reader::ByteSource for DerivedBranch {
    fn byte_len(&self) -> u64 {
        self.byte_len()
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.read_at(offset, buf)
    }
}

impl Branch for DerivedBranch {
    fn byte_len(&self) -> u64 {
        self.byte_len()
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.read_at(offset, buf)
    }

    fn as_reader(&self) -> Box<dyn ReadSeek + '_> {
        Box::new(BranchReader::new(self))
    }

    fn overwrite(&self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.overwrite(offset, bytes)
    }

    fn insert_before(&self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.insert_before(offset, bytes)
    }

    fn delete(&self, offset: u64, len: u64) -> io::Result<()> {
        self.delete(offset, len)
    }

    fn append(&self, bytes: &[u8]) -> io::Result<()> {
        self.append(bytes)
    }

    fn truncate(&self, new_len: u64) -> io::Result<()> {
        self.truncate(new_len)
    }

    /// Walk the piece table to find `parent_offset` in the child's coordinate
    /// space.
    ///
    /// Each `Piece::Parent` entry records a contiguous run of parent bytes that
    /// survive into this branch.  `child_pos` tracks the logical child offset
    /// at which the current piece begins.  When `parent_offset` falls inside a
    /// `Parent` piece the result is `Mapped(child_pos + (parent_offset - po))`.
    /// Bytes removed by a `Delete` or replaced by an `Overwrite` leave gaps
    /// in the parent runs and are returned as `Displaced`.
    fn map_offset_to_fork(&self, parent_offset: u64) -> Option<u64> {
        use crate::piece_table::Piece;
        self.ensure_table();
        let tbl = self.table.borrow();
        let tbl = tbl.as_ref().unwrap();
        let mut child_pos: u64 = 0;
        for piece in &tbl.pieces {
            match piece {
                Piece::Parent {
                    parent_offset: po,
                    len,
                } => {
                    if parent_offset >= *po && parent_offset < po + len {
                        return Some(child_pos + (parent_offset - po));
                    }
                    child_pos += len;
                }
                Piece::Inline { len, .. } => {
                    child_pos += *len as u64;
                }
            }
        }
        None
    }

    /// Walk the piece table to find a contiguous parent range in the child's
    /// coordinate space.
    ///
    /// The range maps successfully only when both `parent_range.start` and
    /// `parent_range.end` fall within the **same** `Piece::Parent` span.  If
    /// an insert has split the run (so start and end land in different pieces),
    /// or any byte in the range was deleted or overwritten, returns `None`.
    fn map_range_to_fork(
        &self,
        parent_range: std::ops::Range<u64>,
    ) -> Option<std::ops::Range<u64>> {
        use crate::piece_table::Piece;
        self.ensure_table();
        let tbl = self.table.borrow();
        let tbl = tbl.as_ref().unwrap();
        let mut child_pos: u64 = 0;
        for piece in &tbl.pieces {
            match piece {
                Piece::Parent {
                    parent_offset: po,
                    len,
                } => {
                    // The entire range must fit inside this one piece.
                    if parent_range.start >= *po && parent_range.end <= po + len {
                        let delta = parent_range.start - po;
                        let span = parent_range.end - parent_range.start;
                        let child_start = child_pos + delta;
                        return Some(child_start..child_start + span);
                    }
                    child_pos += len;
                }
                Piece::Inline { len, .. } => {
                    child_pos += *len as u64;
                }
            }
        }
        None
    }

    fn fork(&self) -> std::sync::Arc<dyn crate::branch::Branch> {
        // Snapshot the current content into an immutable BaseBranch so that
        // subsequent mutations to `self` are not visible in the child.
        let bytes = crate::materialize::materialize(self)
            .expect("fork: failed to materialize branch snapshot");
        let snapshot = crate::base_branch::BaseBranch::from_bytes(bytes).into_arc();
        DerivedBranch::derive_from_base(snapshot)
    }
}

#[cfg(test)]
#[path = "tests/derived_branch.rs"]
mod tests;
