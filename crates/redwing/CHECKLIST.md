# Refactor checklist: `DerivedBranch::overwrite`

Goal: split the ~50-line `overwrite` implementation in [src/derived_branch.rs](src/derived_branch.rs) into named helpers, making the four-phase fixed-point merge algorithm readable in isolation. **Behavior must not change** — every existing test in `tests/`, the integration suite, and the `m8_*` stress suites must pass without modification.

## Background

Today `overwrite` does four things in one body:

1. **Validate** the requested range.
2. **Locate** the trailing overwrite-only suffix of the delta log.
3. **Grow** a merged byte range to fixed point by absorbing every overlapping/adjacent `Overwrite` entry in the suffix.
4. **Rebuild** the merged buffer (older absorbed entries first, then the new write so it wins on overlap) and append it back as a single `Overwrite`, preserving any surviving (disjoint) suffix entries.

The phases are correct but tightly interleaved with `RefCell` borrow scopes, which is the main source of fragility.

## Target shape

```rust
impl DerivedBranch {
    pub fn overwrite(&self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        if bytes.is_empty() { return Ok(()); }
        let end = checked_end(offset, bytes.len())?;
        if end > self.byte_len() {
            return Err(BranchError::OutOfBounds.into());
        }

        let suffix_start = self.find_overwrite_suffix_start();
        let (m_start, m_end) = self.grow_merge_range(suffix_start, offset, end);
        self.rebuild_merged_overwrite(suffix_start, offset, bytes, m_start, m_end);
        *self.table.borrow_mut() = None;
        Ok(())
    }
}
```

Each helper has a single responsibility, holds at most one `RefCell` borrow at a time, and is independently testable.

## Steps

- [x] **Step 1 — Extract `checked_end`**
  - Add a small free function (or `fn` on `DerivedBranch`):
    `fn checked_end(offset: u64, len: usize) -> io::Result<u64>` returning `BranchError::OffsetOverflow` on overflow.
  - Reuse it in `overwrite`, `delete`, and (where relevant) `insert_before`.
  - Run `cargo test`.

- [x] **Step 2 — Extract `find_overwrite_suffix_start`**
  - Signature: `fn find_overwrite_suffix_start(&self) -> usize`.
  - Body: borrow `self.log`, return the index after the last non-`Overwrite` entry (or 0 if none).
  - Replace the inline `rposition` block in `overwrite`.
  - Run `cargo test`.

- [x] **Step 3 — Extract `grow_merge_range`**
  - Signature: `fn grow_merge_range(&self, suffix_start: usize, start: u64, end: u64) -> (u64, u64)`.
  - Body: the fixed-point loop that walks `&self.log.borrow()[suffix_start..]` expanding `(m_start, m_end)` while any `Overwrite` overlaps or touches the merged range. Loop until a pass produces no growth.
  - Pure read (immutable borrow only). No `&mut` needed.
  - Run `cargo test`.

- [x] **Step 4 — Extract `rebuild_merged_overwrite`**
  - Signature:
    ```rust
    fn rebuild_merged_overwrite(
        &self,
        suffix_start: usize,
        new_offset: u64,
        new_bytes: &[u8],
        m_start: u64,
        m_end: u64,
    )
    ```
  - Body:
    1. Drain the suffix (`self.log.borrow_mut().drain(suffix_start..).collect::<Vec<_>>()`).
    2. Partition into `absorbed` (within `[m_start, m_end]`) and `surviving` (outside).
    3. Allocate `merged = vec![0u8; (m_end - m_start) as usize]`.
    4. Apply absorbed entries in original log order.
    5. Apply the new write last so it wins on overlap.
    6. Push surviving entries back, then push the single merged `Overwrite`.
  - The mutable borrow lives only inside this function.
  - Run `cargo test`.

- [x] **Step 5 — Add unit tests for the helpers (in `src/tests/derived_branch.rs`)**
  - `find_overwrite_suffix_start`:
    - empty log → 0
    - log of all overwrites → 0
    - log with insert/delete in the middle → index after the last non-overwrite
  - `grow_merge_range`:
    - new range disjoint from all suffix entries → returns `(start, end)` unchanged
    - one overlapping entry → returns the union
    - bridging case: a new write that touches two previously disjoint entries absorbs both (requires fixed-point — single pass would miss it)
  - These tests directly exercise the algorithm and prevent silent regressions.

- [x] **Step 6 — Add invariant comment block at the top of `overwrite`**
  - Document the three invariants the helpers preserve:
    1. The suffix starts after the last coordinate-shifting delta (insert/delete).
    2. Within the suffix, all entries are `Overwrite` and remain so after rebuild.
    3. The merged entry replaces only entries that overlap the merged range; disjoint entries are preserved in their original log order relative to each other (stable partition).

- [x] **Step 7 — Note worst-case complexity**
  - One-line comment on `grow_merge_range` explaining that the fixed-point loop is O(k²) in the suffix length `k` worst-case (each pass may grow the range by one entry; each pass scans `k` entries). Acceptable because suffixes stay small in practice and the merge prevents log-growth elsewhere.

- [x] **Step 8 — Verify**
  - `cargo build`
  - `cargo clippy --all-targets`
  - `cargo test`
  - All `m8_*` stress suites must still pass.

## Out of scope

- Changing the on-the-wire delta representation.
- Changing the merge semantics (the algorithm output must be byte-for-byte identical).
- Replacing `RefCell` with anything else.
- Touching `insert_before` merging beyond reusing `checked_end` if convenient.
