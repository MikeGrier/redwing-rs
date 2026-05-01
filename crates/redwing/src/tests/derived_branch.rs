// Copyright (c) 2026 Michael J. Grier
use std::{io, io::Cursor, sync::Arc};

use super::{BranchParent, DerivedBranch};
use crate::{base_branch::BaseBranch, branch::Branch, delta::Delta};

fn base_arc(data: &[u8]) -> Arc<BaseBranch> {
    Arc::new(BaseBranch::from_reader(Cursor::new(data.to_vec())).unwrap())
}

fn base_parent(data: &[u8]) -> BranchParent {
    BranchParent::Base(base_arc(data))
}

// ── byte_len delegation ───────────────────────────────────────────────────

#[test]
fn base_parent_byte_len_empty() {
    let p = BranchParent::Base(Arc::new(BaseBranch::empty()));
    assert_eq!(p.byte_len(), 0);
}

#[test]
fn base_parent_byte_len_nonempty() {
    let p = base_parent(b"hello");
    assert_eq!(p.byte_len(), 5);
}

#[test]
fn base_parent_byte_len_single_byte() {
    let p = base_parent(b"X");
    assert_eq!(p.byte_len(), 1);
}

#[test]
fn base_parent_byte_len_256_bytes() {
    let data: Vec<u8> = (0u8..=255).collect();
    let p = base_parent(&data);
    assert_eq!(p.byte_len(), 256);
}

// ── read_at delegation ────────────────────────────────────────────────────

#[test]
fn base_parent_read_at_full() {
    let p = base_parent(b"ABCDE");
    let mut buf = [0u8; 5];
    let n = p.read_at(0, &mut buf).unwrap();
    assert_eq!(n, 5);
    assert_eq!(&buf, b"ABCDE");
}

#[test]
fn base_parent_read_at_middle() {
    let p = base_parent(b"ABCDE");
    let mut buf = [0u8; 2];
    let n = p.read_at(2, &mut buf).unwrap();
    assert_eq!(n, 2);
    assert_eq!(&buf, b"CD");
}

#[test]
fn base_parent_read_at_past_end_returns_zero() {
    let p = base_parent(b"ABCDE");
    let mut buf = [0u8; 4];
    let n = p.read_at(5, &mut buf).unwrap();
    assert_eq!(n, 0);
}

#[test]
fn base_parent_read_at_buf_larger_than_remaining() {
    let p = base_parent(b"AB");
    let mut buf = [0xFFu8; 8];
    let n = p.read_at(1, &mut buf).unwrap();
    assert_eq!(n, 1);
    assert_eq!(buf[0], b'B');
}

#[test]
fn base_parent_read_at_empty_content() {
    let p = BranchParent::Base(Arc::new(BaseBranch::empty()));
    let mut buf = [0u8; 4];
    let n = p.read_at(0, &mut buf).unwrap();
    assert_eq!(n, 0);
}

#[test]
fn base_parent_read_at_empty_buf() {
    let p = base_parent(b"ABCDE");
    let n = p.read_at(0, &mut []).unwrap();
    assert_eq!(n, 0);
}

#[test]
fn base_parent_read_at_single_byte_each_position() {
    let data = b"WXYZ";
    let p = base_parent(data);
    for (i, &expected) in data.iter().enumerate() {
        let mut buf = [0u8; 1];
        let n = p.read_at(i as u64, &mut buf).unwrap();
        assert_eq!(n, 1, "offset {i}");
        assert_eq!(buf[0], expected, "offset {i}");
    }
}

// ── M3-2: DerivedBranch constructors, byte_len, read_at ────────────────

#[test]
fn derive_from_base_empty_byte_len_is_zero() {
    let ds = DerivedBranch::derive_from_base(Arc::new(BaseBranch::empty()));
    assert_eq!(ds.byte_len(), 0);
}

#[test]
fn derive_from_base_nonempty_byte_len_matches_parent() {
    let ds = DerivedBranch::derive_from_base(base_arc(b"Hello!"));
    assert_eq!(ds.byte_len(), 6);
}

#[test]
fn derive_from_base_byte_len_stable_across_multiple_calls() {
    // ensure_table is idempotent — calling byte_len twice must agree
    let ds = DerivedBranch::derive_from_base(base_arc(b"Stable"));
    assert_eq!(ds.byte_len(), ds.byte_len());
}

#[test]
fn derive_from_base_read_at_full_mirrors_parent() {
    let data = b"Hello, World!";
    let ds = DerivedBranch::derive_from_base(base_arc(data));
    let mut buf = vec![0u8; data.len()];
    let n = ds.read_at(0, &mut buf).unwrap();
    assert_eq!(n, data.len());
    assert_eq!(&buf, data);
}

#[test]
fn derive_from_base_read_at_middle() {
    let data = b"ABCDEFGH";
    let ds = DerivedBranch::derive_from_base(base_arc(data));
    let mut buf = [0u8; 3];
    let n = ds.read_at(2, &mut buf).unwrap();
    assert_eq!(n, 3);
    assert_eq!(&buf, b"CDE");
}

#[test]
fn derive_from_base_read_at_past_end_returns_zero() {
    let ds = DerivedBranch::derive_from_base(base_arc(b"ABCDE"));
    let mut buf = [0u8; 4];
    let n = ds.read_at(5, &mut buf).unwrap();
    assert_eq!(n, 0);
}

#[test]
fn derive_from_base_read_at_empty_buf_returns_zero() {
    let ds = DerivedBranch::derive_from_base(base_arc(b"ABCDE"));
    let n = ds.read_at(0, &mut []).unwrap();
    assert_eq!(n, 0);
}

#[test]
fn derive_from_base_read_at_buf_larger_than_remaining() {
    let ds = DerivedBranch::derive_from_base(base_arc(b"AB"));
    let mut buf = [0xFFu8; 8];
    let n = ds.read_at(1, &mut buf).unwrap();
    assert_eq!(n, 1);
    assert_eq!(buf[0], b'B');
}

#[test]
fn derive_from_base_read_at_single_byte_each_position() {
    let data = b"WXYZ";
    let ds = DerivedBranch::derive_from_base(base_arc(data));
    for (i, &expected) in data.iter().enumerate() {
        let mut buf = [0u8; 1];
        let n = ds.read_at(i as u64, &mut buf).unwrap();
        assert_eq!(n, 1, "offset {i}");
        assert_eq!(buf[0], expected, "offset {i}");
    }
}

#[test]
fn derive_from_base_read_at_large_data() {
    let data: Vec<u8> = (0u8..=255).cycle().take(1024).collect();
    let ds = DerivedBranch::derive_from_base(base_arc(&data));
    assert_eq!(ds.byte_len(), 1024);
    let mut buf = vec![0u8; 1024];
    let n = ds.read_at(0, &mut buf).unwrap();
    assert_eq!(n, 1024);
    assert_eq!(buf, data);
}

#[test]
fn derive_from_derived_byte_len_matches_grandparent() {
    let grandparent = base_arc(b"Grandparent");
    let child = DerivedBranch::derive_from_base(grandparent);
    let grandchild = DerivedBranch::derive_from_derived(child);
    assert_eq!(grandchild.byte_len(), 11);
}

#[test]
fn derive_from_derived_read_at_full_mirrors_grandparent() {
    let data = b"ChainTest";
    let child = DerivedBranch::derive_from_base(base_arc(data));
    let grandchild = DerivedBranch::derive_from_derived(child);
    let mut buf = vec![0u8; data.len()];
    let n = grandchild.read_at(0, &mut buf).unwrap();
    assert_eq!(n, data.len());
    assert_eq!(&buf, data);
}

#[test]
fn derive_from_derived_read_at_past_end_returns_zero() {
    let child = DerivedBranch::derive_from_base(base_arc(b"Short"));
    let grandchild = DerivedBranch::derive_from_derived(child);
    let mut buf = [0u8; 4];
    let n = grandchild.read_at(10, &mut buf).unwrap();
    assert_eq!(n, 0);
}

#[test]
fn derive_from_derived_read_at_single_byte_each_position() {
    let data = b"ABCDE";
    let child = DerivedBranch::derive_from_base(base_arc(data));
    let grandchild = DerivedBranch::derive_from_derived(child);
    for (i, &expected) in data.iter().enumerate() {
        let mut buf = [0u8; 1];
        let n = grandchild.read_at(i as u64, &mut buf).unwrap();
        assert_eq!(n, 1, "offset {i}");
        assert_eq!(buf[0], expected, "offset {i}");
    }
}

#[test]
fn derive_from_derived_empty_parent_byte_len_zero() {
    let child = DerivedBranch::derive_from_base(Arc::new(BaseBranch::empty()));
    let grandchild = DerivedBranch::derive_from_derived(child);
    assert_eq!(grandchild.byte_len(), 0);
}

#[test]
fn log_starts_empty() {
    let ds = DerivedBranch::derive_from_base(base_arc(b"anything"));
    assert!(ds.log.borrow().is_empty());
}

// ── M3-3.1: overwrite ────────────────────────────────────────────────────

// Helper: Arc with a single strong reference.
fn owning_ds(data: &[u8]) -> Arc<DerivedBranch> {
    DerivedBranch::derive_from_base(base_arc(data))
}

#[test]
fn overwrite_at_offset_zero_changes_first_bytes() {
    let ds = owning_ds(b"Hello, World!");
    ds.overwrite(0, b"Jello").unwrap();
    let mut buf = vec![0u8; 13];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"Jello, World!");
}

#[test]
fn overwrite_at_last_byte() {
    let ds = owning_ds(b"ABCDE");
    ds.overwrite(4, b"Z").unwrap();
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABCDZ");
}

#[test]
fn overwrite_in_middle() {
    let ds = owning_ds(b"ABCDEFGH");
    ds.overwrite(2, b"XY").unwrap();
    let mut buf = [0u8; 8];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABXYEFGH");
}

#[test]
fn overwrite_entire_content() {
    let ds = owning_ds(b"ABCDE");
    ds.overwrite(0, b"VWXYZ").unwrap();
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"VWXYZ");
}

#[test]
fn overwrite_byte_len_unchanged() {
    let ds = owning_ds(b"ABCDE");
    assert_eq!(ds.byte_len(), 5);
    ds.overwrite(1, b"XYZ").unwrap();
    assert_eq!(ds.byte_len(), 5);
}

#[test]
fn overwrite_idempotent_same_bytes() {
    let data = b"Hello";
    let ds = owning_ds(data);
    ds.overwrite(0, data).unwrap();
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"Hello");
}

#[test]
fn overwrite_second_write_wins_over_first() {
    let ds = owning_ds(b"AAAAA");
    ds.overwrite(1, b"BBB").unwrap();
    ds.overwrite(1, b"CCC").unwrap();
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ACCCA");
}

#[test]
fn overwrite_zero_length_is_noop() {
    let ds = owning_ds(b"ABCDE");
    ds.overwrite(2, b"").unwrap();
    assert_eq!(ds.byte_len(), 5);
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABCDE");
}

#[test]
fn overwrite_out_of_bounds_offset_returns_err() {
    let ds = owning_ds(b"ABCDE");
    let r = ds.overwrite(5, b"X");
    assert!(r.is_err());
    assert_eq!(r.unwrap_err().kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn overwrite_offset_in_range_but_extends_past_end_returns_err() {
    let ds = owning_ds(b"ABCDE");
    let r = ds.overwrite(3, b"XYZ"); // 3+3=6 > 5
    assert!(r.is_err());
    assert_eq!(r.unwrap_err().kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn overwrite_appends_delta_to_log() {
    let ds = owning_ds(b"ABCDE");
    ds.overwrite(1, b"BC").unwrap();
    assert_eq!(ds.log.borrow().len(), 1);
    assert!(matches!(
        ds.log.borrow()[0],
        Delta::Overwrite { offset: 1, .. }
    ));
}

// ── overwrite merge tests ─────────────────────────────────────────────────

#[test]
fn overwrite_merge_adjacent_after_produces_single_log_entry() {
    // [1,3) then [3,5) → [1,5): one entry
    let ds = owning_ds(b"ABCDE");
    ds.overwrite(1, b"BC").unwrap();
    ds.overwrite(3, b"DE").unwrap();
    assert_eq!(ds.log.borrow().len(), 1);
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABCDE");
}

#[test]
fn overwrite_merge_adjacent_before_produces_single_log_entry() {
    // [3,5) then [1,3): adjacent, merged into [1,5)
    let ds = owning_ds(b"ABCDE");
    ds.overwrite(3, b"DE").unwrap();
    ds.overwrite(1, b"BC").unwrap();
    assert_eq!(ds.log.borrow().len(), 1);
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABCDE");
}

#[test]
fn overwrite_merge_overlapping_later_wins() {
    // [1,4) then [2,5) → merged [1,5), new write wins in [2,4)
    let ds = owning_ds(b"AAAAA");
    ds.overwrite(1, b"XXX").unwrap(); // [1,4)
    ds.overwrite(2, b"YYY").unwrap(); // [2,5), new wins
    assert_eq!(ds.log.borrow().len(), 1);
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"AXYYY");
}

#[test]
fn overwrite_merge_fully_contained_new_wins() {
    // New range fully inside prev range
    let ds = owning_ds(b"AAAAA");
    ds.overwrite(0, b"XXXXX").unwrap();
    ds.overwrite(1, b"YYY").unwrap();
    assert_eq!(ds.log.borrow().len(), 1);
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"XYYYX");
}

#[test]
fn overwrite_merge_extends_range() {
    // New range extends prev range to the right
    let ds = owning_ds(b"AAAAA");
    ds.overwrite(0, b"BB").unwrap(); // [0,2)
    ds.overwrite(1, b"CCC").unwrap(); // [1,4), extends right
    assert_eq!(ds.log.borrow().len(), 1);
    let mut buf = [0u8; 4];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"BCCC");
}

#[test]
fn overwrite_no_merge_when_gap_between() {
    // [0,2) and [3,5) have a 1-byte gap — they must NOT merge.
    let ds = owning_ds(b"ABCDE");
    ds.overwrite(0, b"XY").unwrap();
    ds.overwrite(3, b"PQ").unwrap();
    assert_eq!(ds.log.borrow().len(), 2);
}

#[test]
fn overwrite_no_merge_after_insert() {
    // Last log entry is Insert, not Overwrite → new overwrite starts a fresh
    // suffix; the Insert is not absorbed.
    let ds = owning_ds(b"ABCDE");
    ds.insert_before(0, b"Z").unwrap();
    ds.overwrite(1, b"X").unwrap();
    assert_eq!(ds.log.borrow().len(), 2);
}

#[test]
fn overwrite_chain_of_adjacent_writes_coalesces_to_single_entry() {
    // Four sequential 4-byte writes to adjacent blocks must produce exactly
    // one log entry whose byte range covers all four blocks.
    let ds = owning_ds(&[0u8; 16]);
    ds.overwrite(0, &[1u8; 4]).unwrap();
    ds.overwrite(4, &[2u8; 4]).unwrap();
    ds.overwrite(8, &[3u8; 4]).unwrap();
    ds.overwrite(12, &[4u8; 4]).unwrap();
    assert_eq!(
        ds.log.borrow().len(),
        1,
        "all adjacent writes must coalesce into one entry"
    );
    let mut buf = [0u8; 16];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf[0..4], &[1u8; 4]);
    assert_eq!(&buf[4..8], &[2u8; 4]);
    assert_eq!(&buf[8..12], &[3u8; 4]);
    assert_eq!(&buf[12..16], &[4u8; 4]);
}

// ── overwrite global-scan merge tests ────────────────────────────────────

#[test]
fn overwrite_gap_fill_bridges_two_disjoint_entries_into_one() {
    // [0,4) then [8,12) are disjoint.  Writing [4,8) bridges them → 1 entry.
    let parent = vec![0u8; 12];
    let ds = owning_ds(&parent);
    ds.overwrite(0, &[1u8; 4]).unwrap();
    ds.overwrite(8, &[3u8; 4]).unwrap();
    ds.overwrite(4, &[2u8; 4]).unwrap(); // bridge
    assert_eq!(
        ds.log.borrow().len(),
        1,
        "bridge write must coalesce all three"
    );
    let mut buf = [0u8; 12];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf[0..4], &[1u8; 4]);
    assert_eq!(&buf[4..8], &[2u8; 4]);
    assert_eq!(&buf[8..12], &[3u8; 4]);
}

#[test]
fn overwrite_gap_fill_from_right_bridges_two_disjoint_entries() {
    // Write right group first, left group second, then bridge.
    let parent = vec![0u8; 12];
    let ds = owning_ds(&parent);
    ds.overwrite(8, &[3u8; 4]).unwrap();
    ds.overwrite(0, &[1u8; 4]).unwrap();
    ds.overwrite(4, &[2u8; 4]).unwrap(); // bridge
    assert_eq!(ds.log.borrow().len(), 1);
    let mut buf = [0u8; 12];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf[0..4], &[1u8; 4]);
    assert_eq!(&buf[4..8], &[2u8; 4]);
    assert_eq!(&buf[8..12], &[3u8; 4]);
}

#[test]
fn overwrite_bridge_absorbs_only_the_touching_group_leaves_other_intact() {
    // Three disjoint groups: [0,4), [8,12), [20,24).
    // Bridge [4,8) should coalesce the first two into one while leaving
    // [20,24) as a separate entry.
    let parent = vec![0u8; 24];
    let ds = owning_ds(&parent);
    ds.overwrite(0, &[1u8; 4]).unwrap();
    ds.overwrite(8, &[3u8; 4]).unwrap();
    ds.overwrite(20, &[9u8; 4]).unwrap();
    ds.overwrite(4, &[2u8; 4]).unwrap(); // bridges [0,4) and [8,12) only
                                         // Expected: one surviving entry [20,24) + one merged entry [0,12) = 2 total
    assert_eq!(ds.log.borrow().len(), 2);
    let mut buf = [0u8; 24];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf[0..4], &[1u8; 4]);
    assert_eq!(&buf[4..8], &[2u8; 4]);
    assert_eq!(&buf[8..12], &[3u8; 4]);
    assert_eq!(&buf[20..24], &[9u8; 4]);
}

#[test]
fn overwrite_transitive_chain_three_disjoint_groups_one_write() {
    // [0,4), [8,12), [4,8) each separated.  The third write first merges with
    // the second (they touch at 8), extending to [4,12); that range then
    // touches [0,4), so all three coalesce in the fixed-point loop.
    //
    // Specifically: initial groups are [0,4) and [8,12).  New write [4,8)
    // touches [8,12) (adjacent at 8), making [4,12).  [4,12) touches [0,4)
    // (adjacent at 4), so the range grows to [0,12).  Only 1 entry remains.
    let parent = vec![0u8; 12];
    let ds = owning_ds(&parent);
    ds.overwrite(0, &[0xAA; 4]).unwrap();
    ds.overwrite(8, &[0xCC; 4]).unwrap();
    ds.overwrite(4, &[0xBB; 4]).unwrap();
    assert_eq!(ds.log.borrow().len(), 1);
    let mut buf = [0u8; 12];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf[0..4], &[0xAA; 4]);
    assert_eq!(&buf[4..8], &[0xBB; 4]);
    assert_eq!(&buf[8..12], &[0xCC; 4]);
}

#[test]
fn overwrite_new_write_completely_inside_existing_entry_new_wins() {
    // Existing: [0,8) all 0x11.  New write falls at [2,6): the merged entry
    // covers [0,8) with 0x22 in the inner region.
    let ds = owning_ds(&[0u8; 8]);
    ds.overwrite(0, &[0x11; 8]).unwrap();
    ds.overwrite(2, &[0x22; 4]).unwrap();
    assert_eq!(ds.log.borrow().len(), 1);
    let mut buf = [0u8; 8];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf[0..2], &[0x11; 2]);
    assert_eq!(&buf[2..6], &[0x22; 4]);
    assert_eq!(&buf[6..8], &[0x11; 2]);
}

#[test]
fn overwrite_existing_completely_inside_new_write() {
    // Existing: [2,6) all 0x11.  New write covers [0,8): the merged entry
    // covers [0,8) and new bytes win everywhere.
    let ds = owning_ds(&[0u8; 8]);
    ds.overwrite(2, &[0x11; 4]).unwrap();
    ds.overwrite(0, &[0x22; 8]).unwrap();
    assert_eq!(ds.log.borrow().len(), 1);
    let mut buf = [0u8; 8];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, &[0x22u8; 8]);
}

#[test]
fn overwrite_same_range_twice_new_wins() {
    // Writing to the exact same range twice: single entry, new bytes win.
    let ds = owning_ds(&[0u8; 4]);
    ds.overwrite(0, &[0xAA; 4]).unwrap();
    ds.overwrite(0, &[0xBB; 4]).unwrap();
    assert_eq!(ds.log.borrow().len(), 1);
    let mut buf = [0u8; 4];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, &[0xBBu8; 4]);
}

#[test]
fn overwrite_partial_overlap_left_new_wins_in_overlap() {
    // [0,6) then [4,10): overlap in [4,6). New write (applied second) wins there.
    let ds = owning_ds(&[0u8; 10]);
    ds.overwrite(0, &[0x11; 6]).unwrap();
    ds.overwrite(4, &[0x22; 6]).unwrap();
    assert_eq!(ds.log.borrow().len(), 1);
    let mut buf = [0u8; 10];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf[0..4], &[0x11; 4]); // only first write
    assert_eq!(&buf[4..10], &[0x22; 6]); // new write wins in overlap and beyond
}

#[test]
fn overwrite_partial_overlap_right_new_wins_in_overlap() {
    // [4,10) then [0,6): overlap in [4,6). New write wins there.
    let ds = owning_ds(&[0u8; 10]);
    ds.overwrite(4, &[0x11; 6]).unwrap();
    ds.overwrite(0, &[0x22; 6]).unwrap();
    assert_eq!(ds.log.borrow().len(), 1);
    let mut buf = [0u8; 10];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf[0..6], &[0x22; 6]); // new write wins in overlap and its range
    assert_eq!(&buf[6..10], &[0x11; 4]); // only first write
}

#[test]
fn overwrite_64_sequential_64byte_writes_produce_single_entry() {
    // 64 × 64-byte writes to consecutive blocks → one 4096-byte log entry.
    let ds = owning_ds(&[0u8; 4096]);
    for i in 0u8..64 {
        let off = (i as u64) * 64;
        ds.overwrite(off, &[i; 64]).unwrap();
    }
    assert_eq!(
        ds.log.borrow().len(),
        1,
        "64 sequential writes must coalesce"
    );
    let mut buf = vec![0u8; 4096];
    ds.read_at(0, &mut buf).unwrap();
    for i in 0u8..64 {
        let start = i as usize * 64;
        assert_eq!(
            &buf[start..start + 64],
            &vec![i; 64],
            "block {i} has wrong content"
        );
    }
}

#[test]
fn overwrite_out_of_order_writes_coalesce() {
    // Write [0,64), [128,192), [64,128) — interleaved, not in offset order.
    // The third write bridges the first two; all three must coalesce.
    let ds = owning_ds(&[0u8; 192]);
    ds.overwrite(0, &[0x01; 64]).unwrap();
    ds.overwrite(128, &[0x03; 64]).unwrap();
    ds.overwrite(64, &[0x02; 64]).unwrap(); // bridge
    assert_eq!(ds.log.borrow().len(), 1);
    let mut buf = [0u8; 192];
    ds.read_at(0, &mut buf).unwrap();
    assert!(buf[0..64].iter().all(|&b| b == 0x01));
    assert!(buf[64..128].iter().all(|&b| b == 0x02));
    assert!(buf[128..192].iter().all(|&b| b == 0x03));
}

#[test]
fn overwrite_no_merge_when_new_write_is_disjoint_from_all_existing() {
    // Two disjoint existing entries and a new write that touches neither.
    let ds = owning_ds(&[0u8; 20]);
    ds.overwrite(0, &[1u8; 4]).unwrap();
    ds.overwrite(10, &[2u8; 4]).unwrap();
    ds.overwrite(16, &[3u8; 4]).unwrap(); // gap at [14,16) → separate
    assert_eq!(
        ds.log.borrow().len(),
        3,
        "writes with gaps must stay separate"
    );
}

#[test]
fn overwrite_no_merge_across_insert_boundary() {
    // An Insert in the log creates a coordinate-space boundary.
    // Overwrites before the insert cannot merge with overwrites after it,
    // even if their byte ranges appear adjacent.
    let ds = owning_ds(&[0u8; 12]);
    ds.overwrite(0, &[0xAA; 4]).unwrap(); // log[0]: pre-boundary overwrite
    ds.insert_before(4, &[0xFF]).unwrap(); // log[1]: coordinate-space shift
    ds.overwrite(4, &[0xBB; 4]).unwrap(); // log[2]: post-boundary overwrite
    ds.overwrite(8, &[0xCC; 4]).unwrap(); // log[3]: merges with log[2] → combined
                                          // log[0] (overwrite) and log[1] (insert) cannot be merged under any rule.
                                          // log[2] and log[3] are in the same suffix → merged to one entry.
                                          // Total: 3 entries (overwrite, insert, merged-overwrite).
    assert_eq!(ds.log.borrow().len(), 3);
}

#[test]
fn overwrite_no_merge_across_delete_boundary() {
    // A Delete also creates a boundary; overwrites on either side must not merge.
    let ds = owning_ds(&[0u8; 12]);
    ds.overwrite(0, &[0xAA; 4]).unwrap(); // log[0]: pre-boundary
    ds.delete(4, 2).unwrap(); // log[1]: boundary
    ds.overwrite(2, &[0xBB; 4]).unwrap(); // log[2]: post-boundary; does NOT merge with log[0]
    assert_eq!(ds.log.borrow().len(), 3);
}

#[test]
fn overwrite_disjoint_pair_does_not_coalesce_without_bridge() {
    // Two genuinely disjoint writes with a permanent gap must stay as two entries
    // as long as no bridging write arrives.
    let ds = owning_ds(&[0u8; 10]);
    ds.overwrite(0, &[1u8; 3]).unwrap(); // [0,3)
    ds.overwrite(5, &[2u8; 3]).unwrap(); // [5,8) — gap [3,5)
    assert_eq!(ds.log.borrow().len(), 2);
}

#[test]
fn overwrite_single_byte_gap_prevents_coalescence() {
    // [0,4) and [5,9) — exactly one byte apart — must NOT coalesce.
    let ds = owning_ds(&[0u8; 9]);
    ds.overwrite(0, &[1u8; 4]).unwrap();
    ds.overwrite(5, &[2u8; 4]).unwrap();
    assert_eq!(ds.log.borrow().len(), 2);
}

#[test]
fn overwrite_adjacent_at_boundary_coalesces() {
    // [0,4) and [4,8): share boundary byte at index 4 → merge to [0,8).
    let ds = owning_ds(&[0u8; 8]);
    ds.overwrite(0, &[0xAA; 4]).unwrap();
    ds.overwrite(4, &[0xBB; 4]).unwrap();
    assert_eq!(ds.log.borrow().len(), 1);
    let mut buf = [0u8; 8];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf[0..4], &[0xAAu8; 4]);
    assert_eq!(&buf[4..8], &[0xBBu8; 4]);
}

#[test]
fn overwrite_multiple_survivors_and_one_merged() {
    // Three disjoint pairs: [0,2), [4,6), [8,10).  The new write covers
    // [4,6) exactly — it merges with [4,6) only; the others survive.
    let ds = owning_ds(&[0u8; 10]);
    ds.overwrite(0, &[0x11; 2]).unwrap();
    ds.overwrite(4, &[0x22; 2]).unwrap();
    ds.overwrite(8, &[0x33; 2]).unwrap();
    ds.overwrite(4, &[0x44; 2]).unwrap(); // targeted overwrite, same range as [4,6)
                                          // [0,2) and [8,10) survive; [4,6) is merged (replaced by new value).
    assert_eq!(ds.log.borrow().len(), 3);
    let mut buf = [0u8; 10];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf[0..2], &[0x11u8; 2]);
    assert_eq!(&buf[4..6], &[0x44u8; 2]); // new write wins
    assert_eq!(&buf[8..10], &[0x33u8; 2]);
}

// ── M3-3.2: insert_before ────────────────────────────────────────────────

#[test]
fn insert_before_at_offset_zero_prepends() {
    let ds = owning_ds(b"World");
    ds.insert_before(0, b"Hello, ").unwrap();
    let mut buf = vec![0u8; 12];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"Hello, World");
}

#[test]
fn insert_before_at_byte_len_appends() {
    let ds = owning_ds(b"Hello");
    ds.insert_before(5, b"!").unwrap();
    let mut buf = vec![0u8; 6];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"Hello!");
}

#[test]
fn insert_before_in_middle() {
    let ds = owning_ds(b"ABDE");
    ds.insert_before(2, b"C").unwrap();
    let mut buf = vec![0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABCDE");
}

#[test]
fn insert_before_into_empty_base() {
    let ds = DerivedBranch::derive_from_base(Arc::new(BaseBranch::empty()));
    ds.insert_before(0, b"data").unwrap();
    assert_eq!(ds.byte_len(), 4);
    let mut buf = [0u8; 4];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"data");
}

#[test]
fn insert_before_single_byte() {
    let ds = owning_ds(b"AB");
    ds.insert_before(1, b"X").unwrap();
    let mut buf = [0u8; 3];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"AXB");
}

#[test]
fn insert_before_byte_len_grows_by_inserted_length() {
    let ds = owning_ds(b"ABCDE");
    assert_eq!(ds.byte_len(), 5);
    ds.insert_before(2, b"XYZ").unwrap();
    assert_eq!(ds.byte_len(), 8);
}

#[test]
fn insert_before_zero_length_is_noop() {
    let ds = owning_ds(b"ABCDE");
    ds.insert_before(2, b"").unwrap();
    assert_eq!(ds.byte_len(), 5);
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABCDE");
}

#[test]
fn insert_before_out_of_bounds_returns_err() {
    let ds = owning_ds(b"ABCDE");
    let r = ds.insert_before(6, b"X");
    assert!(r.is_err());
    assert_eq!(r.unwrap_err().kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn insert_before_two_inserts_at_same_offset_cumulate() {
    // Two inserts at offset 2: first pushes content right, second inserts
    // at the new logical offset 2 (before the first-inserted bytes).
    let ds = owning_ds(b"AE");
    ds.insert_before(1, b"BC").unwrap(); // -> ABCE
    ds.insert_before(3, b"D").unwrap(); // -> ABCDE
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABCDE");
}

#[test]
fn insert_before_large_payload() {
    let base = vec![0u8; 10];
    let payload: Vec<u8> = (1u8..=255).cycle().take(1000).collect();
    let ds = owning_ds(&base);
    ds.insert_before(5, &payload).unwrap();
    assert_eq!(ds.byte_len(), 1010);
    // Check the inserted bytes appear at offset 5
    let mut buf = vec![0u8; 1000];
    ds.read_at(5, &mut buf).unwrap();
    assert_eq!(buf, payload);
}

#[test]
fn insert_before_appends_delta_to_log() {
    let ds = owning_ds(b"ABCDE");
    ds.insert_before(2, b"X").unwrap();
    assert_eq!(ds.log.borrow().len(), 1);
    assert!(matches!(
        ds.log.borrow()[0],
        Delta::Insert { offset: 2, .. }
    ));
}

// ── insert_before merge tests ─────────────────────────────────────────────

#[test]
fn insert_merge_append_to_previous_insert() {
    // Second insert at prev_off + prev_len → append → single entry
    let ds = owning_ds(b"AE");
    ds.insert_before(1, b"BC").unwrap(); // prev_off=1, prev_len=2, prev_end=3
    ds.insert_before(3, b"D").unwrap(); // offset=3 == prev_end → append
    assert_eq!(ds.log.borrow().len(), 1);
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABCDE");
}

#[test]
fn insert_merge_prepend_to_previous_insert() {
    // Second insert at prev_off → prepend → new bytes appear before old
    let ds = owning_ds(b"AE");
    ds.insert_before(1, b"CD").unwrap(); // prev_off=1
    ds.insert_before(1, b"B").unwrap(); // offset=1 == prev_off → prepend
    assert_eq!(ds.log.borrow().len(), 1);
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABCDE");
}

#[test]
fn insert_merge_into_middle_of_previous_insert() {
    // Insert into the middle of the previously inserted run
    let ds = owning_ds(b"AE");
    ds.insert_before(1, b"BD").unwrap(); // prev_off=1, bytes="BD"
    ds.insert_before(2, b"C").unwrap(); // offset=2, prev_off=1, k=1 → B+C+D
    assert_eq!(ds.log.borrow().len(), 1);
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABCDE");
}

#[test]
fn insert_merge_three_sequential_appends_into_one_entry() {
    let ds = owning_ds(b"Z");
    ds.insert_before(0, b"A").unwrap(); // log: [Insert@0 "A"]
    ds.insert_before(1, b"B").unwrap(); // append → [Insert@0 "AB"]
    ds.insert_before(2, b"C").unwrap(); // append → [Insert@0 "ABC"]
    assert_eq!(ds.log.borrow().len(), 1);
    let mut buf = [0u8; 4];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABCZ");
}

#[test]
fn insert_no_merge_when_offset_before_prev_off() {
    // New insert before the previous insert's position → no merge
    let ds = owning_ds(b"ABCDE");
    ds.insert_before(3, b"XY").unwrap(); // prev_off=3
    ds.insert_before(1, b"Z").unwrap(); // offset=1 < prev_off=3 → no merge
    assert_eq!(ds.log.borrow().len(), 2);
}

#[test]
fn insert_no_merge_when_offset_past_prev_end() {
    // New insert after the end of the previous insert's region → no merge
    let ds = owning_ds(b"ABCDE");
    ds.insert_before(1, b"X").unwrap(); // prev_off=1, prev_end=2
    ds.insert_before(4, b"Y").unwrap(); // offset=4 > prev_end=2 → no merge
    assert_eq!(ds.log.borrow().len(), 2);
}

#[test]
fn insert_no_merge_after_overwrite() {
    // Last log entry is Overwrite, not Insert → no merge
    let ds = owning_ds(b"ABCDE");
    ds.overwrite(0, b"X").unwrap();
    ds.insert_before(1, b"Z").unwrap();
    assert_eq!(ds.log.borrow().len(), 2);
}

// ── M3-3.3: delete ────────────────────────────────────────────────────

#[test]
fn delete_at_offset_zero() {
    let ds = owning_ds(b"XABCDE");
    ds.delete(0, 1).unwrap();
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABCDE");
}

#[test]
fn delete_last_bytes() {
    let ds = owning_ds(b"ABCDEX");
    ds.delete(5, 1).unwrap();
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABCDE");
}

#[test]
fn delete_in_middle() {
    let ds = owning_ds(b"ABXCDE");
    ds.delete(2, 1).unwrap();
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABCDE");
}

#[test]
fn delete_entire_content() {
    let ds = owning_ds(b"ABCDE");
    ds.delete(0, 5).unwrap();
    assert_eq!(ds.byte_len(), 0);
}

#[test]
fn delete_byte_len_shrinks_by_deleted_length() {
    let ds = owning_ds(b"ABCDEFGH");
    assert_eq!(ds.byte_len(), 8);
    ds.delete(2, 3).unwrap();
    assert_eq!(ds.byte_len(), 5);
}

#[test]
fn delete_zero_len_is_noop() {
    let ds = owning_ds(b"ABCDE");
    ds.delete(2, 0).unwrap();
    assert_eq!(ds.byte_len(), 5);
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABCDE");
}

#[test]
fn delete_out_of_bounds_offset_returns_err() {
    let ds = owning_ds(b"ABCDE");
    let r = ds.delete(5, 1);
    assert!(r.is_err());
    assert_eq!(r.unwrap_err().kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn delete_offset_in_range_but_len_extends_past_end_returns_err() {
    let ds = owning_ds(b"ABCDE");
    let r = ds.delete(3, 3); // 3+3=6 > 5
    assert!(r.is_err());
    assert_eq!(r.unwrap_err().kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn delete_surviving_byte_readable() {
    // Delete the middle three bytes, check first and last bytes survive.
    let ds = owning_ds(b"AXYZB");
    ds.delete(1, 3).unwrap();
    assert_eq!(ds.byte_len(), 2);
    let mut buf = [0u8; 2];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"AB");
}

#[test]
fn delete_two_deletes_both_ranges_gone() {
    // "AXBYCZ": delete X at offset 1, then delete Y.
    // Each delta's offset is in the coordinate space after all preceding
    // deltas have been applied (per Delta's documented invariant).
    // After first delete (X gone): stream is "ABYCZ" — Y is now at pos 2.
    let ds = owning_ds(b"AXBYCZ");
    ds.delete(1, 1).unwrap(); // remove X -> "ABYCZ"
    ds.delete(2, 1).unwrap(); // remove Y (now at pos 2) -> "ABCZ"
    assert_eq!(ds.byte_len(), 4);
    let mut buf = [0u8; 4];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABCZ");
}

#[test]
fn delete_appends_delta_to_log() {
    let ds = owning_ds(b"ABCDE");
    ds.delete(1, 2).unwrap();
    assert_eq!(ds.log.borrow().len(), 1);
    assert!(matches!(
        ds.log.borrow()[0],
        Delta::Delete { offset: 1, len: 2 }
    ));
}

// ── M7-2: append ─────────────────────────────────────────────────────────

#[test]
fn append_to_empty_derived_branch() {
    let ds = DerivedBranch::derive_from_base(Arc::new(BaseBranch::empty()));
    ds.append(b"hello").unwrap();
    assert_eq!(ds.byte_len(), 5);
    let mut buf = vec![0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"hello");
}

#[test]
fn append_single_byte() {
    let ds = owning_ds(b"ABC");
    ds.append(b"D").unwrap();
    assert_eq!(ds.byte_len(), 4);
    let mut buf = [0u8; 4];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABCD");
}

#[test]
fn append_multiple_times_read_each_time() {
    let ds = owning_ds(b"A");
    ds.append(b"B").unwrap();
    assert_eq!(ds.byte_len(), 2);
    let mut buf = [0u8; 2];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"AB");

    ds.append(b"C").unwrap();
    assert_eq!(ds.byte_len(), 3);
    let mut buf = [0u8; 3];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABC");

    ds.append(b"DE").unwrap();
    assert_eq!(ds.byte_len(), 5);
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABCDE");
}

#[test]
fn append_agrees_with_insert_before_at_byte_len() {
    let ds1 = owning_ds(b"Hello");
    ds1.append(b" World").unwrap();

    let ds2 = owning_ds(b"Hello");
    ds2.insert_before(5, b" World").unwrap();

    assert_eq!(ds1.byte_len(), ds2.byte_len());
    let len = ds1.byte_len() as usize;
    let mut buf1 = vec![0u8; len];
    let mut buf2 = vec![0u8; len];
    ds1.read_at(0, &mut buf1).unwrap();
    ds2.read_at(0, &mut buf2).unwrap();
    assert_eq!(buf1, buf2);
}

#[test]
fn append_after_overwrite_no_merge() {
    // An overwrite is in the log; append must not merge with it.
    let ds = owning_ds(b"ABCDE");
    ds.overwrite(0, b"X").unwrap();
    ds.append(b"Z").unwrap();
    assert_eq!(ds.log.borrow().len(), 2);
    assert_eq!(ds.byte_len(), 6);
    let mut buf = [0u8; 6];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"XBCDEZ");
}

#[test]
fn append_after_insert_not_at_tail_no_merge() {
    // Previous insert is in the middle — new append offset is past prev_end.
    let ds = owning_ds(b"ABCDE");
    ds.insert_before(2, b"XX").unwrap();
    // byte_len is now 7; prev Insert at offset 2, prev_end 4; 7 > 4 → no merge.
    ds.append(b"Z").unwrap();
    assert_eq!(ds.log.borrow().len(), 2);
    assert_eq!(ds.byte_len(), 8);
    let mut buf = [0u8; 8];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABXXCDEZ");
}

#[test]
fn append_two_consecutive_merge_into_one_log_entry() {
    let ds = owning_ds(b"XY");
    ds.append(b"AB").unwrap();
    ds.append(b"CD").unwrap();
    // Two consecutive appends at byte_len() must merge into one Insert entry.
    assert_eq!(ds.log.borrow().len(), 1);
    if let Delta::Insert { offset, bytes } = &ds.log.borrow()[0] {
        assert_eq!(*offset, 2);
        assert_eq!(bytes.as_slice(), b"ABCD");
    } else {
        panic!("expected a single Insert entry");
    }
    assert_eq!(ds.byte_len(), 6);
    let mut buf = [0u8; 6];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"XYABCD");
}

#[test]
fn append_to_derived_chain_parent_is_derived() {
    let parent = DerivedBranch::derive_from_base(base_arc(b"Root"));
    let child = DerivedBranch::derive_from_derived(parent);
    child.append(b"Tail").unwrap();
    assert_eq!(child.byte_len(), 8);
    let mut buf = [0u8; 8];
    child.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"RootTail");
}

#[test]
fn append_empty_slice_is_noop() {
    let ds = owning_ds(b"ABCDE");
    let before_log_len = ds.log.borrow().len();
    ds.append(b"").unwrap();
    assert_eq!(ds.log.borrow().len(), before_log_len);
    assert_eq!(ds.byte_len(), 5);
}

#[test]
fn append_byte_len_increased_by_payload() {
    let ds = owning_ds(b"START");
    let payload = b"0123456789";
    ds.append(payload).unwrap();
    assert_eq!(ds.byte_len(), 5 + payload.len() as u64);
}

#[test]
fn append_confirmed_via_as_reader() {
    use std::io::Read;
    let ds = owning_ds(b"Hello");
    ds.append(b", World!").unwrap();
    let mut out = Vec::new();
    ds.as_reader().read_to_end(&mut out).unwrap();
    assert_eq!(out, b"Hello, World!");
}

#[test]
fn append_then_materialize_range_of_tail_equals_payload() {
    use crate::materialize::materialize_range;
    let payload = b"PAYLOAD";
    let ds = owning_ds(b"PREFIX");
    ds.append(payload).unwrap();
    let tail_start = ds.byte_len() - payload.len() as u64;
    let tail = materialize_range(&*ds, tail_start, payload.len() as u64).unwrap();
    assert_eq!(tail.as_slice(), payload.as_slice());
}

// ── M7-4: truncate ──────────────────────────────────────────────────────

#[test]
fn truncate_to_current_length_is_noop() {
    let ds = owning_ds(b"ABCDE");
    let before = ds.log.borrow().len();
    ds.truncate(5).unwrap();
    assert_eq!(ds.log.borrow().len(), before);
    assert_eq!(ds.byte_len(), 5);
}

#[test]
fn truncate_by_one_byte() {
    let ds = owning_ds(b"ABCDE");
    ds.truncate(4).unwrap();
    assert_eq!(ds.byte_len(), 4);
    let mut buf = [0u8; 4];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABCD");
}

#[test]
fn truncate_to_zero() {
    let ds = owning_ds(b"ABCDE");
    ds.truncate(0).unwrap();
    assert_eq!(ds.byte_len(), 0);
}

#[test]
fn truncate_then_read_confirms_correct_content() {
    let ds = owning_ds(b"Hello, World!");
    ds.truncate(5).unwrap();
    assert_eq!(ds.byte_len(), 5);
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"Hello");
}

#[test]
fn truncate_on_derived_chain_parent_is_derived() {
    let parent = DerivedBranch::derive_from_base(base_arc(b"RootData"));
    let child = DerivedBranch::derive_from_derived(parent);
    child.truncate(4).unwrap();
    assert_eq!(child.byte_len(), 4);
    let mut buf = [0u8; 4];
    child.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"Root");
}

#[test]
fn truncate_after_insert_shortens_grown_stream() {
    // "AB" + insert "XY" before 1 -> "AXYB" (len 4); truncate to 3 -> "AXY"
    let ds = owning_ds(b"AB");
    ds.insert_before(1, b"XY").unwrap();
    assert_eq!(ds.byte_len(), 4);
    ds.truncate(3).unwrap();
    assert_eq!(ds.byte_len(), 3);
    let mut buf = [0u8; 3];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"AXY");
}

#[test]
fn truncate_after_overwrite_near_tail() {
    // "ABCDE", overwrite 3..5 with "ZZ", then truncate to 4 -> "ABCZ"
    let ds = owning_ds(b"ABCDE");
    ds.overwrite(3, b"ZZ").unwrap();
    ds.truncate(4).unwrap();
    assert_eq!(ds.byte_len(), 4);
    let mut buf = [0u8; 4];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABCZ");
}

#[test]
fn truncate_new_len_greater_than_byte_len_returns_err() {
    let ds = owning_ds(b"ABC");
    let err = ds.truncate(4).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn truncate_new_len_zero_on_empty_is_noop() {
    let ds = DerivedBranch::derive_from_base(Arc::new(BaseBranch::empty()));
    let before = ds.log.borrow().len();
    ds.truncate(0).unwrap();
    assert_eq!(ds.log.borrow().len(), before);
    assert_eq!(ds.byte_len(), 0);
}

#[test]
fn truncate_twice_composes_correctly() {
    let ds = owning_ds(b"ABCDEFGH");
    ds.truncate(6).unwrap();
    assert_eq!(ds.byte_len(), 6);
    ds.truncate(3).unwrap();
    assert_eq!(ds.byte_len(), 3);
    let mut buf = [0u8; 3];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABC");
}

#[test]
fn truncate_byte_len_equals_new_len() {
    let ds = owning_ds(b"ABCDE");
    ds.truncate(2).unwrap();
    assert_eq!(ds.byte_len(), 2);
}

#[test]
fn truncate_then_append_restores_subset_of_original_tail() {
    let ds = owning_ds(b"ABCDE");
    ds.truncate(3).unwrap();
    ds.append(b"XY").unwrap();
    assert_eq!(ds.byte_len(), 5);
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABCXY");
}

// ── splice ────────────────────────────────────────────────────────────────────

#[test]
fn splice_same_length_acts_like_overwrite() {
    // Replace "World" (5 bytes) with "Rust!" (5 bytes) — length unchanged.
    // The trailing "!" from the original string is preserved.
    let ds = owning_ds(b"Hello, World!");
    ds.splice(7, 5, b"Rust!").unwrap();
    assert_eq!(ds.byte_len(), 13);
    let mut buf = vec![0u8; ds.byte_len() as usize];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"Hello, Rust!!");
}

#[test]
fn splice_longer_replacement_grows_branch() {
    let ds = owning_ds(b"abcdef");
    ds.splice(2, 2, b"XYZ").unwrap();
    let mut buf = vec![0u8; ds.byte_len() as usize];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"abXYZef");
    assert_eq!(ds.byte_len(), 7);
}

#[test]
fn splice_shorter_replacement_shrinks_branch() {
    let ds = owning_ds(b"abcdef");
    ds.splice(1, 4, b"X").unwrap();
    let mut buf = vec![0u8; ds.byte_len() as usize];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"aXf");
    assert_eq!(ds.byte_len(), 3);
}

#[test]
fn splice_empty_replacement_is_pure_delete() {
    let ds = owning_ds(b"Hello, World!");
    ds.splice(5, 7, b"").unwrap();
    let mut buf = vec![0u8; ds.byte_len() as usize];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"Hello!");
}

#[test]
fn splice_at_offset_zero() {
    let ds = owning_ds(b"ABCDE");
    ds.splice(0, 2, b"XY").unwrap();
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"XYCDE");
}

#[test]
fn splice_at_end_removes_tail() {
    let ds = owning_ds(b"ABCDE");
    ds.splice(3, 2, b"XY").unwrap();
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABCXY");
}

#[test]
fn splice_entire_content() {
    let ds = owning_ds(b"old");
    ds.splice(0, 3, b"brand new").unwrap();
    let mut buf = vec![0u8; ds.byte_len() as usize];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"brand new");
}

#[test]
fn splice_zero_len_zero_bytes_is_noop() {
    let ds = owning_ds(b"ABC");
    let before_len = ds.byte_len();
    ds.splice(1, 0, b"").unwrap();
    assert_eq!(ds.byte_len(), before_len);
    let mut buf = [0u8; 3];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABC");
}

#[test]
fn splice_zero_len_with_bytes_is_pure_insert() {
    let ds = owning_ds(b"AC");
    ds.splice(1, 0, b"B").unwrap();
    let mut buf = vec![0u8; ds.byte_len() as usize];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ABC");
}

#[test]
fn splice_out_of_bounds_returns_invalid_input() {
    let ds = owning_ds(b"ABC");
    let err = ds.splice(5, 1, b"X").unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn splice_len_extends_past_end_returns_invalid_input() {
    let ds = owning_ds(b"ABCDE");
    let err = ds.splice(3, 10, b"X").unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn splice_chained_calls_compose_correctly() {
    // Replace "quick" then "fox" in two successive splices.
    let ds = owning_ds(b"the quick brown fox");
    // Replace "quick" (offset 4, len 5) with "slow"
    ds.splice(4, 5, b"slow").unwrap();
    // After first splice: "the slow brown fox" (len 18)
    // Replace "fox" — it now starts at offset 15, len 3
    ds.splice(15, 3, b"cat").unwrap();
    let mut buf = vec![0u8; ds.byte_len() as usize];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"the slow brown cat");
}
