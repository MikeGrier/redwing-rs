// Copyright (c) 2026 Michael J. Grier
use std::sync::Arc;

use crate::Branch;

fn thicket(data: &[u8]) -> Arc<dyn Branch> {
    crate::make_thicket_from_bytes(data.to_vec()).main()
}

// ── OM-6.1: no edits — identity mapping ──────────────────────────────────

/// Every in-bounds offset in an unedited fork maps to itself.
#[test]
fn no_edits_in_bounds_offsets_map_to_themselves() {
    let b1 = thicket(b"hello world");
    let b2 = b1.fork();
    for i in 0..b1.byte_len() {
        assert_eq!(
            b2.map_offset_to_fork(i),
            Some(i),
            "offset {i} should map to itself with no edits"
        );
    }
}

/// The offset equal to byte_len (one past the end) is out of bounds → Displaced.
#[test]
fn no_edits_one_past_end_is_displaced() {
    let b1 = thicket(b"hello world");
    let b2 = b1.fork();
    assert_eq!(b2.map_offset_to_fork(b1.byte_len()), None);
}

/// Offset 0 maps to 0 in an unedited fork of a non-empty branch.
#[test]
fn no_edits_offset_zero_maps_to_zero() {
    let b1 = thicket(b"abc");
    let b2 = b1.fork();
    assert_eq!(b2.map_offset_to_fork(0), Some(0));
}

/// Last valid offset maps to itself.
#[test]
fn no_edits_last_valid_offset_maps_to_itself() {
    let b1 = thicket(b"abc");
    let b2 = b1.fork();
    let last = b1.byte_len() - 1;
    assert_eq!(b2.map_offset_to_fork(last), Some(last));
}

/// A large offset (well past the end) is Displaced.
#[test]
fn no_edits_large_offset_is_displaced() {
    let b1 = thicket(b"abc");
    let b2 = b1.fork();
    assert_eq!(b2.map_offset_to_fork(1_000_000), None);
}

/// u64::MAX is Displaced on any reasonably-sized branch.
#[test]
fn no_edits_u64_max_is_displaced() {
    let b1 = thicket(b"abc");
    let b2 = b1.fork();
    assert_eq!(b2.map_offset_to_fork(u64::MAX), None);
}

/// Single-byte branch: offset 0 maps, offset 1 does not.
#[test]
fn no_edits_single_byte_branch() {
    let b1 = thicket(b"x");
    let b2 = b1.fork();
    assert_eq!(b2.map_offset_to_fork(0), Some(0));
    assert_eq!(b2.map_offset_to_fork(1), None);
}

/// Multiple consecutive calls on an unedited fork are stable (idempotent).
#[test]
fn no_edits_repeated_calls_are_idempotent() {
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    for _ in 0..3 {
        for i in 0..b1.byte_len() {
            assert_eq!(b2.map_offset_to_fork(i), Some(i));
        }
    }
}

/// Mapping from a grandparent fork with no edits at either level.
#[test]
fn no_edits_two_hop_identity() {
    let b1 = thicket(b"hello");
    let b2 = b1.fork();
    let b3 = b2.fork();
    for i in 0..b1.byte_len() {
        let via_b2 = match b2.map_offset_to_fork(i) {
            Some(o) => o,
            None => panic!("unexpected Displaced at offset {i} via b2"),
        };
        assert_eq!(
            b3.map_offset_to_fork(via_b2),
            Some(i),
            "two-hop identity failed at offset {i}"
        );
    }
}

/// The Mapped value round-trips: reading the mapped child offset yields the
/// same byte as reading the parent offset in b1.
#[test]
fn no_edits_mapped_offset_reads_same_byte() {
    let data = b"the quick brown fox";
    let b1 = thicket(data);
    let b2 = b1.fork();
    for i in 0..b1.byte_len() {
        if let Some(child_off) = b2.map_offset_to_fork(i) {
            let mut pb = [0u8];
            let mut cb = [0u8];
            b1.read_at(i, &mut pb).unwrap();
            b2.read_at(child_off, &mut cb).unwrap();
            assert_eq!(pb, cb, "byte mismatch at parent offset {i}");
        } else {
            panic!("offset {i} should be Mapped in an unedited fork");
        }
    }
}

/// An empty fork (parent was also empty) has nothing to map.
#[test]
fn no_edits_empty_fork_offset_zero_is_displaced() {
    let b1 = thicket(b"");
    let b2 = b1.fork();
    assert_eq!(b2.map_offset_to_fork(0), None);
}

// ── OM-6.2: single insert before target — target shifts right ────────────

/// After inserting N bytes before a target offset, the target shifts right by N.
#[test]
fn insert_before_target_shifts_target_right() {
    // b1: [a b c d e]  offsets 0..5
    // b2: insert "XY" at offset 2  → [a b X Y c d e]
    // parent offset 2 (was 'c') → child offset 4
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.insert_before(2, b"XY").unwrap();
    // offsets before the insert are unchanged
    assert_eq!(b2.map_offset_to_fork(0), Some(0));
    assert_eq!(b2.map_offset_to_fork(1), Some(1));
    // offsets at and after the insert point shift by the insert length (2)
    assert_eq!(b2.map_offset_to_fork(2), Some(4));
    assert_eq!(b2.map_offset_to_fork(3), Some(5));
    assert_eq!(b2.map_offset_to_fork(4), Some(6));
}

/// The shift equals exactly the number of inserted bytes.
#[test]
fn insert_before_target_shift_equals_insert_len() {
    let insert_len: u64 = 5;
    let b1 = thicket(b"abcdefgh");
    let b2 = b1.fork();
    b2.insert_before(3, b"XXXXX").unwrap();
    for i in 3..b1.byte_len() {
        assert_eq!(
            b2.map_offset_to_fork(i),
            Some(i + insert_len),
            "offset {i} should shift by {insert_len}"
        );
    }
}

/// Insert at the very last byte position shifts that byte and any after it.
#[test]
fn insert_before_last_byte_shifts_last_byte() {
    // b1: [a b c]  insert "ZZ" at offset 2 → [a b Z Z c]
    let b1 = thicket(b"abc");
    let b2 = b1.fork();
    b2.insert_before(2, b"ZZ").unwrap();
    assert_eq!(b2.map_offset_to_fork(0), Some(0));
    assert_eq!(b2.map_offset_to_fork(1), Some(1));
    assert_eq!(b2.map_offset_to_fork(2), Some(4)); // 'c'
}

/// The inserted bytes themselves have no parent origin — a parent offset that
/// lands in the inserted region is not reachable from b1's space anyway, so
/// this is implicitly tested by the shift correctness above.
/// Verify mapped child offsets read the correct byte values.
#[test]
fn insert_before_target_mapped_bytes_are_correct() {
    // b1: "hello"  insert "__" at 3 → "hel__lo"
    let b1 = thicket(b"hello");
    let b2 = b1.fork();
    b2.insert_before(3, b"__").unwrap();
    for (parent_off, expected_child_byte) in
        [(0u64, b'h'), (1, b'e'), (2, b'l'), (3, b'l'), (4, b'o')]
    {
        if let Some(child_off) = b2.map_offset_to_fork(parent_off) {
            let mut buf = [0u8];
            b2.read_at(child_off, &mut buf).unwrap();
            assert_eq!(buf[0], expected_child_byte, "parent offset {parent_off}");
        } else {
            panic!("offset {parent_off} should be Mapped");
        }
    }
}

// ── OM-6.3: single insert after target — target offset unchanged ─────────

/// After inserting N bytes strictly after a target offset, the target
/// offset is unchanged.
#[test]
fn insert_after_target_does_not_shift_target() {
    // b1: [a b c d e]  insert "XY" at offset 4 → [a b c d X Y e]
    // parent offsets 0..4 are before the insert point, so unchanged
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.insert_before(4, b"XY").unwrap();
    for i in 0..4u64 {
        assert_eq!(
            b2.map_offset_to_fork(i),
            Some(i),
            "offset {i} before insert point should be unchanged"
        );
    }
    // offset 4 ('e') is at the insert point → shifts right by 2
    assert_eq!(b2.map_offset_to_fork(4), Some(6));
}

/// Inserting at the very end (append) does not affect any existing offsets.
#[test]
fn insert_at_end_does_not_shift_any_existing_offset() {
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.insert_before(b1.byte_len(), b"XYZ").unwrap();
    for i in 0..b1.byte_len() {
        assert_eq!(
            b2.map_offset_to_fork(i),
            Some(i),
            "append should not shift offset {i}"
        );
    }
}

/// Insert strictly after the target: verify the mapped byte is the right one.
#[test]
fn insert_after_target_mapped_bytes_are_correct() {
    // b1: "world"  insert "!!" at offset 3 → "wor!!ld"
    let b1 = thicket(b"world");
    let b2 = b1.fork();
    b2.insert_before(3, b"!!").unwrap();
    // Check offsets 0, 1, 2 (before the insert — unchanged)
    for (parent_off, byte) in [(0u64, b'w'), (1, b'o'), (2, b'r')] {
        if let Some(child_off) = b2.map_offset_to_fork(parent_off) {
            let mut buf = [0u8];
            b2.read_at(child_off, &mut buf).unwrap();
            assert_eq!(buf[0], byte, "parent offset {parent_off}");
        } else {
            panic!("offset {parent_off} should be Mapped");
        }
    }
}

// ── OM-6.4: insert at offset 0 — every former offset shifts by insert len ─

/// Inserting at offset 0 shifts every parent offset right by the insert length.
#[test]
fn insert_at_offset_zero_shifts_all_offsets() {
    // b1: [a b c d e]  insert "XY" at 0 → [X Y a b c d e]
    // every parent offset i → child offset i+2
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    let insert_len: u64 = 2;
    b2.insert_before(0, b"XY").unwrap();
    for i in 0..b1.byte_len() {
        assert_eq!(
            b2.map_offset_to_fork(i),
            Some(i + insert_len),
            "offset {i} should shift by {insert_len}"
        );
    }
}

/// One-past-end is still Displaced after a prepend.
#[test]
fn insert_at_offset_zero_one_past_end_is_displaced() {
    let b1 = thicket(b"abc");
    let b2 = b1.fork();
    b2.insert_before(0, b"ZZ").unwrap();
    assert_eq!(b2.map_offset_to_fork(b1.byte_len()), None);
}

/// The byte values at shifted child offsets are the original parent bytes.
#[test]
fn insert_at_offset_zero_mapped_bytes_are_correct() {
    // b1: "hi"  insert ">>>" at 0 → ">>>hi"
    let b1 = thicket(b"hi");
    let b2 = b1.fork();
    b2.insert_before(0, b">>>").unwrap();
    // parent offset 0 ('h') → child offset 3
    // parent offset 1 ('i') → child offset 4
    for (parent_off, expected) in [(0u64, b'h'), (1, b'i')] {
        if let Some(child_off) = b2.map_offset_to_fork(parent_off) {
            let mut buf = [0u8];
            b2.read_at(child_off, &mut buf).unwrap();
            assert_eq!(buf[0], expected, "parent offset {parent_off}");
        } else {
            panic!("offset {parent_off} should be Mapped");
        }
    }
}

/// Large insert at offset 0: shift is exactly insert_len for all offsets.
#[test]
fn insert_at_offset_zero_large_insert() {
    let payload = vec![0xFFu8; 100];
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.insert_before(0, &payload).unwrap();
    for i in 0..b1.byte_len() {
        assert_eq!(
            b2.map_offset_to_fork(i),
            Some(i + 100),
            "offset {i} should shift by 100"
        );
    }
}

// ── OM-6.5: delete of target bytes → Displaced ───────────────────────────

/// Every parent offset covered by a delete is Displaced.
#[test]
fn delete_target_bytes_are_displaced() {
    // b1: [a b c d e]  delete offsets 1..3 (b, c) → [a d e]
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.delete(1, 2).unwrap();
    assert_eq!(b2.map_offset_to_fork(1), None);
    assert_eq!(b2.map_offset_to_fork(2), None);
}

/// Parent bytes before the deleted range are still Mapped (unchanged offsets).
#[test]
fn delete_bytes_before_range_still_mapped() {
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.delete(2, 2).unwrap(); // remove 'c','d'
    assert_eq!(b2.map_offset_to_fork(0), Some(0));
    assert_eq!(b2.map_offset_to_fork(1), Some(1));
}

/// Parent bytes after the deleted range shift left by the delete length.
#[test]
fn delete_bytes_after_range_shift_left() {
    // b1: [a b c d e]  delete [1,3) → [a d e]
    // parent offset 3 ('d') → child offset 1
    // parent offset 4 ('e') → child offset 2
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.delete(1, 2).unwrap();
    assert_eq!(b2.map_offset_to_fork(3), Some(1));
    assert_eq!(b2.map_offset_to_fork(4), Some(2));
}

/// Deleting the entire content displaces every offset.
#[test]
fn delete_all_bytes_displaces_every_offset() {
    let data = b"hello";
    let b1 = thicket(data);
    let b2 = b1.fork();
    b2.delete(0, b1.byte_len()).unwrap();
    for i in 0..data.len() as u64 {
        assert_eq!(
            b2.map_offset_to_fork(i),
            None,
            "offset {i} should be Displaced after full delete"
        );
    }
}

/// Delete at exactly the last byte: that byte is Displaced; preceding bytes are Mapped.
#[test]
fn delete_last_byte_only() {
    let b1 = thicket(b"abcd");
    let b2 = b1.fork();
    b2.delete(3, 1).unwrap();
    assert_eq!(b2.map_offset_to_fork(3), None);
    assert_eq!(b2.map_offset_to_fork(0), Some(0));
    assert_eq!(b2.map_offset_to_fork(2), Some(2));
}

/// The surviving bytes after a delete read the right values at mapped offsets.
#[test]
fn delete_target_bytes_survivors_read_correctly() {
    // b1: "abcde"  delete 'b','c' (offset 1, len 2) → "ade"
    // parent 'a'=0 → child 0; parent 'd'=3 → child 1; parent 'e'=4 → child 2
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.delete(1, 2).unwrap();
    for (parent_off, expected) in [(0u64, b'a'), (3, b'd'), (4, b'e')] {
        if let Some(child_off) = b2.map_offset_to_fork(parent_off) {
            let mut buf = [0u8];
            b2.read_at(child_off, &mut buf).unwrap();
            assert_eq!(buf[0], expected, "parent offset {parent_off}");
        } else {
            panic!("offset {parent_off} should be Mapped");
        }
    }
}

// ── OM-6.6: delete before target — target shifts left ────────────────────

/// A delete entirely before the queried offset shifts that offset left by len.
#[test]
fn delete_before_target_shifts_left() {
    // b1: [a b c d e]  delete offset 0 len 2 → [c d e]
    // parent 'c'=2 → child 0; parent 'd'=3 → child 1; parent 'e'=4 → child 2
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.delete(0, 2).unwrap();
    assert_eq!(b2.map_offset_to_fork(2), Some(0));
    assert_eq!(b2.map_offset_to_fork(3), Some(1));
    assert_eq!(b2.map_offset_to_fork(4), Some(2));
}

/// Deleting exactly the byte immediately before the target shifts target by 1.
#[test]
fn delete_one_byte_before_target_shifts_by_one() {
    let b1 = thicket(b"xyz");
    let b2 = b1.fork();
    b2.delete(1, 1).unwrap(); // delete 'y'
                              // 'x' at parent offset 0 is deleted — no, wait: delete(1,1) deletes 'y'
                              // parent 'x'=0 → child 0 (unchanged, before delete)
                              // parent 'y'=1 → Displaced
                              // parent 'z'=2 → child 1 (shifted left by 1)
    assert_eq!(b2.map_offset_to_fork(0), Some(0));
    assert_eq!(b2.map_offset_to_fork(1), None);
    assert_eq!(b2.map_offset_to_fork(2), Some(1));
}

/// Multiple bytes deleted before target; shift equals total delete length.
#[test]
fn delete_multiple_before_target_shift_equals_delete_len() {
    // b1: [0 1 2 3 4 5 6 7 8 9]  delete [2..5] (len 3) → [0 1 5 6 7 8 9]
    // parent offset 5 → child offset 2; parent offset 9 → child offset 6
    let data: Vec<u8> = (0u8..10).collect();
    let b1 = thicket(&data);
    let b2 = b1.fork();
    b2.delete(2, 3).unwrap();
    assert_eq!(b2.map_offset_to_fork(5), Some(2));
    assert_eq!(b2.map_offset_to_fork(9), Some(6));
    // Bytes in the deleted range are Displaced
    for off in 2u64..5 {
        assert_eq!(b2.map_offset_to_fork(off), None, "offset {off}");
    }
}

// ── OM-6.7: overwrite of target bytes → Displaced; surrounding OK ─────────

/// Overwritten parent offsets map to Displaced; bytes outside the overwrite
/// region continue to map correctly.
#[test]
fn overwrite_target_bytes_are_displaced() {
    // b1: "abcde"  overwrite offset 1 len 2 with "XX" → "aXXde"
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.overwrite(1, b"XX").unwrap();
    // 'b' and 'c' are gone from the parent's perspective
    assert_eq!(b2.map_offset_to_fork(1), None);
    assert_eq!(b2.map_offset_to_fork(2), None);
}

/// Bytes before and after an overwrite region map correctly.
#[test]
fn overwrite_surrounding_bytes_still_mapped() {
    // b1: "abcde"  overwrite offset 1 len 2 → "aXXde"
    // 'a'=0 stays at child 0; 'd'=3 stays at child 3; 'e'=4 stays at child 4
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.overwrite(1, b"XX").unwrap();
    assert_eq!(b2.map_offset_to_fork(0), Some(0));
    assert_eq!(b2.map_offset_to_fork(3), Some(3));
    assert_eq!(b2.map_offset_to_fork(4), Some(4));
}

/// Overwrite at offset 0 displaces first N bytes; rest shift by 0.
#[test]
fn overwrite_at_start_displaces_prefix() {
    let b1 = thicket(b"hello");
    let b2 = b1.fork();
    b2.overwrite(0, b"HH").unwrap();
    assert_eq!(b2.map_offset_to_fork(0), None);
    assert_eq!(b2.map_offset_to_fork(1), None);
    assert_eq!(b2.map_offset_to_fork(2), Some(2));
    assert_eq!(b2.map_offset_to_fork(4), Some(4));
}

// ── OM-6.8: delete all — every parent offset → Displaced (extended forms) ─

/// After deleting all content, even a large out-of-range parent offset returns Displaced.
#[test]
fn delete_all_then_large_offset_displaced() {
    let b1 = thicket(b"abc");
    let b2 = b1.fork();
    b2.delete(0, 3).unwrap();
    assert_eq!(b2.map_offset_to_fork(100), None);
    assert_eq!(b2.map_offset_to_fork(u64::MAX), None);
}

// ── OM-6.9: multiple edits — insert then delete ───────────────────────────

/// Insert then delete: verify that the composed piece table produces correctly
/// shifted results for all in-range parent offsets.
#[test]
fn insert_then_delete_composed_shift() {
    // b1: "abcde"
    // b2: fork → insert 2 bytes "XY" at offset 2 → "abXYcde"  (child len 7)
    // b2: delete 1 byte at offset 4 → removes 'c' → "abXYde"  (child len 6)
    // From b1's perspective:
    //   parent 0 ('a') → child 0  (untouched)
    //   parent 1 ('b') → child 1  (untouched)
    //   parent 2 ('c') → Displaced (it was at child 2 after insert, but was then deleted)
    //     actually: insert_before(2, "XY") pushes 'c' to child 4, then delete(4,1) removes it
    //   parent 3 ('d') → child 4  (was at child 5 after insert; delete before shifts left by 1)
    //   parent 4 ('e') → child 5
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.insert_before(2, b"XY").unwrap();
    // After insert: piece table has Parent[0..2], Inline["XY"], Parent[2..5]
    // Now delete child offset 4 len 1 — this is 'c' (first byte of the second Parent piece)
    b2.delete(4, 1).unwrap();
    // parent 0,1 are in the first Parent piece [0..2]: still there
    assert_eq!(b2.map_offset_to_fork(0), Some(0));
    assert_eq!(b2.map_offset_to_fork(1), Some(1));
    // parent 2 ('c') was deleted
    assert_eq!(b2.map_offset_to_fork(2), None);
    // parent 3 ('d') and 4 ('e') survive
    assert_eq!(b2.map_offset_to_fork(3), Some(4));
    assert_eq!(b2.map_offset_to_fork(4), Some(5));
}

/// Delete then insert after deleted region: surviving bytes shifted correctly.
#[test]
fn delete_then_insert_after() {
    // b1: "abcde"  delete 'b','c' (off 1, len 2) → "ade"
    // then insert "ZZ" at child offset 1 → "aZZde"
    // parent 'a'=0 → child 0; parent 'd'=3 → child 3; parent 'e'=4 → child 4
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.delete(1, 2).unwrap();
    b2.insert_before(1, b"ZZ").unwrap();
    assert_eq!(b2.map_offset_to_fork(0), Some(0));
    assert_eq!(b2.map_offset_to_fork(1), None);
    assert_eq!(b2.map_offset_to_fork(2), None);
    assert_eq!(b2.map_offset_to_fork(3), Some(3));
    assert_eq!(b2.map_offset_to_fork(4), Some(4));
}

// ── OM-6.10: u64::MAX with edits ─────────────────────────────────────────

/// After edits, u64::MAX is always Displaced (no branch can be that large).
#[test]
fn u64_max_after_edit_is_displaced() {
    let b1 = thicket(b"data");
    let b2 = b1.fork();
    b2.insert_before(2, b"XX").unwrap();
    assert_eq!(b2.map_offset_to_fork(u64::MAX), None);
}

/// u64::MAX is Displaced even after a delete creates only a small branch.
#[test]
fn u64_max_after_delete_is_displaced() {
    let b1 = thicket(b"hello world");
    let b2 = b1.fork();
    b2.delete(0, 5).unwrap();
    assert_eq!(b2.map_offset_to_fork(u64::MAX), None);
}

// ── OM-6.11: empty branch (zero-length parent) ────────────────────────────

/// An empty parent branch maps offset 0 to Displaced (nothing survives).
#[test]
fn empty_parent_offset_zero_is_displaced() {
    let b1 = thicket(b"");
    let b2 = b1.fork();
    assert_eq!(b2.map_offset_to_fork(0), None);
}

/// Empty parent with inline insert: no parent offset can map to the injected bytes.
#[test]
fn empty_parent_with_insert_offset_zero_displaced() {
    let b1 = thicket(b"");
    let b2 = b1.fork();
    b2.insert_before(0, b"new content").unwrap();
    // parent has no bytes; offset 0 into parent is Displaced
    assert_eq!(b2.map_offset_to_fork(0), None);
    assert_eq!(b2.map_offset_to_fork(5), None);
}

// ── OM-6.12: target at exact piece boundary ───────────────────────────────

/// When an insert splits the parent into two pieces, the first byte of the
/// second piece (the piece boundary) maps correctly.
#[test]
fn piece_boundary_first_byte_of_second_piece_maps_correctly() {
    // b1: "abcde"  insert "XY" at offset 2 → piece table: Parent[0..2], Inline, Parent[2..5]
    // parent offset 2 ('c') is now the first byte of the second Parent piece
    // in child it appears at position 4 (after "ab" + "XY")
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.insert_before(2, b"XY").unwrap();
    assert_eq!(b2.map_offset_to_fork(2), Some(4));
    assert_eq!(b2.map_offset_to_fork(3), Some(5));
    assert_eq!(b2.map_offset_to_fork(4), Some(6));
}

/// Last byte of the first piece (just before the boundary) still maps correctly.
#[test]
fn piece_boundary_last_byte_of_first_piece_maps_correctly() {
    // Same setup: insert at 2 → Parent[0..2], Inline, Parent[2..5]
    // parent offset 1 ('b') is the last byte of the first Parent piece → child 1
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.insert_before(2, b"XY").unwrap();
    assert_eq!(b2.map_offset_to_fork(0), Some(0));
    assert_eq!(b2.map_offset_to_fork(1), Some(1));
}

// ── OM-6.13: target at last byte of final piece ───────────────────────────

/// The very last byte of the last parent piece maps to the last occupied position.
#[test]
fn last_byte_of_final_piece_maps_correctly() {
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    // No edit: last byte of only piece is parent offset 4 → child 4
    assert_eq!(b2.map_offset_to_fork(4), Some(4));
}

/// With an insert at offset 1, last byte of the second parent piece maps correctly.
#[test]
fn last_byte_of_final_piece_after_insert_maps_correctly() {
    // b1: "abcde"  insert "XYZ" at 1 → Parent[0..1], Inline["XYZ"], Parent[1..5]
    // last byte of last piece = parent offset 4 ('e') → child 1+3+4 = 8 - 1 = 7
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.insert_before(1, b"XYZ").unwrap();
    // child layout: a(0) X Y Z b(4) c(5) d(6) e(7)
    assert_eq!(b2.map_offset_to_fork(1), Some(4));
    assert_eq!(b2.map_offset_to_fork(4), Some(7));
}

// ── OM-6.14: two-hop chained fork with edits ─────────────────────────────

/// Chain: b1→b2 (insert) → b3 (delete).  Map a b1 offset to b3 manually
/// by composing two map_offset_to_fork calls.
#[test]
fn two_hop_chained_fork_with_edits() {
    // b1: "abcde"
    // b2 = fork(b1): insert "XY" at 2 → "abXYcde"
    // b3 = fork(b2): delete 'X','Y' at child offsets 2,2 → "abcde" again
    // Map b1 offset 3 ('d'):
    //   b1→b2: offset 3 → Mapped(5)  [shifted right by 2 due to insert]
    //   b2→b3: offset 5 ('c' in b2) → check if 'c' survives in b3
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.insert_before(2, b"XY").unwrap();
    let b3 = b2.fork();
    b3.delete(2, 2).unwrap(); // delete "XY"
                              // Compose: b1 offset 3 → b2 via map
    let b2_offset = match b2.map_offset_to_fork(3) {
        Some(x) => x,
        None => panic!("expected Mapped for b1→b2"),
    };
    assert_eq!(b2_offset, 5);
    // Then b2 offset 5 → b3
    assert_eq!(b3.map_offset_to_fork(b2_offset), Some(3));
}

/// Two-hop: offset that is displaced at the intermediate fork is also displaced at final.
#[test]
fn two_hop_chained_displaced_propagates() {
    // b1: "abcde"
    // b2 = fork(b1): delete 'b','c' (off 1, len 2) → "ade"
    // If b1 offset 1 is Displaced in b2, no need to probe b3
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.delete(1, 2).unwrap();
    let b3 = b2.fork();
    b3.insert_before(0, b"Z").unwrap();
    // b1 offset 1 → Displaced in b2 → Displaced end-to-end
    assert_eq!(b2.map_offset_to_fork(1), None);
    // b1 offset 0 → b2 offset 0 → b3 offset 1 (shifted by the Z insert)
    let b2_off = match b2.map_offset_to_fork(0) {
        Some(x) => x,
        _ => panic!("expected Mapped"),
    };
    assert_eq!(b3.map_offset_to_fork(b2_off), Some(1));
}

// ── OM-6.15: interleaved insert+delete ───────────────────────────────────

/// Interleaved: insert at one position and delete at a different position.
/// Verify offsets before, between, and after both operations.
#[test]
fn interleaved_insert_and_delete() {
    // b1: [0 1 2 3 4 5 6 7 8 9]
    // b2 = fork(b1): delete 3 bytes at offset 2 → [0 1 5 6 7 8 9]  (len 7)
    // b2: insert "AB" at child offset 5 → [0 1 5 6 7 AB 8 9]
    //
    // Parent offset mapping (b1 → b2):
    //   parent 0 → child 0
    //   parent 1 → child 1
    //   parent 2,3,4 → Displaced (deleted)
    //   parent 5 → child 2
    //   parent 6 → child 3
    //   parent 7 → child 4
    //   parent 8 → child 7  (shifted right 2 by the AB insert at child 5)
    //   parent 9 → child 8
    let data: Vec<u8> = (0u8..10).collect();
    let b1 = thicket(&data);
    let b2 = b1.fork();
    b2.delete(2, 3).unwrap();
    b2.insert_before(5, b"AB").unwrap();
    assert_eq!(b2.map_offset_to_fork(0), Some(0));
    assert_eq!(b2.map_offset_to_fork(1), Some(1));
    for off in 2u64..5 {
        assert_eq!(b2.map_offset_to_fork(off), None, "offset {off}");
    }
    assert_eq!(b2.map_offset_to_fork(5), Some(2));
    assert_eq!(b2.map_offset_to_fork(6), Some(3));
    assert_eq!(b2.map_offset_to_fork(7), Some(4));
    assert_eq!(b2.map_offset_to_fork(8), Some(7));
    assert_eq!(b2.map_offset_to_fork(9), Some(8));
}

// ── OM-6.16: forward-order search-and-replace parity ─────────────────────

/// Forward-order search-replace using map_offset_to_fork produces the same
/// result as the simpler reverse-order approach.
///
/// Scenario: replace every occurrence of b'X' in b1 with b"[X]".
/// Forward-order: for each match in b1 order, map the match's b1 offset
/// through the current b2 piece table, then perform the edit on b2.
/// Reverse-order: iterate matches in reverse so prior edits don't shift
/// later offsets (collect all, sort, reverse).
/// Both approaches must yield identical final content.
#[test]
fn forward_order_replace_matches_reverse_order() {
    // b1: "aXbXcXd"  — three X's at offsets 1, 3, 5
    let b1 = thicket(b"aXbXcXd");

    // ── forward order ──
    let b2_fwd = b1.fork();
    let matches: Vec<u64> = (0..b1.byte_len())
        .filter(|&i| {
            let mut buf = [0u8];
            b1.read_at(i, &mut buf).unwrap();
            buf[0] == b'X'
        })
        .collect();
    for parent_off in &matches {
        let child_off = match b2_fwd.map_offset_to_fork(*parent_off) {
            Some(x) => x,
            None => panic!("X at {parent_off} should still be in b2"),
        };
        b2_fwd.delete(child_off, 1).unwrap();
        b2_fwd.insert_before(child_off, b"[X]").unwrap();
    }

    // ── reverse order ──
    let b2_rev = b1.fork();
    for parent_off in matches.iter().rev() {
        b2_rev.delete(*parent_off, 1).unwrap();
        b2_rev.insert_before(*parent_off, b"[X]").unwrap();
    }

    // Both should produce "a[X]b[X]c[X]d"
    let expected = b"a[X]b[X]c[X]d";
    let fwd_bytes = crate::materialize(&*b2_fwd).unwrap();
    let rev_bytes = crate::materialize(&*b2_rev).unwrap();
    assert_eq!(fwd_bytes, expected, "forward order result mismatch");
    assert_eq!(rev_bytes, expected, "reverse order result mismatch");
    assert_eq!(fwd_bytes, rev_bytes, "forward and reverse differ");
}
