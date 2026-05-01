// Copyright (c) 2026 Michael J. Grier
use std::sync::Arc;

use crate::Branch;

fn thicket(data: &[u8]) -> Arc<dyn Branch> {
    crate::make_thicket_from_bytes(data.to_vec()).main()
}

// ── RM-1: no edits — identity range mapping ───────────────────────────────

/// An unedited fork maps any in-bounds range to itself.
#[test]
fn no_edits_in_bounds_range_maps_to_itself() {
    let b1 = thicket(b"hello world");
    let b2 = b1.fork();
    assert_eq!(b2.map_range_to_fork(0..5), Some(0..5));
    assert_eq!(b2.map_range_to_fork(6..11), Some(6..11));
    assert_eq!(b2.map_range_to_fork(0..11), Some(0..11));
}

/// An empty range (start == end) at an in-bounds position maps to itself.
#[test]
fn no_edits_empty_range_in_bounds_maps_to_itself() {
    let b1 = thicket(b"abc");
    let b2 = b1.fork();
    // Empty ranges at various in-bounds positions
    assert_eq!(b2.map_range_to_fork(0..0), Some(0..0));
    assert_eq!(b2.map_range_to_fork(1..1), Some(1..1));
    assert_eq!(b2.map_range_to_fork(3..3), Some(3..3)); // at byte_len
}

/// A range that extends past the end of the branch returns None.
#[test]
fn no_edits_out_of_bounds_range_returns_none() {
    let b1 = thicket(b"abc");
    let b2 = b1.fork();
    assert_eq!(b2.map_range_to_fork(0..4), None); // end past byte_len
    assert_eq!(b2.map_range_to_fork(2..5), None);
    assert_eq!(b2.map_range_to_fork(0..100), None);
}

/// Mapping the full span of the branch.
#[test]
fn no_edits_full_span_maps_to_itself() {
    let b1 = thicket(b"hello");
    let b2 = b1.fork();
    let len = b1.byte_len();
    assert_eq!(b2.map_range_to_fork(0..len), Some(0..len));
}

// ── RM-2: insert before range — range shifts right ────────────────────────

/// Inserting N bytes before the range shifts both endpoints right by N.
#[test]
fn insert_before_range_shifts_right() {
    // b1: "abcde"  insert "XY" at 1 → "aXYbcde"
    // parent range 2..4 ('c','d') → child 4..6
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.insert_before(1, b"XY").unwrap();
    assert_eq!(b2.map_range_to_fork(2..4), Some(4..6));
    // parent range 0..2 ('a','b') is split by the insert — 'a' at child 0, 'b' at child 3
    // so they are NOT contiguous → None
    assert_eq!(b2.map_range_to_fork(0..2), None);
}

/// Inserting strictly before the range: the range itself shifts.
#[test]
fn insert_before_range_start_shifts_whole_range() {
    // b1: "abcde"  insert "ZZZ" at 0 → "ZZZabcde"
    // parent range 1..4 ('b','c','d') → child 4..7
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.insert_before(0, b"ZZZ").unwrap();
    assert_eq!(b2.map_range_to_fork(1..4), Some(4..7));
}

// ── RM-3: insert after range — range unchanged ────────────────────────────

/// Inserting strictly after the range leaves the range mapping unchanged.
#[test]
fn insert_after_range_does_not_change_range() {
    // b1: "abcde"  insert "XY" at 4 → "abcdXYe"
    // parent range 0..3 ('a','b','c') → child 0..3  (unchanged)
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.insert_before(4, b"XY").unwrap();
    assert_eq!(b2.map_range_to_fork(0..3), Some(0..3));
}

// ── RM-4: insert inside range — range is split → None ────────────────────

/// Inserting inside the range breaks contiguity — returns None.
#[test]
fn insert_inside_range_breaks_contiguity() {
    // b1: "abcde"  insert "XX" at 2 (inside range 1..4) → "abXXcde"
    // parent range 1..4 spans across two Parent pieces → None
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.insert_before(2, b"XX").unwrap();
    assert_eq!(b2.map_range_to_fork(1..4), None);
}

/// Insert exactly at range start splits the range.
#[test]
fn insert_at_range_start_breaks_contiguity() {
    // b1: "abcde"  insert "M" at 2 → "abMcde"
    // parent range 2..5 — 'c' is at child 3, 'd' at 4, 'e' at 5
    // BUT 'c' starts a new Parent piece after the Inline "M",
    // and the range 2..5 must be entirely within one piece.
    // Actually parent offset 2 is the start of the second Parent piece,
    // and 2..5 is entirely within that piece — so this should map.
    // Let's try a case where insert is AT range start of the first piece:
    // b1: "abcde"  insert "M" at 0 → "Mabcde"
    // parent range 0..3 — 'a'=child 1, 'b'=child 2, 'c'=child 3
    // 0..3 must be in one piece: Parent[0..5] maps to child 1..6
    // This should work since all are in the same (shifted) Parent piece.
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.insert_before(0, b"M").unwrap();
    // After insert at 0: Parent[0..5] is now at child 1..6
    // parent range 0..3 → child 1..4
    assert_eq!(b2.map_range_to_fork(0..3), Some(1..4));
    // Insert inside at 2, range 0..5 straddles the insert → None
    let b3 = b1.fork();
    b3.insert_before(2, b"M").unwrap();
    assert_eq!(b3.map_range_to_fork(0..5), None);
}

// ── RM-5: delete of bytes in range — returns None ─────────────────────────

/// Deleting any byte within the range returns None.
#[test]
fn delete_inside_range_returns_none() {
    // b1: "abcde"  delete 'c' (off 2, len 1) → "abde"
    // parent range 1..4: 'b' (child 1), 'c' (displaced), 'd' (child 2)
    // Range spans displaced bytes → None
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.delete(2, 1).unwrap();
    assert_eq!(b2.map_range_to_fork(1..4), None);
}

// ── RM-6: delete before range — range shifts left ─────────────────────────

/// Deleting N bytes entirely before the range shifts both endpoints left by N.
#[test]
fn delete_before_range_shifts_left() {
    // b1: "abcde"  delete 'a','b' (off 0, len 2) → "cde"
    // parent range 2..5 ('c','d','e') → child 0..3
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.delete(0, 2).unwrap();
    assert_eq!(b2.map_range_to_fork(2..5), Some(0..3));
}

// ── RM-7: delete after range — range unchanged ────────────────────────────

/// Deleting strictly after the range leaves the range mapping unchanged.
#[test]
fn delete_after_range_does_not_change_range() {
    // b1: "abcde"  delete 'd','e' (off 3, len 2) → "abc"
    // parent range 0..2 ('a','b') → child 0..2 (unchanged)
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.delete(3, 2).unwrap();
    assert_eq!(b2.map_range_to_fork(0..2), Some(0..2));
}

// ── RM-8: overwrite inside range — returns None ───────────────────────────

/// Overwriting any byte within the range returns None (those bytes are gone
/// from the parent's perspective).
#[test]
fn overwrite_inside_range_returns_none() {
    // b1: "abcde"  overwrite offset 2 len 1 with "X" → "abXde"
    // parent range 1..4: 'b' (ok), 'c' (displaced/overwritten), 'd' (ok)
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.overwrite(2, b"X").unwrap();
    assert_eq!(b2.map_range_to_fork(1..4), None);
}

// ── RM-9: two-hop chained range mapping ──────────────────────────────────

/// Map a range through two fork levels by composing two map_range_to_fork calls.
#[test]
fn two_hop_chained_range_mapping() {
    // b1: "abcdefgh"
    // b2 = fork(b1): insert "XX" at 3 → "abcXXdefgh" (child len 10)
    // b3 = fork(b2): delete "XX" at child offsets 3..5 → "abcdefgh" (child len 8)
    //
    // Map b1 range 4..7 ('e','f','g'):
    //   b1 → b2: 4..7 are in Parent[3..8] which maps to child 5..10
    //            so parent 4..7 → child 5+1..5+4 = 6..9
    //   b2 → b3: 6..9 in b2 → after delete of 3..5, b2 Parent[5..10] maps
    //            to child 3..8. So b2 offset 6..9 → b3 3+(6-5)..3+(9-5) = 4..7
    let b1 = thicket(b"abcdefgh");
    let b2 = b1.fork();
    b2.insert_before(3, b"XX").unwrap();
    let b3 = b2.fork();
    b3.delete(3, 2).unwrap();
    let b2_range = b2.map_range_to_fork(4..7).expect("4..7 should map in b2");
    assert_eq!(b2_range, 6..9);
    let b3_range = b3
        .map_range_to_fork(b2_range)
        .expect("b2 range should map in b3");
    assert_eq!(b3_range, 4..7);
}

// ── RM-10: range spanning multiple pieces → None ─────────────────────────

/// A range that spans across a piece boundary (from one Parent piece into
/// another) returns None, since pieces represent non-contiguous parent regions
/// once inline content intervenes.
#[test]
fn range_spanning_two_pieces_returns_none() {
    // b1: "abcde"  insert "XX" at 2 → creates Parent[0..2], Inline, Parent[2..5]
    // parent range 1..3 spans Parent[0..2] and Parent[2..5] → None
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.insert_before(2, b"XX").unwrap();
    assert_eq!(b2.map_range_to_fork(1..3), None);
}

// ── RM-11: mapped range reads correct bytes ───────────────────────────────

/// The bytes at the mapped child range equal the bytes at the parent range.
#[test]
fn mapped_range_reads_correct_bytes() {
    // b1: "the quick brown"  insert "  " at 3 → "the   quick brown"
    // parent range 4..9 ("quick"):
    //   parent 4..9 is in Parent[3..15], shifted by 2 → child 6..11
    let b1 = thicket(b"the quick brown");
    let b2 = b1.fork();
    b2.insert_before(3, b"  ").unwrap();
    let child_range = b2.map_range_to_fork(4..9).expect("4..9 should map");
    let expected = b"quick";
    let mut buf = vec![0u8; (child_range.end - child_range.start) as usize];
    b2.read_at(child_range.start, &mut buf).unwrap();
    assert_eq!(&buf, expected);
}

// ── RM-12: single-byte range ─────────────────────────────────────────────

/// A single-byte range `n..n+1` behaves consistently with map_parent_offset.
#[test]
fn single_byte_range_consistent_with_offset() {
    let b1 = thicket(b"abcde");
    let b2 = b1.fork();
    b2.insert_before(2, b"X").unwrap();
    // Check each parent offset
    for i in 0..b1.byte_len() {
        let by_offset = b2.map_range_to_fork(i..i + 1);
        let by_single = b2.map_offset_to_fork(i).map(|o| o..o + 1);
        assert_eq!(
            by_offset,
            by_single,
            "single-byte range {i}..{} inconsistent",
            i + 1
        );
    }
}
