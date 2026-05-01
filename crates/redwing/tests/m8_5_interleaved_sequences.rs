// Copyright (c) 2026 Michael J. Grier

//! M8-5: Interleaved complex operation sequences.
//!
//! Verifies correctness of combined, non-trivial operation sequences on a
//! single `DerivedBranch`.  Each test interleaves multiple write operations
//! and then reads back via `materialize`, `materialize_range`, `read_byte`,
//! or `as_reader` to confirm the expected result.

use std::{
    io::{Read, Seek, SeekFrom},
    sync::Arc,
};

use redwing::{
    self, bytes_equal, flatten, make_thicket_from_bytes, materialize, materialize_range, Branch,
};

// ── helpers ───────────────────────────────────────────────────────────────────

fn base_arc(data: &[u8]) -> Arc<dyn Branch> {
    make_thicket_from_bytes(data.to_vec()).main()
}

fn derived_from(data: &[u8]) -> Arc<dyn Branch> {
    make_thicket_from_bytes(data.to_vec()).main()
}

// ── Test 1: insert_before then delete → net no-op ────────────────────────────

/// Insert a run of bytes at position x, then delete those exact bytes.
/// The result must equal the original content.
#[test]
fn insert_then_delete_same_region_is_net_noop() {
    let original = b"Hello, World!";
    let d = derived_from(original);
    let insert_at: u64 = 7;
    let payload = b"Beautiful ";
    d.insert_before(insert_at, payload).unwrap();
    assert_eq!(d.byte_len(), original.len() as u64 + payload.len() as u64);
    d.delete(insert_at, payload.len() as u64).unwrap();
    assert_eq!(d.byte_len(), original.len() as u64);
    assert_eq!(materialize(&*d).unwrap(), original);
}

// ── Test 2: overwrite then overwrite back → equality with original ────────────

/// Overwrite a region with new data, then overwrite it back to the original
/// bytes.  `bytes_equal` against the original base must return `true`.
#[test]
fn overwrite_region_then_restore_equals_original() {
    let original: Vec<u8> = (0u8..=127).collect();
    let base = base_arc(&original);
    let d = base.fork();
    let region_start: u64 = 40;
    let region_len = 20usize;
    // First overwrite: fill with 0xFF.
    d.overwrite(region_start, &[0xFFu8; 20]).unwrap();
    // Second overwrite: restore to original bytes.
    let restore: Vec<u8> =
        original[region_start as usize..region_start as usize + region_len].to_vec();
    d.overwrite(region_start, &restore).unwrap();
    assert!(bytes_equal(&*base, &*d).unwrap());
}

// ── Test 3: truncate then append → correct byte_len and content ───────────────

/// Truncate a branch to half its length, then append a known sequence.
/// Verify the final `byte_len` and the exact byte content.
#[test]
fn truncate_then_append_correct_len_and_content() {
    let original: Vec<u8> = (0u8..20).collect();
    let d = derived_from(&original);
    // Truncate to first 10 bytes.
    d.truncate(10).unwrap();
    assert_eq!(d.byte_len(), 10);
    // Append 5 known bytes.
    let suffix = b"ABCDE";
    d.append(suffix).unwrap();
    assert_eq!(d.byte_len(), 15);
    let result = materialize(&*d).unwrap();
    assert_eq!(&result[..10], &original[..10]);
    assert_eq!(&result[10..], suffix);
}

// ── Test 4: repeated insert_before(0, …) builds content in reverse ────────────

/// Insert single bytes at position 0 in sequence 0, 1, …, 9.
/// Because each new byte is inserted before all previous bytes, the final
/// sequence must read as 9, 8, 7, …, 0.
#[test]
fn repeated_insert_at_zero_produces_reversed_order() {
    let d = derived_from(b"");
    for b in 0u8..10 {
        d.insert_before(0, &[b]).unwrap();
    }
    assert_eq!(d.byte_len(), 10);
    let result = materialize(&*d).unwrap();
    let expected: Vec<u8> = (0u8..10).rev().collect();
    assert_eq!(result, expected);
}

// ── Test 5: overwrite overlapping an earlier insert_before region ─────────────

/// Insert bytes in the middle, then overwrite a range that partially overlaps
/// the inserted region and extends into the untouched base bytes.
/// Verify the correct merged content.
#[test]
fn overwrite_overlapping_earlier_insert_region() {
    // Base: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    let base: Vec<u8> = (0u8..10).collect();
    let d = derived_from(&base);
    // Insert [AA, BB, CC] before index 3 → [0,1,2, AA,BB,CC, 3,4,5,6,7,8,9]
    d.insert_before(3, &[0xAAu8, 0xBB, 0xCC]).unwrap();
    assert_eq!(d.byte_len(), 13);
    // Overwrite positions 2..6 (4 bytes) with [0xD0, 0xD1, 0xD2, 0xD3].
    // Positions 2..6 in the new space are: [2, AA, BB, CC].
    d.overwrite(2, &[0xD0u8, 0xD1, 0xD2, 0xD3]).unwrap();
    let result = materialize(&*d).unwrap();
    // Expected: [0, 1, D0, D1, D2, D3, 3, 4, 5, 6, 7, 8, 9]
    let expected: Vec<u8> = vec![0, 1, 0xD0, 0xD1, 0xD2, 0xD3, 3, 4, 5, 6, 7, 8, 9];
    assert_eq!(result, expected);
}

// ── Test 6: delete spanning an overwrite region and untouched base bytes ───────

/// Overwrite a region, then delete a range that spans part of that region
/// plus some untouched base bytes.  Verify the correct remainder.
#[test]
fn delete_spanning_overwrite_region_and_base_bytes() {
    // Base: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    let base: Vec<u8> = (0u8..10).collect();
    let d = derived_from(&base);
    // Overwrite positions 3,4,5 with [AA, BB, CC].
    d.overwrite(3, &[0xAAu8, 0xBB, 0xCC]).unwrap();
    // branch is now: [0, 1, 2, AA, BB, CC, 6, 7, 8, 9]
    // Delete positions 2..7 (5 bytes): removes [2, AA, BB, CC, 6].
    d.delete(2, 5).unwrap();
    let result = materialize(&*d).unwrap();
    // Expected: [0, 1, 7, 8, 9]
    assert_eq!(result, vec![0u8, 1, 7, 8, 9]);
}

// ── Test 7: append then read_byte at every appended position ──────────────────

/// Append 26 known bytes and verify `read_byte` at every appended position.
#[test]
fn append_then_read_byte_at_every_appended_position() {
    let base = b"START-";
    let appended: Vec<u8> = (b'A'..=b'Z').collect();
    let d = derived_from(base);
    d.append(&appended).unwrap();
    assert_eq!(d.byte_len(), (base.len() + appended.len()) as u64);
    for (i, &expected) in appended.iter().enumerate() {
        let offset = (base.len() + i) as u64;
        let got = d.read_byte(offset).unwrap();
        assert_eq!(
            got, expected,
            "offset {offset}: expected {expected:#04x}, got {got:#04x}"
        );
    }
}

// ── Test 8: insert_before in the middle, then materialize_range halves ─────────

/// Insert a separator in the middle of a branch, then materialize both halves
/// independently.  The concatenation of the two halves plus the separator
/// must reconstruct the full new content.
#[test]
fn insert_in_middle_materialize_range_halves_reconstruct() {
    // Base: "ABCDE" (5 bytes).
    let d = derived_from(b"ABCDE");
    // Insert "---" at position 2 → "AB---CDE"
    d.insert_before(2, b"---").unwrap();
    assert_eq!(d.byte_len(), 8);
    let left = materialize_range(&*d, 0, 2).unwrap(); // "AB"
    let sep = materialize_range(&*d, 2, 3).unwrap(); // "---"
    let right = materialize_range(&*d, 5, 3).unwrap(); // "CDE"
    let mut reconstructed = left;
    reconstructed.extend_from_slice(&sep);
    reconstructed.extend_from_slice(&right);
    assert_eq!(reconstructed, b"AB---CDE");
    assert_eq!(materialize(&*d).unwrap(), b"AB---CDE");
}

// ── Test 9: bytes_equal survives flatten + re-derive round-trip ───────────────

/// Build a derived branch with several edits.  Flatten it, then derive a new
/// child from the flattened base without making any edits.  `bytes_equal`
/// between the original derived Branch and the new child must be `true`.
#[test]
fn bytes_equal_after_flatten_rederive_no_edits() {
    let base: Vec<u8> = (0u8..=99).collect();
    let d = derived_from(&base);
    d.overwrite(10, b"XXXX").unwrap();
    d.insert_before(50, b"*").unwrap();
    d.delete(80, 3).unwrap();

    // Flatten then re-derive (no edits).
    let flat = flatten(&*d).unwrap();
    let child = flat.fork();

    assert!(bytes_equal(&*d, &*child).unwrap());
}

// ── Test 10: as_reader Seek then read-to-end matches materialize_range ─────────

/// Build a branch with writes, seek the reader to a known offset, read to
/// end, and verify that the read bytes match `materialize_range` from the
/// same offset.
#[test]
fn as_reader_seek_then_read_matches_materialize_range() {
    let base: Vec<u8> = (0u8..60).collect();
    let d = derived_from(&base);
    // A couple of edits to make the Branch non-trivial.
    d.overwrite(5, b"HELLO").unwrap();
    d.insert_before(30, b">>>").unwrap();

    let seek_to: u64 = 20;
    let total = d.byte_len();
    let expected_tail = materialize_range(&*d, seek_to, total - seek_to).unwrap();

    let mut reader = d.as_reader();
    reader.seek(SeekFrom::Start(seek_to)).unwrap();
    let mut actual_tail = Vec::new();
    reader.read_to_end(&mut actual_tail).unwrap();

    assert_eq!(actual_tail, expected_tail);
}

// ── Bonus Test 11: multi-step sequence with all five operations ───────────────

/// Combine overwrite, insert_before, delete, append, and truncate on one
/// branch.  The expected final content is derived by tracing each operation
/// step-by-step and verified by `materialize`.
#[test]
fn combined_five_operation_sequence_correct_content() {
    // Base: "0123456789" (ASCII digits as byte values 0x30..0x39)
    let d = derived_from(b"0123456789");
    // Step 1: overwrite positions 2..4 with "AB" → "01AB456789"
    d.overwrite(2, b"AB").unwrap();
    // Step 2: insert "XY" before position 5 → "01AB4XY56789"
    d.insert_before(5, b"XY").unwrap();
    // Step 3: delete 2 bytes at position 7 → "01AB4XY789"
    d.delete(7, 2).unwrap();
    // Step 4: append "END" → "01AB4XY789END"
    d.append(b"END").unwrap();
    // Step 5: truncate to 10 → "01AB4XY789"
    d.truncate(10).unwrap();
    let result = materialize(&*d).unwrap();
    assert_eq!(result, b"01AB4XY789");
}

// ── Bonus Test 12: interleaved inserts and deletes maintain correct offsets ────

/// Alternately insert and delete bytes at different positions, verifying that
/// the offset coordinate space is updated correctly after each operation.
#[test]
fn interleaved_inserts_and_deletes_maintain_correct_offsets() {
    // Base: "ABCDE" → 5 bytes.
    let d = derived_from(b"ABCDE");
    // Insert "12" before index 1 → "A12BCDE" (7 bytes)
    d.insert_before(1, b"12").unwrap();
    assert_eq!(d.byte_len(), 7);
    // Step 2: Delete 1 byte at index 4 → removes 'C' → "A12BDE" (6 bytes)
    d.delete(4, 1).unwrap();
    assert_eq!(d.byte_len(), 6);
    // Step 3: Insert "Z" before index 3 → "A12ZBDE" (7 bytes)
    d.insert_before(3, b"Z").unwrap();
    assert_eq!(d.byte_len(), 7);
    // Step 4: Delete 2 bytes at index 0 → removes "A1" → "2ZBDE" (5 bytes)
    d.delete(0, 2).unwrap();
    assert_eq!(d.byte_len(), 5);
    let result = materialize(&*d).unwrap();
    assert_eq!(result, b"2ZBDE");
}
