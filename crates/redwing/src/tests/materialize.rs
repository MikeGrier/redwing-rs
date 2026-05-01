// Copyright (c) 2026 Michael J. Grier
use std::{io::Cursor, sync::Arc};

use super::{bytes_equal, flatten, materialize};
use crate::Branch;

// ── helpers ───────────────────────────────────────────────────────────────

fn thicket(data: &[u8]) -> Arc<dyn Branch> {
    crate::make_thicket_from_bytes(data.to_vec()).main()
}

// ── empty Branch ────────────────────────────────────────────────────────

#[test]
fn materialize_empty_base_branch_returns_empty_vec() {
    let snap = thicket(b"");
    let got = materialize(&*snap).unwrap();
    assert!(got.is_empty());
}

#[test]
fn materialize_empty_derived_branch_returns_empty_vec() {
    let ds = thicket(b"");
    let got = materialize(&*ds).unwrap();
    assert!(got.is_empty());
}

// ── base branch round-trip ──────────────────────────────────────────────

#[test]
fn materialize_single_byte_base() {
    let snap = thicket(b"X");
    assert_eq!(materialize(&*snap).unwrap(), b"X");
}

#[test]
fn materialize_base_matches_original_data() {
    let data = b"Hello, world!";
    let snap = thicket(data);
    assert_eq!(materialize(&*snap).unwrap(), data);
}

#[test]
fn materialize_base_all_byte_values() {
    let data: Vec<u8> = (0u8..=255).collect();
    let snap = thicket(&data);
    assert_eq!(materialize(&*snap).unwrap(), data);
}

#[test]
fn materialize_base_length_matches_byte_len() {
    let data = b"ABCDE";
    let snap = thicket(data);
    let got = materialize(&*snap).unwrap();
    assert_eq!(got.len() as u64, snap.byte_len());
}

// ── derived branch — overwrite ──────────────────────────────────────────

#[test]
fn materialize_derived_overwrite_middle() {
    let ds = thicket(b"AAAAA");
    ds.overwrite(1, b"BB").unwrap();
    assert_eq!(materialize(&*ds).unwrap(), b"ABBAA");
}

#[test]
fn materialize_derived_overwrite_entire_range() {
    let ds = thicket(b"AAAAA");
    ds.overwrite(0, b"BBBBB").unwrap();
    assert_eq!(materialize(&*ds).unwrap(), b"BBBBB");
}

#[test]
fn materialize_derived_multiple_overwrites_coalesced() {
    // Adjacent overwrites coalesce to one log entry; result must still be correct.
    let ds = thicket(b"AAAAA");
    ds.overwrite(0, b"X").unwrap();
    ds.overwrite(1, b"Y").unwrap();
    ds.overwrite(2, b"Z").unwrap();
    assert_eq!(materialize(&*ds).unwrap(), b"XYZAA");
}

#[test]
fn materialize_derived_overwrite_last_byte() {
    let ds = thicket(b"ABCDE");
    ds.overwrite(4, b"Z").unwrap();
    assert_eq!(materialize(&*ds).unwrap(), b"ABCDZ");
}

#[test]
fn materialize_derived_overwrite_first_byte() {
    let ds = thicket(b"ABCDE");
    ds.overwrite(0, b"Z").unwrap();
    assert_eq!(materialize(&*ds).unwrap(), b"ZBCDE");
}

// ── derived branch — insert ─────────────────────────────────────────────

#[test]
fn materialize_derived_insert_at_start() {
    let ds = thicket(b"World");
    ds.insert_before(0, b"Hello, ").unwrap();
    assert_eq!(materialize(&*ds).unwrap(), b"Hello, World");
}

#[test]
fn materialize_derived_insert_at_end_appends() {
    let ds = thicket(b"Hello");
    ds.insert_before(5, b", world!").unwrap();
    assert_eq!(materialize(&*ds).unwrap(), b"Hello, world!");
}

#[test]
fn materialize_derived_insert_in_middle() {
    let ds = thicket(b"AC");
    ds.insert_before(1, b"B").unwrap();
    assert_eq!(materialize(&*ds).unwrap(), b"ABC");
}

#[test]
fn materialize_derived_multiple_inserts_coalesced() {
    let ds = thicket(b"");
    ds.insert_before(0, b"A").unwrap();
    ds.insert_before(1, b"B").unwrap();
    ds.insert_before(2, b"C").unwrap();
    assert_eq!(materialize(&*ds).unwrap(), b"ABC");
}

// ── derived branch — delete ─────────────────────────────────────────────

#[test]
fn materialize_derived_delete_middle() {
    let ds = thicket(b"ABCDE");
    ds.delete(1, 3).unwrap();
    assert_eq!(materialize(&*ds).unwrap(), b"AE");
}

#[test]
fn materialize_derived_delete_first_byte() {
    let ds = thicket(b"ABCDE");
    ds.delete(0, 1).unwrap();
    assert_eq!(materialize(&*ds).unwrap(), b"BCDE");
}

#[test]
fn materialize_derived_delete_last_byte() {
    let ds = thicket(b"ABCDE");
    ds.delete(4, 1).unwrap();
    assert_eq!(materialize(&*ds).unwrap(), b"ABCD");
}

#[test]
fn materialize_derived_delete_all_bytes_returns_empty() {
    let ds = thicket(b"ABCDE");
    ds.delete(0, 5).unwrap();
    assert!(materialize(&*ds).unwrap().is_empty());
}

// ── derived branch — mixed operations ───────────────────────────────────

#[test]
fn materialize_overwrite_then_insert() {
    let ds = thicket(b"ABCDE");
    ds.overwrite(2, b"X").unwrap(); // ABXDE
    ds.insert_before(3, b"Y").unwrap(); // ABXYDE
    assert_eq!(materialize(&*ds).unwrap(), b"ABXYDE");
}

#[test]
fn materialize_insert_then_delete() {
    let ds = thicket(b"ABC");
    ds.insert_before(1, b"XX").unwrap(); // AXXBC
    ds.delete(3, 1).unwrap(); // AXXC
    assert_eq!(materialize(&*ds).unwrap(), b"AXXC");
}

#[test]
fn materialize_overwrite_then_delete() {
    let ds = thicket(b"ABCDE");
    ds.overwrite(0, b"ZZ").unwrap(); // ZZCDE
    ds.delete(2, 2).unwrap(); // ZZE
    assert_eq!(materialize(&*ds).unwrap(), b"ZZE");
}

// ── derived branch chain ────────────────────────────────────────────────

#[test]
fn materialize_two_level_derived_chain() {
    // Base → Derived1 (overwrite) → Derived2 (insert)
    let d1 = thicket(b"AAAAA");
    d1.overwrite(2, b"X").unwrap();
    // d1 content: AAXAA

    let d2 = d1.fork();
    d2.insert_before(3, b"Y").unwrap();
    // d2 content: AAXYAA

    assert_eq!(materialize(&*d2).unwrap(), b"AAXYAA");
}

#[test]
fn materialize_three_level_derived_chain() {
    let d1 = thicket(b"ABC");
    d1.overwrite(0, b"X").unwrap(); // XBC

    let d2 = d1.fork();
    d2.overwrite(1, b"Y").unwrap(); // XYC

    let d3 = d2.fork();
    d3.overwrite(2, b"Z").unwrap(); // XYZ

    assert_eq!(materialize(&*d3).unwrap(), b"XYZ");
}

// ── result matches as_reader + read_to_end ─────────────────────────────

#[test]
fn materialize_matches_read_to_end_on_base() {
    use std::io::Read;
    let data = b"The quick brown fox";
    let snap = thicket(data);
    let mut reader_result = Vec::new();
    snap.as_reader().read_to_end(&mut reader_result).unwrap();
    assert_eq!(materialize(&*snap).unwrap(), reader_result);
}

#[test]
fn materialize_matches_read_to_end_on_derived() {
    use std::io::Read;
    let ds = thicket(b"Hello, world!");
    ds.overwrite(7, b"Rust!").unwrap();
    ds.delete(12, 1).unwrap();

    let mut reader_result = Vec::new();
    ds.as_reader().read_to_end(&mut reader_result).unwrap();
    assert_eq!(materialize(&*ds).unwrap(), reader_result);
}

// ── larger content ────────────────────────────────────────────────────────

#[test]
fn materialize_1k_buffer_base_branch() {
    let data: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
    let snap = thicket(&data);
    assert_eq!(materialize(&*snap).unwrap(), data);
}

#[test]
fn materialize_derived_scatter_overwrites_across_1k_buffer() {
    let data = vec![0xAAu8; 1024];
    let ds = thicket(&data);
    // Write 10 disjoint 16-byte regions at strides of 100 bytes.
    for i in 0u8..10 {
        ds.overwrite((i as u64) * 100, &[i; 16]).unwrap();
    }
    let got = materialize(&*ds).unwrap();
    assert_eq!(got.len(), 1024);
    for i in 0u8..10 {
        let start = i as usize * 100;
        assert_eq!(
            &got[start..start + 16],
            &vec![i; 16],
            "region {i} has wrong content"
        );
    }
    // Bytes outside all written regions must retain the original 0xAA.
    for i in 0u8..10 {
        let start = i as usize * 100 + 16;
        let end = if i < 9 { (i as usize + 1) * 100 } else { 1024 };
        assert!(
            got[start..end].iter().all(|&b| b == 0xAA),
            "gap after region {i} is corrupted"
        );
    }
}

// ── materialize_range ─────────────────────────────────────────────────────

use super::materialize_range;

// Zero-length range returns empty vec.
#[test]
fn materialize_range_zero_len_returns_empty() {
    let snap = thicket(b"ABCDE");
    assert!(materialize_range(&*snap, 0, 0).unwrap().is_empty());
}

#[test]
fn materialize_range_zero_len_at_end_returns_empty() {
    let snap = thicket(b"ABCDE");
    assert!(materialize_range(&*snap, 5, 0).unwrap().is_empty());
}

// Single-byte ranges at each position.
#[test]
fn materialize_range_single_byte_at_start() {
    assert_eq!(materialize_range(&*thicket(b"ABCDE"), 0, 1).unwrap(), b"A");
}

#[test]
fn materialize_range_single_byte_at_end() {
    assert_eq!(materialize_range(&*thicket(b"ABCDE"), 4, 1).unwrap(), b"E");
}

#[test]
fn materialize_range_single_byte_in_middle() {
    assert_eq!(materialize_range(&*thicket(b"ABCDE"), 2, 1).unwrap(), b"C");
}

// Full-branch range is equivalent to materialize.
#[test]
fn materialize_range_full_range_matches_materialize() {
    let data = b"Hello, world!";
    let snap = thicket(data);
    assert_eq!(
        materialize_range(&*snap, 0, snap.byte_len()).unwrap(),
        materialize(&*snap).unwrap()
    );
}

// Middle slice.
#[test]
fn materialize_range_middle_slice() {
    assert_eq!(
        materialize_range(&*thicket(b"ABCDE"), 1, 3).unwrap(),
        b"BCD"
    );
}

#[test]
fn materialize_range_tail_slice() {
    assert_eq!(materialize_range(&*thicket(b"ABCDE"), 3, 2).unwrap(), b"DE");
}

#[test]
fn materialize_range_head_slice() {
    assert_eq!(
        materialize_range(&*thicket(b"ABCDE"), 0, 3).unwrap(),
        b"ABC"
    );
}

// Range on a derived branch — entirely within one parent piece.
#[test]
fn materialize_range_within_parent_piece() {
    // Overwrite [2,4): pieces are [0,2) parent, [2,4) inline, [4,5) parent.
    // Range [0,2) is entirely within the first parent piece.
    let ds = thicket(b"ABCDE");
    ds.overwrite(2, b"XY").unwrap();
    assert_eq!(materialize_range(&*ds, 0, 2).unwrap(), b"AB");
}

// Range on a derived branch — entirely within the inline piece.
#[test]
fn materialize_range_within_inline_piece() {
    let ds = thicket(b"ABCDE");
    ds.overwrite(2, b"XY").unwrap();
    assert_eq!(materialize_range(&*ds, 2, 2).unwrap(), b"XY");
}

// Range spanning two pieces (parent → inline).
#[test]
fn materialize_range_spanning_parent_and_inline_piece() {
    let ds = thicket(b"ABCDE");
    ds.overwrite(2, b"XY").unwrap();
    // Spans [1,4): 'B' from parent, 'XY' from inline.
    assert_eq!(materialize_range(&*ds, 1, 3).unwrap(), b"BXY");
}

// Range spanning inline → second parent piece.
#[test]
fn materialize_range_spanning_inline_and_trailing_parent_piece() {
    let ds = thicket(b"ABCDE");
    ds.overwrite(2, b"XY").unwrap();
    // [3,5): 'Y' from inline, 'E' from parent.
    assert_eq!(materialize_range(&*ds, 3, 2).unwrap(), b"YE");
}

// Range at the very end of the branch.
#[test]
fn materialize_range_at_end_of_derived_branch() {
    let ds = thicket(b"ABCDE");
    ds.overwrite(4, b"Z").unwrap();
    assert_eq!(materialize_range(&*ds, 3, 2).unwrap(), b"DZ");
}

// Range on a derived branch with an insert (length change).
#[test]
fn materialize_range_after_insert_in_derived() {
    // Base: "ACE", insert 'B' at 1 → "ABCE", insert 'D' at 3 → "ABDCE"
    let ds = thicket(b"ACE");
    ds.insert_before(1, b"B").unwrap();
    ds.insert_before(3, b"D").unwrap();
    let got = materialize_range(&*ds, 1, 3).unwrap();
    assert_eq!(got, b"BCD");
}

// Range on a derived branch with a delete.
#[test]
fn materialize_range_after_delete_in_derived() {
    // "ABCDE" delete(1, 3) removes bytes [1,4) → "AE" (2 bytes)
    let ds = thicket(b"ABCDE");
    ds.delete(1, 3).unwrap();
    assert_eq!(materialize_range(&*ds, 1, 1).unwrap(), b"E");
}

// Range exactly the size of the branch after a delete (shrunken length).
#[test]
fn materialize_range_full_range_after_delete() {
    let ds = thicket(b"ABCDE");
    ds.delete(1, 3).unwrap();
    // len is now 2: "AE"
    assert_eq!(materialize_range(&*ds, 0, ds.byte_len()).unwrap(), b"AE");
}

// --- Error cases ---

#[test]
fn materialize_range_out_of_bounds_returns_error() {
    let snap = thicket(b"ABCDE");
    let err = materialize_range(&*snap, 3, 5).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn materialize_range_offset_at_len_with_nonzero_len_returns_error() {
    let snap = thicket(b"ABCDE");
    let err = materialize_range(&*snap, 5, 1).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn materialize_range_overflow_returns_error() {
    let snap = thicket(b"ABCDE");
    let err = materialize_range(&*snap, u64::MAX, 1).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn materialize_range_empty_branch_zero_len_ok() {
    let snap = thicket(b"");
    assert!(materialize_range(&*snap, 0, 0).unwrap().is_empty());
}

#[test]
fn materialize_range_empty_branch_nonzero_len_returns_error() {
    let snap = thicket(b"");
    let err = materialize_range(&*snap, 0, 1).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

// Result of materialize_range must match the corresponding slice of materialize.
#[test]
fn materialize_range_matches_materialize_slice_on_derived() {
    let ds = thicket(b"ABCDEFGH");
    ds.overwrite(2, b"XY").unwrap();
    ds.insert_before(5, b"Z").unwrap();
    let full = materialize(&*ds).unwrap();
    // Check several sub-ranges.
    for start in 0..full.len() {
        for end in start..=full.len() {
            let len = (end - start) as u64;
            let got = materialize_range(&*ds, start as u64, len).unwrap();
            assert_eq!(got, &full[start..end], "range [{start},{end}) mismatch");
        }
    }
}

// ── M7-5: flatten ───────────────────────────────────────────────────────

#[test]
fn flatten_base_branch_equals_original() {
    let snap = thicket(b"Hello, World!");
    let flat = flatten(&*snap).unwrap();
    assert_eq!(flat.byte_len(), snap.byte_len());
    let orig = materialize(&*snap).unwrap();
    let got = materialize(flat.as_ref()).unwrap();
    assert_eq!(got, orig);
}

#[test]
fn flatten_empty_branch_byte_len_zero() {
    let snap = thicket(b"");
    let flat = flatten(&*snap).unwrap();
    assert_eq!(flat.byte_len(), 0);
}

#[test]
fn flatten_derived_with_overwrite_matches_materialize() {
    let ds = thicket(b"ABCDE");
    ds.overwrite(1, b"Z").unwrap();
    let expected = materialize(&*ds).unwrap();
    let flat = flatten(&*ds).unwrap();
    assert_eq!(materialize(flat.as_ref()).unwrap(), expected);
}

#[test]
fn flatten_derived_with_inserts_matches_materialize() {
    let ds = thicket(b"ACE");
    ds.insert_before(1, b"B").unwrap();
    ds.insert_before(3, b"D").unwrap();
    let expected = materialize(&*ds).unwrap();
    let flat = flatten(&*ds).unwrap();
    assert_eq!(materialize(flat.as_ref()).unwrap(), expected);
}

#[test]
fn flatten_derived_with_deletes_matches_materialize() {
    let ds = thicket(b"ABXCDE");
    ds.delete(2, 1).unwrap();
    let expected = materialize(&*ds).unwrap();
    let flat = flatten(&*ds).unwrap();
    assert_eq!(materialize(flat.as_ref()).unwrap(), expected);
}

#[test]
fn flatten_twenty_level_chain_matches_leaf_materialize() {
    let mut current = thicket(b"ROOT");
    for i in 0u8..20 {
        let next = current.fork();
        next.append(&[b'A' + i]).unwrap();
        current = next;
    }
    let expected = materialize(current.as_ref()).unwrap();
    let flat = flatten(current.as_ref()).unwrap();
    assert_eq!(materialize(flat.as_ref()).unwrap(), expected);
}

#[test]
fn flatten_result_usable_as_parent_for_new_derived() {
    let snap = thicket(b"ABCDE");
    let flat = flatten(&*snap).unwrap();
    let child = flat.fork();
    child.overwrite(0, b"Z").unwrap();
    let mut buf = [0u8; 5];
    child.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"ZBCDE");
}

#[test]
fn flatten_then_derive_and_overwrite_correct_content() {
    let snap = thicket(b"ORIGINAL");
    let flat = flatten(&*snap).unwrap();
    let ds = flat.fork();
    ds.overwrite(0, b"MODIFIED").unwrap();
    let got = materialize(ds.as_ref()).unwrap();
    assert_eq!(got, b"MODIFIED");
}

#[test]
fn flatten_one_mb_derived_byte_len_correct() {
    let data: Vec<u8> = (0u8..=255).cycle().take(1024 * 1024).collect();
    let ds = crate::make_thicket_from_bytes(data.clone()).main();
    ds.overwrite(0, &[0xFFu8]).unwrap();
    let flat = flatten(ds.as_ref()).unwrap();
    assert_eq!(flat.byte_len(), 1024 * 1024);
    // spot-check first and last bytes
    assert_eq!(materialize(flat.as_ref()).unwrap()[0], 0xFFu8);
    assert_eq!(
        materialize(flat.as_ref()).unwrap()[1024 * 1024 - 1],
        data[1024 * 1024 - 1]
    );
}

#[test]
fn flatten_twice_results_are_equal() {
    let snap = thicket(b"ABCDE");
    let flat1 = materialize(flatten(&*snap).unwrap().as_ref()).unwrap();
    let flat2 = materialize(flatten(&*snap).unwrap().as_ref()).unwrap();
    assert_eq!(flat1, flat2);
}

#[test]
fn flatten_produces_base_branch_independent_of_parent() {
    // After flattening, the result reads correctly even after dropping the
    // original source.
    let bytes = b"StandaloneData";
    let flat = {
        let snap = thicket(bytes);
        flatten(&*snap).unwrap()
    };
    assert_eq!(materialize(flat.as_ref()).unwrap(), bytes);
}

#[test]
fn flatten_agrees_with_materialize_on_every_byte() {
    let ds = thicket(b"abcdefghij");
    ds.overwrite(3, b"XYZ").unwrap();
    ds.insert_before(7, b"!").unwrap();
    let mat = materialize(&*ds).unwrap();
    let flat = flatten(&*ds).unwrap();
    let flat_bytes = materialize(flat.as_ref()).unwrap();
    assert_eq!(flat_bytes, mat);
}

// ── M7-6: bytes_equal ──────────────────────────────────────────────────

#[test]
fn bytes_equal_two_identical_base_branches() {
    let a = thicket(b"Hello");
    let b = thicket(b"Hello");
    assert!(bytes_equal(&*a, &*b).unwrap());
}

#[test]
fn bytes_equal_same_data_from_bytes_vs_from_reader() {
    let data = b"SameData";
    let a = crate::make_thicket_from_bytes(data.to_vec()).main();
    let b = crate::make_thicket_from_reader(Cursor::new(data.to_vec()))
        .unwrap()
        .main();
    assert!(bytes_equal(a.as_ref(), b.as_ref()).unwrap());
}

#[test]
fn bytes_equal_different_lengths_returns_false() {
    let a = thicket(b"Short");
    let b = thicket(b"LongerString");
    assert!(!bytes_equal(&*a, &*b).unwrap());
}

#[test]
fn bytes_equal_same_length_different_content_returns_false() {
    let a = thicket(b"AAAAA");
    let b = thicket(b"BBBBB");
    assert!(!bytes_equal(&*a, &*b).unwrap());
}

#[test]
fn bytes_equal_both_empty_returns_true() {
    let a = thicket(b"");
    let b = thicket(b"");
    assert!(bytes_equal(&*a, &*b).unwrap());
}

#[test]
fn bytes_equal_empty_vs_nonempty_returns_false() {
    let a = thicket(b"");
    let b = thicket(b"X");
    assert!(!bytes_equal(&*a, &*b).unwrap());
}

#[test]
fn bytes_equal_base_vs_flattened_derived_same_content() {
    let data = b"FlattenMe";
    let a = thicket(data);
    let ds = thicket(data);
    let flat = flatten(&*ds).unwrap();
    assert!(bytes_equal(&*a, flat.as_ref()).unwrap());
}

#[test]
fn bytes_equal_base_vs_derived_with_overwrite_returns_false() {
    let a = thicket(b"ABCDE");
    let ds = thicket(b"ABCDE");
    ds.overwrite(2, b"Z").unwrap();
    assert!(!bytes_equal(&*a, &*ds).unwrap());
}

#[test]
fn bytes_equal_large_equal_branchs_returns_true() {
    let data: Vec<u8> = (0u8..=255).cycle().take(1024 * 1024).collect();
    let a = crate::make_thicket_from_bytes(data.clone()).main();
    let b = crate::make_thicket_from_bytes(data).main();
    assert!(bytes_equal(a.as_ref(), b.as_ref()).unwrap());
}

#[test]
fn bytes_equal_large_branches_differ_in_last_byte_returns_false() {
    let mut data_a: Vec<u8> = (0u8..=255).cycle().take(1024 * 1024).collect();
    let mut data_b = data_a.clone();
    *data_a.last_mut().unwrap() = 0xAA;
    *data_b.last_mut().unwrap() = 0xBB;
    let a = crate::make_thicket_from_bytes(data_a).main();
    let b = crate::make_thicket_from_bytes(data_b).main();
    assert!(!bytes_equal(a.as_ref(), b.as_ref()).unwrap());
}

#[test]
fn bytes_equal_reflexive() {
    let a = thicket(b"Reflexive");
    assert!(bytes_equal(&*a, &*a).unwrap());
}

#[test]
fn bytes_equal_symmetric() {
    let a = thicket(b"Hello");
    let b = thicket(b"World");
    assert_eq!(
        bytes_equal(&*a, &*b).unwrap(),
        bytes_equal(&*b, &*a).unwrap()
    );
}
