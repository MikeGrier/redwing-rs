// Copyright (c) 2026 Michael J. Grier

//! M8-4: Deep chain / stress tests.
//!
//! Verifies that derivation chains hundreds of levels deep remain correct,
//! do not stack-overflow during reads, and produce results that match a
//! direct construction.  The recursion depth for a 500-level chain is well
//! within Rust's default stack limits (~500 small frames on an 8 MB stack).

use std::{io::Read, sync::Arc};

use redwing::{
    self, bytes_equal, flatten, make_thicket_from_bytes, materialize, materialize_range, Branch,
};

// ── helpers ───────────────────────────────────────────────────────────────────

fn base_arc(data: &[u8]) -> Arc<dyn Branch> {
    make_thicket_from_bytes(data.to_vec()).main()
}

/// Build an N-level derivation chain where each level appends one byte.
/// Level i contributes byte `(i % 256) as u8`.  Final byte_len == levels.
fn build_append_chain(levels: usize) -> Arc<dyn Branch> {
    assert!(levels > 0);
    let mut current: Arc<dyn Branch> = make_thicket_from_bytes(vec![]).main();
    current.append(&[0u8]).unwrap();
    for i in 1..levels {
        let next = current.fork();
        next.append(&[(i % 256) as u8]).unwrap();
        current = next;
    }
    current
}

/// Expected byte vector for a chain built by `build_append_chain(levels)`.
fn expected_append_chain_bytes(levels: usize) -> Vec<u8> {
    (0..levels).map(|i| (i % 256) as u8).collect()
}

/// Build a 200-level chain (200 calls to derive, one per loop
/// iteration) where level i overwrites position i with value (i+1) as u8.
/// The chain starts from a 200-byte zeroed base.
fn build_overwrite_chain_200() -> Arc<dyn Branch> {
    const N: usize = 200;
    let mut current: Arc<dyn Branch> = make_thicket_from_bytes(vec![0u8; N]).main();
    current.overwrite(0, &[1u8]).unwrap();
    for i in 1..N {
        let next = current.fork();
        next.overwrite(i as u64, &[(i + 1) as u8]).unwrap();
        current = next;
    }
    current
}

// ── Test 1: 500-level append chain — correct byte_len ────────────────────────

#[test]
fn append_chain_500_levels_has_correct_byte_len() {
    let chain = build_append_chain(500);
    assert_eq!(chain.byte_len(), 500);
}

// ── Test 2: materialize() over 500-level chain ────────────────────────────────

#[test]
fn append_chain_500_levels_materializes_correctly() {
    let chain = build_append_chain(500);
    let result = materialize(&*chain).unwrap();
    let expected = expected_append_chain_bytes(500);
    assert_eq!(result, expected);
}

// ── Test 3: flatten() of 500-level chain ──────────────────────────────────────

#[test]
fn append_chain_500_levels_flatten_yields_correct_bytes() {
    let chain = build_append_chain(500);
    let flat = flatten(&*chain).unwrap();
    assert_eq!(flat.byte_len(), 500);
    let result = materialize(&*flat).unwrap();
    let expected = expected_append_chain_bytes(500);
    assert_eq!(result, expected);
}

// ── Test 4: bytes_equal between 500-level chain and its flatten ───────────────

#[test]
fn append_chain_500_levels_bytes_equal_to_flattened() {
    let chain = build_append_chain(500);
    let flat = flatten(&*chain).unwrap();
    assert!(bytes_equal(&*chain, &*flat).unwrap());
}

// ── Test 5: 200-level overwrite chain — spot-check via read_byte ──────────────

#[test]
fn overwrite_chain_200_levels_spot_check_read_byte() {
    let chain = build_overwrite_chain_200();
    assert_eq!(chain.byte_len(), 200);
    // Level i overwrote position i with value i+1; spot-check several positions.
    for i in [0usize, 1, 50, 99, 100, 150, 198, 199] {
        let expected = (i + 1) as u8;
        let got = chain.read_byte(i as u64).unwrap();
        assert_eq!(
            got, expected,
            "position {i}: expected {expected}, got {got}"
        );
    }
}

// ── Test 6: 200 alternating insert/delete on single branch → empty ──────────

#[test]
fn alternating_insert_delete_200_times_yields_empty() {
    let d: Arc<dyn Branch> = make_thicket_from_bytes(vec![]).main();
    // 100 cycles of (insert_before(0, [42]), delete(0, 1)) = 200 operations.
    for _ in 0..100 {
        d.insert_before(0, &[42u8]).unwrap();
        assert_eq!(d.byte_len(), 1);
        d.delete(0, 1).unwrap();
        assert_eq!(d.byte_len(), 0);
    }
    assert_eq!(d.byte_len(), 0);
    assert_eq!(materialize(&*d).unwrap(), b"");
}

// ── Test 7: 100 successive insert_before(0, …) builds reversed sequence ───────

#[test]
fn successive_insert_at_zero_builds_reversed_sequence() {
    let d: Arc<dyn Branch> = make_thicket_from_bytes(vec![]).main();
    // Insert bytes 0, 1, …, 99 one at a time at position 0.
    // Because consecutive inserts within the same range merge, the final
    // log has a single Insert(offset=0, bytes=[99, 98, …, 1, 0]).
    for i in 0u8..100 {
        d.insert_before(0, &[i]).unwrap();
    }
    assert_eq!(d.byte_len(), 100);
    let result = materialize(&*d).unwrap();
    let expected: Vec<u8> = (0u8..100).rev().collect();
    assert_eq!(result, expected);
}

// ── Test 8: 100 successive truncate calls → byte_len reaches 0 ───────────────

#[test]
fn successive_truncate_100_times_reduces_to_zero() {
    let data: Vec<u8> = (0u8..100).collect();
    let d = make_thicket_from_bytes(data).main();
    for remaining in (0u64..100).rev() {
        d.truncate(remaining).unwrap();
        assert_eq!(d.byte_len(), remaining, "after truncate({remaining})");
    }
    assert_eq!(d.byte_len(), 0);
}

// ── Test 9: as_reader() over 500-level chain → matches materialize ────────────

#[test]
fn append_chain_500_levels_as_reader_matches_materialize() {
    let chain = build_append_chain(500);
    let expected = materialize(&*chain).unwrap();
    let mut actual = Vec::new();
    chain.as_reader().read_to_end(&mut actual).unwrap();
    assert_eq!(actual, expected);
}

// ── Test 10: materialize_range over 500-level chain mid-section ───────────────

#[test]
fn append_chain_500_levels_materialize_range_mid_matches_slice() {
    let chain = build_append_chain(500);
    let full = materialize(&*chain).unwrap();
    // Take a 200-byte window in the middle of the 500-byte chain.
    let offset: u64 = 100;
    let len: u64 = 200;
    let range = materialize_range(&*chain, offset, len).unwrap();
    assert_eq!(range, full[100..300]);
}

// ── Bonus: flatten of overwrite chain satisfies bytes_equal to original ────────

#[test]
fn overwrite_chain_200_levels_flatten_bytes_equal() {
    let chain = build_overwrite_chain_200();
    let flat = flatten(&*chain).unwrap();
    assert!(bytes_equal(&*chain, &*flat).unwrap());
}

// ── Bonus: base_arc round-trip through 1-level chain ─────────────────────────

#[test]
fn single_level_chain_base_still_matches_after_derive() {
    let original_data: Vec<u8> = (0u8..=255).collect();
    let base = base_arc(&original_data);
    let chain = base.fork();
    // No edits: chain should materialize identically to the base.
    assert!(bytes_equal(&*base, &*chain).unwrap());
    assert_eq!(materialize(&*chain).unwrap(), original_data);
}
