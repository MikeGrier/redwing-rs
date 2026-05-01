// Copyright (c) 2026 Michael J. Grier

//! M8-3: u64 / usize overflow defenses.
//!
//! Verifies that operations which internally compute `offset + count` (or
//! `offset + buf.len()`) do not panic and do not silently produce a wrapped
//! result when that sum overflows u64.  All such operations must either use
//! `checked_add` and propagate an `InvalidInput` error, or — in the case of
//! `read_at` which has EOF semantics — simply return `Ok(0)` without any
//! attempt to dereference the out-of-range address.
//!
//! Note: `read_at` is intentionally not required to return an error for
//! out-of-range offsets; it returns `Ok(0)` (EOF semantics).  The tests
//! below verify that these paths are panic-free.

use std::{io, sync::Arc};

use redwing::{make_thicket_from_bytes, materialize_range, Branch};

// ── helpers ───────────────────────────────────────────────────────────────────

fn base(data: &[u8]) -> Arc<dyn Branch> {
    make_thicket_from_bytes(data.to_vec()).main()
}

fn derived_from(data: &[u8]) -> Arc<dyn Branch> {
    make_thicket_from_bytes(data.to_vec()).main()
}

fn is_invalid_input(e: &io::Error) -> bool {
    e.kind() == io::ErrorKind::InvalidInput
}

// ── read_at: no panic for near-overflow offsets (EOF semantics) ───────────────

/// `read_at(u64::MAX, &mut [1])` does not panic; returns `Ok(0)` (past-end).
/// If the impl naively computed `offset + buf.len()` it would overflow; the
/// safe path must avoid that arithmetic.
#[test]
fn read_at_u64_max_does_not_panic() {
    let snap = base(b"hello");
    let mut buf = [0u8; 1];
    let result = snap.read_at(u64::MAX, &mut buf).unwrap();
    assert_eq!(result, 0);
}

/// `read_at(u64::MAX - 3, &mut [4_u8; 8])` does not panic; `(u64::MAX - 3) + 8`
/// would overflow u64, but the offset is already past-end so `Ok(0)` is returned.
#[test]
fn read_at_near_u64_max_with_overflowing_sum_does_not_panic() {
    let snap = base(b"hello");
    let mut buf = [0u8; 8];
    let result = snap.read_at(u64::MAX - 3, &mut buf).unwrap();
    assert_eq!(result, 0);
}

/// Derived `read_at` also handles near-overflow offsets without panicking.
#[test]
fn derived_read_at_u64_max_does_not_panic() {
    let d = make_thicket_from_bytes(b"abc".to_vec()).main();
    let mut buf = [0u8; 1];
    let result = d.read_at(u64::MAX, &mut buf).unwrap();
    assert_eq!(result, 0);
}

// ── overwrite: checked_add prevents overflow ──────────────────────────────────

/// `overwrite(u64::MAX - 1, &[1, 2])`: `(u64::MAX - 1) + 2` overflows u64.
/// Must return `InvalidInput`, not panic.
#[test]
fn overwrite_offset_plus_len_overflow_is_invalid_input() {
    let d = derived_from(b"hello");
    let err = d.overwrite(u64::MAX - 1, &[1u8, 2u8]).unwrap_err();
    assert!(is_invalid_input(&err), "got {:?}", err);
}

/// `overwrite(u64::MAX - 0, &[1])`: `u64::MAX + 1` overflows.
#[test]
fn overwrite_u64_max_single_byte_overflow_is_invalid_input() {
    let d = derived_from(b"hello");
    let err = d.overwrite(u64::MAX, &[42u8]).unwrap_err();
    assert!(is_invalid_input(&err), "got {:?}", err);
}

// ── delete: checked_add prevents overflow ─────────────────────────────────────

/// `delete(u64::MAX - 3, 8)`: `(u64::MAX - 3) + 8` overflows u64.
#[test]
fn delete_offset_near_max_count_causes_overflow_is_invalid_input() {
    let d = derived_from(b"hello");
    let err = d.delete(u64::MAX - 3, 8).unwrap_err();
    assert!(is_invalid_input(&err), "got {:?}", err);
}

/// `delete(5, u64::MAX - 3)` on a 10-byte branch: `5 + (u64::MAX - 3)` overflows.
#[test]
fn delete_large_count_overflow_is_invalid_input() {
    let data: Vec<u8> = (0..10).collect();
    let d = derived_from(&data);
    let err = d.delete(5, u64::MAX - 3).unwrap_err();
    assert!(is_invalid_input(&err), "got {:?}", err);
}

/// `delete(u64::MAX, 1)`: offset alone exceeds any realistic byte_len; checked_add still fires.
#[test]
fn delete_u64_max_offset_is_invalid_input() {
    let d = derived_from(b"hello");
    let err = d.delete(u64::MAX, 1).unwrap_err();
    assert!(is_invalid_input(&err), "got {:?}", err);
}

/// `delete(1, u64::MAX)`: `1 + u64::MAX` overflows.
#[test]
fn delete_one_plus_u64_max_overflow_is_invalid_input() {
    let d = derived_from(b"hello");
    let err = d.delete(1, u64::MAX).unwrap_err();
    assert!(is_invalid_input(&err), "got {:?}", err);
}

// ── insert_before: offset guard prevents huge offsets ────────────────────────

/// `insert_before(u64::MAX, &[1])` on a short branch: `u64::MAX > byte_len` → error.
#[test]
fn insert_before_u64_max_is_invalid_input() {
    let d = derived_from(b"hello");
    let err = d.insert_before(u64::MAX, &[1u8]).unwrap_err();
    assert!(is_invalid_input(&err), "got {:?}", err);
}

/// `insert_before(u64::MAX - 1, &[1])` — still far past end → error.
#[test]
fn insert_before_u64_max_minus_one_is_invalid_input() {
    let d = derived_from(b"hello");
    let err = d.insert_before(u64::MAX - 1, &[1u8]).unwrap_err();
    assert!(is_invalid_input(&err), "got {:?}", err);
}

// ── materialize_range: checked_add prevents offset+len overflow ───────────────

/// `materialize_range(u64::MAX - 3, 8)`: `(u64::MAX - 3) + 8` overflows → error.
#[test]
fn materialize_range_near_max_offset_overflow_is_invalid_input() {
    let snap = base(b"hello");
    let err = materialize_range(&*snap, u64::MAX - 3, 8).unwrap_err();
    assert!(is_invalid_input(&err), "got {:?}", err);
}

/// `materialize_range(5, u64::MAX - 3)`: `5 + (u64::MAX - 3)` overflows → error.
#[test]
fn materialize_range_large_len_overflow_is_invalid_input() {
    let snap = base(b"hello");
    let err = materialize_range(&*snap, 5, u64::MAX - 3).unwrap_err();
    assert!(is_invalid_input(&err), "got {:?}", err);
}

// ── read_byte: offset guard for near-max values ───────────────────────────────

/// `read_byte(u64::MAX)` on a 1-byte branch → error (u64::MAX >= byte_len of 1).
#[test]
fn read_byte_u64_max_on_one_byte_snap_is_invalid_input() {
    let snap = base(&[0xAAu8]);
    let err = snap.read_byte(u64::MAX).unwrap_err();
    assert!(is_invalid_input(&err), "got {:?}", err);
}

/// `read_byte(u64::MAX - 1)` on a short branch → error.
#[test]
fn read_byte_u64_max_minus_one_is_invalid_input() {
    let snap = base(b"hello");
    let err = snap.read_byte(u64::MAX - 1).unwrap_err();
    assert!(is_invalid_input(&err), "got {:?}", err);
}

// ── truncate: no-panic for u64::MAX argument ─────────────────────────────────

/// `truncate(u64::MAX - 1)` on a short branch → error (new_len > byte_len).
#[test]
fn truncate_u64_max_minus_one_is_invalid_input() {
    let d = derived_from(b"hello");
    let err = d.truncate(u64::MAX - 1).unwrap_err();
    assert!(is_invalid_input(&err), "got {:?}", err);
}
