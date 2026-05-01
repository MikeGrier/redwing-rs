// Copyright (c) 2026 Michael J. Grier

//! M8-1: Negative / error-path coverage.
//!
//! Verifies that every write operation and bounded-read helper returns
//! `ErrorKind::InvalidInput` when given out-of-range arguments, and that
//! `read_at` uses EOF semantics (returns `Ok(0)`) rather than an error for
//! offsets at or beyond `byte_len()`.  The distinction is intentional: `read_at`
//! follows POSIX positional-read conventions (past EOF ⇒ Ok(0)), while the write
//! operations and `read_byte` are strict about range validity.

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

// ── read_at: EOF semantics (Ok(0), not an error) ─────────────────────────────

/// `read_at` at exactly `byte_len()` uses EOF semantics: returns `Ok(0)`.
/// This mirrors POSIX pread behaviour and is intentional (not a bug).
#[test]
fn read_at_at_exact_end_returns_ok_zero() {
    let snap = base(b"hello");
    let mut buf = [0u8; 1];
    let result = snap.read_at(snap.byte_len(), &mut buf).unwrap();
    assert_eq!(result, 0, "read_at at byte_len() should return Ok(0)");
}

/// `read_at` with `offset = u64::MAX` also returns `Ok(0)` — past-end is past-end.
#[test]
fn read_at_u64_max_returns_ok_zero() {
    let snap = base(b"xyz");
    let mut buf = [0u8; 1];
    let result = snap.read_at(u64::MAX, &mut buf).unwrap();
    assert_eq!(result, 0);
}

// ── overwrite error paths ─────────────────────────────────────────────────────

/// `overwrite` starting at exactly `byte_len()` with a non-empty slice is an error.
#[test]
fn overwrite_at_exact_end_is_invalid_input() {
    let d = derived_from(b"hello");
    let err = d.overwrite(5, b"!").unwrap_err();
    assert!(
        is_invalid_input(&err),
        "expected InvalidInput, got {:?}",
        err
    );
}

/// `overwrite` that starts inside the branch but extends past the end is an error.
#[test]
fn overwrite_extending_past_end_is_invalid_input() {
    let d = derived_from(b"hello");
    // offset 3, 5 bytes → needs bytes 3..8 but snap is only 5 bytes long
    let err = d.overwrite(3, b"XXXXX").unwrap_err();
    assert!(
        is_invalid_input(&err),
        "expected InvalidInput, got {:?}",
        err
    );
}

/// `overwrite` where `offset + bytes.len()` overflows `u64` is an error.
#[test]
fn overwrite_offset_overflow_is_invalid_input() {
    let d = derived_from(b"hello");
    let err = d.overwrite(u64::MAX, b"!").unwrap_err();
    assert!(
        is_invalid_input(&err),
        "expected InvalidInput, got {:?}",
        err
    );
}

// ── insert_before error paths ─────────────────────────────────────────────────

/// `insert_before` with `offset > byte_len()` is an error.
#[test]
fn insert_before_past_end_is_invalid_input() {
    let d = derived_from(b"hello");
    // byte_len is 5; inserting at offset 6 is out of range
    let err = d.insert_before(6, b"X").unwrap_err();
    assert!(
        is_invalid_input(&err),
        "expected InvalidInput, got {:?}",
        err
    );
}

/// `insert_before` at a very large offset (u64::MAX) is an error.
#[test]
fn insert_before_u64_max_offset_is_invalid_input() {
    let d = derived_from(b"hello");
    let err = d.insert_before(u64::MAX, b"X").unwrap_err();
    assert!(
        is_invalid_input(&err),
        "expected InvalidInput, got {:?}",
        err
    );
}

// ── delete error paths ────────────────────────────────────────────────────────

/// `delete(byte_len(), 1)` — offset is exactly at the end, is an error.
#[test]
fn delete_at_exact_end_is_invalid_input() {
    let d = derived_from(b"hello");
    let err = d.delete(5, 1).unwrap_err();
    assert!(
        is_invalid_input(&err),
        "expected InvalidInput, got {:?}",
        err
    );
}

/// `delete(0, byte_len() + 1)` — count exceeds available bytes, is an error.
#[test]
fn delete_count_past_end_is_invalid_input() {
    let d = derived_from(b"hello");
    let err = d.delete(0, 6).unwrap_err();
    assert!(
        is_invalid_input(&err),
        "expected InvalidInput, got {:?}",
        err
    );
}

/// `delete(offset, count)` where `offset + count` overflows `u64` is an error.
#[test]
fn delete_offset_plus_count_overflow_is_invalid_input() {
    let d = derived_from(b"hello");
    let err = d.delete(u64::MAX, 1).unwrap_err();
    assert!(
        is_invalid_input(&err),
        "expected InvalidInput, got {:?}",
        err
    );
}

/// `delete` starting inside the branch but extending past the end is an error.
#[test]
fn delete_spanning_past_end_is_invalid_input() {
    let d = derived_from(b"hello");
    // snap is 5 bytes; delete offset=3, len=4 → needs bytes 3..7
    let err = d.delete(3, 4).unwrap_err();
    assert!(
        is_invalid_input(&err),
        "expected InvalidInput, got {:?}",
        err
    );
}

// ── truncate error paths ──────────────────────────────────────────────────────

/// `truncate(byte_len() + 1)` is an error.
#[test]
fn truncate_past_end_is_invalid_input() {
    let d = derived_from(b"hello");
    let err = d.truncate(6).unwrap_err();
    assert!(
        is_invalid_input(&err),
        "expected InvalidInput, got {:?}",
        err
    );
}

/// `truncate(u64::MAX)` on a short branch is an error.
#[test]
fn truncate_u64_max_is_invalid_input() {
    let d = derived_from(b"hello");
    let err = d.truncate(u64::MAX).unwrap_err();
    assert!(
        is_invalid_input(&err),
        "expected InvalidInput, got {:?}",
        err
    );
}

// ── materialize_range error paths ─────────────────────────────────────────────

/// `materialize_range(snap, byte_len, 1)` — offset is exactly at the end, is an error.
#[test]
fn materialize_range_offset_at_end_is_invalid_input() {
    let snap = base(b"hello");
    let err = materialize_range(&*snap, 5, 1).unwrap_err();
    assert!(
        is_invalid_input(&err),
        "expected InvalidInput, got {:?}",
        err
    );
}

/// `materialize_range(snap, 0, byte_len + 1)` — len exceeds available bytes, is an error.
#[test]
fn materialize_range_len_past_end_is_invalid_input() {
    let snap = base(b"hello");
    let err = materialize_range(&*snap, 0, 6).unwrap_err();
    assert!(
        is_invalid_input(&err),
        "expected InvalidInput, got {:?}",
        err
    );
}

/// `materialize_range` where `offset + len` overflows `u64` is an error.
#[test]
fn materialize_range_offset_plus_len_overflow_is_invalid_input() {
    let snap = base(b"hello");
    let err = materialize_range(&*snap, u64::MAX, 1).unwrap_err();
    assert!(
        is_invalid_input(&err),
        "expected InvalidInput, got {:?}",
        err
    );
}

/// `materialize_range` starting inside the branch but extending past the end is an error.
#[test]
fn materialize_range_spanning_past_end_is_invalid_input() {
    let snap = base(b"hello");
    // offset=3, len=4 → needs bytes 3..7 but snap is 5 bytes
    let err = materialize_range(&*snap, 3, 4).unwrap_err();
    assert!(
        is_invalid_input(&err),
        "expected InvalidInput, got {:?}",
        err
    );
}

// ── read_byte error paths ─────────────────────────────────────────────────────

/// `read_byte(byte_len())` on a non-empty branch is an error.
#[test]
fn read_byte_at_exact_end_is_invalid_input() {
    let snap = base(b"hello");
    let err = snap.read_byte(snap.byte_len()).unwrap_err();
    assert!(
        is_invalid_input(&err),
        "expected InvalidInput, got {:?}",
        err
    );
}

/// `read_byte(u64::MAX)` on a small branch is an error.
#[test]
fn read_byte_u64_max_is_invalid_input() {
    let snap = base(b"hi");
    let err = snap.read_byte(u64::MAX).unwrap_err();
    assert!(
        is_invalid_input(&err),
        "expected InvalidInput, got {:?}",
        err
    );
}

/// `read_byte` on an empty branch at offset 0 is an error.
#[test]
fn read_byte_on_empty_snap_is_invalid_input() {
    let snap = make_thicket_from_bytes(vec![]).main();
    let err = snap.read_byte(0).unwrap_err();
    assert!(
        is_invalid_input(&err),
        "expected InvalidInput, got {:?}",
        err
    );
}

// ── derived branch inherits the same error paths ───────────────────────────

/// `read_byte` past the end of a derived Branch is also an error.
#[test]
fn derived_read_byte_past_end_is_invalid_input() {
    let d = make_thicket_from_bytes(b"abc".to_vec()).main();
    let err = d.read_byte(3).unwrap_err();
    assert!(
        is_invalid_input(&err),
        "expected InvalidInput, got {:?}",
        err
    );
}

/// `materialize_range` on a derived branch behaves the same as on a base Branch.
#[test]
fn derived_materialize_range_past_end_is_invalid_input() {
    let d = make_thicket_from_bytes(b"abc".to_vec()).main();
    let err = materialize_range(&*d, 0, 4).unwrap_err();
    assert!(
        is_invalid_input(&err),
        "expected InvalidInput, got {:?}",
        err
    );
}
