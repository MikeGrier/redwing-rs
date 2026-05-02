// Copyright (c) 2026 Michael J. Grier

//! M8-2: Zero-length / boundary-zero operations.
//!
//! Verifies that every write operation silently accepts a zero-length argument
//! (no panic, no error, Branch unchanged) and that read helpers return an
//! empty result for a zero-length request.  Also verifies boundary-zero cases
//! such as `read_at` with an empty buffer and `truncate` to the current length.

use std::{io::Read, sync::Arc};

use redwing::{self, bytes_equal, make_thicket_from_bytes, materialize, materialize_range, Branch};

// ── helpers ───────────────────────────────────────────────────────────────────

fn base(data: &[u8]) -> Arc<dyn Branch> {
    make_thicket_from_bytes(data.to_vec()).main()
}

fn derived_from(data: &[u8]) -> Arc<dyn Branch> {
    make_thicket_from_bytes(data.to_vec()).main()
}

// ── read_at with empty buffer ─────────────────────────────────────────────────

/// `read_at(0, &mut [])` on a non-empty Branch returns `Ok(0)` and does not panic.
#[test]
fn read_at_empty_buf_at_start_returns_zero() {
    let snap = base(b"hello");
    let n = snap.read_at(0, &mut []).unwrap();
    assert_eq!(n, 0);
}

/// `read_at(mid, &mut [])` on a non-empty branch returns `Ok(0)`.
#[test]
fn read_at_empty_buf_at_mid_returns_zero() {
    let snap = base(b"hello");
    let n = snap.read_at(2, &mut []).unwrap();
    assert_eq!(n, 0);
}

/// `read_at(byte_len(), &mut [])` — both offset-at-end and empty buffer;
/// returns `Ok(0)` without error.
#[test]
fn read_at_empty_buf_at_end_returns_zero() {
    let snap = base(b"hello");
    let n = snap.read_at(snap.byte_len(), &mut []).unwrap();
    assert_eq!(n, 0);
}

// ── insert_before zero-length is a no-op ─────────────────────────────────────

/// `insert_before(0, &[])` leaves `byte_len` unchanged and content identical.
#[test]
fn insert_before_empty_at_start_is_noop() {
    let d = derived_from(b"hello");
    d.insert_before(0, &[]).unwrap();
    assert_eq!(d.byte_len(), 5);
    assert_eq!(materialize(&*d).unwrap(), b"hello");
}

/// `insert_before(mid, &[])` leaves `byte_len` unchanged.
#[test]
fn insert_before_empty_at_mid_is_noop() {
    let d = derived_from(b"hello");
    d.insert_before(2, &[]).unwrap();
    assert_eq!(d.byte_len(), 5);
    assert_eq!(materialize(&*d).unwrap(), b"hello");
}

/// `insert_before(byte_len(), &[])` — insert at the end with empty slice is also a no-op.
#[test]
fn insert_before_empty_at_end_is_noop() {
    let d = derived_from(b"hello");
    d.insert_before(5, &[]).unwrap();
    assert_eq!(d.byte_len(), 5);
    assert_eq!(materialize(&*d).unwrap(), b"hello");
}

// ── delete zero-length is a no-op ────────────────────────────────────────────

/// `delete(0, 0)` is a no-op.
#[test]
fn delete_zero_len_at_start_is_noop() {
    let d = derived_from(b"hello");
    d.delete(0, 0).unwrap();
    assert_eq!(d.byte_len(), 5);
    assert_eq!(materialize(&*d).unwrap(), b"hello");
}

/// `delete(mid, 0)` is a no-op.
#[test]
fn delete_zero_len_at_mid_is_noop() {
    let d = derived_from(b"hello");
    d.delete(3, 0).unwrap();
    assert_eq!(d.byte_len(), 5);
    assert_eq!(materialize(&*d).unwrap(), b"hello");
}

/// `delete(byte_len(), 0)` — zero-len delete at the end boundary is a no-op.
#[test]
fn delete_zero_len_at_end_is_noop() {
    let d = derived_from(b"hello");
    d.delete(5, 0).unwrap();
    assert_eq!(d.byte_len(), 5);
    assert_eq!(materialize(&*d).unwrap(), b"hello");
}

// ── overwrite zero-length is a no-op ─────────────────────────────────────────

/// `overwrite(0, &[])` is a no-op.
#[test]
fn overwrite_empty_at_start_is_noop() {
    let d = derived_from(b"hello");
    d.overwrite(0, &[]).unwrap();
    assert_eq!(d.byte_len(), 5);
    assert_eq!(materialize(&*d).unwrap(), b"hello");
}

/// `overwrite(mid, &[])` is a no-op.
#[test]
fn overwrite_empty_at_mid_is_noop() {
    let d = derived_from(b"hello");
    d.overwrite(3, &[]).unwrap();
    assert_eq!(d.byte_len(), 5);
    assert_eq!(materialize(&*d).unwrap(), b"hello");
}

// ── append zero-length is a no-op ────────────────────────────────────────────

/// `append(&[])` is a no-op: byte_len unchanged, no log entry added.
#[test]
fn append_empty_is_noop() {
    let d = derived_from(b"hello");
    d.append(&[]).unwrap();
    assert_eq!(d.byte_len(), 5);
    assert_eq!(materialize(&*d).unwrap(), b"hello");
}

/// `append(&[])` on an already-empty derived branch keeps it empty.
#[test]
fn append_empty_to_empty_stays_empty() {
    let d = derived_from(b"");
    d.append(&[]).unwrap();
    assert_eq!(d.byte_len(), 0);
    assert_eq!(materialize(&*d).unwrap(), b"");
}

// ── truncate to current length is a no-op ────────────────────────────────────

/// `truncate(byte_len())` is a no-op: no change to content or length.
#[test]
fn truncate_to_current_len_is_noop() {
    let d = derived_from(b"hello");
    d.truncate(5).unwrap();
    assert_eq!(d.byte_len(), 5);
    assert_eq!(materialize(&*d).unwrap(), b"hello");
}

/// `truncate(0)` on an empty derived branch is a no-op: stays empty.
#[test]
fn truncate_zero_on_empty_is_noop() {
    let d = derived_from(b"");
    d.truncate(0).unwrap();
    assert_eq!(d.byte_len(), 0);
}

// ── materialize_range zero-length returns empty Vec ──────────────────────────

/// `materialize_range(snap, 0, 0)` returns `Ok(vec![])`.
#[test]
fn materialize_range_zero_len_at_start_is_empty() {
    let snap = base(b"hello");
    let result = materialize_range(&*snap, 0, 0).unwrap();
    assert!(result.is_empty());
}

/// `materialize_range(snap, mid, 0)` returns `Ok(vec![])`.
#[test]
fn materialize_range_zero_len_at_mid_is_empty() {
    let snap = base(b"hello");
    let result = materialize_range(&*snap, 3, 0).unwrap();
    assert!(result.is_empty());
}

/// `materialize_range(snap, byte_len, 0)` — offset at end with zero length returns empty.
#[test]
fn materialize_range_zero_len_at_end_is_empty() {
    let snap = base(b"hello");
    let result = materialize_range(&*snap, 5, 0).unwrap();
    assert!(result.is_empty());
}

/// `materialize_range` of zero bytes on an empty branch also returns empty.
#[test]
fn materialize_range_zero_len_on_empty_snap_is_empty() {
    let snap = make_thicket_from_bytes(vec![]).main();
    let result = materialize_range(&*snap, 0, 0).unwrap();
    assert!(result.is_empty());
}

// ── bytes_equal on empty branches ───────────────────────────────────────────

/// `bytes_equal` on two independently-constructed empty branches returns `true`.
#[test]
fn bytes_equal_two_empty_branches_is_true() {
    let a = make_thicket_from_bytes(vec![]).main();
    let b = make_thicket_from_bytes(vec![]).main();
    assert!(bytes_equal(&*a, &*b).unwrap());
}

/// `bytes_equal` on an empty `BaseBranch` and an empty `DerivedBranch` is `true`.
#[test]
fn bytes_equal_empty_base_and_empty_derived_is_true() {
    let a = make_thicket_from_bytes(vec![]).main();
    let d = make_thicket_from_bytes(vec![]).main();
    assert!(bytes_equal(&*a, &*d).unwrap());
}

// ── as_reader over an empty Branch ─────────────────────────────────────────

/// `read_to_end` on a reader over an empty Branch yields zero bytes without error.
#[test]
fn as_reader_on_empty_snap_reads_zero_bytes() {
    let snap = make_thicket_from_bytes(vec![]).main();
    let mut reader = snap.as_reader();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).unwrap();
    assert!(buf.is_empty());
}

// ── zero-length no-ops do not mutate Branch log (content stays identical) ──

/// After several zero-length no-ops, `bytes_equal` to the original base is true.
#[test]
fn zero_length_noop_sequence_leaves_content_unchanged() {
    let original = base(b"abcdefghij");
    let d = original.fork();

    d.insert_before(0, &[]).unwrap();
    d.insert_before(5, &[]).unwrap();
    d.insert_before(10, &[]).unwrap();
    d.overwrite(0, &[]).unwrap();
    d.overwrite(5, &[]).unwrap();
    d.delete(0, 0).unwrap();
    d.delete(5, 0).unwrap();
    d.append(&[]).unwrap();
    d.truncate(10).unwrap();

    assert_eq!(d.byte_len(), 10);
    assert!(bytes_equal(&*original, &*d).unwrap());
}
