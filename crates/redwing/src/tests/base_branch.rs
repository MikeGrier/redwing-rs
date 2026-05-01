// Copyright (c) 2026 Michael J. Grier
use std::{io::Cursor, sync::Arc};

use super::BaseBranch;

// ── empty base ────────────────────────────────────────────────────────────

#[test]
fn empty_byte_len_is_zero() {
    assert_eq!(BaseBranch::empty().byte_len(), 0);
}

#[test]
fn empty_read_at_zero_returns_zero_bytes() {
    let snap = BaseBranch::empty();
    let mut buf = [0u8; 4];
    let n = snap.read_at(0, &mut buf).unwrap();
    assert_eq!(n, 0);
}

#[test]
fn empty_read_at_nonzero_offset_returns_zero() {
    let snap = BaseBranch::empty();
    let mut buf = [0u8; 4];
    let n = snap.read_at(100, &mut buf).unwrap();
    assert_eq!(n, 0);
}

// ── from_reader ───────────────────────────────────────────────────────────

#[test]
fn from_reader_single_byte() {
    let snap = BaseBranch::from_reader(Cursor::new(vec![0xAB])).unwrap();
    assert_eq!(snap.byte_len(), 1);
    let mut buf = [0u8; 1];
    let n = snap.read_at(0, &mut buf).unwrap();
    assert_eq!(n, 1);
    assert_eq!(buf[0], 0xAB);
}

#[test]
fn from_reader_round_trip() {
    let data: Vec<u8> = (0u8..=255).collect();
    let snap = BaseBranch::from_reader(Cursor::new(data.clone())).unwrap();
    assert_eq!(snap.byte_len(), 256);
    let mut out = vec![0u8; 256];
    let n = snap.read_at(0, &mut out).unwrap();
    assert_eq!(n, 256);
    assert_eq!(out, data);
}

#[test]
fn from_reader_read_at_within_bounds() {
    let data = b"Hello, world!";
    let snap = BaseBranch::from_reader(Cursor::new(data)).unwrap();
    let mut buf = [0u8; 5];
    let n = snap.read_at(7, &mut buf).unwrap();
    assert_eq!(n, 5);
    assert_eq!(&buf[..n], b"world");
}

#[test]
fn from_reader_read_at_exact_end() {
    let data = b"abcd";
    let snap = BaseBranch::from_reader(Cursor::new(data)).unwrap();
    let mut buf = [0u8; 4];
    let n = snap.read_at(0, &mut buf).unwrap();
    assert_eq!(n, 4);
    assert_eq!(&buf, b"abcd");
}

#[test]
fn from_reader_read_at_past_end_returns_zero() {
    let data = b"abcd";
    let snap = BaseBranch::from_reader(Cursor::new(data)).unwrap();
    let mut buf = [0u8; 4];
    let n = snap.read_at(4, &mut buf).unwrap();
    assert_eq!(n, 0);
}

#[test]
fn from_reader_buf_larger_than_remaining() {
    let data = b"abc";
    let snap = BaseBranch::from_reader(Cursor::new(data)).unwrap();
    let mut buf = [0xFFu8; 10];
    let n = snap.read_at(2, &mut buf).unwrap();
    assert_eq!(n, 1);
    assert_eq!(buf[0], b'c');
}

#[test]
fn from_reader_empty_buf_returns_zero() {
    let snap = BaseBranch::from_reader(Cursor::new(b"abc")).unwrap();
    let n = snap.read_at(0, &mut []).unwrap();
    assert_eq!(n, 0);
}

// ── from_mmap ─────────────────────────────────────────────────────────────

#[test]
fn from_mmap_round_trip() {
    use std::io::Write;

    use memmap2::Mmap;

    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    let expected: Vec<u8> = (0u8..=127).collect();
    tmp.write_all(&expected).unwrap();
    tmp.flush().unwrap();

    // SAFETY: we do not mutate the file while the map is alive; this is
    // test-only code running in a single thread with a private temp file.
    let mmap = unsafe { Mmap::map(tmp.as_file()).unwrap() };
    let snap = BaseBranch::from_mmap(mmap);

    assert_eq!(snap.byte_len(), 128);
    let mut buf = vec![0u8; 128];
    let n = snap.read_at(0, &mut buf).unwrap();
    assert_eq!(n, 128);
    assert_eq!(buf, expected);
}

#[test]
fn from_mmap_read_at_middle() {
    use std::io::Write;

    use memmap2::Mmap;

    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    tmp.write_all(b"ABCDEFGH").unwrap();
    tmp.flush().unwrap();

    // SAFETY: same as above — private temp file, no concurrent mutation.
    let mmap = unsafe { Mmap::map(tmp.as_file()).unwrap() };
    let snap = BaseBranch::from_mmap(mmap);

    let mut buf = [0u8; 3];
    let n = snap.read_at(2, &mut buf).unwrap();
    assert_eq!(n, 3);
    assert_eq!(&buf, b"CDE");
}

// ── from_bytes ────────────────────────────────────────────────────────────

#[test]
fn from_bytes_empty_vec_byte_len_zero() {
    let snap = BaseBranch::from_bytes(vec![]);
    assert_eq!(snap.byte_len(), 0);
}

#[test]
fn from_bytes_single_byte_readable() {
    let snap = BaseBranch::from_bytes(vec![0xABu8]);
    assert_eq!(snap.byte_len(), 1);
    let mut buf = [0u8; 1];
    let n = snap.read_at(0, &mut buf).unwrap();
    assert_eq!(n, 1);
    assert_eq!(buf[0], 0xAB);
}

#[test]
fn from_bytes_arc_passthrough_no_copy() {
    // When given an Arc<[u8]> the constructor stores it directly.
    let arc: Arc<[u8]> = Arc::from(vec![1u8, 2, 3]);
    let snap = BaseBranch::from_bytes(Arc::clone(&arc));
    assert_eq!(snap.byte_len(), 3);
    let mut buf = [0u8; 3];
    let n = snap.read_at(0, &mut buf).unwrap();
    assert_eq!(n, 3);
    assert_eq!(&buf, &[1u8, 2, 3]);
}

#[test]
fn from_bytes_full_readback_256_values() {
    let data: Vec<u8> = (0u8..=255).collect();
    let snap = BaseBranch::from_bytes(data.clone());
    assert_eq!(snap.byte_len(), 256);
    let mut out = vec![0u8; 256];
    let n = snap.read_at(0, &mut out).unwrap();
    assert_eq!(n, 256);
    assert_eq!(out, data);
}

#[test]
fn from_bytes_readable_via_as_reader_read_to_end() {
    use std::io::Read;
    let data = b"integration ready";
    let snap = BaseBranch::from_bytes(data.as_slice());
    let mut result = Vec::new();
    snap.as_reader().read_to_end(&mut result).unwrap();
    assert_eq!(result, data);
}

#[test]
fn from_bytes_usable_as_derived_parent() {
    use std::sync::Arc;

    use crate::derived_branch::DerivedBranch;

    let base = Arc::new(BaseBranch::from_bytes(b"ABCDE".as_slice()));
    let ds = DerivedBranch::derive_from_base(base);
    ds.overwrite(1, b"X").unwrap();
    let mut buf = [0u8; 5];
    ds.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"AXCDE");
}

#[test]
fn from_bytes_one_mb_byte_len_correct() {
    let data: Vec<u8> = (0..1_048_576).map(|i| (i as u8) ^ 0x33).collect();
    let snap = BaseBranch::from_bytes(data.clone());
    assert_eq!(snap.byte_len(), 1_048_576);
    // Spot-check first and last bytes.
    let mut buf = [0u8];
    snap.read_at(0, &mut buf).unwrap();
    assert_eq!(buf[0], 0x33);
    snap.read_at(1_048_575, &mut buf).unwrap();
    assert_eq!(buf[0], (255u8) ^ 0x33);
}

#[test]
fn from_bytes_two_arcs_same_data_equal_content() {
    let arc: Arc<[u8]> = Arc::from(b"shared".as_slice());
    let snap_a = BaseBranch::from_bytes(Arc::clone(&arc));
    let snap_b = BaseBranch::from_bytes(Arc::clone(&arc));
    assert_eq!(snap_a.byte_len(), snap_b.byte_len());
    let mut ba = [0u8; 6];
    let mut bb = [0u8; 6];
    snap_a.read_at(0, &mut ba).unwrap();
    snap_b.read_at(0, &mut bb).unwrap();
    assert_eq!(ba, bb);
}

#[test]
fn from_bytes_and_from_reader_same_data_equal_content() {
    let data: Vec<u8> = (0u8..128).collect();
    let snap_bytes = BaseBranch::from_bytes(data.clone());
    let snap_reader = BaseBranch::from_reader(Cursor::new(data.clone())).unwrap();
    assert_eq!(snap_bytes.byte_len(), snap_reader.byte_len());
    let mut ba = vec![0u8; 128];
    let mut br = vec![0u8; 128];
    snap_bytes.read_at(0, &mut ba).unwrap();
    snap_reader.read_at(0, &mut br).unwrap();
    assert_eq!(ba, br);
}

#[test]
fn from_bytes_slice_ref() {
    // &[u8] implements Into<Arc<[u8]>>, so this must compile and work.
    let data: &[u8] = b"hello";
    let snap = BaseBranch::from_bytes(data);
    assert_eq!(snap.byte_len(), 5);
    let mut buf = [0u8; 5];
    snap.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"hello");
}

#[test]
fn from_bytes_read_at_middle() {
    let snap = BaseBranch::from_bytes(b"ABCDEFGH".as_slice());
    let mut buf = [0u8; 3];
    let n = snap.read_at(2, &mut buf).unwrap();
    assert_eq!(n, 3);
    assert_eq!(&buf, b"CDE");
}

#[test]
fn from_bytes_read_at_past_end_returns_zero() {
    let snap = BaseBranch::from_bytes(b"abc".as_slice());
    let mut buf = [0u8; 4];
    let n = snap.read_at(3, &mut buf).unwrap();
    assert_eq!(n, 0);
}
