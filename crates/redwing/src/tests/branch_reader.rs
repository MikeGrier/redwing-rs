// Copyright (c) 2026 Michael J. Grier
use std::{
    io::{Cursor, Read, Seek, SeekFrom},
    sync::Arc,
};

use super::ByteSource;
use crate::{base_branch::BaseBranch, derived_branch::DerivedBranch};

// ── helpers ───────────────────────────────────────────────────────────────

fn base(data: &[u8]) -> BaseBranch {
    BaseBranch::from_reader(Cursor::new(data.to_vec())).unwrap()
}

fn read_all(src: &impl ByteSource) -> Vec<u8> {
    let mut buf = vec![0u8; src.byte_len() as usize];
    let mut off = 0u64;
    while (off as usize) < buf.len() {
        let n = src.read_at(off, &mut buf[off as usize..]).unwrap();
        if n == 0 {
            break;
        }
        off += n as u64;
    }
    buf
}

// ── ByteSource impl sanity ────────────────────────────────────────────────

#[test]
fn byte_source_byte_len_matches_branch() {
    let snap = base(b"Hello");
    assert_eq!(snap.byte_len(), 5);
}

#[test]
fn byte_source_read_at_full_range() {
    let snap = base(b"ABCDE");
    let mut buf = [0u8; 5];
    assert_eq!(snap.read_at(0, &mut buf).unwrap(), 5);
    assert_eq!(&buf, b"ABCDE");
}

// ── Read trait: base branch ─────────────────────────────────────────────

#[test]
fn read_full_content_base_branch() {
    let snap = base(b"Hello, world!");
    let mut r = snap.as_reader();
    let mut got = Vec::new();
    r.read_to_end(&mut got).unwrap();
    assert_eq!(got, b"Hello, world!");
}

#[test]
fn read_advances_position() {
    let snap = base(b"ABCDE");
    let mut r = snap.as_reader();
    let mut buf = [0u8; 2];
    assert_eq!(r.read(&mut buf).unwrap(), 2);
    assert_eq!(&buf, b"AB");
    assert_eq!(r.read(&mut buf).unwrap(), 2);
    assert_eq!(&buf, b"CD");
    assert_eq!(r.read(&mut buf).unwrap(), 1);
    assert_eq!(&buf[..1], b"E");
}

#[test]
fn read_returns_zero_at_eof() {
    let snap = base(b"AB");
    let mut r = snap.as_reader();
    let mut buf = [0u8; 4];
    let _ = r.read(&mut buf).unwrap();
    assert_eq!(r.read(&mut buf).unwrap(), 0);
}

#[test]
fn read_empty_branch_returns_zero() {
    let snap = BaseBranch::empty();
    let mut r = snap.as_reader();
    let mut buf = [0u8; 4];
    assert_eq!(r.read(&mut buf).unwrap(), 0);
}

#[test]
fn read_buffer_larger_than_content_clips_to_available() {
    let snap = base(b"Hi");
    let mut r = snap.as_reader();
    let mut buf = [0xFFu8; 16];
    let n = r.read(&mut buf).unwrap();
    assert_eq!(n, 2);
    assert_eq!(&buf[..2], b"Hi");
    // bytes beyond n are untouched
    assert!(buf[2..].iter().all(|&b| b == 0xFF));
}

#[test]
fn read_exact_buffer_size_reads_all() {
    let data = b"exact";
    let snap = base(data);
    let mut r = snap.as_reader();
    let mut buf = [0u8; 5];
    assert_eq!(r.read(&mut buf).unwrap(), 5);
    assert_eq!(&buf, b"exact");
}

// ── Read trait: derived branch with edits ───────────────────────────────

#[test]
fn read_derived_branch_with_overwrite() {
    let base_snap = Arc::new(base(b"AAAAA"));
    let ds = DerivedBranch::derive_from_base(base_snap);
    ds.overwrite(1, b"BB").unwrap();
    let mut r = ds.as_reader();
    let mut got = Vec::new();
    r.read_to_end(&mut got).unwrap();
    assert_eq!(got, b"ABBAA");
}

#[test]
fn read_derived_branch_with_insert() {
    let base_snap = Arc::new(base(b"AC"));
    let ds = DerivedBranch::derive_from_base(base_snap);
    ds.insert_before(1, b"B").unwrap();
    let mut r = ds.as_reader();
    let mut buf = vec![0u8; ds.byte_len() as usize];
    let mut off = 0usize;
    while off < buf.len() {
        let n = r.read(&mut buf[off..]).unwrap();
        if n == 0 {
            break;
        }
        off += n;
    }
    assert_eq!(&buf, b"ABC");
}

#[test]
fn read_to_end_matches_read_all_positional() {
    let base_snap = Arc::new(base(b"Hello"));
    let ds = DerivedBranch::derive_from_base(base_snap);
    ds.overwrite(0, b"World").unwrap();

    let expected = read_all(&*ds);

    let mut r = ds.as_reader();
    let mut got = Vec::new();
    r.read_to_end(&mut got).unwrap();
    assert_eq!(got, expected);
}

// ── Seek: basic positions ─────────────────────────────────────────────────

#[test]
fn seek_start_then_read() {
    let snap = base(b"ABCDE");
    let mut r = snap.as_reader();
    assert_eq!(r.seek(SeekFrom::Start(2)).unwrap(), 2);
    let mut buf = [0u8; 2];
    r.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"CD");
}

#[test]
fn seek_end_then_read_returns_zero() {
    let snap = base(b"ABCDE");
    let mut r = snap.as_reader();
    assert_eq!(r.seek(SeekFrom::End(0)).unwrap(), 5);
    let mut buf = [0u8; 4];
    assert_eq!(r.read(&mut buf).unwrap(), 0);
}

#[test]
fn seek_from_end_negative_reads_tail() {
    let snap = base(b"ABCDE");
    let mut r = snap.as_reader();
    assert_eq!(r.seek(SeekFrom::End(-2)).unwrap(), 3);
    let mut buf = [0u8; 2];
    r.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"DE");
}

#[test]
fn seek_current_advances_forward() {
    let snap = base(b"ABCDE");
    let mut r = snap.as_reader();
    r.seek(SeekFrom::Start(1)).unwrap();
    assert_eq!(r.seek(SeekFrom::Current(2)).unwrap(), 3);
    let mut buf = [0u8; 1];
    r.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"D");
}

#[test]
fn seek_current_zero_is_noop() {
    let snap = base(b"ABCDE");
    let mut r = snap.as_reader();
    r.seek(SeekFrom::Start(3)).unwrap();
    let reported = r.stream_position().unwrap();
    assert_eq!(reported, 3);
    let mut buf = [0u8; 1];
    r.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"D");
}

#[test]
fn seek_past_end_position_is_set_read_returns_zero() {
    let snap = base(b"ABCDE");
    let mut r = snap.as_reader();
    let new_pos = r.seek(SeekFrom::Start(100)).unwrap();
    assert_eq!(new_pos, 100);
    let mut buf = [0u8; 4];
    assert_eq!(r.read(&mut buf).unwrap(), 0);
}

#[test]
fn seek_from_end_positive_goes_past_eof() {
    let snap = base(b"ABCDE");
    let mut r = snap.as_reader();
    let new_pos = r.seek(SeekFrom::End(3)).unwrap();
    assert_eq!(new_pos, 8);
    let mut buf = [0u8; 4];
    assert_eq!(r.read(&mut buf).unwrap(), 0);
}

#[test]
fn seek_before_beginning_returns_error() {
    let snap = base(b"ABCDE");
    let mut r = snap.as_reader();
    assert!(r.seek(SeekFrom::Start(2)).is_ok());
    let err = r.seek(SeekFrom::Current(-10)).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn seek_from_end_before_beginning_returns_error() {
    let snap = base(b"ABCDE");
    let mut r = snap.as_reader();
    let err = r.seek(SeekFrom::End(-10)).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn seek_start_of_empty_branch_then_read() {
    let snap = BaseBranch::empty();
    let mut r = snap.as_reader();
    assert_eq!(r.seek(SeekFrom::Start(0)).unwrap(), 0);
    let mut buf = [0u8; 4];
    assert_eq!(r.read(&mut buf).unwrap(), 0);
}

// ── interleaved reads and seeks ───────────────────────────────────────────

#[test]
fn interleaved_read_seek_read() {
    let snap = base(b"ABCDEFGH");
    let mut r = snap.as_reader();
    let mut buf = [0u8; 2];

    r.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"AB"); // pos → 2

    r.seek(SeekFrom::Start(5)).unwrap();
    r.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"FG"); // pos → 7

    r.seek(SeekFrom::Current(-4)).unwrap(); // pos → 3
    r.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"DE");
}

#[test]
fn seek_back_to_start_rereads_from_beginning() {
    let snap = base(b"HELLO");
    let mut r = snap.as_reader();
    let mut buf = [0u8; 5];

    r.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"HELLO");

    r.seek(SeekFrom::Start(0)).unwrap();
    r.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"HELLO");
}

#[test]
fn seek_to_each_byte_and_read_one_byte() {
    let data = b"ABCDE";
    let snap = base(data);
    let mut r = snap.as_reader();
    let mut buf = [0u8; 1];
    for (i, &expected) in data.iter().enumerate() {
        r.seek(SeekFrom::Start(i as u64)).unwrap();
        r.read_exact(&mut buf).unwrap();
        assert_eq!(buf[0], expected, "byte at index {i}");
    }
}

#[test]
fn read_to_end_after_partial_read() {
    let snap = base(b"ABCDE");
    let mut r = snap.as_reader();
    let mut first = [0u8; 2];
    r.read_exact(&mut first).unwrap();
    assert_eq!(&first, b"AB");

    let mut rest = Vec::new();
    r.read_to_end(&mut rest).unwrap();
    assert_eq!(rest, b"CDE");
}

#[test]
fn seek_then_read_to_end_on_derived() {
    let base_snap = Arc::new(base(b"XYZWV"));
    let ds = DerivedBranch::derive_from_base(base_snap);
    ds.overwrite(2, b"AB").unwrap();

    let mut r = ds.as_reader();
    r.seek(SeekFrom::Start(1)).unwrap();
    let mut got = Vec::new();
    r.read_to_end(&mut got).unwrap();
    assert_eq!(got, b"YABV");
}

// ── M7-3: read_byte ───────────────────────────────────────────────────────

#[test]
fn read_byte_first_byte_of_base_branch() {
    let snap = base(b"Hello");
    assert_eq!(snap.read_byte(0).unwrap(), b'H');
}

#[test]
fn read_byte_last_byte_of_base_branch() {
    let snap = base(b"Hello");
    assert_eq!(snap.read_byte(4).unwrap(), b'o');
}

#[test]
fn read_byte_every_position_of_10_byte_branch() {
    let data = b"0123456789";
    let snap = base(data);
    for (i, &expected) in data.iter().enumerate() {
        assert_eq!(snap.read_byte(i as u64).unwrap(), expected, "offset {i}");
    }
}

#[test]
fn read_byte_derived_at_patched_position() {
    let base_snap = Arc::new(base(b"ABCDE"));
    let ds = DerivedBranch::derive_from_base(base_snap);
    ds.overwrite(2, b"Z").unwrap();
    assert_eq!(ds.read_byte(2).unwrap(), b'Z');
}

#[test]
fn read_byte_derived_at_unpatched_position() {
    let base_snap = Arc::new(base(b"ABCDE"));
    let ds = DerivedBranch::derive_from_base(base_snap);
    ds.overwrite(2, b"Z").unwrap();
    assert_eq!(ds.read_byte(0).unwrap(), b'A');
    assert_eq!(ds.read_byte(4).unwrap(), b'E');
}

#[test]
fn read_byte_derived_with_insert_at_read_position() {
    // insert b"XY" before offset 1 -> "AXYBC"
    let base_snap = Arc::new(base(b"ABC"));
    let ds = DerivedBranch::derive_from_base(base_snap);
    ds.insert_before(1, b"XY").unwrap();
    // position 1 is now 'X'
    assert_eq!(ds.read_byte(1).unwrap(), b'X');
    assert_eq!(ds.read_byte(2).unwrap(), b'Y');
    assert_eq!(ds.read_byte(3).unwrap(), b'B');
}

#[test]
fn read_byte_derived_after_delete_shifts_stream() {
    // delete offset 1 len 1 from "ABCDE" -> "ACDE"
    let base_snap = Arc::new(base(b"ABCDE"));
    let ds = DerivedBranch::derive_from_base(base_snap);
    ds.delete(1, 1).unwrap();
    assert_eq!(ds.read_byte(0).unwrap(), b'A');
    assert_eq!(ds.read_byte(1).unwrap(), b'C');
    assert_eq!(ds.read_byte(3).unwrap(), b'E');
}

#[test]
fn read_byte_offset_equals_byte_len_returns_invalid_input() {
    let snap = base(b"ABC");
    let err = snap.read_byte(3).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn read_byte_offset_past_byte_len_returns_invalid_input() {
    let snap = base(b"ABC");
    let err = snap.read_byte(100).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn read_byte_empty_branch_returns_invalid_input() {
    use crate::base_branch::BaseBranch;
    let snap = BaseBranch::empty();
    let err = snap.read_byte(0).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn read_byte_matches_materialize_at_same_position() {
    use crate::materialize::materialize;
    let base_snap = Arc::new(base(b"ABCDE"));
    let ds = DerivedBranch::derive_from_base(base_snap);
    ds.overwrite(1, b"X").unwrap();
    ds.insert_before(3, b"Z").unwrap();
    let mat = materialize(&*ds).unwrap();
    for (i, &expected) in mat.iter().enumerate() {
        assert_eq!(ds.read_byte(i as u64).unwrap(), expected, "offset {i}");
    }
}

#[test]
fn read_byte_u64_max_offset_returns_invalid_input() {
    let snap = base(b"X");
    let err = snap.read_byte(u64::MAX).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}
