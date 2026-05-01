// Copyright (c) 2026 Michael J. Grier
use super::Delta;

// ── construction round-trips through Debug ────────────────────────────────

#[test]
fn overwrite_debug_shows_hex_offset_and_bytes() {
    let d = Delta::Overwrite {
        offset: 0x10,
        bytes: vec![0xDE, 0xAD],
    };
    let s = format!("{:?}", d);
    assert!(s.contains("0x10"), "offset should appear as hex: {s}");
    assert!(s.contains("0xde"), "bytes should appear as hex: {s}");
    assert!(s.contains("0xad"), "bytes should appear as hex: {s}");
    assert!(
        !s.to_lowercase().contains("overwrite { offset: 16"),
        "no decimal offsets: {s}"
    );
}

#[test]
fn insert_debug_shows_hex_offset_and_bytes() {
    let d = Delta::Insert {
        offset: 0x400,
        bytes: vec![0x00, 0xFF],
    };
    let s = format!("{:?}", d);
    assert!(s.contains("0x400"), "{s}");
    assert!(s.contains("0x00"), "{s}");
    assert!(s.contains("0xff"), "{s}");
}

#[test]
fn delete_debug_shows_hex_offset_and_len() {
    let d = Delta::Delete {
        offset: 0x1000,
        len: 0x80,
    };
    let s = format!("{:?}", d);
    assert!(s.contains("0x1000"), "{s}");
    assert!(s.contains("0x80"), "{s}");
}

#[test]
fn overwrite_empty_bytes_debug() {
    let d = Delta::Overwrite {
        offset: 0,
        bytes: vec![],
    };
    let s = format!("{:?}", d);
    assert!(s.contains("0x0"), "{s}");
}

#[test]
fn insert_empty_bytes_debug() {
    let d = Delta::Insert {
        offset: 0,
        bytes: vec![],
    };
    let s = format!("{:?}", d);
    assert!(s.contains("Insert"), "{s}");
}

#[test]
fn delete_zero_len_debug() {
    let d = Delta::Delete { offset: 0, len: 0 };
    let s = format!("{:?}", d);
    assert!(s.contains("Delete"), "{s}");
}

// ── clone and equality ────────────────────────────────────────────────────

#[test]
fn overwrite_clone_eq() {
    let d = Delta::Overwrite {
        offset: 1,
        bytes: vec![0x01, 0x02],
    };
    assert_eq!(d, d.clone());
}

#[test]
fn insert_clone_eq() {
    let d = Delta::Insert {
        offset: 42,
        bytes: vec![0xAB],
    };
    assert_eq!(d, d.clone());
}

#[test]
fn delete_clone_eq() {
    let d = Delta::Delete { offset: 99, len: 4 };
    assert_eq!(d, d.clone());
}

#[test]
fn distinct_variants_are_not_equal() {
    let a = Delta::Overwrite {
        offset: 0,
        bytes: vec![0x01],
    };
    let b = Delta::Insert {
        offset: 0,
        bytes: vec![0x01],
    };
    assert_ne!(a, b);
}

#[test]
fn overwrite_with_different_offsets_are_not_equal() {
    let a = Delta::Overwrite {
        offset: 0,
        bytes: vec![0x01],
    };
    let b = Delta::Overwrite {
        offset: 1,
        bytes: vec![0x01],
    };
    assert_ne!(a, b);
}

#[test]
fn single_byte_values_round_trip_in_debug() {
    for b in [0x00u8, 0x01, 0x7F, 0x80, 0xFF] {
        let d = Delta::Overwrite {
            offset: 0,
            bytes: vec![b],
        };
        let s = format!("{:?}", d);
        assert!(
            s.contains(&format!("{:#04x}", b)),
            "byte {b:#04x} not found in: {s}"
        );
    }
}
