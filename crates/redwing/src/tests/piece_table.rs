// Copyright (c) 2026 Michael J. Grier
use std::io;

use super::{Piece, PieceTable};
use crate::delta::Delta;

// ── empty delta log ───────────────────────────────────────────────────────

#[test]
fn no_deltas_nonempty_parent_is_one_parent_piece() {
    let pt = PieceTable::build(&[], 5);
    assert_eq!(pt.byte_len, 5);
    assert_eq!(
        pt.pieces,
        vec![Piece::Parent {
            parent_offset: 0,
            len: 5
        }]
    );
    assert!(pt.inline.is_empty());
}

#[test]
fn no_deltas_empty_parent_is_empty() {
    let pt = PieceTable::build(&[], 0);
    assert_eq!(pt.byte_len, 0);
    assert!(pt.pieces.is_empty());
    assert!(pt.inline.is_empty());
}

// ── Overwrite ─────────────────────────────────────────────────────────────

#[test]
fn overwrite_in_middle_produces_three_pieces() {
    // parent: 5 bytes; overwrite bytes [1, 3) with [AA BB]
    let deltas = [Delta::Overwrite {
        offset: 1,
        bytes: vec![0xAA, 0xBB],
    }];
    let pt = PieceTable::build(&deltas, 5);
    assert_eq!(pt.byte_len, 5);
    assert_eq!(
        pt.pieces,
        vec![
            Piece::Parent {
                parent_offset: 0,
                len: 1
            },
            Piece::Inline {
                data_offset: 0,
                len: 2
            },
            Piece::Parent {
                parent_offset: 3,
                len: 2
            },
        ]
    );
    assert_eq!(pt.inline, vec![0xAA, 0xBB]);
}

#[test]
fn overwrite_at_offset_zero() {
    let deltas = [Delta::Overwrite {
        offset: 0,
        bytes: vec![0xFF],
    }];
    let pt = PieceTable::build(&deltas, 4);
    assert_eq!(pt.byte_len, 4);
    assert_eq!(
        pt.pieces,
        vec![
            Piece::Inline {
                data_offset: 0,
                len: 1
            },
            Piece::Parent {
                parent_offset: 1,
                len: 3
            },
        ]
    );
}

#[test]
fn overwrite_at_last_byte() {
    let deltas = [Delta::Overwrite {
        offset: 3,
        bytes: vec![0xEE],
    }];
    let pt = PieceTable::build(&deltas, 4);
    assert_eq!(pt.byte_len, 4);
    assert_eq!(
        pt.pieces,
        vec![
            Piece::Parent {
                parent_offset: 0,
                len: 3
            },
            Piece::Inline {
                data_offset: 0,
                len: 1
            },
        ]
    );
}

#[test]
fn overwrite_entire_parent() {
    let deltas = [Delta::Overwrite {
        offset: 0,
        bytes: vec![1, 2, 3],
    }];
    let pt = PieceTable::build(&deltas, 3);
    assert_eq!(pt.byte_len, 3);
    assert_eq!(
        pt.pieces,
        vec![Piece::Inline {
            data_offset: 0,
            len: 3
        }]
    );
}

#[test]
fn two_sequential_overwrites_same_range_later_wins() {
    // Second overwrite at the same offset should replace the first.
    let deltas = [
        Delta::Overwrite {
            offset: 1,
            bytes: vec![0xAA],
        },
        Delta::Overwrite {
            offset: 1,
            bytes: vec![0xBB],
        },
    ];
    let pt = PieceTable::build(&deltas, 5);
    assert_eq!(pt.byte_len, 5);
    assert_eq!(pt.pieces.len(), 3);
    // Middle piece must reference 0xBB (inline[1]), not 0xAA (inline[0]).
    assert_eq!(
        pt.pieces[1],
        Piece::Inline {
            data_offset: 1,
            len: 1
        }
    );
    assert_eq!(pt.inline, vec![0xAA, 0xBB]);
}

#[test]
fn empty_overwrite_bytes_is_noop() {
    let deltas = [Delta::Overwrite {
        offset: 2,
        bytes: vec![],
    }];
    let pt = PieceTable::build(&deltas, 5);
    assert_eq!(pt.byte_len, 5);
    assert_eq!(
        pt.pieces,
        vec![Piece::Parent {
            parent_offset: 0,
            len: 5
        }]
    );
    assert!(pt.inline.is_empty());
}

// ── Insert ────────────────────────────────────────────────────────────────

#[test]
fn insert_in_middle_produces_three_pieces() {
    let deltas = [Delta::Insert {
        offset: 2,
        bytes: vec![0xCC, 0xDD],
    }];
    let pt = PieceTable::build(&deltas, 4);
    assert_eq!(pt.byte_len, 6);
    assert_eq!(
        pt.pieces,
        vec![
            Piece::Parent {
                parent_offset: 0,
                len: 2
            },
            Piece::Inline {
                data_offset: 0,
                len: 2
            },
            Piece::Parent {
                parent_offset: 2,
                len: 2
            },
        ]
    );
}

#[test]
fn insert_at_offset_zero_prepend() {
    let deltas = [Delta::Insert {
        offset: 0,
        bytes: vec![0x01, 0x02],
    }];
    let pt = PieceTable::build(&deltas, 3);
    assert_eq!(pt.byte_len, 5);
    assert_eq!(
        pt.pieces,
        vec![
            Piece::Inline {
                data_offset: 0,
                len: 2
            },
            Piece::Parent {
                parent_offset: 0,
                len: 3
            },
        ]
    );
}

#[test]
fn insert_at_byte_len_append() {
    let deltas = [Delta::Insert {
        offset: 3,
        bytes: vec![0x09],
    }];
    let pt = PieceTable::build(&deltas, 3);
    assert_eq!(pt.byte_len, 4);
    assert_eq!(
        pt.pieces,
        vec![
            Piece::Parent {
                parent_offset: 0,
                len: 3
            },
            Piece::Inline {
                data_offset: 0,
                len: 1
            },
        ]
    );
}

#[test]
fn insert_into_empty_parent() {
    let deltas = [Delta::Insert {
        offset: 0,
        bytes: vec![0xAB, 0xCD],
    }];
    let pt = PieceTable::build(&deltas, 0);
    assert_eq!(pt.byte_len, 2);
    assert_eq!(
        pt.pieces,
        vec![Piece::Inline {
            data_offset: 0,
            len: 2
        }]
    );
}

#[test]
fn empty_insert_bytes_is_noop() {
    let deltas = [Delta::Insert {
        offset: 1,
        bytes: vec![],
    }];
    let pt = PieceTable::build(&deltas, 3);
    assert_eq!(pt.byte_len, 3);
    assert_eq!(
        pt.pieces,
        vec![Piece::Parent {
            parent_offset: 0,
            len: 3
        }]
    );
}

// ── Delete ────────────────────────────────────────────────────────────────

#[test]
fn delete_in_middle_produces_two_parent_pieces() {
    let deltas = [Delta::Delete { offset: 1, len: 2 }];
    let pt = PieceTable::build(&deltas, 5);
    assert_eq!(pt.byte_len, 3);
    assert_eq!(
        pt.pieces,
        vec![
            Piece::Parent {
                parent_offset: 0,
                len: 1
            },
            Piece::Parent {
                parent_offset: 3,
                len: 2
            },
        ]
    );
    assert!(pt.inline.is_empty());
}

#[test]
fn delete_entire_content() {
    let deltas = [Delta::Delete { offset: 0, len: 4 }];
    let pt = PieceTable::build(&deltas, 4);
    assert_eq!(pt.byte_len, 0);
    assert!(pt.pieces.is_empty());
}

#[test]
fn zero_len_delete_is_noop() {
    let deltas = [Delta::Delete { offset: 1, len: 0 }];
    let pt = PieceTable::build(&deltas, 5);
    assert_eq!(pt.byte_len, 5);
    assert_eq!(
        pt.pieces,
        vec![Piece::Parent {
            parent_offset: 0,
            len: 5
        }]
    );
}

// ── Mixed ─────────────────────────────────────────────────────────────────

#[test]
fn mixed_insert_overwrite_delete() {
    // parent: 5 bytes [a,b,c,d,e]
    // 1. Insert [X=0x58] at offset 2  → [a,b,X,c,d,e]  len=6
    // 2. Overwrite offset 3, [Z=0x5A] → [a,b,X,Z,d,e]  len=6
    // 3. Delete  offset 0, len 2      → [X,Z,d,e]       len=4
    let deltas = [
        Delta::Insert {
            offset: 2,
            bytes: vec![0x58],
        },
        Delta::Overwrite {
            offset: 3,
            bytes: vec![0x5A],
        },
        Delta::Delete { offset: 0, len: 2 },
    ];
    let pt = PieceTable::build(&deltas, 5);
    assert_eq!(pt.byte_len, 4);
    assert_eq!(pt.inline, vec![0x58, 0x5A]);
    // Pieces describe [X, Z, d, e]:
    //   Inline{0,1}=X, Inline{1,1}=Z, Parent{3,2}=[d,e]
    assert_eq!(
        pt.pieces,
        vec![
            Piece::Inline {
                data_offset: 0,
                len: 1
            },
            Piece::Inline {
                data_offset: 1,
                len: 1
            },
            Piece::Parent {
                parent_offset: 3,
                len: 2
            },
        ]
    );
}

// ── read_at ───────────────────────────────────────────────────────────────

/// Helper: build a parent_read closure over a fixed byte slice.
fn make_parent(data: &[u8]) -> impl FnMut(u64, &mut [u8]) -> io::Result<usize> + '_ {
    move |offset: u64, buf: &mut [u8]| {
        if offset as usize >= data.len() {
            return Ok(0);
        }
        let src = &data[offset as usize..];
        let n = buf.len().min(src.len());
        buf[..n].copy_from_slice(&src[..n]);
        Ok(n)
    }
}

#[test]
fn read_at_pure_parent_piece_exactly() {
    // No deltas: one parent piece of 5 bytes.
    let parent = b"ABCDE";
    let pt = PieceTable::build(&[], 5);
    let mut buf = [0u8; 5];
    let n = pt.read_at(0, &mut buf, make_parent(parent)).unwrap();
    assert_eq!(n, 5);
    assert_eq!(&buf, b"ABCDE");
}

#[test]
fn read_at_pure_inline_piece_exactly() {
    // Overwrite entire parent with inline bytes.
    let parent = b"XXXXX";
    let deltas = [Delta::Overwrite {
        offset: 0,
        bytes: vec![1, 2, 3, 4, 5],
    }];
    let pt = PieceTable::build(&deltas, 5);
    let mut buf = [0u8; 5];
    let n = pt.read_at(0, &mut buf, make_parent(parent)).unwrap();
    assert_eq!(n, 5);
    assert_eq!(buf, [1, 2, 3, 4, 5]);
}

#[test]
fn read_at_spanning_two_pieces() {
    // parent [A B C D E]; overwrite [B C] → [A XX D E]
    let parent = b"ABCDE";
    let deltas = [Delta::Overwrite {
        offset: 1,
        bytes: vec![0xBB, 0xCC],
    }];
    let pt = PieceTable::build(&deltas, 5);
    // Read bytes 0..3 spanning Parent{0,1} then Inline{0,2}.
    let mut buf = [0u8; 3];
    let n = pt.read_at(0, &mut buf, make_parent(parent)).unwrap();
    assert_eq!(n, 3);
    assert_eq!(buf, [b'A', 0xBB, 0xCC]);
}

#[test]
fn read_at_spanning_all_pieces() {
    // parent [A B C D E]; overwrite [B C] → [A XX D E]  (3 pieces)
    let parent = b"ABCDE";
    let deltas = [Delta::Overwrite {
        offset: 1,
        bytes: vec![0xBB, 0xCC],
    }];
    let pt = PieceTable::build(&deltas, 5);
    let mut buf = [0u8; 5];
    let n = pt.read_at(0, &mut buf, make_parent(parent)).unwrap();
    assert_eq!(n, 5);
    assert_eq!(buf, [b'A', 0xBB, 0xCC, b'D', b'E']);
}

#[test]
fn read_at_starting_mid_piece() {
    let parent = b"ABCDE";
    let pt = PieceTable::build(&[], 5);
    let mut buf = [0u8; 2];
    let n = pt.read_at(2, &mut buf, make_parent(parent)).unwrap();
    assert_eq!(n, 2);
    assert_eq!(&buf, b"CD");
}

#[test]
fn read_at_offset_equals_byte_len_returns_zero() {
    let parent = b"ABCDE";
    let pt = PieceTable::build(&[], 5);
    let mut buf = [0u8; 4];
    let n = pt.read_at(5, &mut buf, make_parent(parent)).unwrap();
    assert_eq!(n, 0);
}

#[test]
fn read_at_buf_larger_than_remaining() {
    let parent = b"ABCDE";
    let pt = PieceTable::build(&[], 5);
    let mut buf = [0xFFu8; 10];
    let n = pt.read_at(3, &mut buf, make_parent(parent)).unwrap();
    assert_eq!(n, 2);
    assert_eq!(&buf[..2], b"DE");
}

#[test]
fn read_at_empty_table_returns_zero() {
    let pt = PieceTable::build(&[], 0);
    let mut buf = [0u8; 4];
    let n = pt.read_at(0, &mut buf, |_off, _buf| Ok(0)).unwrap();
    assert_eq!(n, 0);
}

#[test]
fn read_at_empty_buf_returns_zero() {
    let parent = b"ABCDE";
    let pt = PieceTable::build(&[], 5);
    let n = pt.read_at(0, &mut [], make_parent(parent)).unwrap();
    assert_eq!(n, 0);
}

#[test]
fn read_at_mid_inline_piece() {
    // Insert [X Y Z] at offset 2 into 5-byte parent: [A B X Y Z C D E]
    let parent = b"ABCDE";
    let deltas = [Delta::Insert {
        offset: 2,
        bytes: vec![0xAA, 0xBB, 0xCC],
    }];
    let pt = PieceTable::build(&deltas, 5);
    // Read starting from offset 3 (inside the inline piece) for 3 bytes: [BB CC C]
    let mut buf = [0u8; 3];
    let n = pt.read_at(3, &mut buf, make_parent(parent)).unwrap();
    assert_eq!(n, 3);
    assert_eq!(buf, [0xBB, 0xCC, b'C']);
}

#[test]
fn read_at_after_delete() {
    // parent [A B C D E]; delete [B C] → [A D E]
    let parent = b"ABCDE";
    let deltas = [Delta::Delete { offset: 1, len: 2 }];
    let pt = PieceTable::build(&deltas, 5);
    assert_eq!(pt.byte_len, 3);
    let mut buf = [0u8; 3];
    let n = pt.read_at(0, &mut buf, make_parent(parent)).unwrap();
    assert_eq!(n, 3);
    assert_eq!(&buf, b"ADE");
}

#[test]
fn read_at_single_byte_steps() {
    // Read each byte of the mixed case one-by-one.
    // parent [A B C D E]; overwrite [B C] → [A XX D E]
    let parent = b"ABCDE";
    let deltas = [Delta::Overwrite {
        offset: 1,
        bytes: vec![0xBB, 0xCC],
    }];
    let pt = PieceTable::build(&deltas, 5);
    let expected = [b'A', 0xBB, 0xCC, b'D', b'E'];
    for (i, &exp) in expected.iter().enumerate() {
        let mut buf = [0u8; 1];
        let n = pt.read_at(i as u64, &mut buf, make_parent(parent)).unwrap();
        assert_eq!(n, 1, "offset {i}");
        assert_eq!(buf[0], exp, "offset {i}");
    }
}
