// Copyright (c) 2026 Michael J. Grier

use std::io;

use crate::delta::Delta;

/// A span in the piece table: either a contiguous range from the parent byte
/// stream or a contiguous range from the accumulated inline byte buffer.
///
/// Changing the variant layout or field types is a breaking change to any
/// persisted or serialised representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Piece {
    /// Bytes read directly from the parent Branch at `parent_offset`.
    Parent { parent_offset: u64, len: u64 },
    /// Bytes read from the inline buffer at `data_offset`.
    Inline { data_offset: usize, len: usize },
}

impl Piece {
    pub(crate) fn len(&self) -> u64 {
        match self {
            Piece::Parent { len, .. } => *len,
            Piece::Inline { len, .. } => *len as u64,
        }
    }

    /// Split `self` into two pieces at `within` bytes from the start.
    /// `self` becomes the left (prefix) half; the returned value is the right
    /// (suffix) half.
    fn split_at(&mut self, within: u64) -> Piece {
        debug_assert!(within > 0);
        match self {
            Piece::Parent { parent_offset, len } => {
                debug_assert!(within < *len);
                let right = Piece::Parent {
                    parent_offset: *parent_offset + within,
                    len: *len - within,
                };
                *len = within;
                right
            }
            Piece::Inline { data_offset, len } => {
                let within = within as usize;
                debug_assert!(within < *len);
                let right = Piece::Inline {
                    data_offset: *data_offset + within,
                    len: *len - within,
                };
                *len = within;
                right
            }
        }
    }
}

/// An ordered list of byte-range spans describing the current content of a
/// Branch, built by replaying a `&[Delta]` log over a parent byte stream.
///
/// # Inline buffer growth
///
/// The `inline` buffer accumulates all bytes from `Insert` and `Overwrite`
/// payloads in arrival order.  `Piece::Inline` entries index into it via
/// `data_offset`.  The buffer is **append-only**: once bytes are appended
/// they are never reordered or freed, even when a later delta supersedes
/// them (for example, when the same region is overwritten repeatedly the
/// older payloads stay in `inline` even though no `Piece::Inline` entry
/// references them).
///
/// In practice this is bounded by the [`crate::derived_branch::DerivedBranch`]
/// merge logic, which collapses overlapping `Overwrite` and consecutive
/// `Insert` deltas in the log itself before they ever reach a piece table
/// rebuild.  For workloads that defeat those merges (many small,
/// non-adjacent writes), call [`crate::flatten`] to collapse a branch into
/// a fresh `BaseBranch` and reclaim the unreferenced inline bytes.
pub(crate) struct PieceTable {
    /// Ordered spans that together describe the full byte content.
    pub(crate) pieces: Vec<Piece>,
    /// Inline byte payload accumulator. Append-only; never reordered.
    pub(crate) inline: Vec<u8>,
    /// Total logical byte length described by `pieces`.
    pub(crate) byte_len: u64,
}

impl PieceTable {
    /// Build a `PieceTable` by replaying `deltas` over a parent whose total
    /// size is `parent_byte_len` bytes.
    ///
    /// Each delta is expressed in the coordinate space of the content after all
    /// preceding deltas in this log have been applied.
    ///
    /// # Panics (debug builds only)
    ///
    /// Panics if any delta references an offset or range that is out of bounds
    /// for the current logical length. Well-formed delta logs (as produced by
    /// `DerivedBranch` write operations) never trigger this.
    pub(crate) fn build(deltas: &[Delta], parent_byte_len: u64) -> Self {
        let mut pieces: Vec<Piece> = if parent_byte_len > 0 {
            vec![Piece::Parent {
                parent_offset: 0,
                len: parent_byte_len,
            }]
        } else {
            vec![]
        };
        let mut inline: Vec<u8> = Vec::new();
        let mut byte_len = parent_byte_len;

        for delta in deltas {
            match delta {
                Delta::Overwrite { offset, bytes } => {
                    if bytes.is_empty() {
                        continue;
                    }
                    let end = offset + bytes.len() as u64;
                    debug_assert!(end <= byte_len, "Overwrite out of bounds");

                    // Split at `end` first so the split at `offset` doesn't
                    // invalidate the `end` boundary.
                    split_before(&mut pieces, end);
                    let start_idx = split_before(&mut pieces, *offset);

                    // Consume exactly `bytes.len()` worth of pieces.
                    let mut remaining = bytes.len() as u64;
                    let mut end_idx = start_idx;
                    while remaining > 0 {
                        remaining -= pieces[end_idx].len();
                        end_idx += 1;
                    }

                    let data_offset = inline.len();
                    inline.extend_from_slice(bytes);
                    let new_piece = Piece::Inline {
                        data_offset,
                        len: bytes.len(),
                    };
                    pieces.splice(start_idx..end_idx, std::iter::once(new_piece));
                    // byte_len is unchanged by an overwrite.
                }

                Delta::Insert { offset, bytes } => {
                    if bytes.is_empty() {
                        continue;
                    }
                    debug_assert!(*offset <= byte_len, "Insert out of bounds");

                    let insert_idx = split_before(&mut pieces, *offset);
                    let data_offset = inline.len();
                    inline.extend_from_slice(bytes);
                    pieces.insert(
                        insert_idx,
                        Piece::Inline {
                            data_offset,
                            len: bytes.len(),
                        },
                    );
                    byte_len += bytes.len() as u64;
                }

                Delta::Delete { offset, len } => {
                    if *len == 0 {
                        continue;
                    }
                    let end = offset + len;
                    debug_assert!(end <= byte_len, "Delete out of bounds");

                    split_before(&mut pieces, end);
                    let start_idx = split_before(&mut pieces, *offset);

                    let mut remaining = *len;
                    let mut end_idx = start_idx;
                    while remaining > 0 {
                        remaining -= pieces[end_idx].len();
                        end_idx += 1;
                    }
                    pieces.drain(start_idx..end_idx);
                    byte_len -= len;
                }
            }
        }

        PieceTable {
            pieces,
            inline,
            byte_len,
        }
    }

    /// Read up to `buf.len()` bytes starting at `offset` from the logical byte
    /// stream described by this piece table.
    ///
    /// `parent_read(parent_offset, buf)` is called for `Parent` pieces; it must
    /// have the same semantics as `BaseBranch::read_at` — returning the number
    /// of bytes placed into `buf`, which may be less than `buf.len()` only when
    /// the parent byte stream is exhausted.
    ///
    /// Returns `Ok(0)` when `offset >= self.byte_len` or `buf` is empty.
    pub(crate) fn read_at(
        &self,
        offset: u64,
        buf: &mut [u8],
        mut parent_read: impl FnMut(u64, &mut [u8]) -> io::Result<usize>,
    ) -> io::Result<usize> {
        if offset >= self.byte_len || buf.is_empty() {
            return Ok(0);
        }

        let mut pos = 0u64; // logical start of the current piece
        let mut filled = 0usize; // bytes written into buf so far

        for piece in &self.pieces {
            let piece_len = piece.len();
            let piece_end = pos + piece_len;

            if piece_end <= offset {
                pos = piece_end;
                continue;
            }

            // The read starts somewhere inside this piece.
            let local_offset = offset + filled as u64 - pos;
            let piece_remaining = piece_len - local_offset;
            let want = (buf.len() - filled) as u64;
            let take = want.min(piece_remaining) as usize;

            match piece {
                Piece::Inline { data_offset, .. } => {
                    let src_start = data_offset + local_offset as usize;
                    buf[filled..filled + take]
                        .copy_from_slice(&self.inline[src_start..src_start + take]);
                }
                Piece::Parent { parent_offset, .. } => {
                    let read_start = parent_offset + local_offset;
                    let mut done = 0usize;
                    while done < take {
                        let n = parent_read(
                            read_start + done as u64,
                            &mut buf[filled + done..filled + take],
                        )?;
                        if n == 0 {
                            break;
                        }
                        done += n;
                    }
                }
            }

            filled += take;
            pos = piece_end;

            if filled == buf.len() {
                break;
            }
        }

        Ok(filled)
    }
}

/// Ensure `logical_offset` falls exactly at a piece boundary.
///
/// Returns the index of the piece starting at `logical_offset`. If
/// `logical_offset` equals the current total length, returns `pieces.len()`.
/// If the offset falls in the interior of a piece, splits that piece and
/// returns the index of its right half.
fn split_before(pieces: &mut Vec<Piece>, logical_offset: u64) -> usize {
    let mut pos = 0u64;
    for i in 0..pieces.len() {
        if pos == logical_offset {
            return i;
        }
        let next_pos = pos + pieces[i].len();
        if logical_offset < next_pos {
            let within = logical_offset - pos;
            let right = pieces[i].split_at(within);
            pieces.insert(i + 1, right);
            return i + 1;
        }
        pos = next_pos;
    }
    pieces.len() // logical_offset == total_len (or pieces is empty and offset == 0)
}

#[cfg(test)]
#[path = "tests/piece_table.rs"]
mod tests;
