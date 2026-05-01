// Copyright (c) 2026 Michael J. Grier

/// A single atomic mutation expressed in the coordinate space of a Branch's parent
/// after all preceding deltas in the same log have been applied.
///
/// # Breaking changes
/// Changing the discriminant or field layout of any variant is a breaking change to any
/// persisted or wire-format use of this type.
#[derive(Clone, PartialEq, Eq)]
pub(crate) enum Delta {
    /// Replace the bytes at `[offset, offset + bytes.len())` with `bytes`.
    /// The stream length is unchanged.
    Overwrite { offset: u64, bytes: Vec<u8> },

    /// Insert `bytes` before the byte currently at `offset`.
    /// An insert at `offset == byte_len()` is an append.
    /// The stream grows by `bytes.len()`.
    Insert { offset: u64, bytes: Vec<u8> },

    /// Remove `len` bytes starting at `offset`.
    /// The stream shrinks by `len`.
    Delete { offset: u64, len: u64 },
}

impl std::fmt::Debug for Delta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Delta::Overwrite { offset, bytes } => {
                write!(f, "Overwrite {{ offset: {:#x}, bytes: [", offset)?;
                fmt_bytes(f, bytes)?;
                write!(f, "] }}")
            }
            Delta::Insert { offset, bytes } => {
                write!(f, "Insert {{ offset: {:#x}, bytes: [", offset)?;
                fmt_bytes(f, bytes)?;
                write!(f, "] }}")
            }
            Delta::Delete { offset, len } => {
                write!(f, "Delete {{ offset: {:#x}, len: {:#x} }}", offset, len)
            }
        }
    }
}

fn fmt_bytes(f: &mut std::fmt::Formatter<'_>, bytes: &[u8]) -> std::fmt::Result {
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 {
            write!(f, " ")?;
        }
        write!(f, "{:#04x}", b)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/delta.rs"]
mod tests;
