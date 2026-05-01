// Copyright (c) 2026 Michael J. Grier

use std::{
    io::{self, Read, Seek},
    sync::Arc,
};

use memmap2::Mmap;

use crate::{base_branch::BaseBranch, branch::Branch, derived_branch::DerivedBranch};

/// A thicket: an owned initial writable Branch and the root of a fork tree.
///
/// Call [`Thicket::main`] to obtain an `Arc<dyn Branch>` that can be
/// passed to [`Branch::fork`], [`crate::materialize`], etc.
pub struct Thicket(Arc<dyn Branch>);

impl Thicket {
    /// Return the main Branch of this thicket.
    ///
    /// The returned handle shares ownership of the same underlying Branch
    /// state as the `Thicket`, so mutations performed through either handle
    /// may be observed through the other. Pass the result to
    /// [`Branch::fork`] to create additional writable branches.
    pub fn main(&self) -> Arc<dyn Branch> {
        self.0.clone()
    }
}

/// Create the first writable Branch in a thicket from an in-memory byte
/// buffer.
///
/// `data` may be a `Vec<u8>`, `&[u8]`, `Arc<[u8]>`, or `Box<[u8]>`.  When
/// the caller already holds an `Arc<[u8]>`, this is a zero-copy operation.
///
/// Call [`Thicket::main`] on the result to obtain an `Arc<dyn Branch>`.
pub fn make_thicket_from_bytes(data: impl Into<Arc<[u8]>>) -> Thicket {
    let base = BaseBranch::from_bytes(data).into_arc();
    Thicket(DerivedBranch::derive_from_base(base))
}

/// Create the first writable Branch in a thicket by reading all bytes from
/// `r`.
///
/// The reader is rewound to position 0 before reading.  Returns
/// `Err` if any I/O error occurs.
///
/// Call [`Thicket::main`] on the result to obtain an `Arc<dyn Branch>`.
pub fn make_thicket_from_reader(r: impl Read + Seek) -> io::Result<Thicket> {
    let base = BaseBranch::from_reader(r)?.into_arc();
    Ok(Thicket(DerivedBranch::derive_from_base(base)))
}

/// Create the first writable Branch in a thicket backed by a
/// demand-paged memory map.
///
/// The map is held inside the base Branch via `Arc`; no bytes are copied
/// unless a write operation later needs to materialise a region into the
/// delta log.
///
/// Call [`Thicket::main`] on the result to obtain an `Arc<dyn Branch>`.
pub fn make_thicket_from_mmap(mmap: Mmap) -> Thicket {
    let base = BaseBranch::from_mmap(mmap).into_arc();
    Thicket(DerivedBranch::derive_from_base(base))
}
