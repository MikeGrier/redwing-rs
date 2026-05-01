// Copyright (c) 2026 Michael J. Grier

//! # redwing
//!
//! A copy-on-write, branching byte-stream library backed by a piece table and
//! a per-branch delta log.  See the [`Branch`] trait for the unified read/write
//! interface and [`Thicket`] for the entry points that produce the first
//! `Branch` in a fork tree.
//!
//! ## Threading model
//!
//! **`redwing` is single-threaded.**  All `Branch` methods take `&self` and
//! mutate state through `RefCell`-backed interior mutability.  The concrete
//! branch types are intentionally not `Send` or `Sync`; sharing an
//! `Arc<dyn Branch>` between threads will fail to compile.  If you need to
//! cross a thread boundary, call [`materialize`] on the source thread to
//! produce a `Vec<u8>`, send those bytes to the destination thread, and then
//! build a fresh branch tree there with [`make_thicket_from_bytes`].
//!
//! Implementors of `Branch` outside this crate must respect the same
//! contract: assume `&self` reads and writes happen serially within a single
//! thread.
//!
//! ## Error handling
//!
//! Fallible operations return `std::io::Result`.  Domain-specific failures
//! carry a [`BranchError`] as the inner error of the returned `io::Error`;
//! use [`branch_error`] to recover the structured cause.

mod base_branch;
pub mod branch;
pub(crate) mod branch_reader;
mod delta;
pub(crate) mod derived_branch;
pub mod error;
pub mod materialize;
pub mod offset_map;
pub(crate) mod piece_table;
pub(crate) mod thicket;

pub use branch::{Branch, ReadSeek};
pub use error::{branch_error, BranchError};
pub use materialize::{bytes_equal, flatten, materialize, materialize_range};
pub use thicket::{
    make_thicket_from_bytes, make_thicket_from_mmap, make_thicket_from_reader, Thicket,
};
