// Copyright (c) 2026 Michael J. Grier

mod base_branch;
pub mod branch;
pub(crate) mod branch_reader;
mod delta;
pub(crate) mod derived_branch;
pub mod materialize;
pub mod offset_map;
pub(crate) mod piece_table;
pub(crate) mod thicket;

pub use branch::{Branch, ReadSeek};
pub use materialize::{bytes_equal, flatten, materialize, materialize_range};
pub use thicket::{
    make_thicket_from_bytes, make_thicket_from_mmap, make_thicket_from_reader, Thicket,
};
