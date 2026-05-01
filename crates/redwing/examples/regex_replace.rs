// Copyright (c) Microsoft Corporation. All rights reserved.

//! regex_replace — in-place regex substitution on arbitrary byte files using
//! the redwing thicket/branch API.
//!
//! Usage:
//!     regex_replace <file> <pattern> <replacement>
//!
//! `<pattern>` is a [`regex::bytes`] regular expression.  `<replacement>` is
//! a replacement string that may reference capture groups via `$0`, `$1`,
//! `$name`, etc.  Use `$$` to produce a literal `$`.
//!
//! The program:
//!   1. Opens `<file>` and memory-maps it.
//!   2. Creates a thicket from the map; `b1` is the initial read-only branch.
//!   3. Forks `b2` as a mutable branch on top of `b1`.
//!   4. Iterates every non-overlapping regex match in `b1` in forward order,
//!      applying each replacement to `b2` immediately (no buffering).  Before
//!      each edit, [`Branch::map_range_to_fork`] translates the `b1` match range to
//!      its current position in `b2`, accounting for length changes introduced
//!      by all prior replacements.
//!   5. Writes the result to a `NamedTempFile` in the same directory (so the
//!      final rename is guaranteed to be same-filesystem).
//!   6. Renames `<file>` to `<file>.bak`, then calls `persist()` to atomically
//!      replace `<file>` with the temp file.
//!   7. Prints a one-line summary: original path, backup path, replacement count.

use std::{env, fs, io::Write as _, path::PathBuf};

use memmap2::MmapOptions;
use redwing::{make_thicket_from_mmap, materialize};
use regex::bytes::Regex;
use tempfile::NamedTempFile;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── argument parsing ──────────────────────────────────────────────────────

    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        eprintln!("Usage: {} <file> <pattern> <replacement>", args[0]);
        std::process::exit(1);
    }
    let file_path = PathBuf::from(&args[1]);
    let pattern = &args[2];
    let replacement = args[3].as_bytes();

    // ── open and memory-map the file ──────────────────────────────────────────

    let file = fs::File::open(&file_path)?;
    // SAFETY: the mapped bytes are only ever read through the Branch read API.
    // All mutations are applied through the Branch write API which maintains
    // its own delta storage; the underlying mapping is never written.
    let mmap = unsafe { MmapOptions::new().map(&file) }?;
    drop(file);

    // ── build the thicket ─────────────────────────────────────────────────────

    let b1 = make_thicket_from_mmap(mmap).main();
    let b2 = b1.fork();

    // ── scan b1 for matches, apply replacements to b2 ────────────────────────

    let re = Regex::new(pattern)?;
    let b1_bytes = materialize(&*b1)?;

    // Iterate matches in b1's coordinate space and apply each replacement to b2
    // immediately.  map_range_to_fork translates each b1 match range to the
    // current b2 position before the edit, so length changes from prior
    // replacements are accounted for without buffering or reversing the list.
    let mut replacement_count: usize = 0;
    for caps in re.captures_iter(&b1_bytes) {
        let m = caps.get(0).unwrap();
        let mut repl = Vec::new();
        caps.expand(replacement, &mut repl);
        let b2_range = b2
            .map_range_to_fork(m.start() as u64..m.end() as u64)
            .expect("match range should survive");
        b2.splice(b2_range.start, b2_range.end - b2_range.start, &repl)?;
        replacement_count += 1;
    }

    let out_bytes = materialize(&*b2)?;

    // Drop the branches before any file-system work: on Windows a mapped file
    // cannot be renamed while a view of it remains open.
    drop(b2);
    drop(b1);

    // ── write temp file, then swap ────────────────────────────────────────────

    let dir = file_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));

    // NamedTempFile::new_in creates a randomly-named file in `dir` (same
    // filesystem as `file_path`) and deletes it automatically if we return
    // early without persisting.
    let mut tmp = NamedTempFile::new_in(dir)?;
    tmp.write_all(&out_bytes)?;

    let bak_path = PathBuf::from(format!("{}.bak", file_path.display()));
    fs::rename(&file_path, &bak_path)?;

    // persist() atomically renames the temp file to `file_path`.  If it fails,
    // restore the backup so the original content is not lost.
    if let Err(e) = tmp.persist(&file_path) {
        let _ = fs::rename(&bak_path, &file_path);
        return Err(e.error.into());
    }

    // ── one-line summary ──────────────────────────────────────────────────────

    println!(
        "{} -> {} ({} replacement{})",
        file_path.display(),
        bak_path.display(),
        replacement_count,
        if replacement_count == 1 { "" } else { "s" },
    );

    Ok(())
}
