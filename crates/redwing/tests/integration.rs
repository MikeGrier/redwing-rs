// Copyright (c) 2026 Michael J. Grier

//! Integration tests for redwing.
//!
//! These tests operate on larger data volumes than the unit tests and exercise
//! the public API end-to-end.

use std::{io::Read, sync::Arc};

use redwing::{self, make_thicket_from_bytes, materialize, Branch};

// ── helpers ───────────────────────────────────────────────────────────────────

/// Build a Branch from a byte slice.
fn thicket(data: &[u8]) -> Arc<dyn Branch> {
    make_thicket_from_bytes(data.to_vec()).main()
}

// ── M6-1: Large-file simulation ───────────────────────────────────────────────

const FILE_SIZE: usize = 10 * 1024 * 1024; // 10 MB
const PATCH_SIZE: usize = 16;
const PATCH_COUNT: usize = 10;

/// Deterministic 10 MB base content: byte at index i = (i as u8) ^ 0xA5.
fn make_base_buffer() -> Vec<u8> {
    (0..FILE_SIZE).map(|i| (i as u8) ^ 0xA5).collect()
}

/// The 10 patch sites: log-even spread across the 10 MB file.
/// Each patch is PATCH_SIZE bytes of 0xFF ^ j for j in 0..PATCH_SIZE.
fn patch_regions() -> Vec<(u64, [u8; PATCH_SIZE])> {
    let step = FILE_SIZE / (PATCH_COUNT + 1);
    (1..=PATCH_COUNT)
        .map(|i| {
            let offset = (step * i) as u64;
            let mut patch = [0u8; PATCH_SIZE];
            for (j, b) in patch.iter_mut().enumerate() {
                *b = 0xFF ^ (j as u8);
            }
            (offset, patch)
        })
        .collect()
}

/// M6-1: Construct a BaseBranch from 10 MB of synthetic data. Derive a
/// Branch, overwrite 10 small (16-byte) regions scattered across the file.
/// Read the full result via `as_reader()`. Verify:
///   1. Total length is unchanged.
///   2. Every edited region contains exactly the patch bytes.
///   3. Every byte outside an edited region retains its original value.
///
/// Design note: the `DerivedBranch` stores only the 10 × 16 = 160-byte
/// patch payloads internally; no copy of the 10 MB base is ever made by the
/// Branch layer. The full content is only reconstructed by the `read_to_end`
/// call below, which is the expected usage pattern.
#[test]
fn m6_1_large_file_scattered_overwrites() {
    let base_buf = make_base_buffer();
    let base = thicket(&base_buf);
    let ds = base.fork();
    let regions = patch_regions();

    for (offset, patch) in &regions {
        ds.overwrite(*offset, patch).unwrap();
    }

    // Read the full result via the Read + Seek adapter.
    let mut result = Vec::new();
    ds.as_reader().read_to_end(&mut result).unwrap();

    // ── assertion 1: length unchanged ────────────────────────────────────────
    assert_eq!(
        result.len(),
        FILE_SIZE,
        "total length changed after overwrites"
    );

    // ── assertion 2: every edited region has new bytes ────────────────────────
    for (offset, patch) in &regions {
        let off = *offset as usize;
        assert_eq!(
            &result[off..off + PATCH_SIZE],
            patch,
            "patch at offset {offset:#010x} was not applied correctly"
        );
    }

    // ── assertion 3: all unedited bytes are unchanged ─────────────────────────
    // Build a boolean mask of patched positions.
    let mut patched = vec![false; FILE_SIZE];
    for (offset, _) in &regions {
        let off = *offset as usize;
        for b in &mut patched[off..off + PATCH_SIZE] {
            *b = true;
        }
    }
    // Scan unpatched positions in 4-KiB pages to keep the loop fast.
    const PAGE: usize = 4096;
    let mut page_buf = [0u8; PAGE];
    for page_start in (0..FILE_SIZE).step_by(PAGE) {
        let page_end = (page_start + PAGE).min(FILE_SIZE);
        let page = &result[page_start..page_end];
        // Quick skip: if no byte in this page is unpatched, move on.
        if patched[page_start..page_end].iter().all(|&p| p) {
            continue;
        }
        // Fill reference page from the deterministic formula.
        for (k, b) in page_buf[..page_end - page_start].iter_mut().enumerate() {
            let idx = page_start + k;
            *b = if patched[idx] {
                // Any value; we won't check patched bytes here.
                0
            } else {
                (idx as u8) ^ 0xA5
            };
        }
        for (k, (&got, &expected)) in page
            .iter()
            .zip(page_buf[..page_end - page_start].iter())
            .enumerate()
        {
            let idx = page_start + k;
            if !patched[idx] {
                assert_eq!(got, expected, "unpatched byte at index {idx} was corrupted");
            }
        }
    }
}

// ── M6-2: Chain depth test ────────────────────────────────────────────────────

const CHAIN_DEPTH: usize = 20;
/// Size of the base buffer: large enough that no two edits share a page.
const CHAIN_BUF_SIZE: usize = CHAIN_DEPTH * 128;
/// Size of each per-level edit payload.
const CHAIN_EDIT_SIZE: usize = 8;

/// M6-2: Construct a chain of 20 `DerivedBranch`s, each layered on top of
/// the previous one, each making one distinct 8-byte overwrite at a unique
/// position. Read the full content from the leaf via `materialize`. Verify that
/// all 20 edits are present at the correct positions and that every unedited
/// byte retains its original value.
///
/// Each level i (0-based) writes `CHAIN_EDIT_SIZE` bytes equal to `(i as u8 +
/// 1) ^ 0x5A` at offset `i * 128`.  The base buffer contains
/// `(idx as u8) ^ 0xFF` at every byte position.
#[test]
fn m6_2_chain_depth_all_edits_visible() {
    // ── build base ───────────────────────────────────────────────────────────
    let base_data: Vec<u8> = (0..CHAIN_BUF_SIZE).map(|i| (i as u8) ^ 0xFF).collect();
    let base = thicket(&base_data);

    // ── build 20-link chain, one overwrite per link ───────────────────────────
    // Each link is an Arc<dyn Branch>; we keep them all alive to prevent
    // any parent from being dropped while the chain is being read.
    let mut chain: Vec<Arc<dyn Branch>> = Vec::with_capacity(CHAIN_DEPTH);

    for i in 0..CHAIN_DEPTH {
        let next = if i == 0 {
            base.fork()
        } else {
            chain[i - 1].fork()
        };
        let edit_offset = (i * 128) as u64;
        let edit_byte = (i as u8 + 1) ^ 0x5A;
        let payload = [edit_byte; CHAIN_EDIT_SIZE];
        next.overwrite(edit_offset, &payload).unwrap();
        chain.push(next);
    }

    // ── read from the leaf ────────────────────────────────────────────────────
    let leaf = chain.last().unwrap();
    let mut result = Vec::new();
    leaf.as_reader().read_to_end(&mut result).unwrap();

    // ── assertion 1: length unchanged ────────────────────────────────────────
    assert_eq!(
        result.len(),
        CHAIN_BUF_SIZE,
        "chain read changed total length"
    );

    // ── assertion 2: every edit is present at the correct position ────────────
    for i in 0..CHAIN_DEPTH {
        let edit_offset = i * 128;
        let expected_byte = (i as u8 + 1) ^ 0x5A;
        let region = &result[edit_offset..edit_offset + CHAIN_EDIT_SIZE];
        assert!(
            region.iter().all(|&b| b == expected_byte),
            "edit {i} at offset {edit_offset} is wrong: {region:?}"
        );
    }

    // ── assertion 3: bytes between edits are unchanged from base ─────────────
    // The gap between edit i and edit i+1 is [i*128 + CHAIN_EDIT_SIZE, (i+1)*128).
    for i in 0..CHAIN_DEPTH {
        let gap_start = i * 128 + CHAIN_EDIT_SIZE;
        let gap_end = if i + 1 < CHAIN_DEPTH {
            (i + 1) * 128
        } else {
            CHAIN_BUF_SIZE
        };
        for (idx, &actual) in result[gap_start..gap_end].iter().enumerate() {
            let idx = gap_start + idx;
            let expected = (idx as u8) ^ 0xFF;
            assert_eq!(
                actual, expected,
                "unedited byte at index {idx} (gap after edit {i}) was corrupted"
            );
        }
    }
}

// ── M6-3: Insert + delete round-trip ─────────────────────────────────────────

/// M6-3: Start from a known buffer. Derive a branch and insert 100 bytes in
/// the middle. Derive again and delete exactly those 100 bytes. Read from the
/// final leaf; the content must exactly equal the original base.
///
/// This test confirms that insert and delete are inverses when applied at the
/// same position and with the same length, across two levels of derivation.
#[test]
fn m6_3_insert_then_delete_round_trip() {
    // ── base ──────────────────────────────────────────────────────────────────
    // 500 bytes: byte at index i is (i as u8).wrapping_mul(3) ^ 0x77.
    const BASE_LEN: usize = 500;
    const INSERT_LEN: usize = 100;
    const INSERT_OFFSET: usize = 200; // midpoint-ish

    let base_data: Vec<u8> = (0..BASE_LEN)
        .map(|i| (i as u8).wrapping_mul(3) ^ 0x77)
        .collect();
    let base = thicket(&base_data);

    // ── level 1: insert 100 bytes at offset 200 ──────────────────────────────
    let insert_payload: Vec<u8> = (0..INSERT_LEN as u8).map(|b| b ^ 0xCC).collect();
    let ds1 = base.fork();
    ds1.insert_before(INSERT_OFFSET as u64, &insert_payload)
        .unwrap();

    // Sanity: length grew by INSERT_LEN.
    assert_eq!(
        ds1.byte_len() as usize,
        BASE_LEN + INSERT_LEN,
        "level 1 length incorrect after insert"
    );

    // ── level 2: delete those same 100 bytes at the same offset ──────────────
    let ds2 = ds1.fork();
    ds2.delete(INSERT_OFFSET as u64, INSERT_LEN as u64).unwrap();

    // Sanity: length is back to original.
    assert_eq!(
        ds2.byte_len() as usize,
        BASE_LEN,
        "level 2 length incorrect after delete"
    );

    // ── read from the leaf and compare to the original base ───────────────────
    let mut result = Vec::new();
    ds2.as_reader().read_to_end(&mut result).unwrap();

    assert_eq!(
        result, base_data,
        "round-trip mismatch: leaf content differs from original base"
    );
}

// ── M6-4: Create-from-empty ───────────────────────────────────────────────────

/// M6-4: Start with `BaseBranch::empty()`, derive one Branch, and build up
/// a byte sequence by inserting known chunks in several steps. Materialize the
/// result and compare it byte-by-byte to the expected sequence assembled
/// independently from the same chunks.
///
/// Insertions (all into offset 0 of the current visible stream, building the
/// sequence by prepend so the merge logic is exercised with split-offsets):
///
///   Step 1: insert b"Hello"   at offset 0  → "Hello"
///   Step 2: insert b", "      at offset 5  → "Hello, "
///   Step 3: insert b"world"   at offset 7  → "Hello, world"
///   Step 4: insert b"!"       at offset 12 → "Hello, world!"
///   Step 5: insert b" Rust."  at offset 13 → "Hello, world! Rust."
#[test]
fn m6_4_create_from_empty_incremental_inserts() {
    let base = make_thicket_from_bytes(vec![]).main();
    let ds = base.fork();

    ds.insert_before(0, b"Hello").unwrap();
    ds.insert_before(5, b", ").unwrap();
    ds.insert_before(7, b"world").unwrap();
    ds.insert_before(12, b"!").unwrap();
    ds.insert_before(13, b" Rust.").unwrap();

    let expected: &[u8] = b"Hello, world! Rust.";

    assert_eq!(ds.byte_len() as usize, expected.len(), "byte_len mismatch");

    let result = materialize(&*ds).unwrap();
    assert_eq!(result, expected, "materialized content mismatch");
}

// ── M6-5: Discard by drop ─────────────────────────────────────────────────────

/// M6-5: Derive a branch from a base, write to it, then drop the derived
/// Branch. Verify that the base Branch is still fully readable and that its
/// content is unchanged.
///
/// This confirms that `DerivedBranch` holds only a shared reference (via
/// `Arc`) to the base and does not mutate or invalidate it on drop.
#[test]
fn m6_5_drop_derived_base_unchanged() {
    let base_data: Vec<u8> = (0u8..=127).collect();
    let base = thicket(&base_data);

    // Derive, write, then let the derived Branch go out of scope.
    {
        let ds = base.fork();
        ds.overwrite(10, b"OVERWRITTEN").unwrap();
        // ds is dropped here.
    }

    // The base Arc still has one owner; it must be fully readable and unchanged.
    assert_eq!(
        base.byte_len() as usize,
        base_data.len(),
        "base byte_len changed after derived drop"
    );
    let result = materialize(&*base).unwrap();
    assert_eq!(
        result, base_data,
        "base content changed after derived branch was dropped"
    );
}
