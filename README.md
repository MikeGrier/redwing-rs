# redwing

A Rust crate for structured, lossless reading and non-destructive editing of arbitrary binary files.

`redwing` is conceptually analogous to the [`rowan`](https://github.com/rust-analyzer/rowan) crate,
which provides a green/red tree representation for source text that supports lossless round-tripping
and incremental, non-committing edits. `redwing` brings the same philosophy to binary file formats.

## Motivation

Binary file formats (PE executables, archives, object files, databases, firmware images, and so on)
share a problem that source text has long had: tools that want to inspect or modify them must
choose between reading the file immutably, or rewriting it destructively. There is no standard
middle ground that allows a consumer to:

1. Read the file fully and losslessly into a structured in-memory model.
2. Propose a set of changes — edits, insertions, deletions, relocations — against that model.
3. Inspect or validate the merged result before committing anything.
4. Either apply the changes back to durable storage, or discard them entirely and leave the
   original untouched.

`redwing` fills that gap.

## Core Concepts

### Lossless Representation

The in-memory model preserves every byte of the original file in its original position. Nothing is
discarded, normalised, or inferred. A round-trip (read → write with no changes) produces a
bit-identical result.

### Change Planning

Edits are expressed as a *change set*: a description of intended mutations (byte range replacements,
insertions, deletions) attached to the model. The change set is separate from the model itself.
The original bytes are never mutated in place.

### Merging

A change set can be *merged* against the current model to produce a new, updated view. The merge
can be inspected, diffed or validated before any I/O occurs. Conflicting changes (overlapping
byte ranges edited independently) are detected at merge time rather than at commit time.

### Committing

Only when the caller is satisfied with a merged result is the data written back to storage.
Commit is an explicit, final step. Until that step, all work is purely in-memory and reversible.

## Relationship to `rowan`

| Concept in `rowan`         | Equivalent in `redwing`                            |
|----------------------------|----------------------------------------------------|
| Source text (UTF-8 string) | Raw byte buffer                                    |
| Green tree (shared, cheap) | Parsed binary view (structural, immutable overlay) |
| Red tree (cursor, owned)   | Mutable working copy with change set attached      |
| Syntax node / token        | Binary region / field (format-specific)            |
| Incremental reparse        | Change set merge (structural re-evaluation)        |
| Edition / mutation API     | Change set builder                                 |

Unlike `rowan`, `redwing` is not tied to any particular grammar formalism. Binary format structure
is described through a trait-based API that format-specific crates implement. `redwing` itself
provides only the generic machinery for byte-level change tracking, merging, and I/O.

## Design Goals

- **Format-agnostic.** The core crate knows nothing about PE, ELF, ZIP, or any other format.
  Format adapters live in separate crates that depend on `redwing`.
- **Lossless by default.** Every read is a lossless read. Lossy projections are opt-in.
- **No silent mutation.** Writes never happen without an explicit commit call.
- **Conflict-safe merging.** Overlapping edits are an error, not undefined behaviour.
- **Suitable for streaming / large files.** The design should not require the entire file to be
  resident in memory at once; memory-mapped and streaming backends are intended to be supported.

## Name

The redwing (*Turdus iliacus*) is a thrush that lives and feeds in rowan and bramble. The name is a
nod to [`rowan`](https://github.com/rust-analyzer/rowan), the crate that inspired this one.

## Status

Early design phase. No public API is stable.
