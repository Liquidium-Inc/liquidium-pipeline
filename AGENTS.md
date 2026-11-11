# Agent Notes

- Always prefer `ChainTokenAmount` for token amount handling (human/native conversion and formatting).
- Do not introduce ad-hoc amount conversion logic when `ChainTokenAmount` can be used.
- If strict validation is required (for submit paths), keep strict conversion utilities explicit and documented.
- Do not use cargo fmt --all, format only lines that have been touched.
- Before creating, renaming, or moving files, inspect the surrounding module and follow its existing naming and directory conventions. Use role-specific names such as `<domain>_finalizer.rs`, `<domain>_adapter.rs` and `<domain>_utils.rs` for core files; do not introduce generic core names or a new folder structure without explicit agreement.
- When a module mixes several concerns, group its files in lifecycle folders and keep module names stable with `#[path]`, as `swappers/icpswap` and `finalizers/cex` do. The core file keeps the main type and its flow, and declares each part, for example `#[path = "deposit/funding.rs"] mod funding;`. Tests go in a `tests/` folder, one file per area, with shared builders in `tests/mod.rs`. Split files with pure moves first (no logic change), and refactor in a separate commit.
- Code comments and doc strings should have plain explanations that are not very large.
- New code must carry decisions in typed errors, not in message text. Return an enum variant the caller matches on; never make a caller re-derive a decision by searching an error string for a phrase. A message is for humans, so rewording it must never change behaviour.
- When an error decides whether money may have moved, make that the variant: see `TransferFailure::{Rejected, Ambiguous}`. Classify as ambiguous whenever unsure — treating a real transfer as if it never happened is the expensive mistake.
