# Agent Notes

- Always prefer `ChainTokenAmount` for token amount handling (human/native conversion and formatting).
- Do not introduce ad-hoc amount conversion logic when `ChainTokenAmount` can be used.
- If strict validation is required (for submit paths), keep strict conversion utilities explicit and documented.
- Do not use cargo fmt --all, format only lines that have been touched.
- Before creating, renaming, or moving files, inspect the surrounding module and follow its existing naming and directory conventions. Use role-specific names such as `<domain>_finalizer.rs`, `<domain>_finalizer_tests.rs`, and `<domain>_utils.rs` when that pattern exists; do not introduce generic names or a new folder structure without explicit agreement.
- Code comments and doc strings should have plain explanations that are not very large.
