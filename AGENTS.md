# Agent Notes

- Always prefer `ChainTokenAmount` for token amount handling (human/native conversion and formatting).
- Do not introduce ad-hoc amount conversion logic when `ChainTokenAmount` can be used.
- If strict validation is required (for submit paths), keep strict conversion utilities explicit and documented.
- Do not use cargo fmt --all, format only lines that have been touched.
- Before creating, renaming, or moving files, inspect the surrounding module and follow its existing naming and directory conventions. Use role-specific names such as `<domain>_finalizer.rs`, `<domain>_finalizer_tests.rs`, and `<domain>_utils.rs` when that pattern exists; do not introduce generic names or a new folder structure without explicit agreement.
- Code comments and doc strings should have plain explanations that are not very large.
- New code must carry decisions in typed errors, not in message text. Return an enum variant the caller matches on; never make a caller re-derive a decision by searching an error string for a phrase. A message is for humans, so rewording it must never change behaviour.
- When an error decides whether money may have moved, make that the variant: see `TransferFailure::{Rejected, Ambiguous}`. Classify as ambiguous whenever unsure — treating a real transfer as if it never happened is the expensive mistake.

## Commits

- Work in small, cohesive chunks that a human can review and audit independently.
- Before creating any commit, show the relevant diff summary, report the verification performed, and obtain explicit user confirmation for that specific commit.
- Do not treat a request for implementation, a commit plan, or approval of an earlier commit as permission to create later commits.
- Stage and commit only files belonging to the approved chunk, and use a message that describes that chunk precisely.
- Do not amend, squash, rebase, or otherwise rewrite commits unless the user explicitly requests it.
