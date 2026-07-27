# Agent Notes

- Always prefer `ChainTokenAmount` for token amount handling (human/native conversion and formatting).
- Do not introduce ad-hoc amount conversion logic when `ChainTokenAmount` can be used.
- If strict validation is required (for submit paths), keep strict conversion utilities explicit and documented.
- Do not use cargo fmt --all, format only lines that have been touched.
