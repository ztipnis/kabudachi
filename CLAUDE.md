# CLAUDE.md

## Source of truth

- `README.md` is the architecture and phased-delivery specification. Only Phases 0 and 1 are implemented; do not implement later-phase design.
- `STYLE_GUIDE.md` governs every code change and review. Do not rewrite unrelated pre-existing code for style alone; address it only when it is a correctness, security, or architecture-boundary risk.
- `CONTRIBUTING.md` is the source for Docker/devcontainer and platform-specific test guidance. `runtime/README.md` documents the Python runtime API.

## Commands

Bazel is canonical for correctness; `cargo` and `uv` are fast local iteration. A change is not complete until the relevant Bazel target passes.

```bash
bazel build //...
bazel test //...

bazel test //core:core_test
bazel test //bindings:bindings_test
bazel test //runtime/tests:test_native
bazel test //runtime/tests:test_placeholder

cargo test --workspace
cargo test -p kabudachi-core
cargo test -p kabudachi-bindings

cd runtime && uv run pytest --continue-on-collection-errors
cd runtime && uv run pytest tests/test_placeholder.py -v
bazel run //:gazelle
```

No lint or format command is configured; do not add one speculatively.

## Architecture

- `proto/`: schemas and generated Rust wire types.
- `core/`: native Rust logic; it must never depend on `pyo3`.
- `bindings/`: the sole PyO3 FFI boundary and `kabudachi._native` extension.
- `runtime/`: developer-facing Python package.
- Keep internal code YAGNI-driven; design public APIs deliberately.
- Do not hand-edit Gazelle-generated runtime BUILD files. Run `bazel run //:gazelle` after Python-file changes; preserve manual BUILD dependencies with `# keep`.
- Update `Cargo.lock` after Rust dependency changes. Add Python dependencies to the runtime package metadata and regenerate its lockfile/manifest as required.
- Keep bindings Rust tests PyO3-free; exercise Python-facing behavior from Python tests.
- For current macOS/Bazel-native-extension and `uv` test caveats, use the explicit procedures in `CONTRIBUTING.md` and the Phase 1 plan.

## Dependencies over homegrown code

Prefer a reputable maintained package to a generic utility. Weigh dependency cost and explain a deliberate in-house implementation in the PR.

## Testing discipline

Use TDD and small commits. Python tests use pytest; Rust tests use `#[test]` through Bazel `rust_test`.
