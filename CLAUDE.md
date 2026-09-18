# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`README.md` is the authoritative architecture spec for kabudachi (a peer-to-peer Python task queue with a compiled Rust native core). Do not implement functionality ahead of the current phase (README §27 — "Suggested implementation phases"). **As of now, no phase has started**: only build/test scaffolding exists (`core`/`bindings` each export one trivial placeholder function; `runtime` is an empty API surface). Don't take README's distributed-systems design (DHT, leader election, TaskRun state machine, etc.) as already implemented — it's the target design, not current code.

## Commands

Bazel is the source of truth for correctness; `cargo`/`uv` are for fast local iteration only — a change isn't done until it passes under Bazel.

```bash
# Whole-repo build/test (canonical)
bazel build //...
bazel test //...

# Single target
bazel test //core:core_test
bazel test //bindings:bindings_test
bazel test //runtime/tests:test_native
bazel test //runtime/tests:test_placeholder

# Rust, fast local iteration (workspace = core + bindings)
cargo test --workspace
cargo test -p kabudachi-core
cargo test -p kabudachi-bindings

# Python, fast local iteration (from runtime/)
cd runtime && uv run pytest --continue-on-collection-errors   # see gotcha below for why this flag is needed
cd runtime && uv run pytest tests/test_placeholder.py -v      # single file, no flag needed

# Regenerate runtime/'s Bazel BUILD files after adding/removing Python files
bazel run //:gazelle
```

There is no lint/format command configured yet (no ruff, no CI) — don't add one speculatively.

To run any of the above the same way regardless of host OS, see `CONTRIBUTING.md` for the Docker/devcontainer setup — verified working end-to-end on linux/arm64, including sidestepping the macOS `.pth` gotcha below entirely.

## Architecture

Three directories, named by function rather than by implementation language:

- **`core/`** — compiled native logic (Rust). **Never depends on `pyo3`.**
- **`bindings/`** — the PyO3 FFI seam exposing `core` to Python, built as a Bazel `pyo3_extension` (`kabudachi._native`). The *only* crate that may depend on `pyo3`.
- **`runtime/`** — the Python developer-facing `kabudachi` package.

`bindings` is kept separate from `core` (not just a thinner split of the same crate) so that `core`'s types can never accidentally become PyO3 wrapper types — a compile-time guarantee, not a review convention — and so `core` stays testable via pure-Rust tests with no Python interpreter involved.

Internal boundaries follow YAGNI ruthlessly: no speculative crate/package splits, split only when real code forces it. The *external* surface (the public `kabudachi` Python API, any published Rust crate API) is allowed to be more deliberately designed, since this targets external users.

### Build system layering

- `runtime/` is built with `aspect_rules_py` (not vanilla `rules_python`), with dependencies managed via `uv` (`pyproject.toml` + `uv.lock`).
- `core/` and `bindings/` are built with `rules_rust`; their `BUILD.bazel` files are hand-authored (Gazelle's Rust support isn't relied on).
- **Gazelle** (`bazel run //:gazelle`) generates `runtime/src/BUILD.bazel`, `runtime/src/kabudachi/BUILD.bazel`, and `runtime/tests/BUILD.bazel` — never hand-edit these directly. A manual dependency added to one of them (e.g. the compiled native extension) must be wrapped in a `# keep` comment so Gazelle preserves it on the next run.
- `runtime/BUILD.bazel` (the top-level one) is *not* Gazelle-generated — it's a hand-written, permanently-empty placeholder that exists only so `//runtime:pyproject.toml`/`//runtime:uv.lock` are valid Bazel labels for `MODULE.bazel`'s `uv.project(...)` extension.
- `core/BUILD.bazel` and `bindings/BUILD.bazel` are hand-authored too. The root `BUILD.bazel`'s Gazelle directives (`# gazelle:map_kind`, `# gazelle:exclude core`, `# gazelle:exclude bindings`) are what keep Gazelle from touching these Rust BUILD files.
- `pyo3`'s version for the **Bazel** build comes from `rules_rust_pyo3`'s own vendored crate universe (pinned in `MODULE.bazel.lock`), *not* from `bindings/Cargo.toml` — bumping the version pinned in `Cargo.toml` has no effect on what Bazel actually builds, and vice versa.
- Adding any new third-party crate dependency to `core` or `bindings` will require wiring a `crate_universe`/`crates_repository` extension into `MODULE.bazel` first — that isn't set up yet.

### Local dev gotchas

`uv run pytest` from `runtime/` can fail on a fresh or first invocation for two unrelated reasons — neither is a real bug, and neither should be "fixed" in test code:

- **`ModuleNotFoundError: No module named 'kabudachi'`**: an intermittent macOS bug where `uv`'s editable-install `.pth` file sometimes picks up the `UF_HIDDEN` flag (via APFS clonefile linking from `~/.cache/uv`), and CPython's `site.py` silently skips hidden `.pth` files. Fix: `chflags nohidden runtime/.venv/lib/python3.12/site-packages/kabudachi.pth` (a full `.venv` rebuild sometimes helps but isn't reliable).
- **The whole session aborts with 0 tests run**: `runtime/tests/test_native.py` needs the Bazel-built extension, so it always fails to import under plain `uv run pytest` (Bazel is the source of truth for that test, not `uv`/`pytest`). pytest aborts the entire session on a collection-time `ImportError` by default, so `test_placeholder.py` never even runs. Work around it with `--continue-on-collection-errors`.

## Testing discipline

Modified Extreme Programming: tests before implementation (TDD), frequent small commits. Python tests use pytest; Rust tests use the built-in `#[test]` harness run through Bazel's `rust_test`.
