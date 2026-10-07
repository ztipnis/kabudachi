# CLAUDE.md

## Source of truth

- `README.md` is the architecture and phased-delivery specification. Only Phases 0-2 are implemented; do not implement later-phase design.
- `STYLE_GUIDE.md` governs every code change and review. Do not rewrite unrelated pre-existing code for style alone; address it only when it is a correctness, security, or architecture-boundary risk.
- `CONTRIBUTING.md` is the source for Docker/devcontainer and platform-specific test guidance. Docker is for CI-parity checks only, and only with user approval (see "Shared build state and agent rules"). `runtime/README.md` documents the Python runtime API.

## Commands

Bazel is canonical for correctness. Run tests through Bazel only.

For fast local iteration, run only `cargo check --workspace --all-targets` or `cargo clippy`, both with the shared target dir. Do not run `cargo test` or `cargo build`: they duplicate Bazel's artifacts and fill the disk.

All agents share one `CARGO_TARGET_DIR` (`~/.cache/kabudachi-cargo-target`). Do not give a worktree its own target dir.

A change is not complete until the relevant Bazel target passes, and, for a PR, until CI is green (see "Completion gate").

```bash
bazel build //...
bazel test //...

bazel test //core:core_integration_test
# One area of an integration binary. The filter is a substring match on the full test name,
# so `election::` also matches `scenario::scenario_election::` (231 tests, not 227). Add
# `--test_arg=--skip --test_arg=scenario::` to get exactly the 227 of the `election` module:
bazel test //core:core_integration_test --test_arg=election:: --test_arg=--skip --test_arg=scenario::
bazel test //core:core_test
bazel test //net:net_integration_test
bazel test //net:net_test
bazel test //testkit:testkit_test
bazel test //bindings:bindings_test
bazel test //runtime/tests:test_native
bazel test //runtime/tests:test_local

cargo check --workspace --all-targets

bazel run //:gazelle
```

No lint or format command is configured; do not add one speculatively.

Python tests run through Bazel only, on the host or in the Linux container (`CONTRIBUTING.md`). `import kabudachi` needs the `kabudachi._native` extension, which only Bazel builds (`uv_build` packages the pure-Python sources alone), so `uv run pytest` cannot import the package or run the Python tests on any host.

## Architecture

- `proto/`: schemas and generated Rust wire types.
- `core/`: native Rust logic; it must never depend on `pyo3`.
- `bindings/`: the sole PyO3 FFI boundary and `kabudachi._native` extension.
- `runtime/`: developer-facing Python package.
- Keep internal code YAGNI-driven; design public APIs deliberately.
- Do not hand-edit Gazelle-generated runtime BUILD files. Run `bazel run //:gazelle` after Python-file changes; preserve manual BUILD dependencies with `# keep`.
- Update `Cargo.lock` after Rust dependency changes. Add Python dependencies to the runtime package metadata and regenerate its lockfile/manifest as required.
- Keep bindings Rust tests PyO3-free; exercise Python-facing behavior from Python tests.
- For current macOS/Bazel-native-extension caveats and running the Python tests in the Linux container, use the explicit procedures in `CONTRIBUTING.md` and the Phase 1 plan.

## Shared build state and agent rules

- Bazel uses one shared disk cache, `~/.cache/kabudachi-bazel` (6 GB cap, set in `.bazelrc`), so a new worktree reuses existing actions instead of rebuilding. The `ci` config clears it. Bazel servers exit after 900 idle seconds (`startup --max_idle_secs`).
- At most 2 agents run Bazel at once. Any further agent reads and edits only.
- Do not run Docker/Colima builds or use remote hosts without user approval.
- Do not run TLC beyond Bound A without user approval. Put TLC state dirs under the scratchpad and delete them after the run.
- A local Claude Code hook (not in the repo) blocks `bazel` and `cargo` when free disk is under 30 GB or the cargo target dir exceeds 3 GB.
- If the hook blocks a command, stop and report to the user; do not work around it.
- Name these cleanup options: `bazel clean --expunge` in finished worktrees, `git worktree remove`, `rm -rf ~/.cache/kabudachi-cargo-target`.
- The hook and `CARGO_TARGET_DIR` live in the main checkout's gitignored `.claude/`. Sessions started from the main checkout load them; copy `.claude/settings.json` and `.claude/hooks/` into a worktree whose session does not.
- When a slice merges, run `bazel clean --expunge` in its worktree, then remove the worktree.

## Dependencies over homegrown code

Prefer a reputable maintained package to a generic utility. Weigh dependency cost and explain a deliberate in-house implementation in the PR.

## Testing discipline

Use TDD and small commits. Python tests use pytest; Rust tests use `#[test]` through Bazel `rust_test`.

## Completion gate: CI must be green

Work that lands through a PR is complete only when CircleCI's `test` job is green on the PR's current head commit, in addition to the local Bazel pass and CodeRabbit's approval. Wait for it with the `github-pr-review-wait` skill's check mode. A red or cancelled job blocks completion: read its log, fix the cause, push, and wait for a green run on the new head. A push after a green run needs its own green run. Rerun a red job only after its cause is understood; a flaky test is a cause to fix, or to record before rerunning as open work under its phase section in `README.md` (not as a GitHub issue). CI setup and its cost model are in `CONTRIBUTING.md` ("Continuous integration").

## Responding to CodeRabbit review comments

After fixing a finding and pushing, reply on its thread with the fix commit and evidence, but do not call the resolve action yourself. CodeRabbit auto-resolves a thread once it confirms the fix on the next review pass.

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships. graphify-out/ is gitignored, so each clone keeps its own graph.

Rules:
- Query the graph through the `graphify` MCP server (`.mcp.json`), not the CLI. For codebase questions, use `mcp__graphify__query_graph` first. Use `mcp__graphify__get_node` and `mcp__graphify__get_neighbors` for a symbol, `mcp__graphify__shortest_path` for how two concepts relate, and `mcp__graphify__get_community` or `mcp__graphify__god_nodes` for structure. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output. The server reloads graph.json after each rebuild.
- Do not run `graphify query`, `graphify path` or `graphify explain` in the shell. Use them only if the MCP tools are unavailable, and say so. Subagents inherit the MCP tools; tell them to use them.
- The server reads `graphify-out/graph.json` in the session's working directory. A worktree has no graph until you build one there: copy the main checkout's `graphify-out/` into it, then run the full rebuild below.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).

### Rebuilding the graph

Keep the graph current; a stale graph misleads every query.

- Code-only rebuilds (`graphify update .`) run automatically from the local git hooks: `post-commit`, `post-checkout` on a branch switch, and `post-merge` after a merge or pull. They run in the background and log to `~/.cache/graphify-rebuild.log`. Hooks are per-clone. If `graphify hook status` shows them missing, run `graphify hook install`.
- The hooks never re-extract docs. Run a full rebuild with the Claude CLI backend when a Markdown or YAML doc changes (README.md, STYLE_GUIDE.md, CONTRIBUTING.md, docs/, runtime/README.md), and at least once per work session:

  ```bash
  graphify extract . --backend claude-cli    # incremental: re-extracts changed files only
  graphify label . --backend=claude-cli      # re-clusters, names communities, rewrites GRAPH_REPORT.md
  ```

  `claude-cli` uses the Claude Code login, so no API key is needed. Do not use `--backend claude`; it needs `ANTHROPIC_API_KEY`. `extract` alone does not rename communities or rewrite the report, so always run `label` after it.
- After a PR merges into `main`, update local `main` (`git checkout main && git pull`), then run the full rebuild above before starting the next branch. Treat this as part of finishing the branch.
- If a rebuild refuses to shrink `graph.json` after code was deliberately deleted, rerun it with `--force`.
