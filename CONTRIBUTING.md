# Contributing to kabudachi

`README.md` is the architecture spec; `CLAUDE.md` documents the build/test commands and the non-obvious parts of how this repo's Bazel/Cargo/uv setup fits together. This file covers running that setup in a container, and what's not yet covered by it.

## Testing with Docker (recommended)

The repo ships a `Dockerfile` (Ubuntu 24.04, with `bazel` via bazelisk, a `rustup`-managed Rust toolchain, and `uv` preinstalled) so tests run the same way regardless of host OS. It's been verified end-to-end on linux/arm64: the Bazel test targets (four at the time; the per-area targets added since have not been re-run in it), `cargo test --workspace`, and `uv run pytest` all pass inside it — and it also sidesteps a macOS-only `.pth`-hiding bug documented in `CLAUDE.md` that only affects `uv run pytest` on a macOS host.

**Build the image and run the full Bazel suite:**

```bash
docker build -t kabudachi-test .
docker run --rm kabudachi-test bazel test //...
```

**Run one area:** each area of `core` and `net` is its own Bazel test target, so a change reruns only the areas that depend on it: `bazel test //core:election_test` (also `//core:configuration_test`, `//core:proptest_test`, `//core:scenario_test`, `//core:scheduler_test`, and the in-crate unit tests `//core:core_test`), `bazel test //net:bootstrap_test` (also `//net:claim_test`, `//net:election_test`, and `//net:net_test`), `bazel test //testkit:testkit_test`, `bazel test //bindings:bindings_test`, and `bazel test //runtime/tests:test_native` (and the other Python targets under `//runtime/tests`). Run inside the container as `docker run --rm kabudachi-test bazel test //core:election_test`.

**Keep Bazel's cache between runs:** `--rm` discards the container's cache, so each run rebuilds from scratch. To reuse it, mount one named volume as a disk cache. Share that one volume across all runs and worktrees; a volume per run or per worktree multiplies disk use. `.bazelrc` caps the disk cache at 10 GB, but Bazel only trims the cache while its server is idle, after the command ends. With `--rm`, the container stops as soon as `bazel test` returns, so the command below starts the trim at once and keeps the container up for 30 seconds before it returns the test result.

```bash
docker run --rm -v kabudachi-bazel-cache:/disk-cache kabudachi-test bash -c \
  'bazel test //... --disk_cache=/disk-cache --experimental_disk_cache_gc_idle_delay=0; status=$?; sleep 30; exit $status'
```

**Run the other local-iteration commands the same way:**

```bash
docker run --rm kabudachi-test cargo test --workspace
docker run --rm kabudachi-test bash -c "cd runtime && uv run pytest --continue-on-collection-errors"
```

**Interactive development:** open the repo in VS Code with the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) and reopen in container — `.devcontainer/devcontainer.json` builds from the same `Dockerfile` and mounts the live working tree, so edits outside the container are immediately visible inside it.

The Dockerfile also `COPY`'s the repo into the image at build time (see `.dockerignore` for what's excluded — `.git`, Bazel/Cargo/uv build output, caches), so `docker build` alone produces a fully self-contained, testable snapshot without needing the bind mount — useful for a future CI job that just wants to build-and-test in one shot.

## Testing without Docker

See `CLAUDE.md`'s `Commands` section — the native (non-Docker) build/test commands and their known local gotchas (macOS `.pth` bug, pytest collection-abort behavior) are documented there, not repeated here.

## Cross-platform testing notes (for when CI is set up)

CI configuration itself is intentionally not set up yet (see the design spec's explicitly-deferred scope). When it is, keep in mind what the Docker setup above does and doesn't cover:

- **Linux**: fully covered by the `Dockerfile` above, regardless of which OS the CI runner's host is — this is the easy case.
- **macOS**: Docker on macOS runs Linux containers under a VM (e.g. Colima, Docker Desktop), so it does **not** exercise macOS-native toolchains. A macOS CI job needs to run natively on a `macos-latest`-style runner, replicating the manual setup this project's history went through: `bazelisk`/`bazel` via Homebrew, a Rust toolchain (rustup or Homebrew), and `uv`. Expect the macOS-only `.pth`-hiding bug documented in `CLAUDE.md` to be relevant for any job that runs `uv run pytest` directly (Bazel's own test execution is unaffected — it doesn't go through `uv`'s editable install).
- **Windows**: also not covered by this Linux-based Dockerfile (Docker Desktop can run Windows containers, but that's a separate, unexplored path). A Windows CI job would need Bazel's Windows-specific setup (which has real differences from Linux/macOS — symlink support requires enabling Developer Mode or running as Administrator, and long-path support may need enabling), plus a Windows Rust toolchain and `uv` build. This hasn't been attempted or verified anywhere in this project yet — treat it as the highest-risk platform when the time comes, the same way PyO3-through-Bazel was treated as the highest-risk step during initial scaffolding.

## PR review setup (CodeRabbit + branch protection on `main`)

`.coderabbit.yaml` configures automatic CodeRabbit review for every PR into `main`, using `STYLE_GUIDE.md` (plus this repo's `CLAUDE.md`) as its review guidelines. None of that takes effect until two one-time, manual steps are done in GitHub's UI — neither can be done from a config file:

### 1. Install the CodeRabbit GitHub App

Install it on this repo (not just your account) at <https://github.com/marketplace/coderabbitai>, or via <https://app.coderabbit.ai>. Until this is installed, `.coderabbit.yaml` is inert.

Once installed, open a throwaway PR to confirm it posts a review — you'll need that PR for step 2 below (GitHub only lets you pick a required status check from checks that have reported at least once).

### 2. Set up a ruleset on `main`

GitHub's newer **Rulesets** (Settings → Rules → Rulesets → New branch ruleset) are more precise than the legacy "branch protection rules" UI for exactly the situation of a solo maintainer, because bypass permissions are per-actor instead of one global "include administrators" checkbox. Configure:

- **Target**: branch `main`
- **Enforcement status**: Active
- **Bypass list**: add yourself via the **Repository admin** role, bypass mode **Always**. This is what lets you merge your own PRs — GitHub never lets a PR author's own review count toward a required-approval count, so without this you'd lock yourself out.
- **Rules to enable**:
  - *Require a pull request before merging* — required approvals: **1**. Your admin bypass covers you; any future outside contributor still needs a real approval from you.
  - *Require status checks to pass* — add the CodeRabbit check from step 1's throwaway PR (shows up as something like `CodeRabbit` in the picker).
  - *Require conversation resolution before merging* — this is the actual mechanism for "all CodeRabbit comments must be resolved before merging." It applies to every review thread, not CodeRabbit-specific, which is fine since CodeRabbit's comments are ordinary review threads.
  - *Block force pushes* and *Restrict deletions* — standard hygiene for a `main` that's otherwise easy to bypass as a solo admin.

Because your bypass mode is "Always," these rules gate anyone else with write access but never block you — you'll still see CodeRabbit's review and can choose to address it before clicking merge, which is the "I approve every PR" behavior in practice, just not a GitHub-enforced deadlock against yourself.

### CodeRabbit features worth trying

- `@coderabbitai configuration` as a PR comment dumps CodeRabbit's *actual* effective config — useful to check `.coderabbit.yaml` is being read as intended, and to catch schema drift against this file.
- `@coderabbitai generate docstrings` / `@coderabbitai generate unit tests` as PR comments (finishing-touches features) will push a follow-up commit.
- `@coderabbitai resolve` resolves all of CodeRabbit's own review threads at once from a comment, instead of clicking through each one.
- Sequence diagrams in the walkthrough are genuinely useful once anything protocol/state-machine-shaped from the README's design (leader election, `TaskRun` transitions) starts landing.

### Gotcha

CodeRabbit runs Clippy natively and automatically wherever it sees a `Cargo.toml` — no `.coderabbit.yaml` entry needed for `core`/`bindings`, unlike `ruff` for Python which is explicitly configured. That said, don't treat PR review as your only Clippy pass: keep running `cargo clippy --workspace` yourself locally/in CI, since it runs on every change rather than only at review time.
