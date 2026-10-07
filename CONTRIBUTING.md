# Contributing to kabudachi

`README.md` is the architecture spec; `CLAUDE.md` documents the build/test commands and the non-obvious parts of how this repo's Bazel/Cargo/uv setup fits together. This file covers running that setup in a container, and what's not yet covered by it.

## Testing with Docker (CI parity, needs approval on the maintainer's laptop)

On the maintainer's laptop, Docker/Colima builds need the user's approval and are not the default; use the native setup in "Testing without Docker" below.

The repo ships a `Dockerfile` (Ubuntu 24.04, with `bazel` via bazelisk, a `rustup`-managed Rust toolchain, and `uv` preinstalled) so tests run the same way regardless of host OS. It's been verified end-to-end on linux/arm64: the four Bazel test targets that existed then passed inside it. The integration targets added since have not been run in it. Python tests run through Bazel there too (`bazel test //runtime/tests/...`), and the container needs no host-specific workaround.

**Build the image and run the full Bazel suite:**

```bash
docker build -t kabudachi-test .
docker run --rm kabudachi-test bazel test //...
```

**Run one crate or area:** `core` and `net` each build their integration tests as one binary with a module per area, so `bazel test //core:core_integration_test` and `bazel test //net:net_integration_test` run every area of that crate. Select one area by passing a libtest filter, which matches a substring of the full test name: `bazel test //core:core_integration_test --test_arg=scheduler::`. Because it is a substring match, `--test_arg=election::` also selects the `scenario::scenario_election::` tests; add `--test_arg=--skip --test_arg=scenario::` to keep only the `election` module, or pass `--test_arg=--exact` with a full test name to run one test (Bazel's own `--test_filter` is not forwarded to these binaries). The in-crate unit tests of `net` are `//net:net_test`; `core`, `testkit` and `bindings` have none (`bindings` is covered by the Python tests). Other targets: `bazel test //runtime/tests:test_native` (and the other Python targets under `//runtime/tests`). Run inside the container as `docker run --rm kabudachi-test bazel test //core:core_integration_test`.

**Keep Bazel's cache between runs:** `--rm` discards the container's cache, so each run rebuilds from scratch. To reuse it, mount one named volume as a disk cache. Share that one volume across all runs and worktrees; a volume per run or per worktree multiplies disk use. The repo's `.bazelrc` points native runs at the shared `~/.cache/kabudachi-bazel`, capped at 6 GB. The volume example below passes its own `--disk_cache=/disk-cache`, which overrides that path; the 6 GB cap still applies. Bazel only trims the cache while its server is idle, after the command ends. With `--rm`, the container stops as soon as `bazel test` returns, so the command below starts the trim at once and keeps the container up for 30 seconds before it returns the test result.

```bash
docker run --rm -v kabudachi-bazel-cache:/disk-cache kabudachi-test bash -c \
  'bazel test //... --disk_cache=/disk-cache --experimental_disk_cache_gc_idle_delay=0; status=$?; sleep 30; exit $status'
```

**Run the Python tests the same way:**

```bash
docker run --rm kabudachi-test bazel test //runtime/tests/...
```

`import kabudachi` needs the `kabudachi._native` extension, which only Bazel builds, so `uv run pytest` cannot run the Python tests, in the container or anywhere else. Python tests go through their Bazel targets.

**Interactive development:** open the repo in VS Code with the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) and reopen in container — `.devcontainer/devcontainer.json` builds from the same `Dockerfile` and mounts the live working tree, so edits outside the container are immediately visible inside it.

The Dockerfile also `COPY`'s the repo into the image at build time (see `.dockerignore` for what's excluded — `.git`, Bazel/Cargo/uv build output, caches), so `docker build` alone produces a fully self-contained, testable snapshot without needing the bind mount — useful for a future CI job that just wants to build-and-test in one shot.

## Testing without Docker

Native runs share one Bazel disk cache, `~/.cache/kabudachi-bazel`, capped at 6 GB, so a new worktree reuses existing actions. Bazel's server exits after 900 idle seconds (`startup --max_idle_secs` in `.bazelrc`). Tests run only through Bazel; for quick checks use `cargo check --workspace --all-targets` with the single shared `CARGO_TARGET_DIR` (`~/.cache/kabudachi-cargo-target`), not a per-worktree target dir. Agents do not use `cargo test`.

See `CLAUDE.md`'s `Commands` section for the native (non-Docker) build/test commands, including the note that Python tests run through Bazel and not `uv run pytest`, and the lockfile restore after Bazel commands.

### Deep property-test runs

The property tests (`core/tests/proptest/`) check 256 cases each by default (the flow test 400, the authority test 40), from a fixed seed in the leadership tests, and each test stops checking after a 60 s wall-clock budget, noting how many cases ran (visible with `--test_arg=--nocapture`, since libtest hides the output of a passing test), so a slow host truncates a run rather than hanging it. Before closing a change to the election, run a deeper one with more cases and another seed:

```bash
PROPTEST_CASES=5000 PROPTEST_RNG_SEED=7 bazel test //core:core_integration_test \
  --test_arg=proptest:: --test_output=all
```

`PROPTEST_CASES` and `PROPTEST_RNG_SEED` are in the target's `env_inherit`, so Bazel passes them to the test without `--test_env` flags, and a different value reruns the test instead of reusing a cached result. Setting `PROPTEST_CASES` also skips the 60 s budget, so a deep run can take minutes.

## Continuous integration

`.circleci/config.yml` runs one CircleCI job on every push: `bazel test --config=ci //...`. The job is built to make the Free plan's 30,000 credits a month last.

**Time guard.** The test step stops after 5 minutes (`timeout 300` around the Bazel command), so a hung test fails the job quickly instead of spending credits until CircleCI's own limit. A healthy run takes well under that. Each property test has its own budget, so on a starved host the budgets of the tests sharing the binary can add up.

**Incremental testing is Bazel's own.** Bazel reruns only the actions whose inputs changed, and third-party outputs come from a remote cache. There is no test-selection script.

**What the job does:**

1. It checks out the commit in Bazel's official image (`gcr.io/bazel-public/bazel`), which ships Bazel itself, so the job installs nothing. The image tag is the Bazel version; the job fails if it differs from `.bazelversion`, so bump both together.
2. It restores Bazel's downloaded archives from a CircleCI cache keyed on the lockfiles.
3. Inside `with_tool_cache` (CircleCI's build tool cache, in beta), it runs `bazel test --config=ci //...`. The `ci` config sets `--build_tests_only`, so the job builds and runs test targets and their dependencies only; targets no test depends on, such as `//:gazelle`, are not built. The tool cache is Bazel's remote cache for the steps inside it.
4. When the lockfiles changed, it saves the downloads under the new key.

**The caches.**

- *Build outputs: the build tool cache.* Every first-party Rust target (`core`, `net`, `testkit`, `bindings`) is tagged `no-remote-cache`, so the tool cache stores third-party outputs only. Those are content-addressed and change only with a dependency change, so the stored size stays about one copy of the third-party build instead of growing with every push. First-party code rebuilds in every job, about 30 CPU-seconds. The tag does not affect a local `--disk_cache`. The tool cache is billed as ordinary cache storage; CircleCI does not document its retention or eviction, so check storage on the plan's Usage page after it has run for a while. The job sets `retention: caches: 1d`: storage is billed when an object is saved, as its size times its retention, so a one-day cache costs a fifteenth of the 15-day default. A cache that expires makes the next run rebuild and save it once.
- *Downloads: `save_cache`.* The key is a checksum of `MODULE.bazel`, `MODULE.bazel.lock`, `.bazelversion`, `Cargo.lock`, and `runtime/uv.lock`, and an older cache is restored by prefix when it changes. Only the archives are cached (about 0.5 GB); Bazel extracts them again in about 15 seconds, and the extracted copies (`/tmp/bazel/repository/contents`) take 1.8 GB. Bazel does not refresh the modification time of downloads it reuses, so the cache cannot be pruned by age; bump the `bazel-downloads-v1` key prefix to start a clean one.

**Measurements** (measured on 2026-09-30 in a Linux container with the `medium` resource class's 2 CPUs and 4 GB; the host was arm64, and CircleCI's `medium` is x86; the whole-job rows ran on `cimg/base:2026.09` with the earlier disk-cache setup, before the job moved to Bazel's image and the build tool cache):

| Run | Time | Credits (10 a minute) |
| --- | --- | --- |
| Cold build and test, before any tuning | 746 s build, 28 s tests | about 130 |
| Cold build and test, prebuilt protoc | 391 s | about 65 |
| Cold build and test, each crate built once | 297 s | about 50 |
| Cold build and test, build tools unoptimized (current) | 188 s | about 32 |
| Whole job, new cache key | 228 s test step, 39 s save | about 45 |
| Whole job, cache hit | 34 s test step, no test runs | about 6 plus the restore |

**Why these choices:**

- *Bazel's caching, not a test-selection tool.* A cache hit already skips every unchanged test, and the cached job's test step takes about 34 seconds, mostly Bazel startup and analysis. A tool such as `bazel-diff` hashes the target graph at two commits, which costs about as much as it could save at this size.
- *Docker `medium` (2 vCPU, 4 GB, x86).* A cold build is throughput-bound, so a larger class costs the same credits for the same work, and cached runs cannot use more CPUs. Arm Docker costs 13 credits a minute, not 10. `.bazelrc`'s `ci` config sets the job's CPU and memory limits, because a Docker executor reports the host's.
- *Cold-build settings in `.bazelrc`, used everywhere.*
  - *Prebuilt protoc.* Compiling protoc from C++ source took about 70% of a cold build's action time.
  - *One output configuration.* aspect_rules_py's `py_test` sets the Python version and venv for everything under it. Without the same values on the command line, `core`, tokio, and every proc macro under a Python test compiled a second time; `pyo3_extension` also forced `opt` on its subtree.
  - *Build tools in fastbuild.* Proc macros, build scripts, and rules_rust's helpers compiled at `opt-level=3`, about 60% of the remaining Rust compile time.
- *No path filtering.* A docs-only push still runs the job; on a cache hit it costs about as much as a dynamic-configuration setup job would.
- *First-party outputs stay out of the remote cache.* Storage costs 420 credits a GB-month beyond the included 2. Test binaries are tens of MB each, so storing every push's first-party outputs would cost more than the minute of rebuild they save.
- *No `store_test_results`.* Failures print through `--test_output=errors`, and stored results count toward storage.
- *No usage reports.* `.bazelrc` sets `DO_NOT_TRACK=1`, which aspect_rules_py's telemetry honors.

**One-time CircleCI project settings** (Project Settings → Advanced). They cannot be set in `config.yml`:

- *Auto-cancel redundant workflows*: on. A new push cancels the running workflow for older commits on the same branch.
- *Only build pull requests*: on, where the project uses the GitHub OAuth integration. A GitHub App project sets a pipeline trigger on pull-request events instead. `main` is always built.

Put `[skip ci]` in a commit message to skip CI for that push.

## Cross-platform testing notes

CI runs Linux only. Keep in mind what the Docker setup above does and doesn't cover:

- **Linux**: fully covered by the `Dockerfile` above, regardless of which OS the CI runner's host is — this is the easy case.
- **macOS**: Docker on macOS runs Linux containers under a VM (e.g. Colima, Docker Desktop), so it does **not** exercise macOS-native toolchains. A macOS CI job needs to run natively on a `macos-latest`-style runner, replicating the manual setup this project's history went through: `bazelisk`/`bazel` via Homebrew, a Rust toolchain (rustup or Homebrew), and `uv`. Run the Python tests through their Bazel targets there too: `uv run pytest` cannot import `kabudachi` without the Bazel-built extension, whatever the host.
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
- Sequence diagrams in the walkthrough are genuinely useful once anything protocol/state-machine-shaped from the design (leader election, `TaskRun` transitions) starts landing.

### Gotcha

CodeRabbit runs Clippy natively and automatically wherever it sees a `Cargo.toml` — no `.coderabbit.yaml` entry needed for `core`/`bindings`, unlike `ruff` for Python which is explicitly configured. That said, don't treat PR review as your only Clippy pass: keep running `cargo clippy --workspace` yourself locally/in CI, since it runs on every change rather than only at review time.
