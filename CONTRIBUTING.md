# Contributing to kabudachi

`README.md` is the architecture spec; `CLAUDE.md` documents the build/test commands and the non-obvious parts of how this repo's Bazel/Cargo/uv setup fits together. This file covers running that setup in a container, and what's not yet covered by it.

## Testing with Docker (recommended)

The repo ships a `Dockerfile` (Ubuntu 24.04, with `bazel` via bazelisk, a `rustup`-managed Rust toolchain, and `uv` preinstalled) so tests run the same way regardless of host OS. It's been verified end-to-end on linux/arm64: all four Bazel test targets, `cargo test --workspace`, and `uv run pytest` all pass inside it — and it also sidesteps a macOS-only `.pth`-hiding bug documented in `CLAUDE.md` that only affects `uv run pytest` on a macOS host.

**Build the image and run the full Bazel suite:**

```bash
docker build -t kabudachi-test .
docker run --rm kabudachi-test bazel test //...
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
