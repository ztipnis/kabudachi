# Style Guide

This is the coding-style companion to `CLAUDE.md` (architecture/build) and `README.md` (design spec). It's what CodeRabbit is pointed at for automated review (see `.coderabbit.yaml`), and what a human reviewer should hold PRs to as well.

If something here conflicts with `CLAUDE.md`, `CLAUDE.md` wins — it documents the actual repo layout and build system, which this file assumes rather than repeats.

## Philosophy

- **YAGNI on internal boundaries.** No speculative crate/package splits, no abstraction introduced ahead of a real second caller. The `core`/`bindings`/`runtime` split exists because PyO3-vs-pure-Rust forces it, not as a template for further subdivision.
- **Don't implement ahead of the current phase.** `README.md` §27 defines the phase order. A PR that adds distributed-systems machinery (DHT, leader election, the `TaskRun` state machine, etc.) before its phase has started is out of scope regardless of code quality.
- **Small, frequent commits under TDD.** Tests before implementation. A PR that adds implementation with no preceding/accompanying test changes is a red flag, not just a style nit.
- **Stop and clarify instead of guessing.** Don't invent a schema, widen a type, or add a branch just to make something compile or pass a test. If the right shape genuinely isn't clear from `README.md`/`CLAUDE.md`/the current phase's tests, that's a "ask" moment, not a "pick something reasonable and move on" moment.
- **Name the lasting job, not the rollout state.** Don't name a module, type, or flag after a migration/rollout phase (`v2`, `_new`, `temporary`, `until_phaseN`) — name what it does once it's done. Phase sequencing lives in `README.md` §27 and PR history, not in identifiers.

## Configurability

Ship opinionated defaults, but make the implementation choices underneath them configurable wherever that costs little.

- **Why.** A CVE, a critical bug or a better technology should be a configuration change rather than a rewrite, and users who want to work at the deepest level of the implementation should be able to swap core pieces to fit their requirements.
- **Prefer runtime configuration.** A constructor argument, a `with_*` builder method or a trait implementation the user supplies at runtime. Where runtime is impractical, fall back to something chosen at recompilation (a cargo feature, or an env var read by `build.rs`). Avoid hard-coding a choice that has no override.
- **What to make swappable.** Third-party choices (hash function, RNG, serializer, protobuf compiler, transport), tunables such as timeouts and fanouts, and the collaborators around the core logic (clock, transport, membership, authority). Depend on a broad trait or interface (for example the RustCrypto `digest` traits) rather than one concrete crate, so alternatives plug in without changes here.
- **Defaults live in one place.** Each default is a named constant or a `Default` impl, and a `with_*` method overrides it, so existing constructors and call sites keep working.
- **Where it stops.** Don't add configurability that brings undue bloat or scope creep: no new dependency or abstraction layer whose only purpose is hypothetical flexibility, and no knob for a safety invariant the design fixes (for example the majority-quorum rule).
- **Say when peers must agree.** If a setting changes behaviour that different workers must compute identically (hash function, candidate ranking, digests), its documentation must say every worker in the shard has to use the same value.

## Readability target

- Code should be skimmable by someone without deep distributed-systems background: they should be able to get the general gist — what a piece of code does, what it reads/writes, what it deliberately doesn't handle — without first learning the theory behind DHTs, consensus, or leader election. Depth belongs in `README.md`, tests, and well-named helpers; the top-level flow of a function stays plain regardless of how intricate the domain is.
- **The literal "what" must be readable line by line with zero domain knowledge — this is mandatory, not aspirational.** Someone with general programming literacy and no idea what a DHT, a leader, or a `TaskRun` is should still be able to trace a function's lines and say what each one mechanically does. The *why* (the domain reasoning) and the *how* (the cleverness of the approach) are allowed to require real domain knowledge — that's exactly what `README.md`, docstrings, and "why" comments are for. But if the literal what isn't clear from the code alone, that's not a style nit to leave for later: a comment there is required.
  - **The one relaxation:** an inherently opaque boundary — dense vector/numeric algebra, a tight FFI marshalling routine, that kind of thing — where making every line self-evident on its own isn't realistic. There, a comment stands in for the line-by-line clarity the code itself can't provide. That's the exception, not a general excuse to skip naming things well.
- **The bar is highest at the public API surface** — `bindings/`'s PyO3 wrappers and `runtime`'s public functions. A caller must never need to understand `core`'s internal machinery to use the public surface correctly.
- Internals inside `core` are allowed to get denser than that — but only when the density is what keeps the public API small and simple, not as an excuse for a muddy public surface. If internal complexity is leaking into how callers have to think, that's a sign the boundary is in the wrong place, not that the caller needs to learn more.
- A function should stay clear even with its docstring deleted — names, signatures, and control flow should carry the meaning on their own; comments and docstrings are backup for what structure can't say, not a substitute for it (see Comments and docstrings below).
- Pull a confusing, multi-step block into its own named function when the name would clarify the flow — extract-and-name beats a long unlabeled block. When a routine is genuinely dense and extracting wouldn't actually clarify anything (a tight numerical or protocol routine, say), short phase-label comments are an acceptable substitute — that's density with a map, not the muddiness this section is against.

## Architecture boundaries (enforced, not just conventional)

- `core/` never depends on `pyo3`. A PR adding a `pyo3` import or dependency to `core/` is a hard blocker, not a suggestion.
- `bindings/` is the *only* crate allowed to depend on `pyo3`. It should stay a thin FFI seam — real logic belongs in `core`, not in the binding layer.
- `runtime/` is the Python-facing package. Internal `core`/`bindings` types should not leak into the public `kabudachi` Python API surface without a deliberate wrapper.

## Avoid shadow models

- Don't add a type whose only job is reshaping data between a source of truth and its consumer, with no domain meaning of its own. A second type that just renames or regroups the same fields as a source type is a shadow model — another definition that can drift from the one it mirrors, for no gain.
- If a consumer needs a different shape than the source produces, extend the source type or shape the value at the point of use. Don't invent a third type to sit between them, and don't write an unclear `*_to_*`/`convert_*` function whose only job is copying near-identical fields from one type to another — that function is the shadow model in disguise.
- A plain tuple, a couple of named locals, or a small stdlib container is enough for a value that's created and consumed immediately within one function, as long as the producing function's name and types make its shape and order unambiguous. Reach for a real named type only once the value is stored, threaded through multiple call sites, or crosses a genuine API boundary.
- **The one deliberate exception is the `bindings/` FFI seam itself.** A `#[pyclass]` wrapper type in `bindings/` that mirrors a `core` type so it can cross into Python *is* the boundary mapping this crate exists to do (see above), not a banned shadow model. Keep it to that job — a thin field-for-field wrapper/conversion — rather than letting it grow logic that duplicates or drifts from `core`.
- **Why this is worth being strict about:** adding or renaming a field should touch as few places as possible. Every shadow type is a place reviewers have to separately confirm "did these two stay in sync," which is cheap to avoid before it exists and a real maintenance tax once a few have accumulated.

## Rust (`core/`, `bindings/`)

- Format with `rustfmt` defaults — there's no `rustfmt.toml` in the repo, so don't hand-format against a personal preference that diverges from stock `rustfmt`.
- Code should be `clippy`-clean. There's no `clippy.toml` or CI wiring for it yet (see `CLAUDE.md`'s "no lint/format command configured yet" note) — run it manually (`cargo clippy --workspace`) before opening a PR.
- Prefer `Result`/`?` propagation over `.unwrap()`/`.expect()` outside of tests and truly-unreachable invariants. If a panic is intentional, a comment should say why the case is unreachable.
- **Test-only support code (fakes, mocks, fixtures, simulation harnesses) must be physically separate from production `src/`, not just feature-flag-gated.** A `#[cfg(feature = "testkit")]` module living inside `core/src/` still means a developer browsing production source trips over test-only code. Use the crate's `tests/` integration-test directory (e.g. `tests/support/`) for shared test support, or a separate dev-dependency crate only once a real cross-crate need exists.
- Public API of a crate (anything not `pub(crate)`) is held to a higher documentation bar than internal code — see README's note that the *external* surface is allowed to be more deliberately designed than internal boundaries.
- No new mutable module-level globals (`static mut`, ad-hoc `once_cell`/`lazy_static` state) — put state on the owning type and pass it explicitly, or thread it through the call chain. True constants are fine.
- Validate untrusted input at the edge — the `bindings/` FFI seam, or wherever `core` first receives externally-derived data — and trust it afterward. Don't re-validate an already-checked invariant deeper in `core`'s call chain "just in case."

## Python (`runtime/`)

- Follow PEP 8 conventions by hand — there's no `ruff`/`black` config in `pyproject.toml` yet, and `CLAUDE.md` is explicit that one shouldn't be added speculatively. Don't introduce per-file formatting quirks a linter would otherwise normalize.
- Public API in `runtime/src/kabudachi/` (anything a user of the package would import) should carry type hints — this is a developer-facing library, and type hints are part of its UX, not internal decoration.
- Use native modern type syntax (`X | None`, `list[X]`, `dict[K, V]`) rather than `typing.Optional`/`List`/`Dict`. `runtime/pyproject.toml` requires Python ≥3.12, so there's no compatibility reason to reach for the older `typing` aliases.
- Tests use `pytest`. Test files live under `runtime/tests/`, not next to the modules they test — keep that separation rather than colocating `test_*.py` inside `runtime/src/kabudachi/`.
- Imports at the top of the file, always — no import inside a function/method/conditional to dodge a cycle. A needed mid-function import is a sign of a design problem (ownership, an accidental cycle) to fix, not paper over.
- No import-time side effects beyond class/function definitions and constant binding — no cache warming, socket/connection setup, or global mutation just from `import kabudachi`. Put that behind an explicit call.
- `kabudachi/__init__.py` re-exports the deliberate public surface, not `from .foo import *` — a reader should be able to tell what's public by reading the file, not by knowing every submodule.

### Functional vs. OOP

kabudachi's public API is functional (free functions over small data types), and that's the default for `runtime/` internals too — but not a dogma:

- **Default to a plain function** for a single, stateless action. Don't wrap it in a class just to give it a home; a module is already a namespace.
- **Reach for a class** when there's real state or lifecycle to own (a connection, a worker loop, anything with setup/teardown), or when a cohesive group of operations shares that state/config closely enough that threading it through separate free functions would be worse than bundling it.
- **A public functional entry point may be backed by a class internally.** A decorator (e.g. task registration) is functional at the call site but is free to be a class under the hood if it needs to accumulate registration state across calls — the class is an implementation detail, not part of the API's shape.
- Don't create a class purely to group unrelated stateless helpers "for organization" — that's what the module already does.

## Comments and docstrings

- Default to no comments. Names and structure should carry the "what."
- Write a comment only when it captures a non-obvious "why": a hidden constraint, a workaround for a specific bug, an invariant that isn't visible from the code around it. If deleting the comment wouldn't confuse a future reader, delete it.
- Don't write comments that reference a specific PR, issue, or task ("added for the X flow", "fixes #123") — that context belongs in the commit message/PR description, and comments like this rot as the code evolves.
- **Gotchas get resolved in review, not preserved in the file.** A mistaken assumption caught in review, a subtle bug found and fixed, a "we tried X, here's why we didn't" — that back-and-forth belongs in the PR conversation and commit history, not as a permanent comment marking the spot it happened. A file that logs every correction it's ever received reads as a narrative of its past, not a description of its current behavior. Keep a comment there only when the code would be genuinely unreadable, or the same mistake would likely get reintroduced, without it — not simply because the fix was non-obvious in the moment it was made.
- **Docstrings: full or omit, never a one-line restatement.** If a function/class needs a docstring to carry intent, reason, or a tradeoff the signature can't, write a real one (summary, then `Args`/`Returns`/`Raises` as needed for Python; a `///` doc comment covering the same ground for Rust). If the name and signature already say everything, skip the docstring entirely.
  ```python
  def is_even(i: int) -> bool:
      """Returns true if i is an even number"""  # adds nothing `is_even` didn't already say — delete it
      return i % 2 == 0
  ```
- A docstring should stand on its own — readable without knowing where sibling code lives or how the rest of the system is wired. Save cross-component context for `README.md`.
- Plain, concrete wording over jargon in names, comments, and log messages — "the leader's term counter" beats "the epoch observable."

## Review scope

- Merged code is trusted for shape and correctness. Reviewing a PR (human or CodeRabbit) means judging the diff in front of you — it is not license to re-scrutinize or rewrite pre-existing code the PR didn't touch.
- Flag something outside the current diff only when it's a real risk: a correctness bug, a security issue, or a violation of the hard architecture boundaries above. A style nit, a naming preference, or a "this could be cleaner" observation on code the PR doesn't change is out of scope for that PR — file it separately if it matters, don't block on it.
- This cuts both ways: a change that touches a file for one reason doesn't need to bring the rest of that file up to the current style bar in the same PR. Fix what the PR is already touching; leave the rest for when it's actually being changed.

## Commit hygiene

- Commit messages should explain *why*, not restate the diff.
- Prefer several small, reviewable commits within a PR over one large one, consistent with the TDD/small-commits discipline in `CLAUDE.md`.
