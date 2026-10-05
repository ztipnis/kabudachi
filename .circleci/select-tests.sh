#!/usr/bin/env bash
# Chooses the Bazel test targets a CircleCI run executes and writes them to the
# file named by $1: either the single line `//...` or one label per line (an
# empty file means no test is impacted).
#
# The output file starts as `//...` and is replaced only at the very end, with
# the real selection or the empty "nothing impacted" state. A script that is
# killed or crashes midway therefore leaves `//...`, never an empty file.
#
# A pull request runs only the tests affected by its changes (bazel-diff). The
# `main` branch and branches without an open pull request run everything. Every
# situation this script cannot judge safely falls back to `//...` and says why;
# it never skips tests silently. It exits 0 unless it cannot put the checkout
# back on the commit under test, which fails the job.
#
# Environment: CIRCLE_BRANCH, CIRCLE_PULL_REQUEST, CIRCLE_SHA1 (set by
# CircleCI), BAZEL_DIFF_BIN (optional path of a bazel-diff binary; skips the
# download and the checksum), SELECT_TESTS_BAZEL_OPTIONS (optional extra Bazel
# command options, e.g. --config=ci).
set -uo pipefail

BAZEL_DIFF_VERSION=v49.1.0
BAZEL_DIFF_URL="https://github.com/Tinder/bazel-diff/releases/download/${BAZEL_DIFF_VERSION}/bazel-diff-rust-linux-amd64"
BAZEL_DIFF_SHA256=ab9dea07341a4d764aaed15225ac5ea65f381fc1a1ae304e9fc0147b5e832a87

out=${1:-}
start=$SECONDS
work=""
original_head=""

log() { echo "select-tests: $*"; }

# Writes the fallback selection and returns 1 so callers can `|| return 0`.
run_everything() {
  log "running //... because $1"
  echo '//...' > "$out"
  return 1
}

# Logs the working tree state, then forces a detached checkout of <commit>, so a
# dirty tree cannot block it.
checkout_commit() {
  log "git status before checking out $1:"
  git status --short | sed 's/^/select-tests:   /'
  git checkout --quiet --force --detach "$1"
}

cleanup() {
  local status=$?
  trap - EXIT
  if [ -n "$original_head" ] && [ "$(git rev-parse HEAD 2>/dev/null)" != "$original_head" ]; then
    checkout_commit "$original_head" || log "WARNING: could not restore $original_head"
    if [ "$(git rev-parse HEAD 2>/dev/null)" != "$original_head" ]; then
      log "ERROR: HEAD is not $original_head after cleanup; failing the job"
      status=1
    fi
  fi
  [ -n "$work" ] && rm -rf "$work"
  log "finished in $((SECONDS - start)) s"
  exit "$status"
}

# Sets $base to the commit the pull request branched from, using GitHub's test
# merge of the head into its base branch (refs/pull/N/merge). The repository is
# private, so no API call without a token; the ref works for stacked pull
# requests, whose base is another branch.
discover_base() {
  local pr=${CIRCLE_PULL_REQUEST##*/} pr_head merge_commit tip pr_head_parent extra tries=5
  case "$pr" in
    ''|*[!0-9]*) run_everything "CIRCLE_PULL_REQUEST has no pull request number ($CIRCLE_PULL_REQUEST)"; return ;;
  esac
  pr_head=$(git rev-parse HEAD) || { run_everything "git rev-parse HEAD failed"; return; }
  local ref=refs/remotes/pull/$pr/merge
  while :; do
    if git fetch --quiet --filter=blob:none origin "+refs/pull/$pr/merge:$ref" 2>/dev/null; then
      # "<merge> <first parent: base tip> <second parent: pull request head>"
      if read -r merge_commit tip pr_head_parent extra < <(git rev-list --parents -n 1 "$ref" 2>/dev/null) \
        && [ -n "$tip" ] && [ -z "${extra:-}" ] && [ "$pr_head_parent" = "$pr_head" ]; then
        break
      fi
    fi
    tries=$((tries - 1))
    if [ "$tries" -le 0 ]; then
      run_everything "refs/pull/$pr/merge is missing or does not match $pr_head (conflicts, or GitHub has not computed it yet)"
      return
    fi
    log "refs/pull/$pr/merge is not ready for $pr_head; retrying in 5 s"
    sleep 5
  done
  if ! git cat-file -e "$tip^{commit}" 2>/dev/null; then
    git fetch --quiet --filter=blob:none origin "$tip" 2>/dev/null \
      || { run_everything "could not fetch the base tip $tip"; return; }
  fi
  base=$(git merge-base "$tip" "$pr_head") \
    || { run_everything "no merge base between $tip and $pr_head"; return; }
  log "pull request #$pr: base tip $tip, merge base $base"
}

# Returns 1 (after writing the fallback) when a changed file can change every
# target's build, which bazel-diff cannot see.
check_global_files() {
  local changed file
  changed=$(git diff --name-only --no-renames "$base" HEAD) \
    || { run_everything "git diff $base HEAD failed"; return 1; }
  while IFS= read -r file; do
    case "$file" in
      .bazelrc|*.bazelrc|MODULE.bazel|MODULE.bazel.lock|.bazelversion|.bazelignore|WORKSPACE*|*/WORKSPACE*|Cargo.lock|runtime/uv.lock|.circleci/*|Cargo.toml|*/Cargo.toml|runtime/pyproject.toml|rust-toolchain*|.cargo/*|.bazeliskrc|*.bzl|*.patch)
        run_everything "$file changed, and it can affect every target"; return 1 ;;
    esac
  done <<< "$changed"
  return 0
}

fetch_bazel_diff() {
  if [ -n "${BAZEL_DIFF_BIN:-}" ]; then
    bazel_diff=$BAZEL_DIFF_BIN
    [ -x "$bazel_diff" ] || { run_everything "BAZEL_DIFF_BIN ($bazel_diff) is not executable"; return 1; }
    return 0
  fi
  local tool sum
  for tool in curl sha256sum; do
    command -v "$tool" >/dev/null 2>&1 || { run_everything "$tool is not installed"; return 1; }
  done
  bazel_diff=$work/bazel-diff
  curl -fsSL --retry 3 --retry-delay 2 -o "$bazel_diff" "$BAZEL_DIFF_URL" \
    || { run_everything "downloading bazel-diff $BAZEL_DIFF_VERSION failed"; return 1; }
  sum=$(sha256sum "$bazel_diff" | cut -d' ' -f1)
  [ "$sum" = "$BAZEL_DIFF_SHA256" ] \
    || { run_everything "the bazel-diff download has sha256 $sum, expected $BAZEL_DIFF_SHA256"; return 1; }
  chmod +x "$bazel_diff"
}

hash_commit() { # <commit> <output file>
  checkout_commit "$1" || return 1
  # Word splitting of the options is intended.
  # shellcheck disable=SC2086
  "$bazel_diff" generate-hashes -w "$PWD" -b bazel --excludeExternalTargets \
    ${SELECT_TESTS_BAZEL_OPTIONS:+--bazelCommandOptions="$SELECT_TESTS_BAZEL_OPTIONS"} \
    "$2"
}

main() {
  if [ -z "$out" ]; then
    echo "usage: select-tests.sh <output file>" >&2
    return 2
  fi
  echo '//...' > "$out"
  trap cleanup EXIT
  trap 'exit 143' TERM
  trap 'exit 130' INT

  if [ "${CIRCLE_BRANCH:-}" = main ]; then
    run_everything "the branch is main"; return 0
  fi
  if [ -z "${CIRCLE_PULL_REQUEST:-}" ]; then
    run_everything "the branch has no open pull request"; return 0
  fi
  local tool
  for tool in git bazel; do
    command -v "$tool" >/dev/null 2>&1 || { run_everything "$tool is not installed"; return 0; }
  done

  original_head=$(git rev-parse HEAD) || { run_everything "git rev-parse HEAD failed"; return 0; }
  work=$(mktemp -d) || { run_everything "mktemp failed"; return 0; }

  local base=""
  discover_base
  [ -n "$base" ] || return 0
  check_global_files || return 0

  local bazel_diff=""
  fetch_bazel_diff || return 0

  log "hashing the targets at the pull request head"
  hash_commit "$original_head" "$work/head.json" \
    || { run_everything "bazel-diff failed at the head"; return 0; }
  log "hashing the targets at the merge base"
  hash_commit "$base" "$work/base.json" \
    || { run_everything "bazel-diff failed at the merge base"; return 0; }
  checkout_commit "$original_head" \
    || { run_everything "could not return to $original_head"; return 0; }

  "$bazel_diff" get-impacted-targets -w "$PWD" -b bazel --excludeExternalTargets \
    --startingHashes "$work/base.json" --finalHashes "$work/head.json" \
    -o "$work/impacted.txt" \
    || { run_everything "bazel-diff get-impacted-targets failed"; return 0; }

  [ -f "$work/impacted.txt" ] \
    || { run_everything "bazel-diff wrote no output"; return 0; }
  local impacted
  impacted=$(grep -c . "$work/impacted.txt")
  log "bazel-diff found $impacted impacted targets"
  : > "$work/selection.txt"
  if [ "$impacted" -gt 0 ]; then
    # Each label is quoted so that characters such as `+` or `@` cannot break
    # the query syntax.
    {
      printf 'tests(set('
      grep . "$work/impacted.txt" | sed 's/.*/"&"/' | tr '\n' ' '
      printf '))\n'
    } > "$work/query.txt"
    # shellcheck disable=SC2086
    bazel query ${SELECT_TESTS_BAZEL_OPTIONS:-} --query_file="$work/query.txt" > "$work/tests.txt" \
      || { run_everything "bazel query for the impacted tests failed"; return 0; }
    # An empty result is legitimate: no test depends on the impacted targets.
    grep '^//' "$work/tests.txt" | sort -u > "$work/selection.txt"
  fi
  cp "$work/selection.txt" "$out"
  if [ ! -s "$out" ]; then
    log "no test targets are impacted; nothing to run"
    return 0
  fi
  log "selected $(wc -l < "$out" | tr -d ' ') test targets:"
  sed 's/^/select-tests:   /' "$out"
}

# The whole file is parsed before main runs, so a checkout that changes this
# script on disk cannot disturb the running copy. The EXIT trap's status is the
# script's status.
main
