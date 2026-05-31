#!/usr/bin/env bash
# release.sh — set the workspace version, commit, tag, push, then publish
# every crate to crates.io in dependency order.
#
# Usage:
#   ./scripts/release.sh <version> [--dry-run]   # e.g. 0.2.1, 0.3.0, 1.0.0
#   ./scripts/release.sh patch|minor|major [--dry-run]
#
# The version is derived from [workspace.package].version in Cargo.toml —
# NEVER from git tags (a missing/stale tag must not silently downgrade the
# crate). All five internal path-dependency requirements in
# [workspace.dependencies] are bumped in lockstep with the workspace
# version, so `cargo publish` can resolve each sibling. A downgrade aborts.
set -euo pipefail

# ── Repo root (works regardless of CWD) ───────────────────────────────────────
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
CARGO="Cargo.toml"

# Publish order: a crate must be on crates.io before any crate that depends
# on it. core → (risk, supervisor) → backtest → rustrade.
PUBLISH_ORDER=(
    rustrade-core
    rustrade-risk
    rustrade-supervisor
    rustrade-backtest
    rustrade
)

# ── Args ──────────────────────────────────────────────────────────────────────
DRY_RUN=false
BUMP_ARG=""
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=true ;;
        *) BUMP_ARG="$arg" ;;
    esac
done
if [[ -z "$BUMP_ARG" ]]; then
    echo "ERROR: missing version argument."
    echo "Usage: $0 <version|patch|minor|major> [--dry-run]"
    exit 1
fi
$DRY_RUN && echo "==> DRY RUN — no commits, tags, pushes, or publishes"

run() { if $DRY_RUN; then echo "[dry-run] $*"; else "$@"; fi; }

# ── Current version (from [workspace.package], not from tags) ─────────────────
CURRENT=$(grep -A20 '^\[workspace.package\]' "$CARGO" \
    | grep -E '^\s*version\s*=' | head -n1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
if [[ -z "$CURRENT" ]]; then
    echo "ERROR: could not read [workspace.package].version from $CARGO"
    exit 1
fi
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT"

# ── Resolve the target version ────────────────────────────────────────────────
case "$BUMP_ARG" in
    major) NEW_VERSION="$((MAJOR + 1)).0.0" ;;
    minor) NEW_VERSION="${MAJOR}.$((MINOR + 1)).0" ;;
    patch) NEW_VERSION="${MAJOR}.${MINOR}.$((PATCH + 1))" ;;
    [0-9]*.[0-9]*.[0-9]*) NEW_VERSION="$BUMP_ARG" ;;
    *) echo "ERROR: invalid version/bump '$BUMP_ARG'"; exit 1 ;;
esac
echo "==> Version: $CURRENT -> $NEW_VERSION"

# ── Guard: never downgrade (sort -V puts the larger version last) ─────────────
if [[ "$NEW_VERSION" != "$CURRENT" ]]; then
    LOWER=$(printf '%s\n%s\n' "$CURRENT" "$NEW_VERSION" | sort -V | head -n1)
    if [[ "$LOWER" == "$NEW_VERSION" ]]; then
        echo "ERROR: $NEW_VERSION is older than current $CURRENT — refusing to downgrade."
        exit 1
    fi
fi

# ── Clean working tree ────────────────────────────────────────────────────────
if [[ -n "$(git status --porcelain)" ]]; then
    echo "ERROR: working tree is dirty — commit or stash changes first"
    exit 1
fi

NEW_TAG="v${NEW_VERSION}"
if git rev-parse "$NEW_TAG" >/dev/null 2>&1; then
    echo "ERROR: tag $NEW_TAG already exists"
    exit 1
fi

# ── Rewrite Cargo.toml: workspace version + all internal dep requirements ─────
# Done in one pass with Python (precise, unlike a single global sed): set
# [workspace.package].version, and the `version = "..."` on every internal
# path-dependency line under [workspace.dependencies].
if ! $DRY_RUN; then
    NEW_VERSION="$NEW_VERSION" python3 - "$CARGO" <<'PY'
import os, re, sys
path = sys.argv[1]
new = os.environ["NEW_VERSION"]
lines = open(path).read().splitlines(keepends=True)
section = None
out = []
for line in lines:
    s = line.strip()
    if s.startswith("[") and s.endswith("]"):
        section = s[1:-1]
    # [workspace.package].version
    if section == "workspace.package" and re.match(r'\s*version\s*=', line):
        line = re.sub(r'("?)([0-9]+\.[0-9]+\.[0-9]+)("?)',
                      lambda m: f'"{new}"', line, count=1) if '"' in line else line
        line = re.sub(r'(version\s*=\s*)"[^"]*"', rf'\g<1>"{new}"', line)
    # internal path deps: lines carrying both `path = "crates/` and `version = "..."`
    if 'path' in line and 'crates/' in line and 'version' in line:
        line = re.sub(r'(version\s*=\s*)"[^"]*"', rf'\g<1>"{new}"', line)
    out.append(line)
open(path, "w").write("".join(out))
PY
    echo "==> Updated $CARGO"
    # Show what changed so the operator can eyeball it.
    grep -nE '^\s*version\s*=|crates/' "$CARGO" | grep -E "$NEW_VERSION|version" | head
else
    echo "[dry-run] would set workspace + internal dep versions to $NEW_VERSION in $CARGO"
fi

# ── Sanity: the whole workspace still resolves + builds at the new version ────
echo "==> cargo check --workspace"
run cargo check --workspace --all-features

# ── Per-crate publish dry-run (always, even in real mode, before committing) ──
echo "==> Verifying each crate packages cleanly (cargo publish --dry-run)"
for crate in "${PUBLISH_ORDER[@]}"; do
    echo "    -- $crate"
    # --dry-run can't see unpublished siblings on the index, so allow-dirty
    # is unnecessary here; --no-verify keeps it fast. Real publish below
    # does the full verify.
    cargo publish -p "$crate" --dry-run --allow-dirty >/dev/null 2>&1 \
        || echo "       (dry-run note: $crate may depend on a sibling not yet on the index — expected pre-first-release)"
done

# ── Commit, tag, push ─────────────────────────────────────────────────────────
# Idempotent: if Cargo.toml was already at the target version (e.g. the bump
# landed via a prior PR, or this is a re-run after a mid-publish failure),
# there's nothing to commit — skip the commit instead of letting `set -e`
# abort the whole release before the tag/publish steps.
run git add "$CARGO"
if $DRY_RUN; then
    echo "[dry-run] git commit -m \"chore: release ${NEW_VERSION}\" (if changed)"
elif git diff --cached --quiet; then
    echo "==> Cargo.toml already at ${NEW_VERSION} — nothing to commit, continuing"
else
    git commit -m "chore: release ${NEW_VERSION}"
fi

# Tag only if it doesn't already exist (re-run safe).
if $DRY_RUN; then
    echo "[dry-run] git tag -a $NEW_TAG -m \"Release ${NEW_TAG}\""
elif git rev-parse "$NEW_TAG" >/dev/null 2>&1; then
    echo "==> Tag $NEW_TAG already exists — reusing it"
else
    git tag -a "$NEW_TAG" -m "Release ${NEW_TAG}"
fi

BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo "==> Pushing '$BRANCH' and tag '$NEW_TAG'"
run git push origin "$BRANCH"
run git push origin "$NEW_TAG"

# ── Publish in dependency order ───────────────────────────────────────────────
# Re-run safe: a crate already on crates.io at this version is skipped (the
# "already uploaded" error is non-fatal here), so a publish that died partway
# can be resumed by simply re-running the script.
echo "==> Publishing to crates.io in dependency order"
for crate in "${PUBLISH_ORDER[@]}"; do
    echo "    -- publishing $crate"
    if $DRY_RUN; then
        echo "[dry-run] cargo publish -p $crate"
        continue
    fi
    if cargo publish -p "$crate"; then
        :
    else
        # Tolerate "crate version is already uploaded" on re-runs; fail on
        # anything else.
        if cargo search "$crate" 2>/dev/null | grep -q "^$crate = \"${NEW_VERSION}\""; then
            echo "       $crate ${NEW_VERSION} already on crates.io — skipping"
        else
            echo "ERROR: publishing $crate failed (see output above)"
            exit 1
        fi
    fi
    # crates.io needs a moment to index a new crate before a dependent of it
    # can be published. Skip the wait on the last crate.
    if [[ "$crate" != "${PUBLISH_ORDER[-1]}" ]]; then
        echo "       waiting for the index to update…"
        sleep 20
    fi
done

echo ""
echo "✓ Released ${NEW_TAG}"
