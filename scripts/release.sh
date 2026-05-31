#!/usr/bin/env bash
# release.sh — bump patch version, commit, tag, push, then publish
# Usage: ./release.sh [--dry-run]
set -euo pipefail

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=true
    echo "==> DRY RUN — no changes will be made"
fi

run() {
    if $DRY_RUN; then
        echo "[dry-run] $*"
    else
        "$@"
    fi
}

# ── 1. Make sure the working tree is clean ────────────────────────────────────
if [[ -n "$(git status --porcelain)" ]]; then
    echo "ERROR: working tree is dirty — commit or stash changes first"
    exit 1
fi

# ── 2. Determine the next version ─────────────────────────────────────────────
LATEST_TAG=$(git tag --list 'v*.*.*' --sort=-version:refname | head -n1)
LATEST_TAG="${LATEST_TAG:-v0.0.0}"
echo "==> Latest tag: $LATEST_TAG"

VERSION="${LATEST_TAG#v}"
MAJOR="${VERSION%%.*}"
REST="${VERSION#*.}"
MINOR="${REST%%.*}"
PATCH="${REST##*.}"

NEW_PATCH=$(( PATCH + 1 ))
NEW_VERSION="${MAJOR}.${MINOR}.${NEW_PATCH}"
NEW_TAG="v${NEW_VERSION}"
echo "==> New version: $NEW_VERSION  (tag: $NEW_TAG)"

# ── 3. Update Cargo.toml ──────────────────────────────────────────────────────
CARGO="Cargo.toml"
if [[ ! -f "$CARGO" ]]; then
    echo "ERROR: $CARGO not found — run this script from the crate root"
    exit 1
fi

CURRENT_CARGO_VERSION=$(grep -E '^version\s*=' "$CARGO" | head -n1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
echo "==> Cargo.toml version: $CURRENT_CARGO_VERSION -> $NEW_VERSION"

if ! $DRY_RUN; then
    sed -i "0,/^\(version\s*=\s*\)\"[^\"]*\"/s//\1\"${NEW_VERSION}\"/" "$CARGO"
fi

# ── 4. Commit and tag (Cargo.lock intentionally excluded if gitignored) ───────
run git add Cargo.toml
run git commit -m "chore: bump version to ${NEW_VERSION}"
run git tag -a "$NEW_TAG" -m "Release ${NEW_TAG}"

# ── 5. Push commit and tag before publishing ──────────────────────────────────
BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo "==> Pushing branch '$BRANCH' and tag '$NEW_TAG'"
run git push origin "$BRANCH"
run git push origin "$NEW_TAG"

# ── 6. Publish ────────────────────────────────────────────────────────────────
echo "==> Publishing to crates.io"
run cargo publish

echo ""
echo "✓ Released ${NEW_TAG}"
