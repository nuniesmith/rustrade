#!/usr/bin/env bash
# release.sh — set the workspace version, commit, tag, push, then publish
# every crate to crates.io in dependency order. Safe to re-run: every step
# is idempotent, so a release that fails partway is resumed by re-running.
#
# Usage:
#   ./scripts/release.sh                 # publish the current Cargo.toml version
#   ./scripts/release.sh <x.y.z>         # set to an explicit version, then publish
#   ./scripts/release.sh patch|minor|major
#   add --dry-run to rehearse without committing, tagging, or publishing.
#
# The version is taken from [workspace.package].version in Cargo.toml —
# NEVER from git tags (a missing/stale tag must not silently downgrade the
# crate). All five internal path-dependency requirements in
# [workspace.dependencies] are bumped in lockstep. A downgrade aborts.
set -euo pipefail

# ── Repo root (works regardless of CWD) ───────────────────────────────────────
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
CARGO="Cargo.toml"

# Publish order: a crate must be on crates.io before any crate that depends
# on it. core → risk → supervisor → backtest → the facade. These are
# crates.io *package* names (the facade's package is `rustrade-framework`,
# though it's imported as `rustrade`).
PUBLISH_ORDER=(
    rustrade-core
    rustrade-risk
    rustrade-supervisor
    rustrade-backtest
    rustrade-framework
)

# ── Args ──────────────────────────────────────────────────────────────────────
DRY_RUN=false
BUMP_ARG=""
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=true ;;
        -h|--help) sed -n '2,15p' "$0"; exit 0 ;;
        *) BUMP_ARG="$arg" ;;
    esac
done
$DRY_RUN && echo "==> DRY RUN — no commits, tags, pushes, or publishes"

run() { if $DRY_RUN; then echo "[dry-run] $*"; else "$@"; fi; }

# ── HTTP fetch + crates.io index helpers (for "already published?" checks) ────
if command -v curl >/dev/null 2>&1 || command -v wget >/dev/null 2>&1; then
    HAVE_FETCH=true
else
    HAVE_FETCH=false
    echo "==> note: neither curl nor wget found — falling back to fixed waits"
fi

fetch() {
    if command -v curl >/dev/null 2>&1; then curl -fsSL "$1" 2>/dev/null
    else wget -qO- "$1" 2>/dev/null; fi
}

# Is exactly this name@version already on the crates.io sparse index?
# All our crate names are >= 4 chars, so the path is aa/bb/<name>.
crate_version_published() {
    local name="$1" ver="$2"
    $HAVE_FETCH || return 1
    local body
    body="$(fetch "https://index.crates.io/${name:0:2}/${name:2:2}/${name}")" || return 1
    grep -q "\"vers\":\"${ver}\"" <<<"$body"
}

# Block until name@version is visible on the index (so the next dependent can
# resolve it), or give up after ~2.5 min and continue.
wait_for_index() {
    local name="$1" ver="$2"
    if ! $HAVE_FETCH; then
        echo "       waiting 25s for the index to update…"
        sleep 25
        return 0
    fi
    printf "       waiting for %s %s to appear on the index" "$name" "$ver"
    for _ in $(seq 1 30); do
        if crate_version_published "$name" "$ver"; then echo " ✓"; return 0; fi
        printf "."
        sleep 5
    done
    echo " (timeout — continuing)"
}

# ── crates.io auth: check, and prompt for a token if missing ──────────────────
ensure_cargo_auth() {
    if $DRY_RUN; then
        echo "==> [dry-run] skipping crates.io auth check"
        return
    fi
    if [[ -n "${CARGO_REGISTRY_TOKEN:-}" ]]; then
        echo "==> Using CARGO_REGISTRY_TOKEN from the environment"
        return
    fi
    local home="${CARGO_HOME:-$HOME/.cargo}"
    if grep -qsE '^\s*token\s*=' "$home/credentials.toml" "$home/credentials" 2>/dev/null; then
        echo "==> crates.io credentials found in $home"
        return
    fi
    echo "==> Not logged in to crates.io."
    echo "    Create a token (scopes: publish-new + publish-update) at:"
    echo "      https://crates.io/settings/tokens"
    # `cargo login` reads the token from stdin without echoing and stores it.
    # If stdin isn't a TTY (CI), require the env var instead of hanging.
    if [[ ! -t 0 ]]; then
        echo "ERROR: no TTY to prompt for a token. Set CARGO_REGISTRY_TOKEN and re-run."
        exit 1
    fi
    cargo login
}

# ── Current version (from [workspace.package], not from tags) ─────────────────
CURRENT=$(grep -A20 '^\[workspace.package\]' "$CARGO" \
    | grep -E '^\s*version\s*=' | head -n1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
if [[ -z "$CURRENT" ]]; then
    echo "ERROR: could not read [workspace.package].version from $CARGO"
    exit 1
fi
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT"

# ── Resolve the target version (no arg = publish the current version as-is) ───
if [[ -z "$BUMP_ARG" ]]; then
    NEW_VERSION="$CURRENT"
    echo "==> No version argument — publishing current version $CURRENT as-is"
else
    case "$BUMP_ARG" in
        major) NEW_VERSION="$((MAJOR + 1)).0.0" ;;
        minor) NEW_VERSION="${MAJOR}.$((MINOR + 1)).0" ;;
        patch) NEW_VERSION="${MAJOR}.${MINOR}.$((PATCH + 1))" ;;
        [0-9]*.[0-9]*.[0-9]*) NEW_VERSION="$BUMP_ARG" ;;
        *) echo "ERROR: invalid version/bump '$BUMP_ARG'"; exit 1 ;;
    esac
    echo "==> Version: $CURRENT -> $NEW_VERSION"
fi
NEW_TAG="v${NEW_VERSION}"

# ── Guard: never downgrade (sort -V puts the larger version last) ─────────────
if [[ "$NEW_VERSION" != "$CURRENT" ]]; then
    LOWER=$(printf '%s\n%s\n' "$CURRENT" "$NEW_VERSION" | sort -V | head -n1)
    if [[ "$LOWER" == "$NEW_VERSION" ]]; then
        echo "ERROR: $NEW_VERSION is older than current $CURRENT — refusing to downgrade."
        exit 1
    fi
fi

# ── Working tree must be clean unless we're only re-publishing the current
#    version (no Cargo.toml edit needed in that case). ──────────────────────────
if [[ "$NEW_VERSION" != "$CURRENT" && -n "$(git status --porcelain)" ]]; then
    echo "ERROR: working tree is dirty — commit or stash changes first"
    exit 1
fi

# Confirm we can talk to crates.io before doing any git mutation.
ensure_cargo_auth

# ── Rewrite Cargo.toml: workspace version + all internal dep requirements ─────
# One precise pass (not a single global sed): set [workspace.package].version
# and the `version = "..."` on every internal path-dependency line.
if [[ "$NEW_VERSION" != "$CURRENT" ]] && ! $DRY_RUN; then
    NEW_VERSION="$NEW_VERSION" python3 - "$CARGO" <<'PY'
import os, re, sys
path = sys.argv[1]
new = os.environ["NEW_VERSION"]
section = None
out = []
for line in open(path).read().splitlines(keepends=True):
    s = line.strip()
    if s.startswith("[") and s.endswith("]"):
        section = s[1:-1]
    if section == "workspace.package" and re.match(r'\s*version\s*=', line):
        line = re.sub(r'(version\s*=\s*)"[^"]*"', rf'\g<1>"{new}"', line)
    if 'path' in line and 'crates/' in line and 'version' in line:
        line = re.sub(r'(version\s*=\s*)"[^"]*"', rf'\g<1>"{new}"', line)
    out.append(line)
open(path, "w").write("".join(out))
PY
    echo "==> Updated $CARGO to $NEW_VERSION"
    grep -nE '^\s*version\s*=|crates/.*version' "$CARGO" | grep "$NEW_VERSION" | head
elif [[ "$NEW_VERSION" != "$CURRENT" ]]; then
    echo "[dry-run] would set workspace + internal dep versions to $NEW_VERSION"
fi

# ── Sanity: the whole workspace still resolves + builds at the version ────────
echo "==> cargo check --workspace"
run cargo check --workspace --all-features

# ── Commit (only if the version actually changed) ─────────────────────────────
if [[ "$NEW_VERSION" != "$CURRENT" ]]; then
    run git add "$CARGO"
    if $DRY_RUN; then
        echo "[dry-run] git commit -m \"chore: release ${NEW_VERSION}\""
    elif git diff --cached --quiet; then
        echo "==> nothing staged to commit"
    else
        git commit -m "chore: release ${NEW_VERSION}"
    fi
fi

# ── Tag (idempotent) ──────────────────────────────────────────────────────────
if $DRY_RUN; then
    echo "[dry-run] git tag -a $NEW_TAG -m \"Release ${NEW_TAG}\""
elif git rev-parse "$NEW_TAG" >/dev/null 2>&1; then
    echo "==> Tag $NEW_TAG already exists — reusing it"
else
    git tag -a "$NEW_TAG" -m "Release ${NEW_TAG}"
fi

# ── Push branch + tag (idempotent; a no-op if already up to date) ─────────────
BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo "==> Pushing '$BRANCH' and tag '$NEW_TAG'"
run git push origin "$BRANCH"
run git push origin "$NEW_TAG"

# ── Publish in dependency order (skip crates already on the index) ────────────
echo "==> Publishing to crates.io in dependency order"
for crate in "${PUBLISH_ORDER[@]}"; do
    if crate_version_published "$crate" "$NEW_VERSION"; then
        echo "    -- $crate $NEW_VERSION already published — skipping"
        continue
    fi
    if $DRY_RUN; then
        echo "    -- [dry-run] cargo publish -p $crate"
        continue
    fi
    echo "    -- publishing $crate $NEW_VERSION"
    if ! cargo publish -p "$crate"; then
        # Tolerate the race where it landed anyway (e.g. a retry); fail on
        # anything else so we never silently skip a genuinely failed crate.
        if crate_version_published "$crate" "$NEW_VERSION"; then
            echo "       ($crate already uploaded — continuing)"
        else
            echo "ERROR: publishing $crate failed (see output above)."
            echo "       Fix the issue and re-run; already-published crates are skipped."
            exit 1
        fi
    fi
    # Let the index catch up so the next dependent resolves (skip on the last).
    if [[ "$crate" != "${PUBLISH_ORDER[-1]}" ]]; then
        wait_for_index "$crate" "$NEW_VERSION"
    fi
done

echo ""
if $DRY_RUN; then
    echo "✓ Dry run complete — ${NEW_TAG} would be released."
else
    echo "✓ Released ${NEW_TAG} — all crates published to crates.io."
fi
