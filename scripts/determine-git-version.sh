#!/bin/bash

# This script calculates version information for the build process
# Usage:
#   - For GitHub Actions: GITHUB_OUTPUT is set automatically
#   - For Makefile: use --makefile flag
#   - For shell sourcing: no flags needed

# Check for --makefile flag
MAKEFILE_MODE=false
if [ "$1" = "--makefile" ]; then
  MAKEFILE_MODE=true
fi

# Resolve the tag for HEAD.
#
# When the workflow was triggered by a tag push, GITHUB_REF is the exact ref
# that fired it (e.g. refs/tags/v0.15.8). Trusting it here makes the picked
# tag deterministic: a release-branch tip commonly carries BOTH a v0.15.8
# and a v0.15.8-beta-2 tag (we tag the beta, promote to stable at the same
# SHA), and `git describe --tags --exact-match` breaks ties by tagger date
# in a way that varies across git versions and fetch orders — so a stable
# build could stamp itself as the earlier beta.
#
# For local Makefile / shell use (no GITHUB_REF), fall back to git and
# prefer a plain vX.Y.Z tag over any prerelease tag pointing at the same
# commit; then fall back to `describe --exact-match` for anything else
# (e.g. -rc, custom prefixes).
GIT_TAG=""
if [ -n "$GITHUB_REF" ] && [[ "$GITHUB_REF" == refs/tags/* ]]; then
  GIT_TAG="${GITHUB_REF#refs/tags/}"
else
  # Prefer a stable release tag (vMAJOR.MINOR.PATCH) if one is present.
  # `grep -E` is POSIX and works on both GNU and BSD (macOS) — unlike
  # `sort -V`, which is a GNU coreutils extension and errors out silently
  # on the default macOS `sort`. Multiple stable tags on one commit is
  # never expected, so head -n 1 is deterministic enough here.
  GIT_TAG=$(git tag --points-at HEAD 2>/dev/null \
    | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' \
    | head -n 1)

  if [ -z "$GIT_TAG" ]; then
    GIT_TAG=$(git describe --tags --exact-match 2>/dev/null || echo "")
  fi
fi

GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GIT_SHA=$(git rev-parse HEAD 2>/dev/null || echo "unknown")
GIT_TIMESTAMP=$(git show -s --format=%cd --date=format:%Y%m%d%H%M%S HEAD 2>/dev/null || date +%Y%m%d%H%M%S)

# Generate version using same logic as Makefile
if [ -z "$GIT_TAG" ]; then
  GIT_VERSION="v0.0.0-${GIT_TIMESTAMP}-${GIT_COMMIT}"
elif [[ "$GIT_TAG" =~ ^v.* ]]; then
  GIT_VERSION="$GIT_TAG"
else
  GIT_VERSION="v0.0.0-${GIT_TIMESTAMP}-${GIT_COMMIT}"
fi

# Output based on mode
if [ -n "$GITHUB_OUTPUT" ]; then
  # GitHub Actions mode
  echo "git_version=$GIT_VERSION" >> $GITHUB_OUTPUT
  echo "git_commit=$GIT_COMMIT" >> $GITHUB_OUTPUT
  echo "git_sha=$GIT_SHA" >> $GITHUB_OUTPUT
  
  echo "Calculated GIT_VERSION: $GIT_VERSION"
  echo "Calculated GIT_COMMIT: $GIT_COMMIT"
  echo "Calculated GIT_SHA: $GIT_SHA"
elif [ "$MAKEFILE_MODE" = true ]; then
  # Makefile mode - output variable assignments
  echo "GIT_VERSION=$GIT_VERSION"
  echo "GIT_COMMIT=$GIT_COMMIT"
  echo "GIT_SHA=$GIT_SHA"
  echo "GIT_TAG=$GIT_TAG"
  echo "GIT_TIMESTAMP=$GIT_TIMESTAMP"
else
  # Shell sourcing mode
  echo "export GIT_VERSION=\"$GIT_VERSION\""
  echo "export GIT_COMMIT=\"$GIT_COMMIT\""
  echo "export GIT_SHA=\"$GIT_SHA\""
  echo "export GIT_TAG=\"$GIT_TAG\""
  echo "export GIT_TIMESTAMP=\"$GIT_TIMESTAMP\""
fi