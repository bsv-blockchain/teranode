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

# Get git values similar to Makefile
GIT_TAG=$(git describe --tags --exact-match 2>/dev/null || echo "")
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