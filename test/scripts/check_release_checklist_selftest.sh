#!/bin/bash
# shellcheck disable=SC2016 # $1 is a perl capture group and $(...) is a literal ref-name payload; neither must expand.
# Exercises scripts/check-release-checklist.sh against checklists built from the template in a scratch repository.
set -euo pipefail
cd "$(dirname "$0")/../.."
SCRIPT="$PWD/scripts/check-release-checklist.sh"
TEMPLATE="$PWD/docs/releases/checklist-template.md"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

git -C "$work" init -q
git -C "$work" -c user.email=selftest@example.com -c user.name=selftest commit -q --allow-empty -m base
beta_sha="$(git -C "$work" rev-parse HEAD)"
other_sha="$(printf '%040d' 1)"
mkdir -p "$work/docs/releases"

# checklist writes the template for <tag> with a Go decision for the beta SHA, then applies an extra perl substitution.
checklist() {
  local tag="$1" extra="${2:-}"
  BETA_SHA="$beta_sha" perl -pe '
    s/^- \*\*Beta SHA:\*\*.*/- **Beta SHA:** $ENV{BETA_SHA}/;
    s/^- \*\*Approver \(maintainer\):\*\*.*/- **Approver (maintainer):** A Maintainer/;
    s/^- \*\*Date:\*\*.*/- **Date:** 2026-11-16/;
    s/^\*\*Decision:\*\* No-Go$/**Decision:** Go/;
  '"$extra" "$TEMPLATE" > "$work/docs/releases/${tag}-checklist.md"
}

checklist v1.0.0
cp "$TEMPLATE" "$work/docs/releases/v1.0.1-checklist.md"
checklist v1.0.2 's/^\*\*Decision:\*\* Go$/- **Decision:** Go/;'
checklist v1.0.3 's/^\*\*Decision:\*\* Go$/**Decision:** Go\n\n**Decision:** No-Go - soak failed/;'
checklist v1.0.4 's/^\*\*Decision:\*\* Go$/**Decision:** Go\n**Decision:** Go/;'
checklist v1.0.5 "s/^(- \\*\\*Beta SHA:\\*\\*).*/\\1 $other_sha/;"
checklist v1.0.6 "s/^(- \\*\\*Beta SHA:\\*\\*).*/\\1 ${beta_sha:0:12}/;"
checklist v1.0.7 's/^(- \*\*Approver \(maintainer\):\*\*).*/$1/;'
checklist v1.0.8 's/^(- \*\*Date:\*\*).*/$1 16 November/;'
checklist v1.0.9 's/\n/\r\n/;'
checklist v1.0.10 's/^\*\*Decision:\*\* Go$/**Decision:** Gone/;'
checklist v1.0.11 's/^\*\*Decision:\*\* Go$/**Decision:** Go\n\n**Decision:**/;'
checklist v1.0.12 's/^\*\*Decision:\*\* Go$/**Decision:** No-Go/;'
git -C "$work" add docs
git -C "$work" -c user.email=selftest@example.com -c user.name=selftest commit -q -m checklists

failures=0

# expect <want-exit> <description> <ref-type> <ref-name>
expect() {
  local want="$1" desc="$2" got=0
  (cd "$work" && "$SCRIPT" "$3" "$4" HEAD "$beta_sha") > "$work/out" 2>&1 || got=$?
  if [ "$got" -ne "$want" ]; then
    echo "FAIL: $desc ($3 $4): exit $got, want $want"; sed 's/^/    /' "$work/out"
    failures=$((failures + 1))
  else
    echo "ok:   $desc ($3 $4): exit $got"
  fi
}

expect 0 "branch push" branch main
expect 0 "branch name with command substitution" branch 'release/x$(touch pwned)'
expect 0 "beta tag" tag v1.0.0-beta-1
expect 0 "rc tag" tag v1.0.0-rc-2
expect 0 "variant beta tag" tag v0.16.1-beta-txmeta-2
expect 0 "variant rc tag" tag v1.0.0-rc-scale-1
expect 0 "non-version tag" tag v-scaling-test-20260506-1
expect 0 "non-version tag with command substitution" tag 'v$(touch pwned)'
expect 1 "version-shaped hotfix tag" tag v0.16.3-hotfix1
expect 1 "four-part version tag" tag v0.16.3.1
expect 1 "beta tag with wrong separator" tag v1.0.0-beta.1
expect 1 "stable tag with no checklist" tag v9.9.9
expect 0 "completed Go checklist" tag v1.0.0
expect 1 "unedited template (No-Go)" tag v1.0.1
expect 0 "Go decision as a list item" tag v1.0.2
expect 1 "Go followed by a later No-Go" tag v1.0.3
expect 1 "two Decision lines" tag v1.0.4
expect 1 "beta SHA differs from the tagged commit" tag v1.0.5
expect 1 "abbreviated beta SHA" tag v1.0.6
expect 1 "no approver" tag v1.0.7
expect 1 "date not in YYYY-MM-DD form" tag v1.0.8
expect 0 "CRLF line endings" tag v1.0.9
expect 1 "decision that only starts with Go" tag v1.0.10
expect 1 "Go followed by a blank Decision line" tag v1.0.11
expect 1 "sole No-Go with full metadata" tag v1.0.12

if [ -e "$work/pwned" ]; then
  echo "FAIL: a ref name ran a command"
  failures=$((failures + 1))
fi

if [ "$failures" -ne 0 ]; then
  echo "FAIL: $failures case(s)"
  exit 1
fi
echo "PASS: all cases"
