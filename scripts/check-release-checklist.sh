#!/usr/bin/env bash
#
# Fails a stable release tag build that has no completed release checklist.
#
# Usage: check-release-checklist.sh <ref-type> <ref-name> [checklist-ref] [tagged-sha]
#
# A stable tag is vX.Y.Z with no suffix. Branches, beta tags (vX.Y.Z-beta-N, and
# variants such as vX.Y.Z-beta-txmeta-N), rc tags (vX.Y.Z-rc-...) and tags that
# are not version-shaped (such as v-scaling-test-*) pass. Any other tag that
# starts with vN. fails.
#
# For a stable tag, docs/releases/<tag>-checklist.md must exist at checklist-ref
# (default origin/main) and record:
#   - exactly one "**Decision:**" line, and it says Go
#   - a "**Beta SHA:**" equal to tagged-sha (default: the commit at HEAD)
#   - an "**Approver (maintainer):**" and a "**Date:**" in YYYY-MM-DD form
#
# The checklist is read from main, not from the tagged commit: a stable tag
# targets the validated beta SHA exactly, so the checklist, written after the
# soak, can never be inside the tagged tree.

set -euo pipefail

ref_type="${1:?ref type required}"
ref_name="${2:?ref name required}"
checklist_ref="${3:-origin/main}"
tagged_sha="${4:-$(git rev-parse HEAD)}"

fail() {
	echo "::error::$1 See docs/howto/releaseProcess.md."
	exit 1
}

# field prints the value of the "**<label>:**" line, with or without a leading
# "- " list marker, and with surrounding whitespace removed.
field() {
	sed -nE "s/^(- )?\*\*$1:\*\*[[:space:]]*(.*)$/\2/p" <<<"$content" | sed -E 's/[[:space:]]+$//'
}

if [[ "$ref_type" != "tag" ]]; then
	echo "Not a tag ($ref_type $ref_name): no release checklist required."
	exit 0
fi

version_re='^v[0-9]+\.[0-9]+\.[0-9]+'

if [[ ! "$ref_name" =~ ^v[0-9]+\. ]]; then
	echo "Not a version tag ($ref_name): no release checklist required."
	exit 0
fi

if [[ "$ref_name" =~ ${version_re}-(beta|rc)-.+$ ]]; then
	echo "Pre-release tag ($ref_name): no release checklist required."
	exit 0
fi

if [[ ! "$ref_name" =~ ${version_re}$ ]]; then
	fail "${ref_name} is not a stable (vX.Y.Z), beta (vX.Y.Z-beta-...) or rc (vX.Y.Z-rc-...) tag."
fi

path="docs/releases/${ref_name}-checklist.md"

if ! content="$(git show "${checklist_ref}:${path}" 2>/dev/null)"; then
	fail "${path} is not on ${checklist_ref}. Merge the completed release checklist to main before tagging ${ref_name}."
fi

decision_count="$(grep -cE '^(- )?\*\*Decision:\*\*' <<<"$content" || true)"

if [[ "$decision_count" -ne 1 ]]; then
	fail "${path} on ${checklist_ref} has ${decision_count} '**Decision:**' lines. It must have exactly one."
fi

decision="$(field 'Decision')"

if [[ "$decision" != "Go" ]]; then
	fail "${path} on ${checklist_ref} records '**Decision:** ${decision}'. A release ships only on a recorded Go decision."
fi

beta_sha="$(field 'Beta SHA' | head -n 1)"

if [[ ! "$beta_sha" =~ ^[0-9a-f]{40}$ ]]; then
	fail "${path} on ${checklist_ref} has no full 40-character '**Beta SHA:**'."
fi

if [[ "$beta_sha" != "$tagged_sha" ]]; then
	fail "${ref_name} points at ${tagged_sha}, but ${path} approves beta SHA ${beta_sha}."
fi

approver="$(field 'Approver \(maintainer\)' | head -n 1)"

if [[ -z "$approver" ]]; then
	fail "${path} on ${checklist_ref} names no approving maintainer."
fi

decision_date="$(field 'Date' | head -n 1)"

if [[ ! "$decision_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
	fail "${path} on ${checklist_ref} has no decision date in YYYY-MM-DD form."
fi

echo "Release checklist ${path} found on ${checklist_ref}: Go by ${approver} on ${decision_date} for ${beta_sha}."
