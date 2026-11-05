#!/usr/bin/env bash
#
# Fails a stable release tag build that has no completed release checklist.
#
# Usage: check-release-checklist.sh <ref-type> <ref-name> [checklist-ref]
#
# A stable tag is vX.Y.Z with no suffix. Every other ref (a branch, or a beta or
# rc tag) passes. For a stable tag, docs/releases/<tag>-checklist.md must exist
# at checklist-ref (default origin/main) and record "**Decision:** Go".
#
# The checklist is read from main, not from the tagged commit: a stable tag
# targets the validated beta SHA exactly, so the checklist, written after the
# soak, can never be inside the tagged tree.

set -euo pipefail

ref_type="${1:?ref type required}"
ref_name="${2:?ref name required}"
checklist_ref="${3:-origin/main}"

if [[ "$ref_type" != "tag" ]]; then
	echo "Not a tag ($ref_type $ref_name): no release checklist required."
	exit 0
fi

if [[ ! "$ref_name" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
	echo "Not a stable tag ($ref_name): no release checklist required."
	exit 0
fi

path="docs/releases/${ref_name}-checklist.md"

if ! content="$(git show "${checklist_ref}:${path}" 2>/dev/null)"; then
	echo "::error::${path} is not on ${checklist_ref}. Merge the completed release checklist to main before tagging ${ref_name}. See docs/howto/releaseProcess.md."
	exit 1
fi

if ! grep -qE '^\*\*Decision:\*\* Go[[:space:]]*$' <<<"$content"; then
	echo "::error::${path} on ${checklist_ref} does not record '**Decision:** Go'. A release ships only on a recorded Go decision. See docs/howto/releaseProcess.md."
	exit 1
fi

echo "Release checklist ${path} found on ${checklist_ref} with a Go decision."
