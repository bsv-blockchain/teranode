#!/usr/bin/env bash
#
# Forbid the %w verb inside teranode errors.New* format strings.
#
# The teranode errors package (errors/errors.go) extracts a trailing error
# argument as the wrapped error BEFORE calling fmt.Errorf, so any %w left in the
# format string is orphaned: it renders as "%!w(MISSING)", or (when no other
# params remain) the literal "%w" survives unformatted. The wrapped error is
# rendered by Error() via " -> " chaining, so %w is always wrong here.
#
# Pass the causing error as the final argument and drop the verb:
#   errors.NewStorageError("failed to read block %s: %w", hash, err)  // WRONG
#   errors.NewStorageError("failed to read block %s", hash, err)      // RIGHT
#
# The pattern uses `.*%w` rather than `[^)]*%w`: a `)` inside the format string
# (e.g. "... (pass --flag to override): %w") must NOT truncate the match, or the
# guard would green-light exactly the shape it forbids (#1335 review, ChiR1).
#
# Known line-based blind spots, all handled at runtime by the defensive strip in
# errors.New so none produce a user-facing %!w:
#   - a %w on a format-string continuation line (call spans multiple lines)
#   - a pre-formatted %%w passed through fmt.Sprintf
#   - a false positive on a comment or doc string that quotes the forbidden shape
#     (a text grep can't tell code from a comment); low risk, accepted.
set -euo pipefail

cd "$(dirname "$0")/.."

# The errors package's own callers use the unprefixed form (New(...), not
# errors.New(...)) so they never match this pattern; only its *_test.go files
# might deliberately construct a prefixed %w string to exercise the strip, so
# exempt just those rather than excluding the whole package tree (#1335, ChiR2).
matches=$(grep -rnE 'errors\.New[A-Za-z]*\(.*%w' \
  --include='*.go' --exclude-dir=vendor . \
  | grep -vE '^\./errors/[^/]*_test\.go:' || true)

if [[ -n "$matches" ]]; then
  echo "ERROR: %w found inside errors.New* format string(s)."
  echo "The trailing error argument is extracted as the wrapped error before"
  echo "formatting, so %w renders as %!w(MISSING) or survives literally."
  echo "Drop the verb; pass the error as the final argument. Error() renders"
  echo "the chain via \" -> \"."
  echo
  echo "$matches"
  exit 1
fi

echo "OK: no %w verbs in errors.New* format strings."
