#!/usr/bin/env bash
# Runs the given command and tracks each Go test binary's peak RSS (VmHWM, the
# kernel's monotonic high-water mark) in the background, then prints a top-10
# peak-memory table when the command ends, so per-binary unit-test memory stays
# visible and regressions are caught early. The summary also runs from the EXIT
# trap, so it prints when the runner is shut down mid-run. The sampler runs under
# `set +e` so a SIGPIPE or no-match can't fail the step; the command's exit status
# is the script's.
#
# Usage: .github/scripts/track-test-memory.sh make test
set -euo pipefail

TRACK="$(mktemp)"

track() {
  set +e
  local st pid hwm cmd name
  while :; do
    for st in /proc/[0-9]*/status; do
      hwm="$(awk '/^VmHWM:/{print $2; exit}' "$st" 2>/dev/null)"
      [ -n "$hwm" ] || continue
      pid="${st#/proc/}"; pid="${pid%/status}"
      cmd="$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null)"
      name="${cmd%% *}"; name="${name##*/}"
      case "$name" in *.test) echo "$hwm $name" >> "$TRACK" ;; esac
    done
    sleep 10
  done
}

job_peak() {
  local f
  for f in /sys/fs/cgroup/memory.peak /sys/fs/cgroup/memory/memory.max_usage_in_bytes; do
    if [ -r "$f" ]; then awk -v b="$(<"$f")" 'BEGIN{printf "%.1fG", b/1073741824}'; return; fi
  done
  awk '/^MemTotal:/{t=$2} /^MemAvailable:/{a=$2} END{printf "%.1fG",(t-a)/1048576}' /proc/meminfo
}

summary() {
  set +e
  kill "$TRACK_PID" 2>/dev/null
  echo "::group::Peak memory  (job peak=$(job_peak))"
  awk '$1>m[$2]{m[$2]=$1} END{for (k in m) printf "%d %s\n", m[k], k}' "$TRACK" 2>/dev/null \
    | sort -rn | head -10 | awk '{printf "PEAKMEM %.1fG %s\n", $1/1048576, $2}'
  echo "::endgroup::"
}

track & TRACK_PID=$!
trap summary EXIT

"$@"
