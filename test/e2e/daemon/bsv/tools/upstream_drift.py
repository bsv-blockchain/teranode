#!/usr/bin/env python3
"""Report drift between registry.yaml and a bitcoin-sv checkout.

The registry is hand-maintained, so this tool deliberately never writes to it.
It only answers: which upstream functional tests are missing from the registry,
and which registry entries no longer exist upstream?

Usage:
    python3 tools/upstream_drift.py /path/to/bitcoin-sv/test/functional

Exit status 1 if there is drift, so it can gate CI.
"""
import os
import re
import sys

# Python files that live in test/functional but are not test scripts.
# Kept in sync with NON_SCRIPTS in bitcoin-sv's test_runner.py.
NON_SCRIPTS = {
    "combine_logs.py",
    "create_cache.py",
    "test_runner.py",
    "bsv_pbv_common.py",
}

REGISTRY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "registry.yaml")


def registry_names(path):
    with open(path) as fh:
        return {m.group(1) for m in re.finditer(r"^\s*-\s+name:\s*(\S+)", fh.read(), re.M)}


def upstream_names(functional_dir):
    if not os.path.isdir(functional_dir):
        sys.exit("not a directory: %s" % functional_dir)

    return {
        f for f in os.listdir(functional_dir)
        if f.endswith(".py") and f not in NON_SCRIPTS
    }


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)

    tracked = registry_names(REGISTRY)
    upstream = upstream_names(sys.argv[1])

    missing = sorted(upstream - tracked)
    stale = sorted(tracked - upstream)

    print("tracked in registry: %d" % len(tracked))
    print("present upstream:    %d" % len(upstream))

    if missing:
        print("\nNEW upstream, untriaged (%d) - add a registry entry:" % len(missing))
        for name in missing:
            print("  + %s" % name)

    if stale:
        print("\nIn registry but GONE upstream (%d) - remove or note the rename:" % len(stale))
        for name in stale:
            print("  - %s" % name)

    if not missing and not stale:
        print("\nno drift")
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
