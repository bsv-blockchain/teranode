---
description: Git workflow rules for fork-based development
globs:
---

All developers work in forked repositories with `upstream` remote pointing to the original repo.

## Syncing and Branching

```bash
# Sync with upstream
git fetch upstream && git rebase upstream/main

# New branch (always from synced main)
git checkout main && git fetch upstream && git rebase upstream/main && git checkout -b <branch>

# Push (if conflicts: STOP and ask user)
git fetch upstream && git rebase upstream/main && git push origin <branch>
```

## Safety Rules

- **NEVER `git reset --hard`** — destroys uncommitted work. Use `git stash` instead.
- **NEVER auto-resolve merge conflicts** — show conflicting files and wait for user approval on resolution strategy.
- If conflicts occur during rebase: stop, show the files, ask the user how to proceed.
