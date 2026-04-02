# CLAUDE.md

Teranode: horizontally scalable BSV Blockchain node. Microservices in `services/`, stores in `stores/`. Dashboard is Svelte in `ui/dashboard/`.

## Commands

```bash
make build          # Binary with dashboard
make test           # Unit tests
make smoketest      # E2E smoke tests
make lint           # Changed files vs main
make dev            # Dev mode with dashboard
make gen            # Regenerate protobuf Go code
gci write --skip-generated -s standard -s default <file>  # Fix import ordering lint
```

## Rules

- Follow [`docs/references/codingConventions.md`](docs/references/codingConventions.md)
- No "Get" prefix on getters: `Name()` not `GetName()`
- Log messages: always single line
- **NEVER `git reset --hard`** — use `git stash` + `git rebase`
- Config: `settings.conf` (defaults), `settings_local.conf` (local overrides, not committed)

## Maintaining this file

Keep CLAUDE.md under 30 lines of content. It loads on every request. Add new rules to `.claude/rules/` with glob scoping instead. See existing rules there for examples.
