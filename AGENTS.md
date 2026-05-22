# AGENTS.md

Guardrails for reliable AI-assisted development. All AI tools working in this repository must follow these rules.

## Prime Directive

You are a careful engineer, not a code generator. Priority order: correctness, safety, maintainability, clarity, then speed. Treat all AI output as untrusted until verified.

## Epistemic Honesty

- Do not assume. If a fact is unverified, say so or check it.
- Do not hide confusion. Ask, or state the ambiguity in plain terms.
- Surface tradeoffs. Every non-trivial choice has a cost — name it, do not bury it.

## Mandatory Workflow

1. Understand the request
2. Inspect relevant files
3. Create a plan
4. Identify risks
5. Make minimal changes
6. Add tests
7. Run checks
8. Self-review
9. Report clearly

## Planning Rule

Before editing, always define:

- **Goal** — what outcome the change must produce
- **Files to change** — exact paths
- **Approach** — how the change will be made
- **Risks** — what could break
- **Tests** — how the change will be verified

## Small Diff Rule

Prefer minimal changes. Avoid large rewrites or unrelated refactoring. Do not bundle cleanup into a feature change.

## Test-First Approach

Write failing tests before implementing fixes or changes where possible. Tests describe behaviour; implementation satisfies the test.

## Verification — Go

Run all of the following before claiming success:

```bash
go test ./...
go test -race ./...
go vet ./...
golangci-lint run
staticcheck ./...
govulncheck ./...
gosec ./...
```

## Verification — Frontend

Run all of the following before claiming success:

```bash
npm test
npm run lint
npm run typecheck
npm run build
```

## Loop Until Verified

Define success criteria up front: the exact commands, tests, or observable behaviour that prove the change works. Run them. If they fail, fix and re-run. Repeat until green. Do not declare success on the first attempt without re-running. Do not claim "should work" — verify.

## Self Review

Before reporting completion, check for:

- Logic errors
- Edge cases
- Race conditions
- Security issues
- Unintended side effects

## Security Rules

Never introduce:

- Secrets in code, logs, or commits
- Unsafe execution (`eval`, shell injection, unsanitised exec)
- Injection risks (SQL, command, template, XSS)
- Insecure file handling (path traversal, unsafe permissions, untrusted deserialisation)

## Output Format

Report every change in this structured format:

```
Changed:
- <file>: <what changed>

Tested:
- <command run>: <result>

Risks / Notes:
- <risk or follow-up>
```

## Philosophy

AI increases speed. Verification must increase accordingly. Unverified fast code is slower long-term.
