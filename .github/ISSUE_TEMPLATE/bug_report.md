---
name: Bug report
about: Create a report to help us improve
title: "[BUG]"
labels: bug
assignees: ''

---

## Severity (required)

Tick exactly one (see the [severity criteria](../../docs/howto/bugReporting.md#severity)):

- [ ] severity:critical — consensus failure, data loss or corruption, funds at risk, or a node that cannot sync or stay up, with no workaround
- [ ] severity:major — a core function is broken or badly degraded (sustained throughput loss, a service needing manual restarts), but a workaround exists
- [ ] severity:minor — cosmetic, documentation, or low-impact issues with an easy workaround

## Describe the bug

A clear and concise description of what the bug is.

## To Reproduce

Steps to reproduce the behavior:

1. Go to '...'
2. Click on '...'
3. Scroll down to '...'
4. See error

## Expected behavior

A clear and concise description of what you expected to happen.

## Screenshots

If applicable, add screenshots to help explain your problem.

## Timeline

When did the bug first occur, or when did you first notice it?

## Desktop (please complete the following information)

- OS: [e.g. iOS]
- Browser [e.g. chrome, safari]
- Version [e.g. 22]

## TERANODE Env

- you can get that at the start of your program and looks something like

```shell

SETTINGS_CONTEXT
----------------
scaling.m1

SETTINGS
--------
SERVICE_NAME=validation-service
advertisingInterval=10s
advertisingURL=
clientName=M1
asset_grpcAddress=blob-service.blob-service.svc.cluster.local:8091
asset_grpcListenAddress=:8091
asset_httpAddress=https://m1.scaling.teranode.network
asset_httpListenAddress=:8090
...
```

## Additional context

Add any other context about the problem here.
