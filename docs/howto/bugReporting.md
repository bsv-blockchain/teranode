# Bug Reporting

When you encounter issues, please follow these guidelines to report bugs to the Teranode support team:

## Before Reporting

1. Check the [troubleshooting documentation](miners/kubernetes/minersHowToTroubleshooting.md) ([Docker](miners/docker/minersHowToTroubleshooting.md)) to ensure the behavior is indeed a bug.
2. Search existing GitHub issues to see if the bug has already been reported.

## Collecting Information

Before submitting a bug report, gather the following information:

1. **Environment Details**:

    - Operating System and version
    - Docker version
    - Docker Compose version

2. **Configuration Files**:

    - `settings_local.conf`
    - Docker Compose file

3. **System Resources**:

    - CPU usage
    - Memory usage
    - Disk space and I/O statistics

4. **Network Information**:

    - Firewall configuration
    - Any relevant network errors

5. **Steps to Reproduce**:

    - Detailed, step-by-step description of how to reproduce the issue

6. **Expected vs Actual Behavior**:

    - What you expected to happen
    - What actually happened

7. **Screenshots or Error Messages**:

    - Include any relevant visual information

## Severity

New bug reports must select a severity using the "Severity" section of the bug report
template, and the maintainer triaging the issue applies the matching `severity:*` label.
Choose the level that matches the observed impact:

- `severity:critical` — consensus failure, data loss or corruption, funds at risk, or a
  node that cannot sync or stay up. No workaround.
- `severity:major` — a core function is broken or badly degraded (e.g. sustained
  throughput loss, a service that needs manual restarts), but a workaround exists.
- `severity:minor` — cosmetic, documentation, or low-impact issues with an easy
  workaround.

This replaces the old convention of encoding severity in the issue title (e.g.
`[P2P-003][medium]`) for new issues going forward. The retired brackets were
`[high]`/`[medium]`/`[low]`, which line up positionally with
`critical`/`major`/`minor`; treat that as orientation when reading old issues, not as an
exact equivalence — re-grade against the criteria above if it matters. Existing issue
titles are not being changed retroactively and existing issues are not being relabelled.

## Submitting the Bug Report

1. Go to the Teranode GitHub repository.
2. Click on "Issues" and then "New Issue"
3. Select the "Bug Report" template
4. Fill out the template with the information you've gathered
5. Submit the issue

## Bug Report Template

When creating a new issue, GitHub will automatically load a template. The template includes the following sections:

```markdown
## Severity (required)
Tick exactly one (see the severity criteria above):
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
You can get that at the start of your program and looks something like:

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

## Additional context

Add any other context about the problem here.
```

## After Submitting

- Be responsive to any follow-up questions from the development team.
- If you discover any new information about the bug, update the issue.
- If the bug is resolved in a newer version, please confirm and close the issue.
