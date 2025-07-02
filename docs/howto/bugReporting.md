# Bug Reporting


When you encounter issues, please follow these guidelines to report bugs to the Teranode support team:

## Before Reporting

1. Check the documentation and FAQ to ensure the behavior is indeed a bug.
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
4. **Network Information:**
    - Firewall configuration
    - Any relevant network errors
5. **Steps to Reproduce**:

    - Detailed, step-by-step description of how to reproduce the issue
6. **Expected vs Actual Behavior**:

    - What you expected to happen
    - What actually happened
7. **Screenshots or Error Messages**:

    - Include any relevant visual information

## Submitting the Bug Report

1. Go to the Teranode GitHub repository.
2. Click on "Issues" and then "New Issue"
3. Select the "Bug Report" template
4. Fill out the template with the information you've gathered
5. Attach the compressed file from the log collection tool
6. Submit the issue

## Bug Report Template

When creating a new issue, use the following template:

```markdown
## Bug Description
[Provide a clear and concise description of the bug]

## Teranode Version
[e.g., 1.2.3]

## Environment
- OS: [e.g., Ubuntu 20.04]
- Docker Version: [e.g., 20.10.7]
- Docker Compose Version: [e.g., 1.29.2]
- Kubectl version

## Steps to Reproduce
1. [First Step]
2. [Second Step]
3. [and so on...]

## Expected Behavior
[What you expected to happen]

## Actual Behavior
[What actually happened]

## Additional Context
[Any other information that might be relevant]

## Logs and Configuration
```

## After Submitting

- Be responsive to any follow-up questions from the development team.
- If you discover any new information about the bug, update the issue.
- If the bug is resolved in a newer version, please confirm and close the issue.
