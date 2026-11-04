# Contributing Guide

* [New Contributor Guide](#contributing-guide)
  * [Pull Request Workflow](#pull-request-workflow)
  * [Inactive pull requests](#inactive-pull-requests)

Welcome! We are glad you are excited to contribute to Teranode.

As you get started, you are in the best position to give us feedback on areas of
our project that we need help with including:

* Problems found during setting up a new developer environment
* Gaps in our documentation

If anything doesn't make sense, or doesn't work when you run it, please open a
bug report and let us know!

## Pull Request Workflow

When contributing to Teranode, we expect users to follow a common workflow in
order to ensure that code changes follow common practices and allows the BSV
Association to limit the number of paid seats needed in our GitHub
organization. This workflow is common in open source development, and there are
many available write-ups that demonstrate this workflow. For our sake, [this
walkthrough](https://github.com/gabrieldemarmiesse/getting_started_open_source)
covers what we expect contributors to follow.

To briefly summarize, we expect users to:
1. Fork this repository 
2. Checkout a branch that tracks `main`
3. Commit changes locally
4. Pushes the branch to their remote fork
5. Open a pull request with a detailed description of the changes

The commands for this are shown
[here](https://github.com/gabrieldemarmiesse/getting_started_open_source?tab=readme-ov-file#All-commands).

## Inactive pull requests

A daily job labels and closes pull requests that nobody is working on.

* After 90 days without human activity a PR gets the `stale` label and a comment.
* 14 days after that, if there is still no activity, it is closed. The branch is
  not deleted; reopen the PR when you pick the work back up.
* Activity means a new commit, a comment, a review, a review comment or a
  reopen by a person. Bot comments and label changes do not count.
* A PR is never labelled or closed if it has the `keep-open` label or is
  approved.
* A PR from an outside contributor is only closed when it is waiting on its
  author: changes were requested and the author has not pushed or replied since.
  A PR waiting on a reviewer is never closed.

The job is `.github/workflows/stale_prs.yaml`. It only reports what it would do
until maintainers turn it on with the `STALE_PRS_ENABLED` repository variable.
