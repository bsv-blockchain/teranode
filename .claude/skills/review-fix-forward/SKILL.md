---
name: review-fix-forward
description: Fix P1-P3 review findings on a team PR yourself and hand the PR back to the assignee. Use when the user picks "fix and hand back" after a PR review, or asks to fix review findings on someone else's PR branch.
---

# Review Fix-Forward

The rules are in `docs/references/reviewProcess.md`. Read that file first. It wins over this skill if they differ.

## Preconditions

Check all of these. If one fails, stop and tell the user.

1. The user asked in this session to fix findings and push. Never push or post without that request.
2. The review has no open P0 finding. A P0 goes back to the assignee as "request changes".
3. You finished the whole review. Do not push after each finding.
4. You can push to the PR branch. For a fork, "Allow edits from maintainers" must be on.

## Steps

1. Read the PR comments for an open claim:

   ```bash
   gh pr view <n> --repo <owner>/<repo> --comments
   ```

   An open claim is a `Fixing review findings:` comment with no later hand-back or cancel comment from its author. If a claim is open, stop. Tell the user. Do not push fixes.

2. Post the claim comment. It names you and the head commit:

   ```bash
   gh pr comment <n> --repo <owner>/<repo> --body "Fixing review findings: <reviewer>, from <head-sha>"
   ```

3. Create a worktree on the PR branch. Do not use the main checkout.

4. Fix each P1–P3 finding in its own commit. The subject starts with `review-fix:`.
   - Fix only what the review found.
   - Skip a finding that needs the assignee's intent or a design decision. Keep it as an open question.
   - Never rebase, force-push, `git reset --hard`, or resolve a merge conflict on your own.
   - If the head moved since the claim, check the findings against the new head first.

5. Run the targeted tests, `go vet`, and lint for the touched packages. Do not push on a failure. Fix it or cancel the claim.

6. Push fast-forward only.

7. Post the hand-back comment:
   - Each finding with its severity and `file:line`.
   - The `review-fix:` commit for each finding.
   - Open findings, with the reason.
   - Questions for the assignee.
   - Re-review advice for each existing approver, with the reason. Use the criteria in `reviewProcess.md`. Do not ask the approver yourself.

8. If you stop without fixing, post a comment that cancels the claim.

## Rules for all text

- Commit messages and comments name no AI tool and carry no attribution lines.
- Write comments in Simplified Technical English. Keep them short.
- Write `#<number>` only when you mean that issue or PR.
- The assignee still owns the PR. Say so in the hand-back. Do not approve your own commits.
