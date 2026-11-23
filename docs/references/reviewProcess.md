# Review Process: Fix-Forward

This process applies to team pull requests that need two or more reviews.
Its goal is fewer CI cycles. A reviewer fixes small findings directly. The
assignee does not need a separate round for each one.

## Severity

| Level | Meaning | Who fixes it |
|---|---|---|
| P0 | Blocks merge. Wrong behaviour, consensus risk, security flaw, data loss. | The assignee |
| P1 | Real defect or missing test. Reachable in normal use. | The reviewer |
| P2 | Weak design, unclear code, missing edge-case test. | The reviewer |
| P3 | Style, naming, comment, or documentation. | The reviewer |

## Roles

- The **assignee** owns the PR from start to merge. This does not change when a reviewer pushes commits.
- A **reviewer** reads the PR, reports findings, and fixes P1–P3 findings when the rules below allow it.

## Rules

1. A reviewer who finds a P0 does not fix it. The reviewer requests changes. The assignee fixes it.
2. A reviewer completes the whole review before the first push. This gives one CI cycle for each review.
3. A reviewer fixes only what the review found. Unrelated changes are not allowed.
4. Each fix is a separate commit. The subject starts with `review-fix:`.
5. A reviewer pushes with fast-forward only. Only the assignee rebases or force-pushes.
6. A reviewer does not fix a finding that needs the assignee's intent or a design decision. The reviewer states it as a question in the hand-back comment.
7. A reviewer who pushes commits to the PR is not an independent reviewer of those commits. The other reviewer must approve the final head commit.
8. Only one reviewer fixes at a time. The claim comment enforces this (see below).
9. A reviewer never resolves a merge conflict without the assignee's approval.

## Claim comment

Before the first fix, the reviewer posts a PR comment that starts with `Fixing review findings:`. The comment names the reviewer and the head commit the fixes start from. This is the claim.

- Before a reviewer starts fixes, the reviewer reads the PR comments for an open claim.
- A claim is open until its author posts the hand-back comment or a comment that cancels the claim.
- If a claim is open, the other reviewer does not push fixes. That reviewer may post findings as comments, and waits for the hand-back.
- If the head commit has moved since the claim, the claimant re-checks the findings against the new head before the first push.
- If a claim looks stale, the other reviewer asks the claimant in a PR comment. The other reviewer does not take over the claim without an answer from the claimant or the assignee.
- A reviewer who stops without fixing posts a cancel comment.

## Hand-back comment

After the push, the reviewer posts one PR comment. It contains:

- The findings, each with its severity and `file:line`.
- The `review-fix:` commit that fixes each finding.
- The findings left open, and the reason for each.
- Any question for the assignee.
- Re-review advice for each existing approver (see below).

## Re-review advice

The hand-back comment also tells the assignee whether each existing approver should review again. The reviewer gives one line for each approver, with the reason.

Re-review is **needed** when the pushed commits do any of these:

- Change runtime behaviour, even in a small way.
- Change what a test asserts, or remove a test.
- Touch consensus, validation, security, or concurrency code.
- Touch code that the approver commented on.

Re-review is **not needed** when the pushed commits only change:

- Names, comments, or formatting.
- Documentation.
- Test code that adds coverage and changes no assertion.

If the case is unclear, advise re-review. The assignee decides, and asks the approver. A reviewer does not ask for the re-review.

The same advice applies when the assignee pushes new commits after an approval. The assignee then writes the advice in a PR comment.

## Assignee duties after a hand-back

1. Read every `review-fix:` commit.
2. Revert or change a commit that is wrong. Give the reason in a PR comment.
3. Fix the open findings and the P0 findings.
4. Ask each approver for a re-review where the advice says it is needed.
5. Ask for the next review, or for the final approval.

## Requirements

- The reviewer has push access to the PR branch.
- For a PR from a fork, "Allow edits from maintainers" is on.
- The reviewer's agent follows the Git rules in `AGENTS.md`. It does not use `git reset --hard`. It does not auto-resolve conflicts.
- No commit or comment names an AI tool.
