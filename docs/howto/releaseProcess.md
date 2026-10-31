# Release Process

This page states how a Teranode release is decided, recorded and announced. The release skill (`.claude/skills/release/SKILL.md`) runs the same steps. This page is the reference for anyone who cuts a release without it.

## Branches and Tags

- `main` is the trunk. Every change lands there first, through a pull request.
- `release/vX.Y` is cut from `main` at the start of a minor's beta phase. Hotfixes arrive as squash-merge cherry-picks.
- A beta tag is `vX.Y.Z-beta-N`, on `release/vX.Y`.
- A stable tag is `vX.Y.Z`. It targets the SHA of the validated beta, never the branch tip. If anything landed on `release/vX.Y` after that beta, the release needs a new beta.

## Release Authority

The [Maintainers](https://github.com/bsv-blockchain/teranode/blob/main/GOVERNANCE.md) decide whether a stable release ships. They use the decision model in `GOVERNANCE.md`: consensus, or a majority vote of maintainers when there is no consensus.

- Each go/no-go decision record names the maintainer who approved it, and the date.
- Any maintainer can stop a release. To stop it, the maintainer records a No-Go decision with the criterion that failed. The No-Go stands until the maintainers record a Go under the same decision model.
- Authority to merge code does not give authority to ship a release. A release ships only on a recorded Go decision.

## Release Checklist and Go/No-Go Record

Each stable release has two records:

- **The published checklist**, `docs/releases/vX.Y.Z-checklist.md`, copied from [the template](https://github.com/bsv-blockchain/teranode/blob/main/docs/releases/checklist-template.md). It is public. It records whether each gate passed, with references that are safe to publish, and the go/no-go decision.
- **The private release record**, which only the maintainers can read. It holds the evidence that must not be published: the nodes that soaked the beta and their soak windows, the sync-from-zero and mid-sync-upgrade nodes, the security review result, and every Critical or High finding with its waiver. Record each security finding as a draft [repository security advisory](https://docs.github.com/en/code-security/security-advisories/working-with-repository-security-advisories/about-repository-security-advisories), and list its advisory ID in the private record. A draft advisory is visible only to admins, security managers and invited collaborators.

The steps:

1. Fill in the private release record. Each item needs its evidence: a report title, a CI run URL, a node identifier, an issue or advisory ID. "Done" without evidence does not satisfy an item.
2. Copy the template to `docs/releases/vX.Y.Z-checklist.md`, and mark each gate Yes, No or N/A, with a public reference where the template asks for one.
3. Record the decision, the approver and the date.
4. Merge the file to `main` in a pull request.
5. Tag `vX.Y.Z` on the beta SHA.
6. Link the file from the GitHub Release body and from the Announcements discussion.

The `release-checklist` job in `teranode_main_build.yaml` enforces steps 4 and 5. For a stable tag, the build fails when `docs/releases/vX.Y.Z-checklist.md` is not on `main`, or when it does not contain the line `**Decision:** Go`. The check reads the file from `main`, because the tagged beta commit cannot contain a checklist written after its soak. Branches and beta tags are not checked. The check applies to release branches cut from `main` after this job was added. Earlier release lines rely on the manual checklist only.

### Checklist Items

The private release record holds this evidence for each domain. The published checklist has one gate per item, with the result and a public reference:

| Domain | Required evidence |
|---|---|
| Operational readiness | The beta tag. The mainnet, testnet and TeraTestNet nodes that ran it, each for at least 24 hours while following the chain tip. A sync from zero. An upgrade or restart during sync. If the seeder, the snapshot export format or the reference hardware changed since the last release: a new seed-to-tip measurement on the [reference hardware](miners/systemRequirements.md), recorded as measured in the [sync guide](miners/kubernetes/minersHowToSyncTheNode.md). Otherwise, N/A and the reason. |
| QA and testing | The CI run URL at the beta SHA for `teranode_main_build.yaml`, with the results of its `test_and_lint` jobs (Go test, smoketest and prunertest). The results for the consensus packages: `services/blockvalidation`, `services/validator`, `services/subtreevalidation`, `services/blockassembly` and `stores/utxo`. |
| Consensus parity | For a release with consensus-affecting scope: a [node-validation](https://github.com/bsv-blockchain/node-validation) run in full mode against the beta SHA that the stable tag will target, or against the immutable image digest built from that SHA. The stable tag does not exist until the checklist merges, so the run cannot use it. Give the report, its verdict, the tool version, the mode and the SHA or digest. For a release without consensus-affecting scope, write N/A and the reason. |
| Security | The security review or scan result for this release, or "none in scope" and the reason. |
| Architecture | The architecture reviews or decision records that affect this release, or "none". |
| Open findings | Every open bug-labelled issue that affects the build. Every open Critical or High finding from any source: miner feedback, security review, internal review, or the acceptance harness. Each one is closed or has a waiver. The findings and waivers are in the private record; the published checklist gives only their counts. |
| Miner feedback register | The number of open items, the number closed since the last release, and where the register is. |

### Waivers

A stable release does not ship while a Critical or High finding is open, unless the finding has a waiver. A waiver states all of these:

- the finding
- the owner, by name
- the date
- the reason the risk is accepted
- the date by which the finding will be closed

Record each waiver in the private release record, and its count in the published checklist. Disclose each waiver in the Known issues section of the release notes, except a waiver for a security finding that is not yet published: disclose that one when its advisory is published.

### Forward Schedule for Consensus-Level Releases

When a release carries a consensus-level or behaviour-affecting change, publish a forward schedule in the Announcements category before the beta phase starts, so that operators can plan. The schedule states three dates:

- the code freeze for the release branch
- the readiness confirmation, which is the date of the go/no-go decision
- the activation, which is the release date, or the activation height for a change that activates at a height

The schedule adds to the go/no-go record. It does not replace it.

## Release Notes

Write the release notes manually; for the commit list format, see the release skill. The Announcements discussion uses this template:

```markdown
# vX.Y.Z — <one-line theme>

**Released:** <YYYY-MM-DD>
**GitHub:** https://github.com/bsv-blockchain/teranode/releases/tag/vX.Y.Z
**Container:** ghcr.io/bsv-blockchain/teranode:vX.Y.Z
**Release checklist:** https://github.com/bsv-blockchain/teranode/blob/main/docs/releases/vX.Y.Z-checklist.md

## Upgrade priority
<Critical | Recommended | Optional> — <one-sentence reason>.

## Highlights
- <Top change> (#PR)

## For miners
<Required when the release has a consensus-level or behaviour-affecting change.
What a miner must verify before and after upgrading.>

## For node operators
<Required when the release has a consensus-level or behaviour-affecting change.
What a non-mining node operator must verify before and after upgrading.>

## Known issues
<Every open bug-labelled issue that affects this build, with its number and any
workaround. Every waiver. Every open non-code item, such as an open governance
item from miner feedback, with its timeline or deferral reason. "None" only when
all of these are empty.>
```

Rules:

- **Upgrade priority.** *Critical* when the release fixes a known production incident class or a consensus correctness issue. *Recommended* for meaningful operational fixes with no known production impact. *Optional* for performance or features only. A release with a consensus correctness change that is not rated Critical must say why.
- **Audiences.** A release with a consensus-level or behaviour-affecting change must have both the "For miners" and "For node operators" sections. Omit both only when the release has no such change.
- **Known issues.** At tag time, check the issue tracker. "None" is correct only when the tracker shows no open bug that affects the build.
- **No node names.** "mainnet", "testnet" and "teratestnet" are fine. Hostnames expose the fleet layout.
