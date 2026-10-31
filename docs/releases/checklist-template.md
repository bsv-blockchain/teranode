# vX.Y.Z Release Checklist

Copy this file to `docs/releases/vX.Y.Z-checklist.md`. See [Release Process](../howto/releaseProcess.md).

This file is public. It records whether each gate passed, with references that are safe to publish. Do not write node names, peer identifiers, or details of a security finding that is not yet published. That evidence goes in the maintainers' private release record.

- **Release:** vX.Y.Z
- **Beta candidate:** vX.Y.Z-beta-N
- **Beta SHA or image digest:**
- **Consensus-affecting scope:** Yes / No

## Gates

Write Yes, No or N/A in "Passed". A No blocks a Go decision.

| Gate | Passed | Public reference |
|---|---|---|
| Mainnet, testnet and TeraTestNet nodes on the beta for 24 hours or more, following the chain tip | | Private release record |
| Sync from zero on the beta | | Private release record |
| Upgrade or restart during sync on the beta | | Private release record |
| Seed-to-tip time on the reference hardware, measured again if the seeder, the snapshot export format or the reference hardware in `systemRequirements.md` changed since the last release. N/A otherwise, with the reason. | | |
| `teranode_main_build.yaml` run at the beta SHA, with its `test_and_lint` jobs (Go test, smoketest, prunertest) | | CI run URL |
| Consensus package results at the beta SHA (`services/blockvalidation`, `services/validator`, `services/subtreevalidation`, `services/blockassembly`, `stores/utxo`) | | CI run URL |
| node-validation run in full mode against the beta SHA or its image digest. N/A when the release has no consensus-affecting scope, with the reason. | | Verdict, tool version, mode |
| Security review or scan for this release | | Private release record |
| Architecture reviews or decision records that affect this release | | Links, or "none" |
| Every open bug-labelled issue that affects the build is listed in Known issues | | Issue numbers |
| Every open Critical or High finding, from any source, is closed or has a waiver in the private release record | | Number open, number closed in this release, number waived |
| Miner feedback register updated | | Open items, items closed since the last release |

## Go/No-Go Decision

- **Criteria:** every gate is Yes or N/A, and every open Critical or High finding is closed or waived.
- **Approver (maintainer):**
- **Date:**

**Decision:** No-Go
