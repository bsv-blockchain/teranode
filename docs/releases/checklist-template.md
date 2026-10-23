# vX.Y.Z Release Checklist

Copy this file to `docs/releases/vX.Y.Z-checklist.md` and fill in every item with its evidence. See [Release Process](../howto/releaseProcess.md).

- **Release:** vX.Y.Z
- **Beta candidate:** vX.Y.Z-beta-N (SHA: )
- **Consensus-affecting scope:** Yes / No

## Operational Readiness

| Item | Evidence |
|---|---|
| Mainnet nodes on the beta for 24 hours or more, following the chain tip | |
| Testnet node on the beta for 24 hours or more, following the chain tip | |
| TeraTestNet node on the beta for 24 hours or more, following the chain tip | |
| Sync from zero on the beta | |
| Upgrade or restart during sync on the beta | |
| Seed-to-tip time on the reference hardware, measured again if the seeder, the snapshot export format or the reference hardware in `systemRequirements.md` changed since the last release. Or N/A and the reason. | |

## QA and Testing

| Item | Evidence |
|---|---|
| `teranode_main_build.yaml` run at the beta SHA, with its `test_and_lint` jobs (Go test, smoketest, prunertest) | |
| Consensus package results at the beta SHA (`services/blockvalidation`, `services/validator`, `services/subtreevalidation`, `services/blockassembly`, `stores/utxo`) | |

## Consensus Parity

| Item | Evidence |
|---|---|
| node-validation run in full mode against the beta SHA, or the immutable image digest built from it: report, verdict, tool version, mode, SHA or digest. Or N/A and the reason. | |

## Security

| Item | Evidence |
|---|---|
| Security review or scan result for this release, or "none in scope" and the reason | |

## Architecture

| Item | Evidence |
|---|---|
| Architecture reviews or decision records that affect this release, or "none" | |

## Open Findings

| Item | Evidence |
|---|---|
| Open bug-labelled issues that affect the build | |

### Critical and High Findings

One row for each Critical or High finding from any source that is open, or that this release closed. Write "None" in the first row when there are none.

| Finding | Source | Domain | Severity | Status |
|---|---|---|---|---|

- **Finding:** the issue link or the identifier that the source gave it.
- **Source:** miner feedback, security review, internal review, audit, or acceptance harness.
- **Domain:** security, architecture, requirements, QA and testing, or operational.
- **Severity:** Critical or High.
- **Status:** Closed in this release, or Waived. A waived finding also has a row in Waivers.

### Waivers

| Finding | Owner | Date | Reason the risk is accepted | Close by |
|---|---|---|---|---|

## Miner Feedback Register

| Item | Evidence |
|---|---|
| Open items | |
| Items closed since the last release | |
| Where the register is | |

## Go/No-Go Decision

- **Criteria:** every item above has evidence, and every open Critical or High finding is closed or waived.
- **Open items and their disposition:**
- **Approver (maintainer):**
- **Date:**

**Decision:** No-Go
