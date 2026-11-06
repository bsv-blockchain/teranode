# Requirements Sources Register

This register lists the authoritative sources of Teranode requirements. It records the owner, version, date and location of each source. It does not copy the requirements themselves. Each source stays the single source of truth for its own content.

## Ownership and Change Process

- The [Maintainers](https://github.com/bsv-blockchain/teranode/blob/main/GOVERNANCE.md) own this register.
- A change to the register is a pull request. The git history of this file is its version history, and the pull request review is its approval record.
- When a source gets a new version, a pull request updates its row in the same change that relies on the new version.
- When a field is not recorded in the source, the row says "Not recorded". The Maintainers get the value from the source owner and update the row.

## Register

| ID | Source | Owner | Version / date | Location | Scope |
| --- | --- | --- | --- | --- | --- |
| RS-01 | BSV protocol consensus rules, as implemented by SVNode | BSV Blockchain Association | Current network rules | [Network Consensus Rules](networkConsensusRules.md) | Consensus-critical behaviour. Teranode must match the protocol exactly. |
| RS-02 | Teranode Main Business Requirements (`202407_Teranode_Main_Business_Requirements.pdf`) | Not recorded | 2024-07 | External | Business requirements for the Teranode programme |
| RS-03 | TNG Teranode Requirements and Test Plan | TNG, Director of Emerging Technology | v1.3, 2026-04-28, Draft | External, internal use only | Integration requirements and test plan of an adopting operator |
| RS-04 | Teranode Milestone 5 Acceptance (`Teranode_Milestone5_Acceptance_2026-02-19.pdf`) | Not recorded | 2026-02-19 | External | Milestone 5 acceptance record |
| RS-05 | Teranode Performance Report: Independent Milestone Verification & Claims-to-Evidence Attestation | Halborn Inc. | V2.0, 2026-02-11 | External | Independent verification of milestone claims, including the 1M TPS benchmark |
| RS-06 | Teranode Threat Modeling Report (`Teranode_Threat_Modeling_Report_Halborn.pdf`) | Halborn Inc. | Not recorded | External | Security requirements derived from the threat model |
| RS-07 | Teranode architecture and design documentation (`docs/topics/`) | Maintainers | Git history | [Topics](../topics/teranodeIntro.md) | Describes the design. It is not a source of requirements on its own. |
