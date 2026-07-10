# Location-Locked Transactions (Proof-of-Presence Review Bounties)

Research and design for a system where a venue's on-chain review bounty can only
be unlocked by proving 30 continuous minutes of physical presence at the venue's
GPS location, streamed at 1 Hz over a peer-to-peer payment channel.

Start with [`proposal.md`](proposal.md), then [`design.md`](design.md) for the
research findings (BRC-100 wallets, Craig Wright's data-privacy and payment-channel
model) and the full protocol, [`specs/proof-of-presence/spec.md`](specs/proof-of-presence/spec.md)
for testable requirements, and [`tasks.md`](tasks.md) for the phased plan.

**Placement note**: these documents are authored as an
`openspec/changes/location-locked-transactions/` change for the
[`bsv-blockchain/arcade`](https://github.com/bsv-blockchain/arcade) repository —
the implementation lives in Arcade (new presence-oracle mode + api_server routes),
and the identical change is committed on Arcade branch
`claude/location-locked-transactions-4gm9vv`. They are mirrored here because the
session's GitHub integration currently has read-only access to the arcade
repository. **Teranode itself requires no changes**: the design was verified
against Teranode's finality rules (`util/lock_time.go` TNJ-13 non-final rejection,
post-Genesis BIP68 short-circuit in `services/validator/TxValidator.go`) and works
within them — all 1 Hz channel updates stay peer-to-peer, and only final
transactions reach the node.
