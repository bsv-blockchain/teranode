# Location-Locked Transactions: Proof-of-Presence Review Bounties

## Why

Online reviews are unverifiable: anyone can review a venue they never visited. BSV
gives us the tools to fix this economically rather than editorially — a venue
publishes a small on-chain bounty that can only be unlocked by someone who can
*prove* they were physically at the venue, continuously, for 30 minutes. Spending
the bounty is what authorises publishing the review, so every review is anchored to
a cryptographic proof of presence.

The mechanism follows two bodies of prior art researched for this change (see
`design.md` for sources and detail):

1. **The BRC-100 wallet standard** — the vendor-neutral wallet-to-application
   interface (createAction/signAction, BRC-42 key derivation, certificates,
   baskets) that the phone app uses so it works with any compliant BSV wallet
   instead of shipping its own key management.
2. **Craig Wright's writing on transaction data privacy and payment channels** —
   raw data (here: 1 Hz GPS samples) never goes on-chain; only salted
   commitments do. High-frequency updates ride a peer-to-peer payment channel
   (nSequence-updated non-final transactions) and only the channel settlement is
   broadcast. A fresh key pair is used per interaction so presence proofs don't
   build a trackable identity.

This is deliberately consistent with Teranode's consensus stance: Teranode rejects
non-final transactions outright (`util/lock_time.go`, rule TNJ-13 — "It is up to
the user to properly set up payment channels… Teranode shouldn't be aware or care
about them"). All 1 Hz traffic therefore stays off-node between the phone and the
presence oracle; Arcade only ever sees final transactions.

## What Changes

- New **presence-oracle** service (new `--mode presence-oracle`): terminates the
  phone's payment-channel WebSocket, validates 1 Hz GPS commitments against the
  venue geofence, accumulates them into a Merkle tree, and co-signs channel
  settlement + bounty unlock when 30 continuous minutes are proven.
- New **api_server routes**: venue bounty registration (`POST /v1/bounty`),
  channel open/negotiate (`POST /v1/presence/channel`), and bounty status
  (`GET /v1/bounty/:outpoint`).
- **Broadcast path reuse**: channel funding, channel settlement, and bounty-unlock
  (review) transactions are ordinary final transactions submitted through the
  existing `api_server → tx_validator → propagation` pipeline; `bump_builder`
  BUMPs give the reviewer an SPV proof that their review is mined.
- New **Android reference client** (separate repo, specified here): foreground
  location service sampling GPS at 1 Hz, BRC-100 wallet integration
  (wallet-toolbox), channel client, review composer.
- **No Teranode changes.** The design works within Teranode's existing finality
  rules (post-Genesis nSequence semantics, nLockTime finality, BIP68
  short-circuited post-Genesis).

## Capabilities

### New Capabilities

- `proof-of-presence`: the end-to-end protocol — bounty creation, presence
  channel lifecycle (open → 1 Hz commitment updates → settle), presence
  verification policy (geofence, continuity, anti-spoofing), bounty unlock
  scripts, and review publication. See
  `specs/proof-of-presence/spec.md`.

### Modified Capabilities

- `api-server`: gains the bounty/channel HTTP routes (delta to be written when
  implementation starts; routes are additive).

## Impact

- **New service mode** in the arcade binary; Kafka topic `arcade.presence_settled`
  from presence-oracle to api_server for bounty state.
- **Store**: new tables/collections for bounties, channels, and presence Merkle
  roots.
- **Trust model**: v1 uses an oracle co-signature (2-of-2 with the reviewer). The
  oracle is trusted to verify presence but *cannot* spend alone, and every
  attestation it signs is publicly auditable against the on-chain Merkle root. A
  trust-minimised full-script variant is specified as v2 (see design.md §6.2).
- **Privacy**: raw GPS never leaves the phone unencrypted and never touches the
  chain; on-chain data is limited to salted HMAC commitments and Merkle roots.
- **Not a geolocation consensus system**: GPS is client-reported. The design
  layers device attestation, venue beacon nonces, and plausibility checks
  (design.md §8), but the residual risk of a determined spoofer is explicitly
  accepted and priced by the bounty size.
