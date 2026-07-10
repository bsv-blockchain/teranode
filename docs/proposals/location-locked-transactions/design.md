# Design: Location-Locked Transactions (Proof-of-Presence Review Bounties)

A venue publishes an on-chain bounty spendable only by a person who proves they
stayed at the venue's GPS location for 30 continuous minutes. The proof is built
from 1 Hz GPS commitments streamed over a peer-to-peer payment channel; unlocking
the bounty authorises publishing a review that is cryptographically tied to the
presence proof.

## 1. Research Summary

### 1.1 BRC-100 wallets

[BRC-100](https://github.com/bitcoin-sv/BRCs/blob/master/wallet/0100.md) is the
unified, vendor-neutral, unchanging wallet-to-application interface for BSV
([BSV Association standards page](https://bsvassociation.org/protocol/standards/)).
Properties this design relies on:

- **`createAction` / `signAction`** — an application hands the wallet a
  description of outputs (including *custom locking scripts*) and the wallet
  builds, signs, and tracks the transaction. The phone app never handles seed
  material; it works against any compliant wallet (BSV Desktop, BSV Browser,
  Metanet Mobile, or anything built on
  [wallet-toolbox](https://github.com/bsv-blockchain/wallet-toolbox), the
  reference implementation).
- **BRC-42 key derivation** — two parties derive fresh, unlinkable key pairs per
  interaction from master keys. Every channel update and the final review key are
  one-time keys: presence proofs from different days/venues cannot be correlated
  into a movement profile.
- **Baskets** — the unlocked bounty token is held in an app-specific basket
  ("reviews"), so the wallet UI can show "review credits" distinctly from cash.
- **Certificates** — BRC-100 identity certificates support the *one bounty per
  person* sybil control (§8) via selective disclosure, without publishing
  identity on-chain.
- **Encryption / HMAC primitives** — the interface exposes HMAC creation under
  derived keys, which is exactly what the 1 Hz GPS commitments use (§4.2).

### 1.2 Craig Wright on transaction data and privacy

Sources: [Bitcoin's privacy model](https://medium.com/@craig_10243/bitcoins-privacy-model-7ef7e79caf9f),
[nSequence and P2P exchange](https://medium.com/@craig_10243/nsequence-and-p2p-exchange-9e4cbf32124c),
[Cryptography and Bitcoin](https://medium.com/@craig_10243/cryptography-and-bitcoin-b64db06299e3),
The Bitcoin Masterclasses on
[data privacy](https://coingeek.com/bitcoin-masterclass-with-craig-wright-session-2-the-how-when-and-what-of-data-privacy/),
[hashed identity](https://medium.com/@bsvsuperfan/the-bitcoin-masterclasses-with-craig-wright-day-2-session-2-hashing-your-identity-and-keeping-your-e04b8ad06b56),
[verifying data while keeping it private](https://coingeek.com/verifying-data-while-keeping-it-private-and-why-its-important-the-bitcoin-masterclasses-3-with-craig-wright/),
[trusted P2P economies](https://coingeek.com/bitcoin-masterclass-session-4-with-craig-wright-how-to-build-trusted-p2p-economies/),
and [privacy is not as easy as you think](https://coingeek.com/bitcoin-masterclass-with-craig-wright-day-2-session-3-privacy-is-not-as-easy-as-you-think/).

The recurring principles, mapped to this design:

| Wright principle | Application here |
| --- | --- |
| Privacy = new key pair per transaction; keys are not identity | BRC-42 one-time keys for every channel update and the review output |
| Know what belongs on-chain vs off-chain; identity/raw data stays off-chain | Raw GPS samples never leave the phone unencrypted; only salted HMAC commitments and a Merkle root reach the chain |
| Hash + salt (HMAC) lets a counterparty verify data you disclosed to them while the world sees only a commitment | Oracle receives `(sample, salt)` pairs over the encrypted channel and can verify each commitment; the chain sees only the root |
| nSequence payment channels: non-final transactions updated P2P in-flight until nLockTime; miners see only settlement | The 1 Hz stream is 1,800 channel updates between phone and oracle; exactly two transactions (funding, settlement) are broadcast |
| Reviews/attestations should be tied to a provable real-world event to kill fake reviews (Masterclasses P2P-economies session) | The review output can only exist as the spend of a presence-locked bounty |
| Selective disclosure on dispute | The reviewer can reveal any committed sample + salt to a third party to prove a single point without revealing the rest of the trail |

### 1.3 Teranode and non-final transactions (verified in-repo)

- `teranode/util/lock_time.go` (rule TNJ-13): a transaction whose inputs carry
  non-final sequence numbers and whose `nLockTime` is still in the future is
  **rejected** — "It is up to the user to properly set up payment channels or use
  an escrow service for their non-final transactions. Teranode shouldn't be aware
  or care about them."
- `teranode/services/validator/TxValidator.go`: BIP68 relative lock-times are
  enforced only below the Genesis activation height; post-Genesis the check
  short-circuits (original nSequence semantics).

Consequence: **channel updates must be exchanged peer-to-peer** (phone ↔ oracle
WebSocket), never broadcast. Only final transactions flow through Arcade. This is
not a limitation — it is the intended Bitcoin channel model, and it is what makes
1 Hz updates free.

## 2. Actors

- **Reviewer phone app** (Android, §9): GPS sampling, BRC-100 wallet, channel client.
- **Venue**: funds the bounty UTXO, defines the geofence (lat, lon, radius) and
  dwell requirement (30 min).
- **Presence oracle** (new arcade service): channel counterparty; verifies
  commitments in real time; co-signs settlement and unlock. Trusted-but-auditable
  in v1 (§6.1), minimised in v2 (§6.2).
- **Arcade**: broadcast pipeline + BUMP builder for the three final transactions.
- **Teranode datahubs**: mining/validation; see only final transactions.

## 3. Transaction Topology

```
 Venue                    Reviewer phone                     Chain
   │                            │
   │ (a) Bounty tx ─────────────┼──────────────────────────▶ UTXO_bounty
   │                            │ (b) Channel funding tx ──▶ UTXO_channel
   │                            │
   │                    1 Hz: 1,800 signed non-final updates
   │                    (P2P WebSocket, NEVER broadcast)
   │                            │
   │                            │ (c) Settlement tx ───────▶ commits MerkleRoot(P)
   │                            │      nLockTime = T_open + 30 min
   │                            │
   │                            │ (d) Unlock/review tx ────▶ spends UTXO_bounty,
   │                            │      carries review + proof reference
```

Four on-chain transactions total, regardless of dwell time. The 1 Hz data volume
(1,800 messages ≈ a few hundred KB) stays inside the channel.

## 4. Protocol

### 4.1 Bounty creation (venue)

Venue calls `POST /v1/bounty` with geofence `G = (lat₀, lon₀, r)`, dwell
`D = 1800 s`, and review terms; funds `UTXO_bounty` with the v1 locking script
(§6.1). Arcade indexes the bounty and serves it to nearby apps.

### 4.2 Channel open (phone ↔ oracle)

1. App and oracle derive session keys via BRC-42; all channel traffic is
   end-to-end encrypted.
2. App funds `UTXO_channel` (a small amount covering oracle service fee +
   settlement fee) via BRC-100 `createAction`.
3. Both parties sign channel state tx `C₀` spending `UTXO_channel`:
   - `nLockTime = T_open + 1800` (Unix time), so **the channel cannot settle
     early**: Teranode's `ValidLockTime` makes any pre-30-minute settlement
     attempt with non-final sequences invalid. Time enforcement is
     transaction-level, not script-level (post-Genesis BSV has no CLTV/CSV).
   - `nSequence = 0` (non-final).

### 4.3 1 Hz presence updates

Each second `i ∈ [1, 1800]`, the app:

1. Reads GPS sample `sᵢ = (latᵢ, lonᵢ, tᵢ, meta)`; `meta` carries anti-spoofing
   evidence (§8): device-attestation token hash, venue BLE beacon nonce if present.
2. Draws a fresh salt `kᵢ`, computes commitment `cᵢ = HMAC(kᵢ, sᵢ)`.
3. Sends `{i, sᵢ, kᵢ, cᵢ, sig}` to the oracle over the encrypted channel and
   locally appends `cᵢ` to its Merkle tree.
4. Signs channel update `Cᵢ`: same outputs, `nSequence = i`, output datum updated
   to the running Merkle root. Higher sequence replaces lower, per original
   Bitcoin channel semantics.

The oracle verifies, per sample: `cᵢ = HMAC(kᵢ, sᵢ)`; `haversine(sᵢ, G) ≤ r`;
`tᵢ` monotonic with `tᵢ − tᵢ₋₁ ≈ 1 s`; plausibility checks (§8). "The GPS hasn't
changed" is defined as **every sample inside the geofence**, not bitwise-identical
coordinates — real GPS jitters, and 1,800 identical readings are themselves
treated as spoofing evidence. A bounded gap policy (e.g. ≤ 10 consecutive missed
or out-of-fence seconds, ≤ 60 total) tolerates GPS/radio flakiness; exceeding it
aborts the attempt and the channel settles as a refund minus oracle fee.

### 4.4 Settlement

After update 1,800 and once wall-clock passes `nLockTime`, both parties sign the
final settlement `C₁₈₀₀` (all `nSequence = 0xFFFFFFFF`): pays the oracle fee,
returns change, and carries an output embedding
`MerkleRoot(c₁…c₁₈₀₀) ‖ H(G) ‖ T_open ‖ T_close`. The app submits it through
Arcade's normal pipeline; `bump_builder` produces its BUMP.

### 4.5 Unlock and review

The reviewer builds the unlock tx spending `UTXO_bounty`:

- **Inputs**: `UTXO_bounty`, unlocked per §6.
- **Outputs**: bounty value to the reviewer's BRC-42-derived key (into the
  "reviews" basket), plus a data output carrying the review text, `H(G)`, and the
  settlement txid — the review is thereby *provably* bound to the presence proof.

The oracle signs only if the settlement tx (with the Merkle root it verified) is
mined — checked by SPV against a BUMP, per Wright's
[SPV model](https://coingeek.com/dr-craig-wright-talks-simplified-payment-verification/).

## 5. What "proof" means here

On-chain script can verify **signatures over data, hashes, and transaction
structure — not physics**. The chain proves: *the oracle attested* (v1) or *a
well-formed attestation chain exists* (v2) that 1,800 in-fence, monotonic,
1 Hz-spaced samples were committed between `T_open` and `T_close ≥ T_open + 1800`,
and that the settlement could not be finalised before `nLockTime`. Whether the
device truly sat at the table is an oracle/app-layer question (§8). This split —
consensus verifies commitments, edges verify the world — is exactly Wright's
on-chain/off-chain division.

## 6. Locking script options for `UTXO_bounty`

### 6.1 v1 — oracle co-signature (ship this)

```
OP_2 <reviewer_session_pubkey> <oracle_pubkey> OP_2 OP_CHECKMULTISIG
```

(or the equivalent two `OP_CHECKSIGVERIFY`s to avoid multisig quirks). Simple,
cheap, works today. The oracle's signature is its public, slashable attestation:
anyone can demand the committed samples' salts for audit (the reviewer discloses
selectively) and compare against the on-chain root. Venue and oracle must not
collude with the reviewer — acceptable at review-bounty stakes.

### 6.2 v2 — trust-minimised script (sCrypt, post-Genesis big scripts)

Post-Genesis BSV removes script-size and op-count limits, so a full verifier is
possible: an sCrypt contract using the OP_PUSH_TX technique to introspect the
spending transaction, verifying (a) inclusion of the settlement tx's Merkle root
via its BUMP, (b) `T_close − T_open ≥ 1800`, (c) a random-challenge spot-check —
the unlocker must reveal `n` randomly selected committed samples (indices derived
from the settlement txid, unpredictable at commitment time) each with valid salt,
in-fence coordinates, monotonic timestamps, and a device-key signature. Spot-check
keeps the script tractable (verify `n ≈ 32` of 1,800, cheat-probability bounds by
standard sampling argument) instead of verifying all 1,800 in-script. The oracle
degrades to a dumb relay; presence policy moves into consensus-verifiable script.
Specified for later — do not block v1 on it.

## 7. Privacy properties (Wright-aligned)

- **On-chain**: HMAC commitments' Merkle root, hashed geofence id, timestamps,
  one-time keys. No coordinates, no identity, no linkage between two proofs by
  the same person.
- **Oracle sees**: session-keyed samples for one channel; cannot link sessions
  across BRC-42 derivations without the reviewer's cooperation.
- **Selective disclosure**: any single `(sᵢ, kᵢ)` can be revealed to a disputant
  and checked against the root; the rest of the trail stays private.
- **Broadcast ≠ publish**: the 1 Hz "broadcast" the user asked for is to the
  *channel counterparty*, not to the world — this is the design's central privacy
  decision, and it is also what makes it economically free.

## 8. Threat model (honest limits)

| Threat | Mitigation | Residual |
| --- | --- | --- |
| Mock-location / rooted device | Play Integrity / hardware key attestation token hashed into `meta` each sample | Attestation bypass on compromised devices |
| GPS replay from a real prior visit | Salts + oracle-issued per-second nonces echoed in `meta`; timestamps checked against oracle clock | — |
| Static spoof (identical coords) | Jitter analysis: real GPS varies; zero-variance trails rejected | Sophisticated jitter simulation |
| Remote spoofer never on site | Venue BLE/Wi-Fi beacon broadcasting rotating nonces that must appear in `meta` (optional per venue) | Nonce relay by an on-site accomplice |
| Sybil: one person, many bounties | One unlock per BRC-100 identity certificate per venue per epoch (selective disclosure, off-chain check by oracle) | Certificate farming |
| Oracle collusion / laziness | Attestations publicly auditable vs on-chain roots; v2 script removes the trust | v1: collusion possible at bounty-sized stakes |

Design stance: this raises the cost of a fake review from zero to "defeat device
attestation + be near the venue + burn an identity certificate", priced against a
small bounty. It does not claim spoof-proof geolocation; nothing on-chain can.

## 9. Android reference app

- Foreground service + FusedLocationProvider at 1 Hz (`PRIORITY_HIGH_ACCURACY`);
  30 min ≈ modest battery cost, WebSocket kept alive on the channel.
- BRC-100 wallet via wallet-toolbox (no in-app key custody); `createAction` for
  funding and unlock txs; HMAC/derivation via the wallet interface.
- Play Integrity API token per session, hash chained into samples.
- UX: venue list (from `GET /v1/bounty` by proximity) → "start dwell" → live
  progress ring (1800 s) → settle → compose review → unlock tx → BUMP-backed
  "verified visit" badge.

## 10. Arcade service changes

- `services/presence_oracle/` — new mode: WebSocket channel endpoint, per-channel
  state machine (OPEN → STREAMING → SETTLEABLE → SETTLED | ABORTED), sample
  verifier, Merkle accumulator, co-signer (key in KMS/HSM, not config).
- `api_server` — bounty CRUD + channel bootstrap routes; SSE for bounty state.
- Kafka `arcade.presence_settled` — presence-oracle → api_server bounty-state
  updates, mirroring existing topic conventions.
- Store — `bounties`, `presence_channels`, `presence_roots` tables.
- Reuse unchanged: `tx_validator`, `propagation`, `bump_builder`, chaintracks SPV.

## 11. Alternatives considered

- **Broadcast a real tx every second (as literally requested)**: 1,800 on-chain
  txs per dwell; publishes a movement trail (privacy failure per §1.2); costs
  1,800× fees. Rejected in favour of channel updates — same cadence, two on-chain
  txs, no public trail.
- **nLockTime-only "proof"** (no samples): proves elapsed time, not presence;
  rejected as the sole mechanism, retained as the early-settlement guard.
- **Hash-puzzle bounty** (venue reveals preimage on-site): proves a moment of
  presence, not 30-minute dwell; preimage is trivially shareable. Rejected.
- **BIP68 relative locktimes in-script**: not enforced post-Genesis on BSV
  (verified in Teranode validator). Rejected.
