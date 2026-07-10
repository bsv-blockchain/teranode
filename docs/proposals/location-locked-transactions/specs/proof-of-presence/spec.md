# Proof-of-Presence Capability

## ADDED Requirements

### Requirement: Bounty registration and locking

A venue SHALL be able to register a review bounty consisting of a geofence
(latitude, longitude, radius in metres), a dwell requirement in seconds (default
1800), review terms, and a funded UTXO whose locking script requires both the
reviewer's session key signature and the presence oracle's signature (v1 script,
design.md §6.1).

#### Scenario: Venue registers a bounty
- **WHEN** a venue POSTs a valid bounty definition with a funding transaction to `/v1/bounty`
- **THEN** arcade broadcasts the funding transaction through the standard pipeline
- **AND** the bounty is queryable by outpoint and by geographic proximity

#### Scenario: Malformed geofence rejected
- **WHEN** a bounty is registered with a non-positive radius or dwell below 60 s
- **THEN** the API responds 400 and nothing is broadcast

### Requirement: Presence channel lifecycle

The presence oracle SHALL run a peer-to-peer payment channel per dwell attempt:
an on-chain funding transaction, a sequence of non-final channel updates
(`nSequence` strictly increasing, `nLockTime = T_open + dwell`) exchanged only
over the channel WebSocket, and a single final settlement transaction. Non-final
updates MUST NEVER be submitted to the broadcast pipeline (Teranode rejects
non-final transactions, TNJ-13).

#### Scenario: Early settlement impossible
- **WHEN** a settlement with non-final input sequences is attempted before `nLockTime`
- **THEN** the oracle refuses to co-sign
- **AND** any unilateral broadcast is rejected by the network as non-final

#### Scenario: Successful settlement
- **WHEN** 1,800 valid sequential updates have been exchanged and wall-clock time exceeds `nLockTime`
- **THEN** both parties sign a final settlement embedding the Merkle root of all sample commitments, `H(geofence)`, `T_open`, and `T_close`
- **AND** the settlement is broadcast via the standard pipeline and BUMP-proven by `bump_builder`

### Requirement: Per-second presence commitments

Each channel update SHALL carry exactly one GPS sample commitment
`cᵢ = HMAC(kᵢ, sᵢ)` with fresh salt `kᵢ`, where the raw sample `sᵢ` is disclosed
only to the oracle over the encrypted channel. The oracle SHALL verify, per
sample: commitment correctness, geofence containment (haversine distance ≤
radius), timestamp monotonicity at 1 s cadence, and anti-spoofing metadata
(device-attestation hash, echoed oracle nonce). Raw coordinates MUST NOT appear
in any transaction or any on-chain data.

#### Scenario: Out-of-fence sample
- **WHEN** a sample's coordinates fall outside the geofence beyond the configured gap allowance
- **THEN** the oracle aborts the attempt and the channel settles as a refund minus fee, with no presence attestation

#### Scenario: Zero-jitter trail rejected
- **WHEN** all received samples carry bitwise-identical coordinates
- **THEN** the oracle classifies the trail as spoofed and aborts without attestation

### Requirement: Bounty unlock bound to presence proof

The bounty UTXO SHALL be spendable only with the oracle co-signature, which the
oracle SHALL grant only after SPV-verifying (via BUMP) that the corresponding
settlement transaction is mined. The unlock transaction SHALL carry the review
payload, the hashed geofence identifier, and the settlement txid in a data
output, binding the review to the presence proof.

#### Scenario: Review published with proof linkage
- **WHEN** a reviewer submits an unlock transaction after a mined settlement
- **THEN** the oracle co-signs, the transaction is broadcast, and the review data output references the settlement txid

#### Scenario: No settlement, no unlock
- **WHEN** a reviewer requests co-signature without a mined settlement for that bounty and session key
- **THEN** the oracle refuses and the bounty remains locked

### Requirement: Key freshness

All reviewer keys used in channels, settlements, and unlock transactions SHALL be
one-time keys derived via BRC-42 through the BRC-100 wallet interface. Two dwell
attempts by the same user MUST NOT be linkable on-chain via key reuse.

#### Scenario: Distinct sessions use distinct keys
- **WHEN** the same wallet performs two dwell attempts
- **THEN** the session public keys, settlement outputs, and unlock outputs share no common public key across the two attempts
