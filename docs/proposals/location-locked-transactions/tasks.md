# Tasks

## 1. Research (complete)

- [x] 1.1 BRC-100 wallet-to-application interface: createAction/signAction, BRC-42
      derivation, certificates, baskets, HMAC primitives; wallet-toolbox as the
      reference implementation. Findings in `design.md` §1.1.
- [x] 1.2 Craig Wright on transaction data privacy: on-chain vs off-chain data
      division, salted-hash/HMAC commitments, new-key-per-transaction privacy
      model, nSequence payment channels, provable-event reviews. Findings and
      sources in `design.md` §1.2.
- [x] 1.3 Teranode finality behaviour verified in-repo: non-final rejection
      (`util/lock_time.go`, TNJ-13) and post-Genesis BIP68 short-circuit
      (`services/validator/TxValidator.go`). Consequence: channel updates stay
      P2P; no Teranode changes required.

## 2. Protocol fixtures (do first, pure Go, no service wiring)

- [ ] 2.1 Sample commitment codec: `cᵢ = HMAC(kᵢ, sᵢ)` encode/verify + Merkle
      accumulator with test vectors.
- [ ] 2.2 Channel state machine: OPEN → STREAMING → SETTLEABLE → SETTLED |
      ABORTED, with nSequence monotonicity and nLockTime guards; property tests
      for the gap policy (≤10 consecutive, ≤60 total out-of-fence seconds).
- [ ] 2.3 v1 bounty locking/unlocking script builders (go-bt) + unit tests,
      including the failure cases (missing oracle sig, wrong session key).

## 3. Presence oracle service

- [ ] 3.1 New `--mode presence-oracle`: WebSocket endpoint, per-channel verifier
      (commitment, haversine geofence, cadence, jitter, attestation metadata),
      Merkle accumulator, abort/refund path.
- [ ] 3.2 Co-signer with oracle key from KMS (never from config file); SPV check
      of settlement BUMP before unlock co-signature.
- [ ] 3.3 Kafka topic `arcade.presence_settled`; store tables `bounties`,
      `presence_channels`, `presence_roots`.

## 4. API server routes

- [ ] 4.1 `POST /v1/bounty`, `GET /v1/bounty/:outpoint`, proximity query;
      `POST /v1/presence/channel` bootstrap. Write the `api-server` spec delta.
- [ ] 4.2 SSE bounty-state stream reusing the existing SSE pipeline.

## 5. Android reference client

- [ ] 5.1 Foreground service, FusedLocationProvider @ 1 Hz, Play Integrity token
      per session; channel client over WebSocket.
- [ ] 5.2 BRC-100 integration via wallet-toolbox: createAction for funding and
      unlock; BRC-42 session keys; "reviews" basket.
- [ ] 5.3 Dwell UX: progress ring, abort handling, review composer, BUMP-backed
      "verified visit" badge.

## 6. v2 (deferred, do not block)

- [ ] 6.1 sCrypt spot-check verifier (OP_PUSH_TX, settlement-txid-derived random
      challenge over committed samples) replacing the oracle co-signature.
