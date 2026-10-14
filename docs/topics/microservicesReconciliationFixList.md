# Reconciliation Fix List

Actionable follow-ups distilled from [microservicesReconciliationNotes.md](microservicesReconciliationNotes.md)
(84 findings, 60 open questions). Tick each item as it is addressed; add the PR/commit
reference next to the tick when one exists.

Already-fixed findings are excluded (sections 16, 21, 22 shipped fixes with tests).
Group H items are blocked on maintainer decisions per the notes' "Decisions taken"
section — decide before patching. `§` references are sections in the notes file.

Progress: `for f in microservicesReconciliationFixList.md; do d=$(grep -c '^- \[x\]' $f); t=$((d + $(grep -c '^- \[ \]' $f))); echo "$d / $t done"; done`

## A. One-line config / example fixes (files the examples already own)

- [x] Set `pruner_block_trigger = OnBlockMined` in Docker mainnet/testnet stacks — with
      the persister commented out, the default `OnBlockPersisted` waits on a notification
      that never arrives; deletion never starts, retention stops bounding anything (§38, Q20)
- [x] Fix `imagePullPolicy: Never` on `alertSystem` in the mainnet example — copy-paste
      residue from the dev fixture; alerting won't start on a clean node (§25, Q15)
- [x] ~~Fix `asset_concurrency_get_legacy_block_reader` comment in `settings.conf`: says
      `0 (unlimited)`, actual default is `-1` (one reader per CPU) (§48, Q28)~~ —
      **no change needed, disproven on re-check**: upstream commit `19baf6e32` flipped
      the code default `-1` → `0` (unlimited) and updated both comment texts together;
      verified `default:"0"` in `settings/asset_settings.go`, the `initSemaphore`
      comment (`0 = unlimited, -1 = NumCPU`) in `services/asset/repository/repository.go`,
      and the `settings.conf` line now all agree at this branch's base
- [x] Fix `rpc_user`/`rpc_pass` `longdesc`: says empty creds disable auth, code and
      `services/rpc/README.md` fail closed — fix whichever loses the decision (F2) (§60, Q37)
- [x] Enable ingest backpressure **and** a bounded block-assembly queue together in
      examples — one cannot work without the other; shipped trade is an unbounded queue
      plus two invisible failure modes (§17, §18, Q9) — applied in
      `deploy/kubernetes/teranode/teranode-configmap.yaml` (the env-based config the Cluster
      CRs consume), not the Docker anchors: no Docker stack runs the validator, so the pair
      cannot be carried together there — a lone queue cap would be exactly the half-enabled
      state the notes warn about
- [x] Set `peer_registry` persistence in the single-node stacks, or drop the setting
      description's promise that an on-disk store is the single-node default — bans are
      forgotten on every restart (§31, Q16)
- [x] Add a commented `kind: Secret` placeholder (or a sentence) for
      `grpc_admin_api_key` in `deploy/kubernetes/teranode/teranode-cr.yaml` — REQUIRED for
      startup, defined nowhere under `deploy/` (§79, Q56)
- [x] Set `asset_httpPublicAddress` in the examples, or reword the start-up warning to say
      "private node that happens to gossip" is a supported posture (§50, Q29)
- [x] Point the transaction blob store somewhere in `settings.conf`, or comment the line
      with why the node doesn't remember transactions; reconcile the four test rigs
      mounting an empty directory (§67, Q45)
- [x] Comment the dev fixture with the services it switches off (Validator et al.) so devs
      don't believe they exercised the transaction path (§45, Q25)
- [x] Comment the Docker compose files that the Teranode alert service is not deployed —
      the unrelated `alertmanager` decoy answers the question wrongly (§66, Q44)
- [x] Add `ulimits` blocks (or an install-guide line) for blob-touching services —
      file-store budget of 768 reads / 256 writes assumes a raised `ulimit -n` that no
      container sets (§69, Q47)
- [x] Kubernetes examples: deploy the pruner or comment that pruning is off in cluster
      topology — the two shapes fail in opposite directions vs Docker (§55, §56, Q34)

## B. Cheap observability / logging additions (small code changes)

- [x] Alert service: record successful freezes in the response — "froze five coins" and
      "matched nothing" are currently the same reply (Q41) — applied as per-fund success
      logs plus a processed/applied summary (`services/alert/node.go`), with a test; the
      response body itself still lists only failures because
      `AddToConsensusBlacklistResponse` lives in the external `go-bn` module — adding a
      `processed` field there is a separate upstream change
- [ ] Distinguish a missing deletions ledger from being caught-up — three situations share
      one log line, and the tolerance for one is keyed to an error the shipped file store
      never emits (§42, Q23)
- [ ] Log the coin archive's fixed 100-block lag at start-up (or make it a setting) so the
      coupling is visible before downtime (§43, Q24)
- [ ] Log loudly when the coin store is *unwired*, matching the existing loud log for a
      *disabled* preservation phase (§53, Q31)
- [ ] Warn and count the mined-set wait the way the assembly wait already does — today the
      common skip is uncounted debug noise (§54, Q32)
- [ ] Asset Server: one start-up line naming the active tier mode and allowlist size —
      elevation is unreachable today and an operator sees no change (§46, Q26)
- [ ] Pruner: announce its trigger mode at start-up (or a pruning-passes counter) so
      "never triggered" isn't discovered via disk growth (§38, Q21)
- [ ] File store: log once at start-up when no default retention is in force (§43, Q35)
- [ ] Metadata cache: restore the `tx_meta_cache_size` gauge — commented out while the
      same file's doc block still advertises it; `map_size` can't substitute (§75, Q53)

## C. Behaviour fixes

- [ ] Filing retry loop: pause on the unresolvable branches (pruned source data retried
      back-to-back forever), drop the pause on the two branches that don't need it (§39, Q22)
- [ ] Make SQLite and Aerospike agree on `utxostore_reassignedUtxoSpendableAfterBlocks` —
      the knob works on the dev store and is ignored on the production one (§64, Q43)
- [ ] Confiscation: persist the replacement locking script, or refuse with a clear message
      — the output is currently stranded for both owners (§63, Q42)
- [ ] `getchaintips`: shorten the five-minute cache, invalidate on chain update, or
      document the staleness (§61, Q40)
- [ ] RPC door: bound-and-queue instead of refuse + `Connection: close` if machine traffic
      is in scope, else rename "maximum clients" and say what it limits (§58, Q38)
- [ ] `pruner_skipDuringCatchup`: rename to reflect "requires an operator command", or
      test for actual catch-up (§55, Q33)

## D. Single-source-of-truth / consistency

- [ ] Hash-prefix table: one shared constant (or a test walking both tables) instead of
      the Pruner's copy of `daemon_stores.go` defaults behind a "must match" comment; the
      copy is the path taken in shipped config and the failure message blames the volume
      mount (§68, Q46)
- [ ] Peer-registry TTL floor: express the five minutes once (three constants, two
      services, batcher is load-bearing) or guard with a test (§32, Q18)
- [ ] `validator_txmeta_numPartitions` (32) vs `KAFKA_PARTITIONS_HIGH` (8): reconcile at
      start-up with a metadata query — the async producer has no fallback (§74, Q52)
- [ ] Txmetacache bucket count: carry in the v2 wire header or log on both sides —
      build-tag-selected, Kafka partitions by it, no check that both binaries were
      compiled alike (§73, Q51)
- [ ] Write a four-byte marker of `utxostore_utxoBatchSize` into the UTXO data directory
      and check at open — it is part of a coin's address and must never change post-run,
      yet nothing records what a directory was written with (§70, Q48)
- [ ] Name the un-prefixed `double_spend_window_millis` key consistently (it drives two
      unrelated behaviours) or document where operators look (§19, Q11)

## E. Tests to add

- [ ] Enumerate validator option proto fields and assert each is listed in
      `nonDefaultValidationOptions` or exempt — replaces a fragile manual contract (§16, Q10)
- [ ] Coinbase maturity at the exact boundary (`d == CoinbaseMaturity` and `+1`) — makes
      the `>=` → `>` fix executable (§24, Q14)
- [ ] Assert the response to an unauthenticated RPC request when no credentials are
      configured — settles F2 permanently (§60, Q37)

## F. Auth / security posture (decide, then small change)

- [ ] Six unauthenticated gRPC doors (Validator, Block Assembly, Block Validation,
      Subtree Validation, Pruner, Propagation) with `security_level_grpc = 0`: either wire
      auth or state "the private network is the perimeter" in `settings.conf` (§78, Q55)
- [ ] Empty `rpc_user`/`rpc_pass`: choose fail-closed vs auth-disabled; fix the losing
      sentence (see A3, E3) (§60, Q37)
- [ ] `maxrawtxfee`: add to `settings.conf`, or move the policy into the Validator so it
      applies to every door, not just RPC (§57, Q36)
- [ ] Heavy-route anonymous ceiling: decide whether the two-bucket split doubling 10 → 20
      sustained is intended; if not, halve the default (§47, Q27)
- [ ] Limited-tier allowlist: prune the 19 of 44 unimplemented methods, or label the list
      as granting permission not capability (§59, Q39)

## G. Dead code / naming cleanup

- [ ] Kubernetes resolver: registered then silently overwritten in the same call — wire it
      or delete the package (§80, Q57)
- [ ] `aerospike` build tag: no longer gates the store, but four Makefile targets and
      `AGENTS.md` say it does — remove the tag or restore the gating (§72, Q50)
- [ ] UDP multicast fast lane: document as receive-only (no sender exists; default
      interface is macOS `en0`), or build the sender (§82, Q58)
- [ ] Comment the deletion-scheduler interface in `stores/blob/options/pruner_client.go` —
      the queue is owned by Blockchain, and the filename points the next reader at the
      pruner (§52, Q30)
- [ ] Ledger store multi-instance machinery (reservation table, advisory lock, catalog
      probe): say the topology is coming (with a footnote) or retire three maintained
      mechanisms (§83, Q59)
- [ ] `ReasonInvalidSubtree`: add an intent comment so nobody "tidies" it into
      `AddBanScore` and starts banning peers for network jitter — gated on H1 (§2, Q1)

## H. Blocked on a maintainer decision (per "Decisions taken", deliberately untouched)

- [ ] Is reputation-only for invalid subtrees policy? (gates G6) (§2, Q1)
- [ ] Which front door is *intended* vs which ships; should examples deploy Propagation
      (§13, §26, Q5–Q7)
- [ ] Is the unbounded ingest queue an accepted risk now the drain is bounded (§17–19, Q12)
- [ ] Correct the howto's false "no transition from CATCHINGBLOCKS back to IDLE" — needs
      an explicit exception to the no-editing-existing-files decision (§30, Q17)
- [ ] Resolve the three-way overloading of "level" — also blocked by that decision (§8, Q4)
- [ ] Measure the in-memory chain-check flag's duty cycle (and
      `blockchain_use_in_memory_chain_check` vs the five-connection SQL pool) — measurement
      decides whether to flip or document (§33, §84, Q19/Q60)
