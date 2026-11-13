# Reconciliation Fix List

Actionable follow-ups distilled from [microservicesReconciliationNotes.md](microservicesReconciliationNotes.md)
(84 findings, 60 open questions). Tick each item as it is addressed; add the PR/commit
reference next to the tick when one exists.

Already-fixed findings are excluded (sections 16, 21, 22 shipped fixes with tests).
Group H items are blocked on maintainer decisions per the notes' "Decisions taken"
section — decide before patching. `§` references are sections in the notes file;
the notes are edited independently and renumber, so a citation points at the
numbering at the time the row was written — verify against current text.
If a checkbox and its commit ever disagree, the code is authoritative and the
row gets re-verified, not the other way round.

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
- [x] Distinguish a missing deletions ledger from being caught-up — three situations share
      one log line, and the tolerance for one is keyed to an error the shipped file store
      never emits (§42, Q23)
- [x] Log the coin archive's fixed 100-block lag at start-up (or make it a setting) so the
      coupling is visible before downtime (§43, Q24) — start-up line in `Start()`; turning
      the constant into a setting stays open as the heavier option
- [ ] Active mode should reject async timeout + sync persistence combination (§44, Q27) —
      premise not verifiable on this branch: `persister_active_mode`, `persister_async_timeout`
      and the persistence-mode knobs appear nowhere in this codebase's settings or service,
      so there is no combination to reject here. An earlier checklist row for this item was
      lost in an edit and the work attributed to it is not in the branch history. Re-check
      the note against the branch it was written from before applying or dropping the item
- [x] Log loudly when the coin store is *unwired*, matching the existing loud log for a
      *disabled* preservation phase (§52, Q31 — the checklist previously cited §53; the
      phases are in `services/pruner/worker.go`)
- [x] Warn and count the mined-set wait the way the assembly wait already does — today the
      common skip is uncounted debug noise (§53 in the current notes — Q32's §54 citation
      predates a renumbering)
- [x] Asset Server: one start-up line naming the active tier mode and allowlist size —
      elevation is unreachable today and an operator sees no change (§46, Q26)
- [x] Pruner: announce its trigger mode at start-up (or a pruning-passes counter) so
      "never triggered" isn't discovered via disk growth (§38, Q21)
- [x] File store: log once at start-up when no default retention is in force (§43, Q35)
- [x] Metadata cache: restore the `tx_meta_cache_size` gauge — commented out while the
      same file's doc block still advertises it; `map_size` can't substitute (§75, Q53)
      — restored, sourced from `ValidEntriesCount`; the raw map count (`EntriesCount`)
      no longer exists in `CacheStats`

## C. Behaviour fixes

- [x] Filing retry loop: pause on the unresolvable branches (pruned source data retried
      back-to-back forever), drop the pause on the two branches that don't need it (§39, Q22)
      — same-block failures (bytes/persist/mark) now escalate from `persistSleep` to a 5-minute
      cap and announce once as needing an operator; the tip query retries after one second;
      the idle branch keeps its flat sleep
- [x] Make SQLite and Aerospike agree on `utxostore_reassignedUtxoSpendableAfterBlocks` —
      the knob works on the dev store and is ignored on the production one (§64, Q43)
      — one shared resolver in the utxo store package, wired into both ReassignUTXO paths;
      setting docs and a commented settings.conf line say the same
- [x] Confiscation: persist the replacement locking script, or refuse with a clear message
      — the output is currently stranded for both owners (§63, Q42)
      — refused by default with a per-input message naming the opt-in setting
      (`alert_confiscationAllowReassignWithoutScript`); persistence deferred to upstream
      issue 1725, which is a storage-format change touching both stores' read paths
- [x] `getchaintips`: shorten the five-minute cache, invalidate on chain update, or
      document the staleness (§61, Q40) — invalidated on chain update by construction:
      the cache entry is keyed by the best block hash, so a new block, reorg, or
      invalidation retires it instantly; polling inside one tip still hits
- [x] RPC door: bound-and-queue instead of refuse + `Connection: close` if machine traffic
      is in scope, else rename "maximum clients" and say what it limits (§58, Q38)
      — took the rename-and-say branch: key label, longdesc, field comment and settings.conf
      now all say per-request/Connection-close/shed-with-503; refusals carry `Retry-After: 1`.
      Queueing left unchosen deliberately — Q38's "is machine traffic in scope" is a per-deployment
      question and silent queueing trades the door's shed-fast guarantee for latency nobody asked for
- [x] `pruner_skipDuringCatchup`: rename to reflect "requires an operator command", or
      test for actual catch-up (§55, Q33 — the notes renumbered; this is section 54's finding)
      — key kept (renaming a setting key is breaking), name fully disclosed: desc/usage/longdesc
      now state the operator-RUN requirement and the never-RUN stop-switch, and the guard
      warns by FSM state name when it halts (IDLE escalated, throttled 10min/1h)

## D. Single-source-of-truth / consistency

- [x] Hash-prefix table: one shared constant (or a test walking both tables) instead of
      the Pruner's copy of `daemon_stores.go` defaults behind a "must match" comment; the
      copy is the path taken in shipped config and the failure message blames the volume
      mount (§68, Q46) — one table in `storetypes.DefaultHashPrefix`, both sides call it;
      pinned-value and cross-side equality tests added
- [x] Peer-registry TTL floor: express the five minutes once (three constants, two
      services, batcher is load-bearing) or guard with a test (§32, Q18) — one leaf package
      (`services/p2p/peerregistry`) holding publish cadence, reassert window, multiplier and
      the derived floor; both services reference it and a test guards the derivation
- [x] `validator_txmeta_numPartitions` (32) vs `KAFKA_PARTITIONS_HIGH` (8): reconcile at
      start-up with a metadata query — the async producer has no fallback (§74, Q52)
      — validator now reconciles at start-up (broker metadata via optional
      `kafka.TopicPartitionCounter`, URL `partitions=` fallback, bucket divisibility),
      errors naming both numbers; v1 default still unaffected
- [x] Txmetacache bucket count: carry in the v2 wire header or log on both sides —
      build-tag-selected, Kafka partitions by it, no check that both binaries were
      compiled alike (§73, Q51)
      — v2 header bytes 2:4 now carry the producer's BucketsCount (uint16 LE, was
      reserved zeros); subtreevalidation warns (throttled) and counts a prometheus
      metric, legacy netsync warns, on any mismatch; zero stays parseable as the
      pre-buckets producer format so retained topic messages survive a rolling upgrade
- [x] Write a four-byte marker of `utxostore_utxoBatchSize` into the UTXO data directory
      and check at open — it is part of a coin's address and must never change post-run,
      yet nothing records what a directory was written with (§70, Q48)
      — the Aerospike engine now keeps one record in a dedicated `utxo_layout` set of the
      namespace: recorded CREATE_ONLY on first open (race loser re-reads the winner),
      every later open refuses with both numbers named on disagreement
- [x] Name the un-prefixed `double_spend_window_millis` key consistently (it drives two
      unrelated behaviours) or document where operators look (§19, Q11)
      — `blockassembly_doubleSpendWindowMillis` is canonical (key tag, settings.conf,
      longdesc now naming both the mining hold-back and the drain bound plus the
      incident); the unprefixed key loads on as a deprecated alias, prefixed wins

## E. Tests to add

- [x] Enumerate validator option proto fields and assert each is listed in
      `nonDefaultValidationOptions` or exempt — replaces a fragile manual contract (§16, Q10)
      — already on main as of the B-022 fix itself (upstream #1781):
      `http_options_guard_descriptor_test.go` walks the proto descriptor, sets every
      wire field non-default (handling add_tx_to_block_assembly's inverted default),
      demands the guard name it, and pins the covered-field count so the walk cannot
      pass vacuously. Re-checked green on this branch; no work needed.
- [x] Coinbase maturity at the exact boundary (`d == CoinbaseMaturity` and `+1`) — makes
      the `>=` → `>` fix executable (§24, Q14)
      — `catchup_fork_depth_boundary_test.go` pins d = maturity-1/maturity/maturity+1
      at validateForkDepth directly; verified mutation-sensitive (flipping the operator
      back to `>=` fails exactly the boundary case and nothing else)
- [x] Assert the response to an unauthenticated RPC request when no credentials are
      configured — settles F2 permanently (§60, Q37)
      — `auth_empty_credentials_test.go` pins the fail-closed behaviour at both decision
      points (credential assembly yields the zero array when either half is empty;
      `checkAuth` rejects no-header, plausible-header, and the empty-login header),
      with a positive control so the rejections cannot be harness artifacts. The
      contradicting longdesc sentence itself is group F's decision (see F2)

## F. Auth / security posture (decide, then small change)

- [ ] Six unauthenticated gRPC doors (Validator, Block Assembly, Block Validation,
      Subtree Validation, Pruner, Propagation) with `security_level_grpc = 0`: either wire
      auth or state "the private network is the perimeter" in `settings.conf` (§78, Q55)
      — PARKED (maintainer posture): the facts are verified (only Blockchain, Legacy and P2P
      install the `grpc_admin_api_key` interceptor; the other six start with nothing to check
      and nothing encrypts them). Which way to state it is the decision: wiring the key into
      six services is a deployment-coupling change (every interior caller needs the key),
      while the prose option blesses the current topology. Needs one answer:
      is any gRPC port of a production deployment reachable by anything that is not
      a trusted service?
- [ ] Empty `rpc_user`/`rpc_pass`: choose fail-closed vs auth-disabled; fix the losing
      sentence (see A3, E3) (§60, Q37)
      — PARKED (maintainer posture): today's behaviour is fail-closed and E3's tests
      (`auth_empty_credentials_test.go`) now pin it, so the risk of drifting accidentally is
      gone; what remains is a deliberate choice. Keeping fail-closed means rewriting the
      "Leave empty to disable authentication" longdesc of `rpc_user`/`rpc_pass` to say
      empty locks the door. Choosing auth-disabled changes what every development context
      without credentials means (anything listening could be driven) and would rewrite
      E3's assertions instead of the prose. Needs one answer: should a node with no
      credentials accept unauthenticated RPC at all?
- [x] `maxrawtxfee`: add to `settings.conf`, or move the policy into the Validator so it
      applies to every door, not just RPC (§57, Q36)
      — added to `settings.conf` beside the RPC credentials with its units, bypass,
      default provenance (bitcoin-sv DEFAULT_TRANSACTION_MAXFEE) and the honest note that
      the validator and P2P paths accept the same transaction unchecked; moving the rule
      into the Validator remains the open half, recorded in the comment itself
- [ ] Heavy-route anonymous ceiling: decide whether the two-bucket split doubling 10 → 20
      sustained is intended; if not, halve the default (§47, Q27)
      — PARKED (maintainer intent): the split's purpose (a catching-up peer keeps its floor
      while a bulk caller floods another heavy route) is documented and honest about the
      cost; the start-up line even prints the doubled number. What is not recorded anywhere
      is whether 20/s sustained per unverified IP across the heavy surface is an accepted
      capacity figure or an oversight of the split. Halving `asset_httpHeavyRateLimit`
      would restore the old total but also halve the floor catch-up was the point of
      protecting. Needs one answer: is the doubled anonymous ceiling accepted, or should
      the default move to 5?
- [x] Limited-tier allowlist: prune the 19 of 44 unimplemented methods, or label the list
      as granting permission not capability (§59, Q39)
      — labelled, not pruned: the allowlist comment and `rpc_limit_user`'s longdesc now
      say permission set, not capability list, and `limited_tier_capability_test.go`
      pins the exact 19-name intersection (handler code-pointer identity) so the label
      cannot drift silently; pruning changes which error text integrations see and stays
      a deliberate future call

## G. Dead code / naming cleanup

- [x] Kubernetes resolver: registered then silently overwritten in the same call — wire it
      or delete the package (§80, Q57)
      — the lie removed: the `kubernetes` case no longer registers the in-repo builder it
      immediately overwrote (same scheme key, map assignment), and the comment now names
      which case runs which implementation and that production selects the upstream one.
      End state is behaviorally identical — that is exactly why the overwrite was
      invisible — so the remaining wire-it-or-delete call (Q57) stays open, now stated
      at the site instead of contradicted by it; misleading `[VALIDATOR]` prefixes gone
      from a shared helper's logs
- [x] `aerospike` build tag: no longer gates the store, but four Makefile targets and
      `AGENTS.md` say it does — remove the tag or restore the gating (§72, Q50)
      — alignment in the honest direction (the tree's actual state): the four files whose
      `// //go:build aerospike` double-slash made prose-not-constraint are cleaned up and
      their "requires the build tag" prose corrected, `AGENTS.md` now says the tag gates
      one integration test and the store is always compiled, and the Makefile gains a
      note at the build targets. Restoring real gating would need build-gated factory
      wiring — a change of a different size, left as the open alternative in the notes
- [x] UDP multicast fast lane: document as receive-only (no sender exists; default
      interface is macOS `en0`), or build the sender (§82, Q58)
      — documented as receive-only at all three places an operator meets it (the
      `ipv6_addresses`/`ipv6_interface` longdescs, the settings.conf comment, and the
      start-up log line itself): ingest path not propagation path, empty allowlist is
      unauthenticated accept-everything, `en0` default is macOS and Linux must set the
      interface. Sender re-verified absent (the only UDP write in the tree is UPnP SSDP);
      building it remains the open alternative (Q58)
- [x] Comment the deletion-scheduler interface in `stores/blob/options/pruner_client.go` —
      the queue is owned by Blockchain, and the filename points the next reader at the
      pruner (§52, Q30)
      — the comment now says outright: Blockchain's queue implements and runs this, the
      pruner neither implements the interface nor owns the queue, the filename is read
      as "blockchain", and the fail-the-write-not-drop-the-promise asymmetry is named
      for the next tracer; rename stays the open alternative (Q30), comment is the
      fix-list branch
- [x] Ledger store multi-instance machinery (reservation table, advisory lock, catalog
      probe): say the topology is coming (with a footnote) or retire three maintained
      mechanisms (§83, Q59)
      — the package doc now names all three mechanisms and the topology they serve, and
      is honest about both halves: the cross-process layers buy readiness for a second
      instance no repository deployment runs, while their in-process work (two ingestion
      paths racing one block during IBD, promised ids surviving a restart) pays their way
      today. Retirement stays the deliberate option if Q59 answers "no second instance"
- [ ] `ReasonInvalidSubtree`: add an intent comment so nobody "tidies" it into
      `AddBanScore` and starts banning peers for network jitter — gated on H1 (§2, Q1)
      — PARKED because its own gate is: the comment to write depends on whether
      reputation-only is policy (H1/Q1, maintainer decision). Facts verified in the mean
      time: the constant is declared, allowlisted for metric cardinality, penalty-ready on
      the Blockchain side, and charged by no production path; the allowlist is a
      cardinality guard, not a usage inventory. Once H1 answers, the comment writes
      itself — "policy, keep it reputation-only" or "gap, wire it with these numbers"

## H. Blocked on a maintainer decision (per "Decisions taken", deliberately untouched)

- [ ] Is reputation-only for invalid subtrees policy? (gates G6) (§2, Q1)
      — CURRENT, awaiting decision (audited this pass): `ReasonInvalidSubtree` still declared,
      allowlisted, charged by no production path; every caller uses another reason. Either answer
      is actionable at once — policy → add the intent comment (G6 unlocks); gap → wire it
      with a named penalty
- [ ] Which front door is *intended* vs which ships; should examples deploy Propagation
      (§13, §26, Q5–Q7)
      — CURRENT, awaiting decision (audited this pass): still no Propagation service in the base
      compose under deploy/docker/base/; ingest arrives via netsync and the UDP lane, matching the
      notes. Which door the project intends is prose-and-deployment history; the answer decides
      whether examples gain Propagation or the docs stop implying it is the front door
- [ ] Is the unbounded ingest queue an accepted risk now the drain is bounded (§17–19, Q12)
      — CURRENT, awaiting decision (audited this pass): the double-bounded drain fix (one
      clock reading, item count, dequeue-as-it-goes) stands in the tree with its
      tests, so the failure mode is now OOM-by-producer-rate rather
      than OOM-by-wedged-drain. Whether that residual is an accepted risk is an operator-facing
      capacity call, not code
- [ ] Correct the howto's false "no transition from CATCHINGBLOCKS back to IDLE" — needs
      an explicit exception to the no-editing-existing-files decision (§30, Q17)
      — CURRENT, awaiting decision (audited this pass): the sentence is verbatim at
      docs/howto/miners/docker/minersHowToSyncTheNode.md:230 and the transition remains wired and
      exercised by the documented stop path. The only thing blocking the fix is the standing
      decision not to edit existing howto files; the fix itself is two sentences
- [ ] Resolve the three-way overloading of "level" — also blocked by that decision (§8, Q4)
      — CURRENT, awaiting decision (audited this pass): `security_level_grpc` (0/1/2 transport
      security), log level strings, and FSM-state "level" in dashboards all still read the same
      word in `settings.conf` and the dashboard. The cleanup is naming; the block is the same
      no-editing-existing-files decision, widened by how many files carry the word
- [ ] Measure the in-memory chain-check flag's duty cycle (and
      `blockchain_use_in_memory_chain_check` vs the five-connection SQL pool) — measurement
      decides whether to flip or document (§33, §84, Q19/Q60)
      — CURRENT, awaiting measurement (audited this pass): the flag still ships false and the
      SQLite path still hard-caps its pool at five (the #1051 arena-oom guard in `util/sql.go`).
      Nothing in the tree measures the flag's duty cycle; until someone runs it, flip-vs-document
      is a guess, which is exactly why this item stays in H
