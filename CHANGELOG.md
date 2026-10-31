# Changelog

Notable changes to Teranode, with an emphasis on anything an operator must act on.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

This file records **breaking changes**: anything that alters the behaviour of an
existing deployment without the operator changing their configuration, or that
requires a configuration or deployment change before upgrading. Changes that only
add capability, fix a bug without altering a documented contract, or remove
configuration that was already inert are not listed as breaking; a few are noted
under "Not breaking, but worth knowing" where they look alarming but are not.

## [Unreleased]

Entries marked "shipped in v0.16.0" were already in the v0.16.0 release (2026-10-05).
Upgrading from v0.16.0 changes nothing for them; upgrading from an earlier release, they apply in full.

### Breaking — action required before upgrading

- **Conflict undo now checks current-chain confirmation.** BlockAssembly stops
  recovery if a transaction affected by rollback or reverse processing is still
  confirmed in any current-chain block, or if that check fails. The WAL remains
  available for diagnosis. A stale forward intent that was applied and has at least
  one winner confirmed on the current chain keeps the applied state and is completed
  without a rollback, as a reorg keeps a transaction of an orphaned block; this also
  covers a partial re-mine. When BlockAssembly never reached that block, startup
  reverses the winners that the current chain does not confirm. A confirmed winner
  that no longer owns its inputs keeps the intent pending. Pruned parents of confirmed
  winners no longer block startup; current-chain confirmation authorizes those inputs. The parent locks it still
  holds are released. A rollback leaves an input that the operation freed with a
  later owner outside the operation, and the loser that spent it stays conflicting. Failed
  WAL cleanup is now returned and counted in
  `teranode_utxo_conflict_wal_completion_failures_total`. Affected operators must
  resolve the pending recovery instead of clearing the WAL or restarting an old
  build. This does not repair previously corrupted UTXO state. (#2023)

- **Conflict recovery extends the UTXO WAL format and store interface.** SQL stores
  add recovery and lock-owner columns; Aerospike stores add durable checklist
  records. External `utxo.Store` implementations must implement
  `SaveConflictRecovery`, `LoadConflictRecovery`, `RestoreConflictSpends`,
  `SetConflictLocks`, and `ReleaseConflictLocks`. Upgrade BlockAssembly with its
  store implementation. Do not downgrade while new conflict intents are pending.
  A forward intent is now keyed by its block hash, so acknowledgement needs no WAL
  scan; a record written by an older build keeps its old key. Store callers that add
  a winner that is still Conflicting to a block with a pending forward intent now
  receive an error. A winner that is already applied is dropped. Finish the
  saved result's subtree and queue updates and acknowledge the intent with
  `CompleteConflictIntentsForBlock` before submitting additional Conflicting winners. Legacy
  forward intents have no before-image and require operator repair;
  unresolved intents now prevent BlockAssembly startup, and so does an intent whose
  replay failed at startup, even when its checklist reads finished. Preserve the WAL
  and store backup for diagnosis instead of deleting the pending record. See
  [recovery guidance](docs/topics/conflict-recovery.md). (#2023)

- **`CheckBlockSubtrees` gRPC errors keep Teranode error codes.** Storage and
  consensus errors include gRPC status metadata. Invalid URLs give `InvalidArgument`
  status. External clients must use `errors.UnwrapGRPC` to read the error chain
  and must accept these status codes. Teranode clients can read this format. (#1650)

- **`grpc_admin_api_key` is now mandatory for the Blockchain service.** The service
  validates it at startup and refuses to start when it is empty, is a well-known
  placeholder (`testkey`, `changeme`, …), or is shorter than 16 characters. A value
  with surrounding whitespace is trimmed as it loads rather than rejected. Every Blockchain RPC except `HealthGRPC` requires
  the key, and the HTTP `invalidate`/`revalidate` routes require it too.

  Every service that talks to Blockchain must be given the same value. Upgrade
  credential-capable clients first and Blockchain last, or perform a coordinated
  restart — a Blockchain pod that starts before its clients have the key will reject
  them. The key travels in plaintext at `security_level_grpc=0`; use verified TLS
  across untrusted networks. (#1755)

- **Kubernetes: Kafka now sits behind a NetworkPolicy.** (shipped in v0.16.0)
  `deploy/kubernetes/kafka/kafka-shared-networkpolicy.yaml` restricts which pods may
  reach the shared Kafka broker. Pods outside the Kafka broker's namespace lose Kafka
  connectivity once the policy is applied; the rule admits every pod in that namespace,
  regardless of labels, so checking pod labels tells you nothing. If your Teranode
  services run in a different namespace, move Kafka into it or replace the rule with a
  `namespaceSelector` allowlist before applying.

  Nothing changes on upgrade until someone applies the manifest, and it then takes
  effect only on a CNI that enforces NetworkPolicy — one that does not will accept the
  object and silently ignore it. The file's header carries a probe for confirming
  enforcement. (#1617)

- **Out-of-tree `utxo.Store` implementations must add two methods.** `SpendAndCreateMulti`
  (`utxo.DefaultSpendAndCreateMulti` provides it) and `ParentOutputsForValidation` are now
  part of the interface, so an external store stops compiling until it implements them.
  (#1875)

- **Legacy sync requires a `file://` temp store.** Affects every node running the legacy
  service. `temp_store` must be a `file://` URL: legacy sync now streams every downloaded
  block body into it and parks out-of-order blocks there, and a scheme it cannot enumerate
  on restart would leak every parked blob, so the service refuses to start on one. The
  shipped default is already `file://`, so only a node that redirected `temp_store` to
  another backend must change. Provision disk for it — the park holds up to 100 GiB of
  blocks ahead of the chain. (#1699)

- **The shipped `bitcoin`/`bitcoin` RPC credentials are removed.** `settings.conf` no longer sets
  `rpc_user`/`rpc_pass` (only the `test` context keeps them). With both empty, the JSON-RPC server
  rejects every request, so miners, scripts and monitoring that relied on the default pair stop
  working. Set `rpc_user` and `rpc_pass` in `settings_local.conf` before upgrading. (#1790)

- **Asset admin routes fail closed without credentials.** `asset_requireAuthCredentials` defaults to
  `true`: while `rpc_user` or `rpc_pass` is empty, `POST fsm/state`, `POST block/invalidate`,
  `POST block/revalidate`, `GET settings`, and the dashboard's `/api/p2p/*` POSTs answer 401, and
  dashboard login fails. Set the credentials above, or set `asset_requireAuthCredentials = false`
  to restore anonymous admin access (not recommended). (#1790)

- **Blockchain read RPCs are bounded, and old clients break against an upgraded Blockchain.**
  Count and range parameters on the read RPCs are rejected with `InvalidArgument` above
  `blockchain_maxBlockHeadersPerRequest` (default 1,000,000),
  `blockchain_maxBlocksByHeightRange` (2000), `blockchain_maxInvalidBlocksPerRequest` (10000)
  and `blockchain_maxMedianTimePastHeights` (10000), whatever the caller's authentication.
  Callers that request larger counts receive errors.

  **Upgrade order:** the median-time-past cap rejects the whole-chain request that every
  pre-change client sends on first load, and those clients do not retry `InvalidArgument`.
  During a rolling upgrade, an old-image validator, subtreevalidation or blockvalidation pod
  talking to an upgraded Blockchain pod fails every `EnsureMTPLoaded` and validates nothing
  until it is reimaged. No chain state is corrupted. Upgrade Blockchain last, or raise
  `blockchain_maxMedianTimePastHeights` for the duration of the rollout. (#1776)

- **The alert service now picks its network identity by `network`, and validates
  `alert_genesis_keys`.** `alert_genesis_keys`, `alert_protocol_id` and `alert_topic_name`
  default to empty, and each empty one takes the go-alert-system default for the
  configured network: testnet and stn nodes now join their own alert network
  (`/bitcoin-testnet/alert-system/0.0.1`, testnet genesis keys) instead of the mainnet
  protocol and keys. Mainnet resolves to the same values as before. Regtest, teratestnet
  and tstn keep the mainnet keys and protocol on `bitcoin_alert_system_<network>` and log
  a warning. A non-empty `alert_topic_name` is now honoured on every network.

  Affects testnet/stn nodes and any deployment that sets these keys. Clear values copied
  from the old mainnet defaults into `settings_local.conf`, helm values or env, or a
  testnet node stays on the mainnet alert network. An explicit `alert_genesis_keys` must
  now hold 3 to 10 hex-encoded compressed public keys (blank entries are dropped, repeats
  kept once), or the alert service fails to start with a configuration error. An alert
  store that holds only the genesis alert and was seeded with other keys (a testnet or stn
  store from v0.16.x or earlier) has its active keys reset to the resolved genesis keys on
  first start, with a warning; there is no need to delete `alert.db`. (#1955)

### Breaking — behaviour changes on upgrade

- **A short catch-up from `RUNNING` hands its blocks' unseen transactions to block
  assembly, and catch-up no longer fetches a chain with less work than the best tip.**
  Every catch-up moves the FSM to `CATCHINGBLOCKS`, and subtree validation used to stop
  handing transactions to block assembly in that state. A running node that fetched a
  short competing branch therefore wrote the branch's transactions to the UTXO store but
  not to the pool, and a child spending one of them could enter the pool without its
  parent. A catch-up that starts from `RUNNING` and fetches at most
  `blockvalidation_catchup_keep_block_assembly_max_blocks` blocks (new, default 6) now
  keeps the hand-off on; such a run uses the level path, never the batched catch-up path.
  Set the value to 0 to restore the old behaviour. Separately, catch-up now ends as a
  no-op, charging no peer, when the header fetch reached the announced block and the
  whole offered chain has less cumulative work than the best tip; equal work is still
  fetched. The check runs after the existing fork checks, so a fork deeper than coinbase
  maturity and the deep-fork secret-mining case are still refused with an error and
  charged as before. In a mixed-version deploy, a subtree validation older than its block
  validation ignores the new request field and keeps the hand-off off. The existing block-context queue-shed policy still accepts and unlocks
  a transaction without a template entry on `ErrThresholdExceeded`; durable recovery
  from that case is not added here. (#2075, partially addresses #2067)

- **Catch-up quick validation waits for each block's `.subtree` writes before it commits
  the block, and a shutdown waits for a block's in-flight commit.** A block was stored with
  `subtrees_set` while its `.subtree` files could still be in flight, so a failed write
  left a block recorded as having files it did not have. Each quick-validated block now
  waits for its own writes to land; a write that fails leaves the block uncommitted for
  the next catch-up run. Once the block is stored, unlocking its outputs is retried on a
  context that a cancel does not stop, for up to 5 minutes. When `AddBlock` returns an
  error, a retried existence check, bounded at 15 seconds, decides whether to finish the
  commit or retry the block later. Shutdown now waits for a block's commit that is already
  under way, including that check and the unlock, but only up to `service_manager_stopTimeout`
  (default 30s), less up to 5s that shutdown keeps for flushing the invalid-block Kafka
  producer. A stop during catch-up can now use the rest of that time. If the blockchain or
  UTXO store hangs past the stop timeout at shutdown, the block's outputs stay locked
  (tracked in #2015). To give the commit longer at shutdown, raise
  `service_manager_stopTimeout` and the pod's `terminationGracePeriodSeconds` together.
  (#2011)

- **A mining candidate keeps its coinbase, and a solution without one is mined with it.**
  Affects miners and pools that use `getminingcandidate` and `submitminingsolution`. As in
  bitcoin-sv, block assembly now builds a candidate's coinbase once, keeps it on the job,
  and returns it in the new `coinbase_tx` field of the `MiningCandidate` message. A
  `submitminingsolution` call with no `coinbase` is mined with that coinbase. Before, block
  assembly rebuilt one with a fresh random extranonce, so its merkle root was never the one
  the miner hashed and every such solution failed the proof-of-work check, deleted the job
  and reset block assembly. A solution's `version` now goes into the header only, no longer
  into the coinbase as well. `getminingcandidate` with `coinbase=true` returns the kept
  coinbase: a version 1 transaction paying the configured miner wallet addresses by P2PKH,
  where it used to return a separately built version 2 transaction with one pay-to-pubkey
  output that block assembly never saw. Miners that submit their own coinbase are not
  affected, and neither is a node without `miner_wallet_private_keys`: it still serves
  candidates, with no coinbase on the job, and refuses only a coinbase-less solution and
  `coinbase=true`, as it did before. A solution that breaks a consensus rule no longer removes the job, so other
  miners on the same candidate keep its coinbase and can still submit; the job goes when its
  TTL lapses or a block is mined. A solution whose hash misses the target is rejected without
  resetting block assembly, which it used to do. If `miner_wallet_private_keys` is set but
  cannot build a coinbase, block assembly now logs a warning with the cause, at most once a
  minute. Upgrade block assembly no later than the RPC service: an RPC service on this
  build answers `getminingcandidate` with `coinbase=true` with an error while it is talking
  to an older block assembly, which sends no coinbase. (#2027)

- **A policy-mode script error is classified by a consensus re-run.** When policy
  validation fails on `SCRIPTNUM_OVERFLOW`, `OP_COUNT`, `SCRIPT_SIZE`, `PUBKEY_COUNT`,
  `SCRIPTNUM_MINENCODE` or `STACK_SIZE`, the validator runs the scripts again under
  consensus limits. The tx is `TX_POLICY` if that run passes and `TX_INVALID` if it
  fails. A script number over `maxscriptnumlengthpolicy` but within int64 is `TX_POLICY`
  again, and is published to `KAFKA_TX_POLICY_REJECTED`. A pre-Genesis UTXO over a
  consensus limit is `TX_INVALID` instead of `TX_POLICY`. `maxnonstdtxvalidationduration`
  is now read, with a default of 1000 ms, and bounds the re-run only. A re-run that does
  not finish in time is reported as `TX_POLICY`, as sv-node does. Set it to 0 to remove
  the bound. The timeout is checked only between opcodes, so a new setting,
  `validator_consensusRerunSlots` (default 1), caps how many re-runs run at once. When
  no slot is free the tx is reported as `TX_POLICY` without a re-run. Set it to 0 to
  turn re-runs off. While re-runs are unavailable, through a full slot or a timeout,
  every policy script failure is `TX_POLICY`, including an int64 overflow. The re-run's
  stack memory is capped at `maxstackmemoryusagepolicy` rather than the unlimited
  consensus default, and reaching the cap is `TX_POLICY`. The exception, with or
  without a re-run, is `STACK_SIZE` on a tx with a pre-Genesis input: it can be the
  1000-element consensus limit, so it is `TX_INVALID`. RPC and propagation clients that act on the error code see the new codes (#2068).

- **A block's tx is created as conflicting only when it is a double spend and nothing
  else.** Block validation used to create a tx as conflicting as soon as one of its inputs
  was already spent by another tx, dropping every other failed input. A block holding a
  tx that double-spent a mempool tx and also spent an immature coinbase, or a frozen or
  mismatched output, was accepted where SV nodes reject it. Such a block now fails
  validation instead. Each input is classified by its own error, whatever its position in
  the tx.

  A block whose tx spends an immature coinbase, with or without a double spend, is now
  marked invalid: the validator returns `TX_INVALID` for it on the block path, at the
  block's own height. It used to fail as a processing error, so the block was retried and
  never marked invalid, and the peer that sent it was not charged. This includes an
  immature output another tx already spent: the SQL store reports that spend only as
  spent, so the validator reads the parent of each double-spent output on the block path
  and checks its maturity itself, a read per failed spend. Outside a block the
  same spend is still a plain rejection, and below a checkpoint a failure stays local. A
  frozen or mismatched output still fails without a verdict: freezes and reassignment
  holds are this node's state, set at the height it applied the alert.

  A block no longer takes a tx's stored conflicting record as its own verdict. Another
  candidate block, at another height, may have created the record, and a tx valid there
  can be invalid at this block's height. On every block path such a tx is now checked
  again at the block's height for coinbase maturity and finality, from the store, without
  writing anything. Either failure is `TX_INVALID`, as for a tx the block validates
  (#2022); a peer subtree still takes the record as before. Each conflicting tx in a
  block costs a read of its body and its parents, including in a
  subtree this node validated before the block arrived. Relative lock times (BIP68,
  before Genesis), the script rules of the Genesis boundary and frozen outputs are not
  checked again, and a stored record that is not conflicting is still taken as it was.
  (#2038)

- **Peer subtrees holding conflicting txs are stored instead of rejected.** A subtree
  announced by a peer used to fail validation when it held a tx conflicting with one
  this node accepted, and the block referencing it then fetched and validated the
  whole subtree again. Those txs, and txs spending them, are now kept aside, not
  written to the UTXO store, in a new `subtreeConflicting` file next to the
  subtree, which is stored as `subtreeToCheck` with the same retention. The
  block that references the subtree creates them as conflicting. Nothing conflicting
  reaches the UTXO store before a block with proof of work references it.

  The validator now also publishes every tx it rejects as conflicting, submitted ones
  included, raw on the policy-rejected topic, flagged by the new `conflicting` field
  of `KafkaTxPolicyRejectedTopicMessage`. Subtree validation publishes the txs it keeps
  aside that are not yet known as conflicting there too, flagged `keptAside`, after
  checks that need no parent (structure, money range, policy size). Every subtree
  validator caches them, kept-aside entries in a separate cache with their own budget
  (`subtreevalidation_txPolicyRejectedCacheMaxMB` each), so a descendant in a later
  subtree is kept aside too, once its parent is known: on the pod that kept the parent
  aside as soon as it did, on the others once the publish arrives. A descendant whose
  subtree reaches it before that still fails the subtree, as before. The rejected txs no
  longer count towards the fail-fast miss thresholds. Without `kafka_txPolicyRejectedConfig`, each subtree
  validator still learns the descendants it keeps aside itself, but not those another
  one kept aside, nor the validators' rejections. With
  `subtreevalidation_txPolicyRejectedCacheEnabled` off (it is on by default), conflicting
  txs are kept aside within a subtree only. Descendants in the same subtree as their
  conflicting parent still count towards the fail-fast thresholds. Kept-aside txs are
  counted in `teranode_subtreevalidation_conflicting_kept_aside`. (#2038)

  Only the kept-aside txs the peer sent are written next to the subtree, without the
  previous-output data of extended format; those this node already held in its caches are
  resolved again when the block needs them; if they have been evicted by then and the peer
  cannot serve them, the block's subtree fails and is retried. The new
  `subtreevalidation_keptAsideMaxBytesPerSubtree` (16 MB by default) caps what one subtree
  keeps aside: over it, the subtree fails as before, though the txs it kept aside stay in
  the kept-aside cache, within that cache's budget. There is no per-peer cap: a peer can
  still announce many subtrees, each under the cap, of txs it sends itself.

  Subtree validation now also produces on the policy-rejected topic: with Kafka topic
  ACLs, grant it write access.

  Upgrade every subtree validator together. One without this change ignores the
  `keptAside` field and caches kept-aside txs, which a peer can fabricate, in its
  policy-rejected cache, where they evict the validators' entries until it is
  upgraded.

  A tx rejected because every input that failed to spend is already spent by another
  tx, or is an output of a conflicting tx, now also carries the new error code
  `TX_REJECTED_CONFLICTING` (43, gRPC `FailedPrecondition`) in its error chain; one with
  an input that failed for another reason, frozen for example, does not, in whatever
  order its inputs come. The code clients see is unchanged (`UTXO_SPENT`, HTTP 409): the
  new code is not a public cause. A gRPC client built before this change that receives
  the code reads it as an unknown `ERR` value. The legacy service drops an orphan
  rejected as conflicting from its orphan pool when its parent arrives; it used to
  delete the parent's key instead, so the orphan stayed until it expired and was
  validated again for each further parent that arrived.

- **Blocks whose cached subtrees hold a tx the store flags conflicting are now rejected.**
  Subtree validation used to bless a block whose subtrees it had already cached on
  that cache alone, so a tx demoted to conflicting after caching could still pass.
  It now binds the block body to its header whenever any subtree is cached, and
  reads each such tx's `Conflicting` flag from the UTXO store, in RUNNING too. The
  extra reads scale with transactions per block and are counted in
  `teranode_subtreevalidation_cached_subtree_conflict_probes_total`. (#1938)

  To keep the txmeta cache honest, block assembly and the validator now publish
  DELETEs on the txmeta topic for every tx they demote to conflicting. **When
  `kafka_txmetaConfig` is set, block assembly now connects to that topic at startup**
  (creating it if missing) and exits if the broker stays unreachable, so its pod needs
  produce access to the topic. Dropped DELETEs are counted in
  `teranode_subtreeprocessor_txmeta_delete_dropped_total`, and late writes the cache
  refused in `teranode_tx_meta_cache_tombstoned_writes_evicted_total`. The new
  `subtreevalidation_txMetaTombstoneWindow` (default `30m`) sets how long a DELETE
  keeps a tx out of the cache; it must exceed the worst txmeta consumer lag.

  Subtree validation now refuses to start when the txmeta cache cannot be built
  (including a non-positive `subtreevalidation_txMetaTombstoneWindow`). It used to
  log the failure and run with no store, then panic on first use.

- **The legacy service now bounds relayed transactions and transaction announcements,
  with two new settings.** Transactions relayed by legacy peers are validated by a fixed
  pool of workers, twice the largest of `utxostore_storeBatcherSize`,
  `utxostore_spendBatcherSize` and `validator_sendBatchSize`, and never fewer than 1024
  (4096 with the defaults in `settings.conf`). They wait in a queue four times that long,
  and the transactions queued or being validated hold at most
  `legacy_relayTxBytesBudget` serialized bytes, default 256 MiB, except that one
  transaction larger than that is still taken when nothing else is held; zero means one
  transaction at a time. The node holds about three times the budget while transactions
  are being validated, as the setting's description explains. A transaction that finds
  the queue full or the budget spent is dropped, not waited for; the node relies on
  getting it again from another peer or in the block that mines it. Drops are counted in
  the new `teranode_legacy_netsync_relayed_tx_dropped_total`. With the workers busy for
  the validation latency L, the pool validates at most workers / L transactions a second,
  so a node whose relayed-transaction validation is slow drops some of a heavy relay
  stream. Transaction announcements (`inv` items) are queued in chunks of 4096 behind
  `legacy_invQueueMaxHashes` (default 400,000 hashes, eight maximum-size announcements;
  a value below 4096 falls back to the default) and a per-peer share of 100,000 hashes,
  and sixteen workers take whatever is queued and look it up in UTXO store batch reads
  of up to 4096 hashes, checking the set of hashes already asked for before the read, so
  a transaction every peer announces is read about once instead of once per peer. Announcements over either
  limit are dropped and counted in hashes in the new
  `teranode_legacy_netsync_inv_dropped_total` (label `reason`), and the new gauge
  `teranode_legacy_netsync_inv_queued_hashes` shows the queue. The set of hashes asked
  for is capped at the same number as the queue. Outside the RUNNING state, where
  transaction announcements were already ignored without a lookup, they no longer get a
  goroutine each. Before this, each
  relayed transaction and each announced item got its own goroutine with no ceiling,
  and on 2026-10-19 that reached 223,118 goroutines and got the node killed for
  exceeding its memory limit. (#2045)

- **A failed revert of an invalid block's mined tags now holds block assembly until it
  succeeds.** When block validation reverts the mined tags of an invalid block and
  `SetMinedMulti` fails, or a subtree read fails with anything other than not-found, the
  revert now returns an error instead of logging a warning and marking the block done.
  The block stays at `mined_set=false`, so block assembly waits for it before it starts,
  resets or reorgs, exactly as it already does for a valid block whose mined update
  fails. Reverting the tag is what marks a transaction unmined again, so carrying on
  would rebuild block assembly without those transactions. The wait lasts as long as the
  store fault: the setTxMined retry loop increments
  `teranode_blockvalidation_setmined_retry_total`, a block that still fails after 10
  attempts logs `manual_intervention_required` and increments
  `teranode_blockvalidation_setmined_drops_total`, and the mined-not-set sweep then picks
  it up again. Those alerts can now fire for invalid blocks. Operators who see them should
  fix the UTXO or subtree store fault; block assembly resumes on the next successful
  attempt. A subtree that is absent from the store is still skipped. (#2016)

- **`utxostore_updateTxMinedStatus` is removed, and it was live.** Affects only nodes that
  set it to `false`. Mined-tagging, the step after `AddBlock` that writes a block's ID onto
  each of its transactions, now always runs. So does the check it carries, which rejects a
  block that re-includes a transaction an ancestor block already mined. With the key at
  `false`, neither ran, yet the block was still flagged as mined. The loader does not report
  unknown keys, so a leftover `false` is ignored without an error and those nodes take on
  the mined-tagging write load again. Delete the key from your config. (#2019)
- **`ProcessBlock` refuses a legacy block whose parent is not stored.** Block validation's
  `ProcessBlock` gRPC call used to answer success for a block with baseURL `legacy` and no stored
  parent: it handed the block to catch-up and stored nothing. It now returns `ERR_SERVICE_ERROR`.
  An empty `BaseUrl` defaults to `legacy`, so any external caller that omits it gets the same
  refusal and must submit the parent first. Legacy sync keeps the block and retries it once the
  parent commits. Blocks announced by p2p peers still go to catch-up. (#2021)
- **A panic on the main block validation goroutines no longer stops the node.** The
  priority-queue block workers and the catchup consumer now recover a panic, as the
  `blockFoundCh` workers and the Kafka block message handler (#2010) already did. The
  `errgroup` goroutines that block validation starts recover their own panics too: the
  catchup fetch and validate pipeline, the parallel stages of quick validation, and the
  service start and startup sweeps (issue bitcoin-sv/teranode#4993). A panic in a plain
  `go` statement in block validation still stops the process. Before, the process stopped
  and restarted, so an alert on a crash loop found it. That alert will not fire now for the
  covered paths. Monitor the new counter `teranode_blockvalidation_recovered_panics_total`
  (label `path`: `worker`, `catchup`, `kafka`, `block_found`, or for a goroutine those
  start, `catchup_fetch`, `block_validation` or `service`) and the `PANIC` log line, which
  has the stack and, on most paths, the block hash. Five more changes come with it:
    - A Kafka block message that panics is committed and retried in memory, like any
      other local fault, up to 8 times with backoff. The announcing peer is not blamed.
      Each attempt that panics counts on `path="kafka"`.
    - A catchup that panics counts as a failed attempt with the error type `local_panic`. It
      is not charged to the peer. So is a catchup whose validation waited on a validation of
      the same block that panicked on another goroutine, also when an alternative peer served
      that catchup. In that case the node tries no further alternative peer in the cycle. It
      counts toward
      `blockvalidation_catchup_max_attempts_per_block` unless it validated at least one block
      first. In that condition it resets the count, as each other failed catchup that made
      progress does.
    - After `blockvalidation_max_corrupt_attempts_per_block` (default 3) worker or
      `blockFoundCh` panics for one block from one peer, the node stops that peer's
      announcements and deliveries of the block before it fetches the block, for
      `blockvalidation_corrupt_attempt_cooldown` (default 10 minutes). The panic counts for
      the peer that served the attempt that panicked. On the `blockFoundCh` path that is the
      announcing peer, or the peer that served the block after the announcer answered 429.
      A capped peer is not fetched from as that 429 fallback either. A block this node has
      already stored is still answered without error. On the `blockFoundCh` path the cap
      applies only to blocks announced over Kafka. The `BlockFound` gRPC client sends no peer
      ID, and a block announced without one is never capped.
    - The error that a `WaitToComplete` caller gets after a `blockFoundCh` worker panic no
      longer has the panic value in its text. The panic value is in the `PANIC` log line.
    - A panic in a goroutine that a validation started is scored like the panics above: a
      catchup ends as `local_panic` and no peer is charged. In catchup quick validation
      it ends that catchup attempt, rather than falling back to normal validation of the
      block. On the legacy route it counts
      toward the per-peer panic cap above. Legacy sync gets `ERR_SERVICE_ERROR`
      for it, and also for a delivery that the cap then stops, so the parked block is kept
      and retried rather than rejected. A panic in the
      Kafka consumer or gRPC server goroutine of the service start stops the service with an
      error rather than the process with a crash. (#2012, #2035)

- **Legacy peers that send BIP152 messages are no longer disconnected.** go-wire
  is now v1.4.0, which decodes `cmpctblock`, `getblocktxn` and `blocktxn`. Until
  now a legacy peer that sent one got a `malformed` reject and was disconnected.
  Now the message is logged at debug level and ignored, and the peer stays
  connected. Teranode never sends `sendcmpct`, so a well-behaved peer should not
  send these. The bump also bounds how much the wire decoder allocates up front
  for a declared element count. (#1996)

- **A local processing error is no longer charged to a peer.** During catchup and
  announced-block fetches, a `ProcessingError` with no peer or consensus code in its
  chain no longer records a reputation failure or a malicious report, and no longer
  signals a sync-peer rotation. Catchup treats it like other local faults: it clears the
  block's markers and cached alternative announcers, does not try alternative peers, and
  counts the attempt toward `blockvalidation_catchup_max_attempts_per_block`. Before, local faults such as a failed conflict-resolution
  read lowered the serving peer's reputation and could drop it as a sync source. The
  catchup error type for these is now `local_processing_fault`. Faults that a peer causes
  are now coded `NETWORK_INVALID_RESPONSE` or `NETWORK_ERROR`, and they are still charged:
  bytes that do not match the requested hash, headers with no common ancestor, a
  checkpoint mismatch, and a failed fetch. The catchup dashboard shows these as
  `network_error`. A checkpoint mismatch was shown as `checkpoint_verification_failed`
  before. Because these codes count as network errors, more failures now take the
  existing network-error retry paths. On the announced-block path, a block served for the
  wrong hash, or a block whose subtrees the peer served with bad bytes, now tries the
  alternative sources, and the block worker re-queues it. A block whose transaction
  spends an immature coinbase or is non-final is now rejected as an invalid
  transaction, not as a processing error. Catchup reports the serving peer malicious
  for it, the direct block path strikes the peer for a corrupt body, and the legacy
  route rejects the block as invalid. A block that spends a UTXO this node has frozen,
  or that hits a UTXO hash mismatch in this node's store, stays a local fault.
  (#2022)

- **Catchup rejects headers that do not form one chain on the common ancestor.**
  Before, a peer could offer headers whose first entry built on some other stored
  block, and catchup carried on under the wrong heights with only a warning. Now
  catchup stops before it touches local state, and the error is coded
  `NETWORK_INVALID_RESPONSE`, so it is charged to the peer. A header batch whose
  headers do not link to each other, or that does not build on the last header of
  the batch before it, is rejected the same way as soon as it arrives, before the
  fetch is counted as a success for the peer. Catchup also now clears
  `mined_set` on the blocks a fork replaces only after every header check has
  passed, just before it fetches blocks, so a fork rejected by a later check, or a
  peer with nothing past the ancestor, no longer un-stamps the node's own blocks.
  No action needed. (#2034)

- **This release removes four Aerospike pruner metrics.** External monitoring systems must
  remove queries for `utxo_pruner_parents_skipped_pruned_total`,
  `utxo_pruner_pruned_set_size`, `utxo_pruner_pruned_set_saturated`, and
  `utxo_pruner_pruned_set_rotations`. These metrics reported zero because the pruner
  no longer skipped parents. After this upgrade, their queries give no data. (#1969)

- **Block validation keeps a worker slot free for the tip, and holds a block behind a
  parent that is still validating.** Blocks that do not extend the current tip may now
  hold at most `blockvalidation_max_parallel_forks - 1` worker slots. With
  `blockvalidation_max_parallel_forks = 1` nothing changes. A block whose parent is still
  being fetched, queued or validated (by a worker, catchup, gRPC or legacy sync) now waits
  in the queue for it instead of starting catchup, so the node no longer moves to
  CATCHINGBLOCKS and stops mining while a slow parent validates. `blockValidationMaxRetries` (default 3), until now unread, caps how often a
  block is requeued after a failed fetch (retried after 5 s) or after validation hits a
  deadline such as `subtreevalidation_check_block_subtrees_timeout` (retried after
  1 min). A block that timed out used to be dropped until a peer announced it again;
  failed fetches were never actually retried. `0` turns these retries off.
  `teranode_blockvalidation_priority_queue_processed_total` now labels `priority` with the
  block's class (`chain_extending`, `near_fork`, `deep_fork`) instead of always `unknown`;
  update any dashboard query that filters on `unknown`. (#1997)

- **Peer-announced subtrees are validated at most 32 at a time.** The new
  `subtreevalidation_peerSubtreeConcurrency` (default 32) bounds how many peer
  subtrees a subtree validator works on at once; until now the bound was NumCPU. After a
  block, a backlog of peer subtrees used to be admitted all at once, each allocating
  about 1 GiB for 1M transactions. The rest now wait, holding no lock, and
  `teranode_subtreevalidation_peer_subtree_admission_wait` / `_waiting` show the queue.
  Raise the setting if subtrees queue while CPU is idle; `0` restores the old
  behaviour. On the `lockfree` txmeta cache, `teranode_tx_meta_cache_evictions` now
  also counts an estimate of entries lost to ring wraps, so it is no longer 0 there. (#1967)

- **P2P HTTP endpoints are now rate limited.** (shipped in v0.16.0) `/health` and the websocket upgrade
  path are limited per source address, defaulting to 100 requests per second
  (`p2p_httpRateLimit`); requests over the limit receive HTTP 429. These endpoints
  were previously unlimited. Health checkers or probes polling faster than this will
  start being rejected. Set `p2p_httpRateLimit = 0` to disable. (#1566)

- **Settings that previously did nothing now take effect.** (shipped in v0.16.0) Each of the following
  carried a `key:` struct tag and appeared in the reference documentation, but
  `NewSettings()` never populated it, so the field stayed at its Go zero value no
  matter what was in `settings.conf`. They are now wired.

  **This affects every deployment, not only those that set a value.** The loader
  supplies non-zero code defaults, and the three `p2p_peer_registry_*` keys appear
  nowhere in `settings.conf`, so a node that has never mentioned them now runs the cleanup
  loop hourly, evicting peers on a 24-hour TTL under a 10,000-entry LRU bound.
  Review these before rolling out.

  | Setting | Previously | Now |
  | --- | --- | --- |
  | `p2p_peer_registry_max_size` | `0` — LRU bound disabled | configured value enforced (default 10,000) |
  | `p2p_peer_registry_ttl` | `0` in settings, coerced to 24h inside Blockchain | configured value applies (default 24h); a positive value below 5m is raised to 5m with a warning |
  | `p2p_peer_registry_cleanup_interval` | `0` — the cleanup loop never ran | cleanup runs at the configured interval (default 1h); peers are evicted |
  | `pruner_skipDuringCatchup` | `false` — flag unreachable | configurable |
  | `pruner_skipProcessExpiredPreservations` | `false` — flag unreachable | configurable |
  | `aerospike_enable_preserve_filter_expressions` | permanently `false` | configurable |
  | `blockchain_raw_miner_tag` | `false` — raw miner tags unreachable | configurable; `true` returns the coinbase text unsanitised |
  | `legacy_upnp` | `false` — the UPnP path had never executed | configurable; `true` runs it |
  | `blockchain_peerRegistryStore` | unset — the peer registry was never persisted | configurable; a blob URL makes Blockchain load the registry at startup and persist it |
  | `blockchain_peerRegistrySaveInterval` | `0` | applies (default 60s) once a registry store is configured |

  The peer-registry entries are the ones most likely to be noticed: with the cleanup
  interval at zero the eviction loop never started, so registries grew without bound.
  After upgrading, peers are evicted on the configured TTL and LRU bound.

  The remaining rows are default-off or unset and appear nowhere in `settings.conf`, so
  they change nothing unless you set them — but a node that did set one gets it honoured
  for the first time. `legacy_upnp = true` is the sharpest of those: it enables a UPnP
  port-mapping path that has never run in this codebase.
  (#1645, #1643, #1641, #1644)

- **`txMetaCacheTrimRatio` is renamed `subtreevalidation_txMetaCacheTrimRatio`.** (shipped in v0.16.0) The
  shipped value carries over and a default node is unaffected: `settings.conf` set the
  old key to `5` and the old code read exactly that key, so the effective ratio was
  already 5 and still is. What breaks is a local override — a value set under the old
  name in `settings_local.conf` is now ignored, silently, because the loader does not
  report unknown keys. Rename it. Only affects nodes running
  `subtreevalidation_txMetaCacheBucketType = preallocated`; the code default is `2`.
  (#1746)

- **The Aerospike store batcher now reads `utxostore_storeBatcherDurationMillis`.**
  Same class of surprise as the table above, by a different route: the key was always
  populated, but the Aerospike store's Create batcher read `aerospike_storeBatcherDuration`
  and ignored it. A node on the shipped `settings.conf` sees no change — that file sets
  the key to `10`, and the old Aerospike default was 10ms. A node running Aerospike that
  set `utxostore_storeBatcherDurationMillis` to anything else was being ignored and is
  now honoured; a node that set it to 200ms for catch-up moves from 10ms to 200ms.
  `aerospike_storeBatcherDuration` is deprecated: above 0 it still overrides the new key,
  and logs a warning at startup. Remove it and set the UTXO-store key instead. (#1903)

- **`aerospike_enable_preserve_filter_expressions` is mutually exclusive with
  `aerospike_use_native_teranode_ops` for preserve operations.** (shipped in v0.16.0) `PreserveTransactions`
  is native-op-covered (`subOpPreserveUntil`). With native ops active and this flag
  false, the preserve write and its prune-eligibility filter go through the native
  operate path. Setting this flag true short-circuits that dispatch and uses a
  client-side BatchWrite instead, so preserve silently stops using the native path
  while spend and setMined continue to use it. Do not enable both unless that trade
  is intended. (#1644)

- **JSON-RPC error codes and messages change.** Affects every node. `getblock`,
  `getblockheader`, `invalidateblock` and `reconsiderblock` answer `-5 "Block not found"`
  for a block this node does not have, and `getblockbyheight` answers
  `-8 "Block height out of range"`; all five previously returned `-32603` carrying the
  internal error chain. `sendrawtransaction` answers `-25 "TX rejected: Missing inputs"`
  when a parent is unknown. A new `-28 "Still loading, try again shortly"` comes back from
  `submitminingsolution`, `generate` and `generatetoaddress` while the node is not ready —
  it is transient, so retry rather than fail. Malformed arguments to `getrawtransaction`,
  `submitminingsolution`, `freeze`, `unfreeze` and `reassign` return `-8`/`-5` instead of
  `-32603`.

  Error *text* is trimmed everywhere: a fault with nothing safe to show returns the literal
  `internal error` and the detail goes to the log only, so anything that pattern-matches on
  RPC error strings needs re-checking. `reconsiderblock` on a block that is not marked
  invalid now succeeds, clearing invalid descendants, instead of returning `-25`.
  `getblockhash` is a known exception and still answers `-5` for an out-of-range height.
  (#1637, #1953)

- **Asset serves only validated subtrees.** Affects every node, as a server. The subtree
  routes (`GET /subtree/:hash` and its `/hex`, `/json` variants, `GET /subtree/:hash/txs/json`,
  `GET /block/:hash/subtrees/json`) no longer fall back to the pre-validation
  `subtreeToCheck` copy, so between fetching a block's subtrees and finishing their
  validation this node returns 404 for them rather than publishing bytes it has not checked.
  The window is not confined to catch-up: a large tip block opens it for seconds to minutes
  on a fully synced node. Peers catching up from this node must retry or try another peer;
  Teranode's own catchup client already does. `GET /block_legacy` is exempt. Asset's
  `GET /api/v1/tx/:hash` also gains an `X-Teranode-Not-Found: tx` header on a genuine miss,
  and a UTXO-store fault where the tx store has no copy returns 500 where it used to return
  404. (#1745, #1637)

- **A transaction whose two-phase-commit unlock fails is accepted, not rejected.** Affects
  every node. The validator returns a new `ERR_TX_UNLOCK_FAILED` (code 42, gRPC `Aborted`)
  for a transaction that was spent, created and delivered but whose unlock exceeded
  `validator_twoPhaseCommitTimeout`; propagation, `sendrawtransaction`, the Kafka intake,
  legacy sync and subtree validation all treat it as acceptance. Clients that classify any
  validator error as a rejection will now count accepted transactions as failures, and a
  service receiving code 42 over gRPC from a newer peer sees an unknown enum value. Watch
  `teranode_validator_unlock_failed` and `teranode_subtreevalidation_bless_unlock_failed`:
  a non-zero rate means those records stay locked until mined, and their children get
  `TX_LOCKED`. (#1925)

- **Block assembly resets on a reorg that reaches UTXO retention.** Affects every node. A
  reorg whose deeper side reaches the effective UTXO-store retention
  (`global_blockHeightRetention` + `utxostore_blockHeightRetentionAdjustment`, 288 by
  default) now triggers a full block-assembly reset at any chain height; previously the only
  reset trigger was a reorg of at least `CoinbaseMaturity` blocks above height 1000. In
  practice this changes two cases: reorgs at or past retention below height 1000, and nodes
  that lowered `global_blockHeightRetention` below coinbase maturity, which now reset much
  sooner. Expect the mining-candidate gap a reset brings. The log line names which rule
  fired. (#1937)

- **A failed batched spend now rolls back its partial spends, synchronously.** Affects every
  node. When a batched `Spend` fails or is cancelled, the spends that already landed are
  reverted so a parent never names a spender whose record was never created. The rollback
  runs on the caller's goroutine, bounded by the new `utxostore_spendRollbackTimeout`
  (default 120s), so a failing spend can hold its caller far longer than it used to. Lower
  it if caller latency matters more than rollback completeness, and watch the new
  `teranode_{aerospike,sql}_utxo_partial_spend_rollbacks{outcome}` and
  `teranode_{aerospike,sql}_utxo_spend_rollback_failed` counters. Transient store errors now
  trigger the rollback too (previously only spent, conflicting, frozen and hash-mismatch
  errors did), so a retry after one can find its inputs taken by another transaction and be
  answered as a double spend. (#1376)

- **Spending an output index the parent does not have is invalid, not a processing error.**
  Affects every node. Propagation answers HTTP 400 (`TX_INVALID`) for such a transaction
  where it answered 500, and a block containing one is rejected as invalid instead of being
  retried indefinitely. (#1875)

- **Script-number overflow is a consensus failure, not a policy one.** Affects every node;
  only consumers of the policy-rejection topic need to act. The script engine is now built
  against bitcoin-sv 1.2.3 (GoBDK v1.3.0), where a script number overflowing int64 is a
  consensus error. Transactions rejected for it are reported as invalid rather than
  policy-rejected, and stop appearing on `KAFKA_TX_POLICY_REJECTED`. `OP_SPLIT` also
  rejects positions above INT32_MAX. A node on an earlier build accepts transactions and
  blocks that this one rejects, so upgrade every node together. (#1911)

- **The shipped Docker compose healthchecks actually run.** Affects operators of
  `deploy/docker/base/docker-teranode.yml`. Every `test:` line split its pipeline across
  array elements under `CMD-SHELL`, so only a bare `netstat` ran and every container
  reported healthy on the first probe — which made every
  `depends_on: condition: service_healthy` gate inert. They are real commands now, and the
  Asset check was pointed at the port it actually binds (8090, not 8091). Startup ordering
  is therefore enforced for the first time: a service that cannot bind its port leaves its
  dependents unstarted instead of letting them come up and fail downstream, so a
  misconfiguration surfaces as a stuck `docker compose up` rather than a crash loop further
  along. Teranode service containers also run with `init: true`. (#869)

- **`blockassembly_livenessStallTimeout` measures something different.** Affects only nodes
  that set it above 0; the default is still `0s` and those nodes see nothing. Block
  movement, reorgs, resets and the unmined reload now beat the heartbeat as they progress,
  so a long but progressing catch-up or reorg no longer ages it the way it did and a value
  that was tripping spuriously may now be safe. In the other direction, a wait on block
  validation beats only on an answered poll, so a blockchain-service outage spanning such a
  wait trips liveness deliberately. Re-measure
  `teranode_blockassembly_liveness_heartbeat_age_seconds` across a restart, a reorg and a
  large block before keeping your current value. (#1832)

- **Legacy block download is multi-peer by default.** Affects every node that syncs over
  the legacy protocol, with no config change. `legacy_multiPeerBlockDownload` defaults to
  `true`, with `legacy_maxBlocksInTransitPerPeer` 16 (was effectively 2),
  `legacy_blockDownloadWindow` 1024, `legacy_parkStoreTimeout` 10s and the three
  `legacy_blockDownloadTimeout*Percent` keys (100 / 600 / 50). None appear in
  `settings.conf`, so these are code defaults every deployment picks up. Set
  `legacy_multiPeerBlockDownload = false` for the old single-peer behaviour.

  Peer handling changes with it: a peer streaming a needed block below 100 KB/s is
  disconnected as a staller, `getheaders` goes only to outbound or whitelisted peers, and a
  peer that sends an unrequested or oversized block is no longer disconnected (the body is
  read and discarded, and an oversized hash is not requested again for ten minutes). Above
  the last checkpoint, mainnet and testnet download no blocks until the header chain has
  SV Node's minimum chain work. (#1699)

- **Six legacy batcher settings are removed, and they were live.** Affects only nodes that
  set them, but those nodes lose the tuning silently — the loader does not report unknown
  keys. Removed: `legacy_storeBatcherSize`, `legacy_storeBatcherConcurrency`,
  `legacy_spendBatcherSize`, `legacy_spendBatcherConcurrency`, `legacy_outpointBatcherSize`
  and `legacy_outpointBatcherConcurrency` (that last one had no reader even before). Legacy
  netsync no longer creates or spends UTXOs at all; block validation does, so the
  equivalent levers are `utxostore_storeBatcherSize`, `utxostore_batcherMaxConcurrent` and
  the new `blockvalidation_quick_validate_create_concurrency`. A deployment that carried
  `legacy_storeBatcherConcurrency.docker.m = 4` to keep a single-node Aerospike from being
  force-fed now relies on `utxostore_batcherMaxConcurrent` instead. Also removed, also with
  readers: `legacy_blockFailureBackoffBase`, `legacy_blockFailureBackoffMaxDuration`,
  `legacy_blockPrefetchBufferBytes` and `blockvalidation_legacy_below_checkpoint_fail_closed`.
  Delete all of these from your config. (#1699)

- **Legacy netsync metrics are removed and renamed.** Affects every node with legacy
  dashboards or alerts. `teranode_legacy_peer_server_OnBlock` becomes
  `..._OnBlockOnDisk`; the netsync per-phase and per-transaction histograms
  (`prepare_subtrees`, `pre_validate_transactions`, `validate_transactions`,
  `validate_transactions_legacy_mode`, `extend_transactions`, `create_utxos`,
  `block_tx_*`) are gone with the code that emitted them; and
  `teranode_legacy_netsync_handle_block_direct` keeps its name while timing something else
  (the converted-block commit). New `teranode_legacy_netsync_download_*`, `park_*`,
  `blocks_owed` and `bytes_ahead` series replace them. Rebuild those panels before
  upgrading or they go blank. (#1699)

- **`subtreevalidation_processTxMetaUsingCache_Concurrency` now bounds the whole service.**
  Affects every node, with no config change. The key keeps its name and its default of 32,
  but it was a per-call errgroup limit and is now two server-wide worker pools, one for
  peer-announced subtrees and one for block validation. At the same value, total probe
  concurrency under a burst of subtrees drops sharply — which is the point, since the
  unbounded version starved Kafka txmeta ingestion — but it is a throughput change nobody
  configured. Raise it if probe throughput matters more to you than ingestion latency.
  `subtreevalidation_processTxMetaUsingCache_BatchSize` is removed (it was live); lookups
  are split by cache-bucket range now. (#1957)

- **Kafka consumers on `replay=0` topics start at the latest offset.** Affects a node the
  first time a consumer group is created — a fresh deployment, a renamed group, or one
  whose offsets expired — on `invalid-blocks`, `invalid-subtrees` and `rejected-tx`. Those
  consumers now start at the end of the topic; franz-go's default was the start, so such a
  group previously replayed everything still retained. Groups with committed offsets are
  unaffected. Shutdown also waits up to 10 seconds per consumer to drain in-flight records.
  (#1952)

- **The block persister rebuilds half-persisted blocks and backs off on failure.** Affects
  every node running the block persister with `blockpersister_processUTXOFiles = true` (the
  default). "Already persisted" now means the block file *and* both utxo files exist, and
  the utxo files are written before the block file so the block file is the last marker. A
  block left with a `.block` file but no `.utxo-additions`/`.utxo-deletions` — what a crash
  under an earlier version left behind, since those wrote the block file first — is deleted
  and rebuilt on the next pass, so expect re-done work on first run. A block that fails to
  persist is now retried once per `blockpersister_persistSleep` (default 10s) instead of in
  a tight loop: recovery from a transient store failure is slower, but it no longer spins.
  The sleep is interruptible, so shutdown no longer waits it out. (#1949, #1915)

- **Allowlisted signed peers get 413 for oversized bodies.** With a non-empty
  `asset_peerAuthAllowlist`, a signed request body larger than
  `max(1 MiB, 32 x subtreevalidation_missingTransactionsBatchSize + 4096)` (this node's value) is
  rejected with 413. Keep allowlisted peers' `subtreevalidation_missingTransactionsBatchSize` at or
  below this node's, or their catchup fails against it. Nodes with an empty allowlist (the default)
  never read the body and are unaffected. (#1788)

- **`teranode_asset_http_peer_auth_result_total` labels change meaning.** The allowlist check now
  runs first, so on a node with an empty allowlist every well-formed signed request counts as
  `not_allowlisted`; `replay`, `bad_digest`, `unknown_key` and `ok` are no longer emitted there, and
  `bad_sig` means only malformed headers. A new value, `body_too_large`, is added. Alerts on
  `result="replay"`, `"bad_digest"` or `"bad_sig"` go quiet on default nodes. (#1788)

- **Six more Asset routes are rate limited as heavy endpoints.** `GET /subtree/:hash/txs/json`,
  `/blockgraphdata/:period`, `/utxos/:hash/json` and the three `/merkle_proof/:hash` variants now
  share the per-IP heavy bucket (`asset_httpHeavyRateLimit`, default 10 req/s) instead of only the
  global limit (1024 req/s). SPV and merkle-proof clients, explorers, or anything behind one IP
  making more than 10 heavy requests a second will get 429. With `asset_peerAuthAllowlist` empty
  (the default) miners and peers are in the same per-IP bucket. Raise `asset_httpHeavyRateLimit`,
  or allowlist trusted peers. (#1789)

- **Cross-origin requests no longer get credentials by default.** With `asset_corsAllowOrigins`
  empty (the default) Asset still reflects any origin, but without
  `Access-Control-Allow-Credentials`; only exact-match listed origins get credentials. Credentialed
  cross-origin browser callers break, including the Vite dev dashboard on `:5173`. List them, e.g.
  `asset_corsAllowOrigins = http://localhost:5173`. The production dashboard is same-origin and
  unaffected. CORS is not CSRF protection. (#1793)

- **Setting `asset_trustedProxyCIDRs` stops trusting private and link-local proxies implicitly.**
  Only loopback and the listed CIDRs are trusted. If your ingress or load balancer sits on an
  unlisted RFC1918 address, every client resolves to the proxy's IP and shares one rate-limit and
  ban-list key. Add internal proxy ranges to the list explicitly. Nothing changes while the setting
  is empty. (#1793)

- **Global rate-limit 429s leave the access log and request metrics.** The access log now runs
  after the global limiter, so those 429s appear only in
  `teranode_asset_http_rate_limited_total{scope="global"}`, not in
  `teranode_asset_http_request_duration_seconds`/`..._response_size` or the WARN access log. The
  `method` label collapses non-standard methods to `other`. (#1793)

- **Catchup-route 429s move to `scope="heavy_catchup"`.** `GET /subtree/:hash`,
  `/subtree_data/:hash`, `/blocks/:hash`, `/block/:hash` and `POST /subtree/:hash/txs` now have their
  own heavy limiter and metric label; queries on `teranode_asset_http_rate_limited_total{scope="heavy"}`
  stop counting them. Use `scope=~"heavy|heavy_catchup"`. (#1801)

- **HTTP 429 has its own error code, `ERR_SERVICE_RATE_LIMITED` (52).** It was the generic
  `ERR_SERVICE_ERROR` (59). The new code is retryable and is not a transient local error. Code
  matching `ErrServiceError` on a 429, or log alerts on `SERVICE_ERROR (59)`, will miss it; an older
  service receiving it over gRPC sees an unknown enum value. Upgrade services together. (#1802)

- **A peer's 429 on an announced block is free only 3 times per 10 minutes.** When a peer announces
  a block and its `/block/:hash` answers 429, that 429 is not charged to its reputation up to 3 times
  per peer in a fixed 10-minute window; past that it is charged like any other failed fetch. Within
  the allowance, the block is fetched instead from up to 2 other peers whose advertised tip is that
  block, so those peers can see `/block/:hash` requests for blocks they did not announce to us.
  (#1942)

- **Catchup fetches retry a peer's 429.** `POST /subtree/:hash/txs` and `GET /subtree_data` retry 429
  and 503 up to 6 times with backoff (honouring `Retry-After` up to 5s) instead of reporting the
  peer at once, so a serving peer can see up to 6 requests per batch. These requests are now signed
  when a peer key is configured, and the catchup POST is sent as `application/octet-stream`. (#1802)

- **One 10-minute deadline covers each missing-transactions batch.**
  `subtreevalidation_missingTransactionsFetchTimeout` (default 10m) bounds the POST, its retries and
  the body read, replacing the per-request `http_streaming_timeout` on this path. If you raised
  `http_streaming_timeout` above 10m, set this too. (#1802)

- **`/lastblocks` returns at most 1000 blocks, and a negative `n` returns 400.** `n` was unbounded,
  and `n=-1` returned every block on SQLite (and a 500 on Postgres). It is now clamped to 1000, or
  lower via `asset_maxLastNBlocks`. (#1792)

- **Common-ancestor header routes reject more than 256 locator hashes with 400.** Applies to
  `/headers_to_common_ancestor/:hash` and `/headers_from_common_ancestor/:hash` (all formats). The
  catchup client sends about 32, so only non-standard clients are affected. A negative `n` on these
  routes also returns 400 now. (#1792)

- **`asset_concurrency_get_legacy_block_reader` now bounds whole streams, and defaults to 0
  (unlimited).** The permit used to be released after setup; it is now held for the whole,
  client-paced block stream. The default moved from -1 (NumCPU) to 0, so default nodes are less
  restricted than before. If you set an explicit value, it now caps concurrent `GET /block_legacy`,
  `/rest/block/:hash.bin` and SV-peer block streams, and extra requests wait 30s and then fail.
  Re-size it for whole-stream semantics, or remove it. Two optional keys arrive with it:
  `asset_legacyPeerPoolToken` (empty by default, redacted from `GET /settings`) and
  `asset_concurrency_get_legacy_block_reader_peer` (0 = unlimited), which together give
  this node's own legacy peer server a separate permit pool. Set the token to the same
  value on the legacy and Asset services to enable that; leaving it empty keeps the single
  shared pool. (#1794)

- **Batch POSTs are capped at 16384 records by default.** `asset_maxBatchRecords` defaults to
  16384. `POST /utxos` (and `/hex`, `/json`) above it returns 413. On `POST /subtree/:hash/txs` the
  cap is never below this node's `subtreevalidation_missingTransactionsBatchSize`, but a peer
  configured with a larger batch size gets 413, which is not retried and is reported against the
  serving peer. Raise `asset_maxBatchRecords` (or set 0 to disable it on a node with no untrusted
  callers), and keep peer batch sizes at or below this node's. (#1797)

- **A transaction store fault now answers 500 instead of 404.** `GET /tx/:hash` (and
  `/hex`, `/json`) decided its 404 partly on the error text: anything containing
  "not found" got one. A misconfigured Aerospike namespace reports `Namespace not
  found`, so a node that could not reach its store told every caller the transaction
  did not exist. The 404 is now decided by error class alone, and a genuine miss is
  unchanged. Expect 5xx where you previously saw 404s if a store is unhealthy —
  which is the point, but it will move alert thresholds that counted on the old
  shape. The same collapse on the HTTP blob store's own client and server is fixed
  too; no shipped configuration uses that store. (#1963)

- **A block whose coinbase is not final is rejected.** Affects every node and every miner.
  bitcoin-sv applies its `IsFinalTx` rule to the coinbase as to any other transaction, and
  Teranode now does too: a block whose coinbase has a non-final input sequence and an
  nLockTime not yet reached is invalid (`bad-txns-nonfinal`). From CSV onwards a time-based
  nLockTime is measured against the parent's median time past. `submitminingsolution`
  refuses such a coinbase instead of mining a block peers reject, and keeps the job, so a
  corrected coinbase can still be submitted against it. The time-based check needs the
  parent's median time past, which is looked up only after the solution has passed the
  proof-of-work check. A solution's time and nonce are no longer copied into the coinbase
  as its nLockTime and input sequence; they go into the header only, as in bitcoin-sv. A
  node on an earlier build accepts blocks this one rejects, so upgrade every node
  together. (#2017)

- **A block's conflicting transactions are checked in one walk, and
  `utxostore_conflictingChildrenMaxNodes` becomes a per-root budget for it.** Subtree
  validation used to check a block's conflicting transactions one at a time, re-walking
  the cones they share. A long conflicting chain cost about N²/2 store reads, so a
  1,788-transaction chain took about five hours. That is far past the 30-minute
  `subtreevalidation_check_block_subtrees_timeout`, and since each retry started again
  from zero, the node wedged. `CheckBlockSubtrees` on cached subtrees, and the store
  lookup of fresh subtrees, batched or not, now walk all of a block's conflicting transactions once
  on a shared visited set, from N roots (the transactions and their counter-spenders).
  The walk may visit the setting (default 100000) times N, which is the old aggregate
  bound of N separate walks, so any block whose every per-transaction walk fit still
  passes. Two costs change. In the worst case, the visited set can hold N times the
  setting in memory, where each walk used to peak at the setting. A single runaway cone
  now fails closed with `ERR_UTXO_WALK_LIMIT_EXCEEDED` after N times the setting in
  reads, rather than after the setting. One smaller change comes with it, on the cached
  path only. A counter-conflicting record missing from the store no longer skips that
  transaction's whole check: the record counts as not mined, and the rest is still
  checked. The fresh path is unchanged: a missing record still fails the check with a
  retryable `TX_NOT_FOUND`, as blessing a missing transaction does.

  The conflicting-walk histograms change meaning on these two paths:
  `teranode_utxo_conflicting_walk_nodes`, `teranode_utxo_conflicting_walk_depth` and
  `teranode_utxo_conflicting_walk_duration`.
    - Before: one sample per per-transaction walk, usually two per conflicting transaction
      (its counter-spender's walk and its own).
    - Now: one sample per block-level batch, covering every root. For the same block the
      sample count drops, and each sample's nodes, depth and duration rise.
    - A batch sample's nodes can legitimately exceed `utxostore_conflictingChildrenMaxNodes`,
      up to that budget times the batch's roots. An alert on walk size near the setting
      reads differently after upgrading.
    - Blessing a single missing transaction, and conflict processing, still record one
      sample per walk, so the histograms now mix both kinds.

  (bsv-blockchain/teranode#2037)

- **A proven conflict now rejects a block even when part of the conflict check
  failed.** Affects `CheckBlockSubtrees`: cached subtrees, fresh subtrees, and
  missing transactions it fetches and blesses, alike. When a block's conflicting
  transactions are checked against the candidate chain, a transaction that is
  provably invalid used to lose to any retryable failure elsewhere in the same
  check: a pruned parent record, a store fault, the
  `utxostore_conflictingChildrenMaxNodes` walk limit, a frozen output, or another
  input of the same transaction that could not be checked. On the fresh path the
  verdict was also returned as a context cancellation, so block validation retried
  it even when nothing else failed. Either way the block was retried, never
  rejected, and the announcing peer was not scored for it. Now, if any
  counter-conflicting transaction that was read is mined on the candidate chain,
  the block is rejected with `TX_INVALID` whatever else failed, and scored as any
  invalid transaction is; the retryable error is returned only when nothing is
  proven. A block that used to retry indefinitely may now be rejected on the first
  attempt. This includes a walk over the `utxostore_conflictingChildrenMaxNodes`
  budget that already reached a mined counter-conflicting transaction (a
  conflicting transaction, a counter-spender or a counter-spender's descendant;
  the conflicting transactions' own descendants are only checked for frozen
  outputs): raising the setting no longer changes that verdict. The fresh-path change also reaches `ValidateSubtreeInternal`,
  so `CheckSubtreeFromBlock` and live peer subtrees return a proven conflict as
  `TX_INVALID` too. On the live path, `subtreeMessageHandler` now logs it at error
  level ("error processing kafka message") where it logged a "Context canceled,
  skipping" warning, so operators will see new error-level log lines for such
  subtrees. Nothing to configure. (bsv-blockchain/teranode#2052)

- **Conflict-check failures in subtree validation are no longer reported as
  context cancellations.** Two follow-ups to the entry above, both in subtree
  validation's conflict check:
  - On the fresh path (`CheckBlockSubtrees` on a new subtree, `CheckSubtreeFromBlock`,
    live peer subtrees), a store fault, the `utxostore_conflictingChildrenMaxNodes`
    walk limit, a frozen output, an input out of range and the fail-fast
    missing-tx threshold are no longer wrapped as a context cancellation. Each is
    still retried and never charged to a peer. What changes is how they show up:
    on the live path `subtreeMessageHandler` logs them at error level ("error
    processing kafka message") where it logged a "Context canceled, skipping"
    warning, and a catch-up run that ends on the walk limit, a frozen output or an
    out-of-range input records `local_processing_fault` instead of
    `local_context_cancelled` as its last error type. A missing record, a
    cancelled check, and a record the store could not decode keep the old
    classification.
  - On the fresh path a proven conflict is rejected with `TX_INVALID` even when
    the check's context is cancelled while it runs, as the cached path and
    blessing a missing transaction already did. Before, it was retried.

  Nothing to configure. (bsv-blockchain/teranode#2066)

### Not breaking, but worth knowing

- **Peer subtrees announced during a catchup are validated afterwards instead of
  lost.** While catching up, the subtree validator used to drop every
  peer-announced subtree, so the next block arrived with all of them unvalidated
  and had to read their transactions from the UTXO store. Up to
  `subtreevalidation_skippedSubtreeBackfillMax` (default 4096, newest kept) of them
  are now validated through the normal peer-subtree path once the node is RUNNING
  again, using at most half of `subtreevalidation_peerSubtreeConcurrency`, so
  expect a burst of peer subtree validation right after a catchup. Subtrees
  announced while IDLE, or more than 30 minutes before their replay, are still
  dropped. New metric:
  `teranode_subtreevalidation_skipped_peer_subtrees_total{outcome}` (recorded,
  evicted, expired, requeued, dropped, replayed, replay_failed). Set the key to `0` for the previous
  behaviour. (#2055)

- **A failed `ProcessConflicting` no longer wedges block assembly, and its error is
  capped.** The old rollback cleared `Conflicting` on every transaction the cascade
  visited, including winners that were already flagged. Every retry then failed with
  `tx is not conflicting`. The operation now saves a recovery checklist before it
  changes any state. Rollback restores the recorded spend owners and flags. A retry
  of a run that already applied returns the saved result. The returned loser set
  never contains a winner. The forward rollback accepts the winners' own block as
  confirmation, and still refuses a winner that is confirmed in any other
  current-chain block. A parent locked by another holder is no longer a reason to
  fail the operation: the operation neither takes nor releases that lock. The
  `MANUAL INTERVENTION REQUIRED` error lists the first 20 rollback failures and a
  count of the rest, instead of one entry per transaction. Log searches that counted
  `rollback step 2 (re-spend` entries find no such entry now. Nodes that are already
  wedged are not repaired by this change. (#2023)

- **Mining candidates no longer build on a stale block assembly tip.** When block
  assembly is idle but its tip differs from the Blockchain service tip,
  `GetMiningCandidate` now returns an empty candidate on the Blockchain service tip.
  Before, it returned a full candidate on the old tip, so a miner produced orphan
  blocks. The new counter `teranode_blockassembly_stale_tip_candidates_total` and
  the Warn log fire only when the gap lasts more than 10 seconds at the same
  block assembly tip. A shorter gap is the normal delay before block assembly
  hears of a new block. A block assembly tip that is ahead of the Blockchain service
  tip is not counted. Alert on a sustained non-zero rate. If the Blockchain service
  tip cannot be read, the candidate is built as before. (#2023)

- **Mining candidates no longer build on the old tip during a reorg.** While block
  assembly is in the `reorging`, `reconciling` or `resetting` state, it can still
  hold the tip from before the reorg. `GetMiningCandidate` built candidates on that
  old tip, so a miner could mine on the losing side of a reorg. In these states it
  now returns an empty candidate on the Blockchain service tip, as it already did in
  `blockchainSubscription` and `movingUp`. Mining pools can see more empty candidates
  while a reorg or a block assembly reset runs, including a reset at an unchanged
  tip after a storage error. No action is required. (#1983)

- **Two Blockchain RPCs start working.** `GetLatestBlockHeaderFromBlockLocator` and
  `GetBlockHeadersFromOldest` were implemented under names that did not satisfy the
  generated server interface, so the embedded `UnimplementedBlockchainAPIServer`
  answered both and every gRPC caller got `Unimplemented`. They now dispatch, and a
  caller that treated `Unimplemented` as "this node does not support it" starts
  receiving real responses. A compile-time interface assertion now fails the build
  if a handler name drifts again. (#1848)

- **Store URL passwords no longer appear in logs or the settings dump.** Store URLs
  carry credentials in their userinfo, so a URL in a log line was a working
  credential in a log line. Every logging site that passed one is now wrapped in
  `urlutil.Redact`, across the commands (`seeder`, `seedimport`, `monitor`,
  `diagnose`, `settings`, `utxopersister`, `teranodecli`), the services (blockchain,
  legacy, p2p, propagation, pruner, subtreevalidation, asset) and the stores (blob
  file, blob http, Aerospike). `settings.ExportMetadata` redacts URL values too, so
  the settings endpoint and dump no longer return them. A new test,
  `TestNoUnredactedURLsInLogCalls`, fails the build if an unredacted URL reaches a
  log call again. Log scrapers or dashboards matching a full store URL will see a
  masked one. Rotate any credential that has already passed through a log sink you
  do not control. (#1747)

- **Dead configuration keys were removed.** These had no readers anywhere in the
  codebase, so removing them changes no behaviour. If you have them in a config
  file they were already doing nothing and will continue to be ignored — the
  config loader has no schema and does not reject or report unknown keys, which
  is precisely why these survived as long as they did. Removed:
  `securityLevelGRPC` — a casing variant that never matched the real
  `security_level_grpc` (#1674) — and `blockchain_subscription_timeout` (#1643); both
  removals shipped in v0.16.0. #1744 removed more: among them `http_sign_response` (the live key is
  `asset_sign_http_responses`, and response signing stays off), `p2p_shared_key`,
  `utxostore_outpointBatcherConcurrency` (the live key is
  `utxostore_outpointBatcherMaxConcurrent`), `utxostore_dbTimeoutMillis`, `spent_utxo_ttl`
  and `blockvalidation_subtree_fetch_timeout`.

  **`blockmaxsize` was not removed and is still live.** #1640, shipped in v0.16.0, deleted a duplicate
  `BlockSettings.MaxSize` field that carried the same tag; the key itself is read
  at `settings/settings.go` into `Policy.BlockMaxSize`, enforced in Block Assembly
  and reported over RPC as `maxminedblocksize`. Leave it set.

- **The Postgres circuit breaker is now reachable.** (shipped in v0.16.0) The breaker could not be turned
  on because its settings were never populated. Two forms exist and both are wired
  now: a global `postgres_circuitBreakerEnabled` and a per-service
  `<prefix>_postgres_circuitBreakerEnabled` that overrides it. The default remains
  off, so no deployment changes behaviour unless you opt in. (#1642)

- **Development nodes get their own RPC password.** `teranode-dev` now writes
  `rpc_user.dev.<name>` and a random `rpc_pass.dev.<name>`. A dev block written before this change
  has neither, so `teranode-dev generate`/`rpc` get 401 until you re-run `teranode-dev init`.
  New startup warnings flag plaintext admin access, cookies without `Secure`, and empty credentials. (#1790)

- **Websocket caps are available but off by default** (`asset_maxWebsocketConnections`,
  `asset_maxWebsocketConnectionsPerIP`, `asset_websocketReadLimit`). The per-IP cap separates only
  clients whose own or proxy-appended address is public. The p2p relay now forwards only its five
  relay channels (others had no subscribers), and startup warns about a plaintext p2p websocket and
  an unset read limit. (#1791)

- **Invalid `asset_corsAllowOrigins` entries stop Asset at startup** (a glob such as `*`, `null`, a
  path or userinfo). Responses gain an `X-Signature-Scope` header. (#1793)

- **The heavy-route limit is looser.** The two heavy buckets are independent per IP, so one IP can
  sustain up to 2x `asset_httpHeavyRateLimit` across all heavy routes, and the catchup-route burst
  rises from 10 to 40 at defaults (the catchup fan-out, capped at 4x the rate). If you relied on the
  old ceiling, lower `asset_httpHeavyRateLimit` or set `asset_httpHeavyRateBurst`. (#1801)

- **`asset_maxNBlocks` never goes below 100**, the catchup client's batch size; a lower value is
  raised with a startup warning. `asset_maxBlockHeaders` applies only to
  `/headers_to_common_ancestor`, never to the catchup route. (#1792)

- **Legacy block and on-demand `subtree_data` streams fail rather than serving a reconstructed
  transaction that is incomplete.** The local-service client used for SV-peer block serving no
  longer follows redirects, so point `asset_httpAddress` at its final address. (#1794)

- **A `POST /subtree/:hash/txs` body that isn't a whole number of 32-byte hashes returns 400
  instead of 500.** (#1797)

- **New switches can withhold backend detail from anonymous callers** (`asset_publicErrorDetail`,
  `asset_publicHealthDetail`, `asset_healthStrictStatus`, `asset_publicPeersDetail`,
  `asset_txMetaRawEnabled`); all default to today's output. `asset_healthStrictStatus = true` makes a
  Kubernetes readiness probe on `:8090/health` fail whenever a dependency is unhealthy (for example
  Aerospike `stop_writes`), removing Asset pods from their Service. The Asset error log line gains a
  `correlation_id=` field. (#1796)

- **`blockvalidation_quick_validate_create_concurrency` is new, default 0 (derived).** The
  create-wave limit was `utxostore_storeBatcherSize * 8`; it is now
  `max(min(storeBatcherSize * utxostore_batcherMaxConcurrent, 16384), storeBatcherSize * 8)`.
  At the shipped `utxostore_storeBatcherSize = 2048` the two are equal and nothing changes.
  A node that lowered that key gets a higher derived concurrency — at 100 the limit goes
  from 800 to 6400. Set this key explicitly to pin the old value. (#1699)

- **The below-checkpoint fast route is documented but still off.**
  `blockvalidation_outpoint_only_below_checkpoint` and
  `blockvalidation_legacy_unified_below_checkpoint` now appear in `settings.conf`, both
  `false` as before. If you turn them on, the legacy and block-validation services must
  carry the same values for both and the same UTXO store type, since each evaluates the
  route from its own config. A new `ERR.BLOCK_BODY_MISMATCH` (122) and an additive
  `header_proven` field on `ProcessBlockRequest` come with this work. With the route on,
  an older legacy service sends no proof and every below-checkpoint block takes full
  validation; keep the legacy and block validation services on the same version. (#1699)

- **A reorg no longer kills the node when the store has pruned a winning transaction.**
  Any record missing in `MarkTransactionsOnLongestChain` used to be fatal, taking the
  process down mid-reorg. Transactions being moved *onto* the longest chain that the store
  no longer holds are now counted and skipped; a transaction missing while being moved
  *off* the longest chain is still treated as corruption and is still fatal. Aerospike and
  SQL alike. (#1934)

- **Catch-up hands the UTXO store a block's transactions as one list.** During catch-up
  above the checkpoint, and on legacy sync while the node is in `CATCHINGBLOCKS`, checked
  transactions go to the store in lists rather than one at a time, and legacy sync leaves
  the per-subtree check to block validation. Outside catch-up nothing changes.
  `subtreevalidation_spendAndCreateMultiMaxTxs` (default 1048576, matching the load batch
  size) caps a list; smaller values make deep blocks slower. Four additive metrics are
  added (`teranode_subtreevalidation_batch_step`, `_batch_txs`, `_batch_parent_outputs`,
  `_batch_lists`); no existing metric or label changes. The list path is taken only when the
  validator runs in-process. (#1875)

- **The seeder reads a snapshot footer from the stream, so a piped snapshot seeds.** It now
  refuses a named pipe outright when `utxostore_utxoBatchSize` is above 0, because that
  path reads the stream twice. Set it to 0, or unpack the snapshot to disk. (#1894)

- **The block graph is bucketed in the store**, and `GetBlockGraphData` responses (gRPC and JSON)
  gain a `bucket_seconds` field (omitted, i.e. 0, for ranges of 24h or less). The returned data
  points are unchanged. (#1798)

- **Block validation no longer keeps its large maps between blocks, and
  `block_parentSpendsCapacityMultiplier` changed role for the in-memory path.** The maps are
  allocated per block and dropped afterwards, so an idle block-validation node holds neither.
  Without `block_diskMapDirs`, duplicate inputs are found by sorting, and the multiplier only
  sets how many partitions the entries are sorted in; it no longer sizes or preallocates
  anything, and a wrong value never changes the result. The duplicate-input error is
  unchanged; the reported transaction is now always the earliest spender. The disk-backed
  maps are unchanged. (#1902, #1928)

- **Merkle-proof lookups no longer fail when another client disconnects.** The shared
  main-chain lookup behind `/merkle_proof/*` and `/txmeta*` runs on Asset's own context with a
  30s limit. A newly derived proof that does not reproduce the block header merkle root now
  returns an error instead of a proof that fails verification. (#1795)

- **With `blockassembly_txMapDirs` set, block assembly releases the cleared half of its
  transaction index.** Resident memory between blocks falls, and
  `teranode_subtreeprocessor_diskmap_filter_ram_bytes` no longer counts the capacity a cleared
  half used to keep. Limits sized from the old gauge can be lowered. (#1904)

- **The in-memory Kafka broker no longer keeps every message.** Nodes running
  `KAFKA_SCHEMA = memory` held every produced message for the life of the process; delivered
  messages are now released. `InMemoryBroker.TruncateTopic` is removed. (#1906)
