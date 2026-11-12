# Changelog

Breaking changes in Teranode: what an operator or integrator must do before or during an
upgrade, and what stops working if they don't. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

A change gets an entry when it:

- removes or renames a setting, adds a required one, changes what an existing one does, or
  makes previously valid config fail at startup;
- needs access a deployment didn't need before (credentials, Kafka topic ACLs);
- breaks wire, proto, gRPC or HTTP API compatibility for external clients, including error
  codes or status codes clients act on;
- changes which blocks or transactions are valid, or otherwise imposes an upgrade order or rules
  out a downgrade. Services that can be upgraded separately count;
- removes or renames a metric or label value, or changes what one means, so dashboards and
  alerts break;
- changes a store or interface in a way that breaks out-of-tree implementations.

A bug fix that does any of these still gets an entry. Not listed: other bug fixes (even
observable ones), new limits or defaults that don't break sane existing use, internal behaviour
changes, log changes, error-message wording and performance changes. The release notes and the
PRs cover those.

Each entry is 1–3 lines: what changed, who is affected, what to do, and the PR. Detail lives
in the PR. When a stable version is tagged, the entries it ships move from `[Unreleased]` to
a `[X.Y.Z]` section dated with the GitHub release date. Upgrading across several versions,
read every section between yours and the target.

## [Unreleased]

### Breaking

- **A block whose transaction spends an output mined only outside the block's own history is now
  invalid.** Earlier versions accepted it when that output's block was on the main chain, so
  upgrade every block validation pod together; a mixed fleet can disagree on such a fork block. (#2174)
- **`blockassembly_storeTxInpointsForSubtreeMeta=false` now only skips the subtree meta files.**
  Block assembly keeps every unmined tx's parent links in memory in both modes. With it off, the
  restart reload and a block move-back now read each tx's inputs from the UTXO store (an external tx
  from the blob store), and memory per unmined tx rises; nothing to change. (#2128)
- **Upgrade every validator and subtree-validation pod before you set
  `blockassembly_maxTransactionsInMemory`, and keep the default 0 until then.** An old validator
  never sends `inBlock`, so a new block assembly with the limit set refuses its block transactions
  and the old validator unlocks them without a template entry. With the limit set,
  `teranode_validator_invalid_transactions` no longer counts a transaction refused at the limit;
  `teranode_validator_held_limit_shed_transactions` does. A new validator setting,
  `validator_kafkaBackpressureResumeHeldPercent` (default 80), sets where a held-count pause
  resumes. (#1593)
- **`grpc_admin_api_key` is required.** Blockchain refuses to start if it is empty, a placeholder
  or under 16 characters, and every RPC but `HealthGRPC` needs it. Give every client service the
  same key and upgrade Blockchain last, or restart them together. (#1755)
- **Default RPC credentials are removed.** With `rpc_user`/`rpc_pass` unset, JSON-RPC rejects every
  request and Asset admin routes and dashboard login answer 401. Set both in `settings_local.conf`
  before upgrading; `teranode-dev` users re-run `teranode-dev init`. (#1790)
- **Legacy sync requires a `file://` `temp_store`.** The legacy service refuses to start on any
  other scheme. Only nodes that moved `temp_store` off the default must change. Legacy sync now
  holds up to 100 GiB of downloaded blocks there: provision the disk. (#1699)
- **The alert service picks its network by `network` and validates `alert_genesis_keys`.** On
  testnet and stn, clear `alert_genesis_keys`, `alert_protocol_id` and `alert_topic_name` if they
  hold mainnet values. An explicit key list needs 3–10 compressed public keys or startup fails. (#1955)
- **Block Validation `BlockFound` with `wait_to_complete` returns the block's outcome.** It no
  longer answers success at enqueue. A rejected block returns its validation error, whose code
  varies; one already stored invalid returns `ERR_BLOCK_INVALID`; a failed fetch returns at once with
  a retryable error; a block sent to catchup or dropped returns a retryable `ERR_SERVICE_ERROR`; a
  caller whose context ends gets `ERR_CONTEXT_CANCELED`. Under opt-in optimistic peer-block mining,
  success means the block was added, not fully validated. gRPC callers that wait must handle these.
  (#2132)
- **`utxostore_updateTxMinedStatus` is removed.** Nodes that set it to `false` now always run
  mined-tagging and its duplicate-transaction check, with no way to turn them off. Delete the
  key; the loader ignores it. (#2019)
- **Six legacy batcher settings are removed:** `legacy_storeBatcherSize`,
  `legacy_storeBatcherConcurrency`, `legacy_spendBatcherSize`, `legacy_spendBatcherConcurrency`,
  `legacy_outpointBatcherSize` and `legacy_outpointBatcherConcurrency`. Delete them. (#1699)
- **Four more legacy settings are removed:** `legacy_blockFailureBackoffBase`,
  `legacy_blockFailureBackoffMaxDuration`, `legacy_blockPrefetchBufferBytes` and
  `blockvalidation_legacy_below_checkpoint_fail_closed`. Delete them. (#1699)
- **`subtreevalidation_processTxMetaUsingCache_BatchSize` is removed** and `..._Concurrency` now
  sizes two service-wide pools instead of each call. Delete the first; raise the second if
  subtree validation slows down. (#1957)
- **`blockassembly_livenessStallTimeout` measures progress, not main-loop passes.** Catch-ups,
  reorgs and block-validation waits now beat while they progress. If you set it, re-size it from
  `teranode_blockassembly_liveness_heartbeat_age_seconds`. (#1832)
- **Blockchain caps MTP range requests (`blockchain_maxMedianTimePastHeights`, default 10000).**
  Older validator, subtree and block validation services send whole-chain MTP requests, which it
  rejects, so they stop validating. Upgrade Blockchain last or raise the cap above chain height. (#1776)
- **Conflict recovery adds five `utxo.Store` methods and changes the UTXO WAL.** Upgrade block
  assembly together with the UTXO store and don't downgrade while conflict-recovery records are
  pending; they block startup. Follow [conflict recovery](docs/topics/conflict-recovery.md). (#2023)
- **PostgreSQL UTXO store: stop previous-release processes before upgrading.** One that starts while
  the new `px_transactions_conflicting` index builds blocks all `transactions` reads and writes until
  the build ends. Out-of-tree `utxo.Store` implementations add `ConflictingTxHashes`. (#2110)
- **Script-number overflow is a consensus failure.** Earlier builds accept transactions and blocks
  this one rejects: upgrade all your nodes together. These transactions are no longer published to
  the `kafka_txPolicyRejectedConfig` topic. (#1911)
- **A block with a non-final coinbase is invalid** (`bad-txns-nonfinal`), as in SV Node. Earlier
  builds accept such blocks: upgrade all your nodes together. Miners must build a final
  coinbase. (#2017)
- **Upgrade block assembly no later than the RPC service.** A newer RPC service answers
  `getminingcandidate` with `coinbase=true` with an error against an older block assembly. The
  returned coinbase is now the one block assembly mines. (#2027)
- **Upgrade the validator after propagation, RPC, legacy and subtree validation.** A newer validator
  answers `ERR_TX_UNLOCK_FAILED` (42) for an accepted transaction whose unlock timed out, and older
  callers treat it as a rejection. Applies where the validator runs as its own service. (#1925)
- **Upgrade subtree and block validation together.** A peer's HTTP 429 is now
  `ERR_SERVICE_RATE_LIMITED` (52), not `ERR_SERVICE_ERROR` (59); an older service gets an unknown
  code over gRPC. Log alerts on `SERVICE_ERROR (59)` for 429s must match the new code. (#1802)
- **Subtree validation produces to the `kafka_txPolicyRejectedConfig` topic.** Grant it write
  access where Kafka ACLs are on, and upgrade every subtree validator together: an older one
  mistakes the new messages for validator rejections. (#2038)
- **Block assembly needs produce access to the `kafka_txmetaConfig` topic.** When that is set,
  block assembly connects at startup and exits if the broker stays unreachable. (#1938)
- **Unmined transactions are refused while the node is `CATCHINGBLOCKS`**, including a short
  catch-up from `RUNNING`. Propagation answers 503 and `sendrawtransaction` `-10`. Clients
  must resubmit once the node is `RUNNING`. (#2094)
- **JSON-RPC error codes change.** Missing blocks answer `-5`, bad heights and arguments `-8`,
  missing inputs `-25` and not-ready `-28` (retry), most of them `-32603` before. Re-check clients
  that act on error codes. (#1637, #1953)
- **Some script failures move between `TX_POLICY` and `TX_INVALID`.** A policy-limit failure is
  re-run under consensus limits: `TX_POLICY` if that passes, else `TX_INVALID`. Propagation clients
  that retry or drop by these codes should re-check; RPC codes don't change. (#2068)
- **Six heavy Asset routes are limited to `asset_httpHeavyRateLimit` (10 req/s per IP)**: the
  `/merkle_proof/*` routes, `/utxos/:hash/json`, `/subtree/:hash/txs/json` and `/blockgraphdata`.
  SPV clients and explorers above that get 429: raise the limit or allowlist peers. (#1789)
- **`/lastblocks` returns at most 1000 blocks** (lower with `asset_maxLastNBlocks`) and a negative
  `n` returns 400. Common-ancestor header routes return 400 above 256 locator hashes. Explorers that
  page with a larger `n` must page in 1000s. (#1792)
- **Peers must not use a larger `subtreevalidation_missingTransactionsBatchSize` than this node.**
  Larger batches get 413 (`asset_maxBatchRecords`, default 16384, and the allowlisted signed-body
  cap), which fails the peer's catch-up. Align batch sizes or raise the cap. (#1788, #1797)
- **Asset no longer sends credentials to cross-origin callers by default.** Credentialed browser
  callers on other origins, the Vite dev dashboard included, must be listed in
  `asset_corsAllowOrigins`. An invalid entry (`*`, `null`, a path) stops Asset at startup. (#1793)
- **`asset_trustedProxyCIDRs`, when set, no longer trusts private proxies implicitly.** List
  internal ingress ranges, or every client shares the proxy's rate-limit and ban key. (#1793)
- **`asset_concurrency_get_legacy_block_reader` caps whole block streams**; extra requests fail
  after 30 s. Re-size or remove it (default now 0, unlimited). Legacy block serving no longer follows
  redirects: point `asset_httpAddress` at its final address, or SV peers get no blocks. (#1794)
- **Four Aerospike pruner metrics are removed**: `utxo_pruner_parents_skipped_pruned_total`,
  `utxo_pruner_pruned_set_size`, `utxo_pruner_pruned_set_saturated` and
  `utxo_pruner_pruned_set_rotations`. Remove their queries. (#1969)
- **Legacy netsync metrics are removed or renamed.** `teranode_legacy_peer_server_OnBlock` becomes
  `teranode_legacy_peer_server_OnBlockOnDisk`, the per-phase histograms are gone and
  `teranode_legacy_netsync_handle_block_direct` times the block commit. Rebuild those panels. (#1699)
- **`teranode_blockvalidation_priority_queue_processed_total{priority}`** now holds
  `chain_extending`, `near_fork` or `deep_fork` instead of `unknown`. Update queries that filter
  on `unknown`. (#1997)
- **`teranode_asset_http_peer_auth_result_total{result}` changes meaning.** With an empty
  allowlist every well-formed signed request is `not_allowlisted`, so alerts on `replay`,
  `bad_digest` or `bad_sig` go quiet: populate `asset_peerAuthAllowlist` or retarget them. (#1788)
- **Asset catch-up route 429s move to `scope="heavy_catchup"`** on
  `teranode_asset_http_rate_limited_total`. Use `scope=~"heavy|heavy_catchup"`. (#1801)
- **Global-limit 429s leave `teranode_asset_http_request_duration_seconds` and
  `..._response_size_bytes`**, and their `method` label folds non-standard methods into `other`.
  Count those 429s with `teranode_asset_http_rate_limited_total{scope="global"}`. (#1793)
- **Out-of-tree `utxo.Store` implementations must add `SpendAndCreateMulti` and
  `ParentOutputsForValidation`.** `utxo.DefaultSpendAndCreateMulti` provides the first. (#1875)
- **Block assembly can fail to start while a skipped-fork mark is unfinished.** A restart finishes
  it before loading the pool, and fails if the subtree or UTXO store doesn't answer or
  `BlockAssemblerSkippedForkMark` is malformed: restart once they answer, or clear that key. (#2065)
- **`teranode_blockassembly_processing_stuck_total` gains three reasons:**
  `skipped_fork_mark_unfinished`, `skipped_fork_mark_record`, `skipped_fork_subtree_lost`. An alert
  on the total without a `reason` filter now fires on them too. (#2065)
- **Asset block-list endpoints honour `fromHeight` with `includeOrphans`.** They now page down
  instead of returning the chain's top; clients passing both get the blocks at or below
  `fromHeight`. (#2065)
- **`blockassembly_maxBlockReorgCatchup` is removed.** It never capped a reorg; it only sized two
  unused maps. A config that still sets it starts as before and the value is ignored, so no
  action is needed beyond deleting the line. (#2163)

## [0.16.0] - 2026-10-05

### Breaking

- **Set a real `grpc_admin_api_key` on every service.** `settings.conf` no longer ships `testkey`,
  placeholders are ignored, and P2P won't start on a key under 16 characters off loopback. Bans,
  reputation reset, peer connect and mutating P2P RPCs need it. (#1257, #1532, #1575)
- **`p2p_grpcListenAddress` defaults to `localhost:${P2P_GRPC_PORT}`.** Outside the `docker.m`,
  `docker.ss` and `operator` contexts, a P2P service in its own container or pod is unreachable until
  you set `p2p_grpcListenAddress = :${P2P_GRPC_PORT}`. (#1532)
- **P2P refuses to start when `p2p_listen_addresses` names anything but a wildcard bind**; narrower
  values were ignored before. libp2p always listens on `p2p_port`, and `p2p_share_private_addresses`
  no longer advertises listen addresses: use `p2p_advertise_addresses`. (#1684)
- **`blockpersister_batchMissingTransactions` is removed**; the persister always batches. Delete the
  key. (#1656)
- **Block Assembly refuses to start with no saved state on a chain past genesis.** This hits nodes
  seeded with the v0.15.9 seeder that never ran Block Assembly: reseed with the v0.16.0 seeder or set
  the state with `rewindblockchain`. (#1120)
- **The LEGACYSYNCING FSM state, `LEGACYSYNC` event and `LegacySync` RPC are gone.** Scripts using
  `setfsmstate --fsmstate legacysyncing` must use `catchingblocks`. A persisted LEGACYSYNCING state
  becomes CATCHINGBLOCKS on startup. (#1042)
- **`RUN` is refused below the highest checkpoint from every FSM state, IDLE included.** Tooling that
  forces a syncing node to `running` gets an error: send `catchingblocks` and the node promotes itself
  once caught up. (#1685)
- **Peer fetches refuse DataHub hostnames that resolve to a private address** (RFC1918, `fc00::/7`,
  `100.64.0.0/10`). Nodes that peer over a private network or internal DNS must set
  `p2p_allow_private_ips = true`, or they can't catch up from each other. (#1749)
- **Kubernetes: shared Kafka sits behind a NetworkPolicy** once
  `deploy/kubernetes/kafka/kafka-shared-networkpolicy.yaml` is applied. Services in another
  namespace lose Kafka: move Kafka or add a `namespaceSelector` before applying. (#1617)
- **The reference Kubernetes manifests read `blockchain_store` and `utxostore` from the
  `teranode-operator-secrets` Secret.** Create it before re-applying `deploy/kubernetes/teranode/`.
  (#1180)
- **BIP34/66/65 are enforced as in SV Node.** From each network's activation height, blocks below
  version 2/3/4 or without a minimally encoded coinbase height are invalid. Earlier builds accept them,
  and mine a non-minimal height below 32768: upgrade all your nodes together. (#1301, #1338)
- **Median-time-past uses the 11 blocks ending at the parent, as in SV Node.** Earlier builds measured
  it about 90 blocks further back and accept blocks this one rejects: upgrade all your nodes
  together. (#1481)
- **tstn has a new genesis block.** Wipe tstn chain data and resync. Regtest activates Genesis at
  height 100 and Chronicle at 200 (were 10000 and 15000): regenerate regtest chains and don't mix
  versions on one. (#1220, #1592)
- **Upgrade subtree validation no later than a remote validator.** The validator drops the parent
  metadata older subtree validation sends, so those reject blocks with same-block parent chains
  (`bad-txns-unconfirmed-input-in-block`). In-process validators (the default) are unaffected. (#1075)
- **`POST /txs` returns the rejection's status, not 500.** A rejected batch gets 400, 403, 409 or
  422, 500 means a server fault, and already-known transactions count as success. Update clients that
  treat 500 as a rejection. (#1295)
- **`/api/v1/merkle_proof/:hash` no longer accepts a subtree root** and returns 404 unless the hash
  is a mined transaction. Request per-transaction proofs. (#1716)
- **P2P HTTP is rate-limited per source by `p2p_httpRateLimit` (default 100 req/s).** Tools that
  poll it faster get 429: raise the limit or set it to 0. (#1566)
- **The Asset Centrifuge websocket rejects cross-origin browser connections.** List a dashboard or
  web app served from another host in `asset_centrifugeAllowOrigins`. Non-browser clients are
  unaffected. (#1770)
- **Pruner metrics move from `pruner_*` to `teranode_pruner_*`** (`pruner_errors_total` becomes
  `teranode_pruner_errors_total`). Update dashboards and alerts. (#1564)
- **Txmeta-cache metrics change.** `teranode_subtreevalidation_set_tx_meta_cache_kafka` becomes
  `..._kafka_batch`, `..._set_tx_meta_cache_kafka_count` counts entries, not Kafka messages, and
  `teranode_tx_meta_cache_hit_old_tx` is gone. (#1211)
- **`teranode_blockvalidation_setmined_retry_total` and `..._setmined_drops_total` lose their
  `blockhash` label.** Queries that group or filter on it must aggregate. (#918, #1102)
- **`teranode_validator_invalid_transactions` no longer counts Kafka missing-parent rejections**;
  they go to `teranode_validator_missing_parent_transactions`. Alerts should sum both. (#1617)
- **`txMetaCacheTrimRatio` is renamed `subtreevalidation_txMetaCacheTrimRatio`.** A local
  override under the old name is ignored; rename it. Only the `preallocated` txmeta cache
  reads it. (#1746)
- **Out-of-tree `utxo.Store` implementations break.** `Create` and `Spend` become `SpendAndCreate`,
  `SetBlockState` is added, and `utxo.Spend.BlockIDs` is removed. (#1326, #1398, #1485)
- **`utxo.Store` adds `BeginConflictIntent`, `CompleteConflictIntent`, `PendingConflictIntents`,
  `SupportsOutpointOnlySpend` and `DeleteComplete`.** Aerospike users with set-scoped privileges also
  allow the new `<set>_conflictwal` set. (#1126, #1178, #1502)
- **The blockchain `Store` interface requires `Close(ctx) error`.** Out-of-tree blockchain stores
  must add it; a no-op is fine. (#1127)
