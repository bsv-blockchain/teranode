# Changelog

Breaking changes in Teranode: what an operator or integrator must do before or during an
upgrade, and what stops working if they don't. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

A change gets an entry when it:

- removes or renames a setting, adds a required one, or makes previously valid config fail
  at startup;
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
- **Blockchain caps MTP range requests (`blockchain_maxMedianTimePastHeights`, default 10000).**
  Older validator, subtree validation and block validation services validate nothing against it.
  Upgrade Blockchain last, or set the cap above the chain height until the rest are upgraded. (#1776)
- **Conflict recovery adds five `utxo.Store` methods and changes the UTXO WAL.** Upgrade block
  assembly together with the UTXO store and don't downgrade while conflict-recovery records are
  pending; they block startup. Follow [conflict recovery](docs/topics/conflict-recovery.md). (#2023)
- **Script-number overflow is a consensus failure.** Earlier builds accept transactions and blocks
  this one rejects: upgrade all your nodes together. These transactions are no longer published to
  the `kafka_txPolicyRejectedConfig` topic. (#1911)
- **A block with a non-final coinbase is invalid** (`bad-txns-nonfinal`), as in SV Node. Earlier
  builds accept such blocks: upgrade all your nodes together. Miners must build a final
  coinbase. (#2017)
- **Upgrade block assembly no later than the RPC service.** A newer RPC service answers
  `getminingcandidate` with `coinbase=true` with an error against an older block assembly. The
  returned coinbase is now the one block assembly mines. (#2027)
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
- **Six heavy Asset routes are limited to `asset_httpHeavyRateLimit` (10 req/s per IP)**: the
  `/merkle_proof/*` routes, `/utxos/:hash/json`, `/subtree/:hash/txs/json` and `/blockgraphdata`.
  SPV clients and explorers above that get 429: raise the limit or allowlist peers. (#1789)
- **Peers must not use a larger `subtreevalidation_missingTransactionsBatchSize` than this node.**
  Larger batches get 413 (`asset_maxBatchRecords`, default 16384, and the allowlisted signed-body
  cap), which fails the peer's catch-up. Align batch sizes or raise the cap. (#1788, #1797)
- **Asset no longer sends credentials to cross-origin callers by default.** Credentialed browser
  callers on other origins, the Vite dev dashboard included, must be listed in
  `asset_corsAllowOrigins`. An invalid entry (`*`, `null`, a path) stops Asset at startup. (#1793)
- **`asset_trustedProxyCIDRs`, when set, no longer trusts private proxies implicitly.** List
  internal ingress ranges, or every client shares the proxy's rate-limit and ban key. (#1793)
- **`asset_concurrency_get_legacy_block_reader` caps whole block streams.** An explicit value now
  limits concurrent legacy block streams for their full length; extra requests fail after 30 s.
  Re-size or remove it (default now 0, unlimited). (#1794)
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
- **Out-of-tree `utxo.Store` implementations must add `SpendAndCreateMulti` and
  `ParentOutputsForValidation`.** `utxo.DefaultSpendAndCreateMulti` provides the first. (#1875)

## [0.16.0] - 2026-10-05

### Breaking

- **Kubernetes: shared Kafka sits behind a NetworkPolicy** once
  `deploy/kubernetes/kafka/kafka-shared-networkpolicy.yaml` is applied. Services in another
  namespace lose Kafka: move Kafka or add a `namespaceSelector` before applying. (#1617)
- **`txMetaCacheTrimRatio` is renamed `subtreevalidation_txMetaCacheTrimRatio`.** A local
  override under the old name is ignored; rename it. Only the `preallocated` txmeta cache
  reads it. (#1746)
