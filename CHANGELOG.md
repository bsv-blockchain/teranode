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

- **This release removes four Aerospike pruner metrics.** External monitoring systems must
  remove queries for `utxo_pruner_parents_skipped_pruned_total`,
  `utxo_pruner_pruned_set_size`, `utxo_pruner_pruned_set_saturated`, and
  `utxo_pruner_pruned_set_rotations`. These metrics reported zero because the pruner
  no longer skipped parents. After this upgrade, their queries give no data. (#1969)

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

### Not breaking, but worth knowing

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
