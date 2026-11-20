# Legacy Service Settings

**Related Topic**: [Legacy Service](../../../topics/services/legacy.md)

Every key below is loaded in the `Legacy: LegacySettings{` block of
`settings/settings.go`. That block is the only thing that makes a setting
configurable. A struct tag in `settings/legacy_settings.go` is documentation,
not wiring: a field with a tag and no loader line arrives as its zero value
whatever an operator writes.

Two values are shown for each key:

- **Loader fallback** is the default `settings/settings.go` passes. A node uses
  it only when no configuration file sets the key.
- **settings.conf** is the base value in the committed `settings.conf`. A node
  that runs on the repository configuration uses this value, unless
  `settings_local.conf` or a context-specific key (`key.<context>`) sets
  another. "not set" means the file has no base line for the key, so the loader
  fallback applies.

`${DATADIR}` is `./data` in `settings.conf` (`/data` in the `operator`
context), and `${LEGACY_GRPC_PORT}` is `8099`.

## Configuration Settings

| Setting | Type | Loader fallback | settings.conf | Environment Variable | Usage |
|---------|------|-----------------|---------------|---------------------|-------|
| WorkingDir | string | "../../data" | `${DATADIR}/legacy` | legacy_workingDir | Directory for legacy peer data, resolved to an absolute path and created at startup. Holds the address book when SavePeers is true. Read from the config directly, not through the settings struct |
| ListenAddresses | []string | [] | not set | legacy_listen_addresses | Pipe-separated addresses to accept wire-protocol peers on. Empty falls back to the node's outbound interface IP and the network's default port |
| ConnectPeers | []string | [] | not set | legacy_connect_peers | Pipe-separated peers to dial. A non-empty list puts the node in connect-only mode |
| OrphanEvictionDuration | time.Duration | 10m | not set | legacy_orphanEvictionDuration | How long an orphan transaction is held. On eviction it gets one last validation attempt |
| MaxOrphanTxs | int | 100 | not set | legacy_maxOrphanTxs | Cap on orphan transactions held in memory. Inserting past the cap evicts the oldest by insertion time. 0 leaves the pool unbounded |
| RelayTxBytesBudget | int64 | 268435456 | not set | legacy_relayTxBytesBudget | Serialized bytes of relayed transactions queued for validation or being validated. A transaction over it is dropped and counted; zero or a negative value means one transaction at a time. The node holds about three times this while transactions are being validated |
| InvQueueMaxHashes | int | 400000 | not set | legacy_invQueueMaxHashes | Announced transaction hashes held from the peer's read loop until a UTXO store lookup finishes with them, node-wide; one peer may hold 100000 at once. Announcements over either limit are dropped and counted. Not covered: at most legacy_invHandlerSlots handler goroutines for transaction-only messages, and up to 50000 missing hashes per peer waiting for a getdata, which is fixed. Covers only the direct route: with kafka_legacyInvConfig set, announcements staged for Kafka are charged only when read back (issue 2103). Below 4096 falls back to the default with a warning; zero is not unbounded |
| InvLookupWorkers | int | 16 | not set | legacy_invLookupWorkers | Workers that look transaction announcements up in the UTXO store, capping the store reads in flight from this path; each holds at most about 1.3 MB. Below 1 falls back to the default with a warning |
| InvHandlerSlots | int | 1024 | not set | legacy_invHandlerSlots | Goroutines allowed at once to handle inv messages carrying only transaction announcements, which wait on the blockchain service. A message finding every slot taken is dropped and counted under handlers_busy; block announcements always get a handler. Below 1 falls back to the default with a warning; zero is not unbounded |
| PrintInvMessages | bool | false | false | legacy_printInvMessages | Log every inventory message sent and received |
| GRPCAddress | string | "" | `localhost:${LEGACY_GRPC_PORT}` | legacy_grpcAddress | Address other services dial to reach the legacy service. Client creation fails when empty |
| AllowBlockPriority | bool | true | true | legacy_allowBlockPriority | Negotiate the BSV multistream BlockPriority policy, which carries block traffic on its own TCP stream. False also refuses an inbound createstream |
| GRPCListenAddress | string | "" | `:${LEGACY_GRPC_PORT}` | legacy_grpcListenAddress | Bind address for the legacy gRPC server |
| SavePeers | bool | false | not set | legacy_savePeers | Persist the address book. When false the address manager is given no directory and keeps nothing across a restart |
| AllowSyncCandidateFromLocalPeers | bool | false | not set | legacy_allowSyncCandidateFromLocalPeers | Regtest only. False admits only 127.0.0.1 and localhost peers as sync candidates. On every other network the setting is not read |
| TempStore | *url.URL | "file://./data/tempstore" | `file://${DATADIR}/tempstore?checksum=false` | temp_store | Blob store for temporary data. The out-of-order block park writes here and needs a file:// URL |
| PeerIdleTimeout | time.Duration | 125s | not set | legacy_peerIdleTimeout | Disconnect a peer after this long with no message. A multistream association with recent traffic on another stream resets the timer. Half this value bounds the streaming pipeline's admission wait |
| PeerSendTimeout | time.Duration | 20m | not set | legacy_peerSendTimeout | Disconnect a peer whose socket has accepted no bytes for at least this long. The check runs once per timeout window, so a peer that stalls partway through a window is dropped after between one and two times this value; SV Node `-p2ptimeout` drops it after one. Same default as SV Node. Non-positive falls back to 20m |
| MaxAddnodePeers | int | 8 | not set | legacy_maxAddnodePeers | Ceiling on addnode peers, budgeted separately from MaxPeers and enforced on both the startup list and the runtime RPC |
| ReplenishInterval | time.Duration | 2s | not set | legacy_replenishInterval | How often the connection manager dials to close its outbound deficit. 0 restores the one-minute ticker and disables the event-driven wake |
| MaxFeelerPeers | int | 1 | not set | legacy_maxFeelerPeers | Peer slots reserved for short-lived feeler probes, and the cap on probes at once. 0 disables feelers and the reservation together |
| FeelerInterval | time.Duration | 120s | not set | legacy_feelerInterval | Mean of the randomised gap between feeler probes. Not a disable lever: a non-positive value falls back to 120s with a warning |
| FeelerHandshakeTimeout | time.Duration | 25s | not set | legacy_feelerHandshakeTimeout | How long a feeler waits for a version message. Must stay under the 30s peer negotiate timeout |
| PeerProcessingTimeout | time.Duration | 3m | 10m | legacy_peerProcessingTimeout | Per-message processing watchdog. Not armed for block messages while prefetch ingestion is active. Also the pre-admission deadline on an inbound peer, and part of the block-failure map TTL |
| BlockDownloadTimeoutBasePercent | int64 | 100 | not set | legacy_blockDownloadTimeoutBasePercent | Ceiling on one block download at the chain tip, as a percentage of the target block interval. Floored at 30 minutes, so values at or below 300 change nothing on a 10-minute chain |
| BlockDownloadTimeoutBaseIBDPercent | int64 | 600 | not set | legacy_blockDownloadTimeoutBaseIBDPercent | The same ceiling while catching up. Also floored at 30 minutes, which the 600 default clears on a 10-minute chain |
| BlockDownloadTimeoutPerPeerPercent | int64 | 50 | not set | legacy_blockDownloadTimeoutPerPeerPercent | Extra ceiling per other peer with a block download outstanding. The total is floored at 30 minutes, so this only adds patience |
| MultiPeerBlockDownload | bool | true | not set | legacy_multiPeerBlockDownload | Spread block requests over every eligible peer. False assigns them all to the sync peer, disconnects a stalled sync peer instead of demoting it, and ignores notfound |
| MaxBlocksInTransitPerPeer | int | 16 | not set | legacy_maxBlocksInTransitPerPeer | Most block bodies one measured peer may owe at once with multi-peer download on; a peer whose speed is not measured holds 1. A measured peer holds this value scaled by its rate against the fastest peer's, rounded up, at least 1. Not reduced for large blocks: the 100 GiB park backstop (`parkBackstopBytes`) guards the disk instead. In every mode it also sizes the pipeline's admission budget, at four slots per peer, and the peer package's download-timeout budget |
| BlockDownloadWindow | int | 1024 | not set | legacy_blockDownloadWindow | Block bodies the whole node may have outstanding, counting every peer together, and the read-ahead depth: how far above the committed tip a block may be asked for at all. A count, not svnode's per-peer height range |
| ParkStoreTimeout | time.Duration | 10s | not set | legacy_parkStoreTimeout | Deadline on each park blob store operation, bounding the wait for the file store's shared permits. Values below 1s are raised to 1s |
| PeerRegistryEnabled | bool | true | not set | legacy_peerRegistryEnabled | Mirror connected legacy peers into the centralized peer registry so the dashboard can show them |
| PeerRegistrySyncInterval | time.Duration | 10s | not set | legacy_peerRegistrySyncInterval | How often that mirror reconciles connected legacy peers into the registry |
| StoreBatcherSize | int | 1024 | 1024 | legacy_storeBatcherSize | svp2p: **CRITICAL** - Store operation batch size |
| StoreBatcherConcurrency | int | 32 | 32 | legacy_storeBatcherConcurrency | svp2p: **CRITICAL** - Store operation parallelism |
| SpendBatcherSize | int | 1024 | 1024 | legacy_spendBatcherSize | svp2p: **CRITICAL** - Spend operation batch size |
| SpendBatcherConcurrency | int | 4 | 4 | legacy_spendBatcherConcurrency | svp2p: **CRITICAL** - Spend operation parallelism |
| OutpointBatcherSize | int | 1024 | 1024 | legacy_outpointBatcherSize | svp2p: **CRITICAL** - Outpoint operation batch size |
| OutpointBatcherConcurrency | int | 32 | 32 | legacy_outpointBatcherConcurrency | svp2p: Outpoint operation parallelism |
| BlockFailureBackoffBase | time.Duration | 5s | not set | legacy_blockFailureBackoffBase | svp2p: Base per-block backoff after a transient storage/service failure (0 disables) |
| BlockFailureBackoffMaxDuration | time.Duration | 150s | not set | legacy_blockFailureBackoffMaxDuration | svp2p: Cap on the per-block backoff window and the failure-tracking map TTL, kept below the 180s sync-peer stall window (0 disables) |
| BlockPrefetchBufferBytes | int64 | 268435456 | not set | legacy_blockPrefetchBufferBytes | svp2p: Byte budget for blocks downloaded ahead of processing during sync (0 disables prefetch) |
| TargetOutboundPeers | int | 8 | not set | legacy_targetOutboundPeers | svp2p: outbound peers the addrman-driven dialer keeps (SVNode DEFAULT_MAX_OUTBOUND_CONNECTIONS) |
| BlockDownloadSlowFetchTimeout | time.Duration | 30s | not set | legacy_blockDownloadSlowFetchTimeout | svp2p: a block still not delivered after this may be fetched in parallel from another peer |
| BlockDownloadMaxParallelFetch | int | 3 | not set | legacy_blockDownloadMaxParallelFetch | svp2p: maximum peers a single block is fetched from at once |
| MinSyncPeerNetworkSpeed | uint64 | 51200 | not set | legacy_minSyncPeerNetworkSpeed | svp2p: bytes/s below which the sync peer is rotated (0 disables). Falls back to `legacy_config_MinSyncPeerNetworkSpeed` with a deprecation warning |
| DisableBanning | bool | false | not set | legacy_disableBanning | svp2p: the bsvd `--nobanning` switch. Falls back to `legacy_config_DisableBanning` with a deprecation warning |
| DisableDNSSeed | bool | false | not set | legacy_disableDNSSeed | svp2p: the bsvd `--nodnsseed` switch; with it off the fixed-seed list still applies after 60 s. Falls back to `legacy_config_DisableDNSSeed` with a deprecation warning |
| CompactBlocks | bool | false | not set | legacy_compactBlocks | svp2p: send `sendcmpct(0,1)` after verack and accept `cmpctblock` (the BIP152 receive path). Every failure falls back to a full `getdata` |
| CompactBlocksRecentTxs | int | 5000000 | not set | legacy_compactBlocksRecentTxs | svp2p: capacity of the recent-transaction hash ring that compact-block reconstruction matches short IDs against. About 105 bytes per hash (~504 MiB at the default), allocated only when `legacy_compactBlocks` is on |

## Configuration Dependencies

### Peer Connection Management

- `ListenAddresses` controls incoming connections. Empty falls back to the IP of
  the interface that reaches the internet, plus the active network's default
  port, which is 8333 on mainnet.
- `ConnectPeers` forces outgoing connections to specific peers.
- When `ConnectPeers` is set, `MaxPeers` is lowered to the length of the list and
  the address-book dialler is not installed, so the configured list is the node's
  entire connectivity.
- `ConnectPeers` does not switch off DNS seeding. The flag that does is decided
  before the settings list is read, so a node with `legacy_connect_peers` set
  still seeds its address book from DNS.
- `SavePeers` controls whether the address book is written to `WorkingDir`.
- There is no working `legacy_upnp` key. The struct field exists but nothing
  loads it, so it is always false. UPnP is reached through `legacy_config_Upnp`,
  which is copied onto the legacy config struct by field name.
- In svp2p, a non-empty `ConnectPeers` also switches off DNS seeding, the
  fixed-seed fallback and the address-book dialler.

### svp2p and the `legacy_config_*` namespace

The svp2p service (`-svp2p=1`, mutually exclusive with `-legacy=1`) reuses the `legacy_*` keys
above so a cutover changes no settings. Three keys the legacy service reads through its
reflective `legacy_config_<Field>` loader have svp2p-owned names: `legacy_disableBanning`,
`legacy_disableDNSSeed` and `legacy_minSyncPeerNetworkSpeed`. svp2p reads the old spelling as
a fallback when the new key is unset and prints
`WARN: setting legacy_config_X is deprecated ... set legacy_Y instead` at startup. The
`legacy_config_*` namespace is removed with the legacy service.

svp2p also honours `excessiveblocksize` at the wire: it is the receive cap for a single block
(blocks over 4 GiB use the SVNode extended message header), and a value of 0 — documented as
"unlimited" for validation — is mapped to the 4 GiB default at the wire, with a warning at start.

### Compact blocks (svp2p)

`legacy_compactBlocks` turns on the BIP152 compact-block RECEIVE path, and nothing else. svp2p
sends `sendcmpct(announce=false, version=1)` once per peer after verack, and it accepts a
`cmpctblock` a peer sends. svp2p never announces a block as `cmpctblock` and never serves one.

`announce=false` tells the peer to keep announcing by `headers` or `inv`. That announce bool is
the FIRST of `sendcmpct`'s two fields and the version is the second
(`net_processing.cpp:2390-2394`; go-wire's `MsgSendcmpct` is `{SendCmpct bool, Version uint64}`),
and it IS BIP152 high-bandwidth mode. There is no separate flag for it.

svp2p also answers NOTHING when a peer asks it for a compact block. A `getdata` for
`MSG_CMPCT_BLOCK` is not a recognised inv type (`services/svp2p/protocol/serving.go:495-505`,
`:336-339`), so the entry draws a warning log and no reply — not even a `notfound`
(`services/svp2p/protocol/getdata.go:246-248`). An inbound `getblocktxn` is refused the same way
(`services/svp2p/protocol/peer.go:843-849`). SVNode falls back to the full block in that case
(`net_processing.cpp:1310-1312`); this port does not. Turning the flag on makes such a request
possible, because SVNode reads our `sendcmpct` as a willingness to provide compact blocks
(`net_processing.cpp:1942-1946`).

Every reconstruction failure falls back to an ordinary `getdata` for the whole block, so the flag
costs bandwidth rather than correctness. It is not free of consequence for the announcing peer: a
`readFailed` costs that peer nothing, and any known holder may then serve the block, but a
`readInvalid` scores it 100 and disconnects it. When that peer was the only known holder, the
block waits for another announcement. See
[svp2p compact blocks](../../../topics/services/svp2p_compact_blocks.md) for the message flow,
the outcome table, and the divergences from SVNode.

`legacy_compactBlocksRecentTxs` sizes `bridge.RecentTxIndex`, the ring of recently seen
transaction hashes that stands in for the mempool SVNode matches short IDs against. Teranode has
no mempool, so the ring is fed by the txmeta topic's ADD entries alone — those
that are neither coinbase nor block-originated, since `services/svp2p/bridge/kafka.go:337-340`
and `:344-351` skip both classes, which is what keeps mined transactions out of the ring. Budget
about 105 bytes per hash: a 32-byte ring slot plus roughly 70 bytes in the dedup map. At the
5,000,000 default that is about 504 MiB of resident heap once the ring is full — a 160 MiB ring
and a ~344 MiB map, measured by `TestRecentTxIndex_FootprintAtDefaultCapacity`. That test LOGS
the breakdown and asserts only loose bounds — a heap delta above 300 MiB and below 1 GiB — and it
is skipped unless `SVP2P_MEASURE_INDEX=1` is set, so no CI run re-checks the 504 MiB figure. A
match adds a
transient 160 MiB copy of the ring for the length of one call. The ring grows into its capacity
as hashes arrive, so a node that never fills it never pays for the whole of it. A value of 0 or
below falls back to the default rather than disabling the index.

Cutover guidance:

- Leave `legacy_compactBlocks` at `false` unless the node has the ~504 MiB of headroom the
  default ring needs, plus the CPU for one pass over the ring per announced block.
- Turn it on for one node first and watch the reconstruction log lines. A low hit rate is not a
  fault: it degrades to one `getblocktxn` that carries most of the block, which is no worse than
  a `getdata`.
- Lower `legacy_compactBlocksRecentTxs` on a memory-tight node before turning the feature off. A
  smaller ring reconstructs less and still saves the round trip on the transactions it holds.
- The index is allocated only when the flag is on, so a node with the flag off pays nothing.

### Feeler Probes

- A feeler is a short-lived probe that connects to an address the node is **not**
  otherwise using, waits for the version exchange to prove somebody is home, marks
  the address as verified, and hangs up. Its purpose is to keep the pool of
  known-reachable addresses from decaying, so a lost peer can be replaced quickly.
- `MaxFeelerPeers` is both the number of probes allowed at once and the number of
  peer slots held back for them. The reservation comes out of the peer-admission
  ceiling (`legacy_config_MaxPeers`, 20 by default), never out of the automatic
  outbound target, so probing can never cost the node a peer it chose to dial.
- Probes start only once the automatic outbound tier is already at its target.
- Selection resolves the address it picks and skips any it cannot resolve, so an
  address this layer has no way of dialling costs one draw rather than the whole
  probe interval. OnionCat addresses are the case that always takes this path:
  the address book accepts them but there is no onion dial path here.
- Feelers switch themselves off, reservation included, in three cases:
  `MaxFeelerPeers` at zero or below; connect-only mode (`ConnectPeers` set); and a
  peer cap too tight to reserve a slot without pushing the admission ceiling below
  the outbound target. Each logs its reason at startup.
- `FeelerHandshakeTimeout` must stay below the peer package's 30-second negotiate
  timeout. If it does not, the peer package hangs up first and a silent host is
  logged at warning as a lost peer rather than being hung up on quietly by the
  probe; values at or above the peer timeout are reduced to 29s with a warning.
  A non-positive value falls back to 25s, also with a warning. Both warnings are
  emitted once, at startup, and the deadline the feeler settled on is on the
  `[Feeler] Starting` line.

### Peer Timeout Management

- `PeerIdleTimeout` at 125s sits above the protocol's two-minute ping interval, so
  a healthy quiet peer is not dropped.
- `PeerProcessingTimeout` bounds one message's handling. While prefetch ingestion
  is active it is not armed for block messages, because a block can legitimately
  wait on the admission budget for longer than this; a stalled block download is
  then caught by the sync-peer stall detector and the idle timer instead.

### Sync Candidate Selection

- `AllowSyncCandidateFromLocalPeers` is read on regtest and nowhere else.
- On regtest, false means a peer is a sync candidate only if its host is
  `127.0.0.1` or `localhost`. True skips that test and admits any host.
- On every other network a peer is a sync candidate if it advertises the
  `SFNodeNetwork` service flag, and this setting has no part in the decision.

### Block Priority

- With `AllowBlockPriority` true and a peer that sends an association ID in its
  version message, the node registers a multistream association and defers
  registering the peer with the sync manager until the protoconf exchange, so the
  block stream exists before any getdata goes out.
- The stream policy itself is negotiated on protoconf, when the peer lists
  BlockPriority among its stream policies. An outbound peer then opens its
  required streams synchronously.
- With it false, the node registers no association and disconnects any peer that
  sends a createstream.

### Block Download

- `BlockDownloadWindow` and `MaxBlocksInTransitPerPeer` bound how many block
  bodies are outstanding, and `BlockDownloadWindow` doubles as the read-ahead
  depth: how far above the committed tip a block may be asked for at all, which
  is what decides how much disk the park needs.
- Blocks are chosen by a pass over that range: drop what is already on disk, in
  the park, arriving, being converted, given up on or already owed, then hand the
  rest to peers with budget. Below the last checkpoint each block goes to the
  measured peer with the lowest (blocks owed + 1) / rate: a block size is not
  known before the download, so the schedule uses none, and no block waits while
  a peer has room. A peer holds the per-peer depth scaled by its rate against
  the fastest peer's. A peer with no rate gets one block from the top of the
  window. Measured rates of outbound peers are kept by address, with the time
  each was measured, in `legacy-peer-rates.json` in the working directory. A
  remembered rate loses half its weight per hour of age, an entry older than
  24 hours is dropped, and the file keeps at most the 1,000 newest. Below the
  last checkpoint the range comes from the header cache, which runs to the next
  checkpoint; above it, blocks are asked for from invs and the pass re-asks from
  the download ledger. There is no download cursor, so nothing carries a
  position between passes.
- A block whose owners have sent nothing for the 60-second retry window is
  offered to another peer on the next pass. An owner that has delivered a block
  asked after this one has passed this one over, and its bytes do not count for
  it. Above the last checkpoint this applies to every block, one block per quiet
  owner. Below it, a block with one active owner that is still sending other
  block bytes, and has not passed this one over, stays with it, and the watcher
  judges it; the watcher also treats a passed-over block as one its owner will
  not deliver.
- During headers-first sync, a block above the committed tip that has been
  arriving for 30 seconds at under 100 KB/s, and will not finish before the
  chain needs it, is asked of one other peer, and the slow peer is disconnected.
  Every copy of the block must be that slow, a further copy waits 30 seconds
  after the last one, and at most three peers send or are expected to send a
  block at once. A peer let off a block that sends nothing is not counted. The
  slow peer is kept connected when the peer asked is one that was let off the
  block and asked again, or, at the three-copy limit, when such a peer is still
  connected and sends nothing. These are SV Node's slow-fetch timeout, stalling
  rate and parallel-fetch limit.
- During headers-first sync, the watcher examines up to 64 owed blocks from the
  tip every 5 seconds. A block that will land 30 seconds or more after each block
  below it is also asked of one other peer: for a block arriving, whose size its
  first bytes declare, the peer that lands a full copy in half the owner's time;
  for a queued block, the fastest peer with twice the owner's rate that lands
  the block, at the recent mean size, in half the owner's time. A copy with more
  than half its bytes gets no extra request unless it will take more than 5
  minutes, and a block whose every copy arrives under 100 KB/s is left to the
  race, with no block above it counted as late. A block is judged while it has
  one live copy, and the watcher and the race together ask for at most two extra
  copies of a block in 10 minutes. There is no setting for it; the metric is
  `teranode_legacy_netsync_frontier_races_total`.
- `MultiPeerBlockDownload` set to false keeps the pass but gives every block to
  the sync peer, bounded by the block-size ladder alone.

### Block Prefetch

Asynchronous block admission is always on and never active on regtest. The
block's bytes have already been streamed to files by the time admission runs,
so each block charges one slot against a budget sized as
`MaxBlocksInTransitPerPeer` times four. At the defaults that is 64 blocks.

### Out-of-Order Block Park

- The park needs a temp store it can enumerate after a restart, so the node refuses
  to start on any `temp_store` URL that is not a `file://` store, and on a node
  with no temp store at all. Every downloaded block goes through the park.
- `ParkStoreTimeout` bounds the wait for the file store's process-wide permits,
  which are shared with subtree writes, transaction writes and both persisters. It
  does not bound the work the store does once it holds a permit.

### Peer Registry Mirror

`PeerRegistryEnabled` and `PeerRegistrySyncInterval` control the mirror that
makes legacy peers visible in the dashboard beside libp2p peers.

The mirror is a read-only visibility path. It feeds no sync, catchup or
peer-selection decision, and the legacy service's own sync engine
(`services/legacy/netsync`) is unaffected by it either way.

- Each tick snapshots the connected legacy peers and pushes only what changed,
  so an idle peer costs no RPC.
- Entries are registered with the wire-protocol transport type and keyed
  `legacy:host:port`, which keeps them distinguishable from libp2p peers at
  every layer.
- A peer that disappears is marked disconnected once, then left for the
  registry TTL to reap.
- Each registry call is bounded independently, so an unresponsive blockchain
  service delays a tick rather than stalling the mirror.

#### Requirements

- The blockchain service must be reachable, since it hosts the registry. When
  the registry client is unavailable the mirror does not start and the legacy
  service runs normally without dashboard peer visibility.

#### Recommendations

- Keep `PeerRegistryEnabled` at true for operator visibility. Set it to false
  only if the extra registry traffic is unwelcome on a node carrying very many
  legacy connections.
- The default 10s interval sits well below the legacy two-minute ping interval.
  Values under one second are pointless, because the underlying peer statistics
  do not change that fast.

## Service Dependencies

| Dependency | Interface | Usage |
|------------|-----------|-------|
| SubtreeStore | blob.Store | **CRITICAL** - Merkle subtree storage and verification |
| TempStore | blob.Store | **CRITICAL** - Temporary data storage, including the out-of-order block park |
| UTXOStore | utxo.Store | **CRITICAL** - UTXO operations |
| BlockchainClient | blockchain.ClientI | **CRITICAL** - Blockchain operations and state queries |
| ValidatorClient | validator.Interface | **CRITICAL** - Transaction validation |
| SubtreeValidationClient | subtreevalidation.Interface | **CRITICAL** - Subtree validation |
| BlockValidationClient | blockvalidation.Interface | **CRITICAL** - Block validation |
| BlockAssemblyClient | *blockassembly.Client | **CRITICAL** - Block assembly operations |

## Validation Rules

| Setting | Validation | Impact | When Checked |
|---------|------------|--------|-------------|
| GRPCAddress | Must not be empty | Client creation returns a configuration error | During client initialization |
| TempStore | Must be set | Daemon returns "temp_store config not found" | During store construction |
| ListenAddresses | Falls back to the outbound interface IP and the network's default port if empty | Network connectivity | During server start |
| ParkStoreTimeout | Raised to 1s if lower | A zero deadline would fail every store operation instantly | During sync manager construction |
| MaxFeelerPeers | Zeroed by connect-only mode, or by a peer cap too tight to hold the reservation | Feelers and their slot reservation are both switched off, with the reason logged | During server construction |
| FeelerInterval | Non-positive falls back to 120s with a warning | Probe pacing | When the feeler loop starts |
| FeelerHandshakeTimeout | Non-positive falls back to 25s; 30s or more is reduced to 29s, both with a warning | A deadline at or above the peer negotiate timeout lets the peer package tear the connection down first | When the feeler loop starts |
| Block download timeout percents | Result floored at 30 minutes and capped at the largest budget these settings can produce | The scaling can only widen a block's deadline, never narrow it | Per block download |

## Configuration Examples

### Basic Configuration

```text
legacy_listen_addresses = "0.0.0.0:8333"
legacy_savePeers = false
```

### Forced Peer Connections

```text
legacy_connect_peers = "peer1.example.com:8333|peer2.example.com:8333"
```

Connect-only mode lowers the peer cap to the length of the list and switches
feeler probes off. It does not switch off DNS seeding.

### Bounding Read-Ahead

```text
legacy_blockDownloadWindow = 32
legacy_maxBlocksInTransitPerPeer = 8
```

Lower values keep the park small and the node's memory footprint down, at the
cost of a sync more exposed to one slow peer.
