# Legacy Service Settings

**Related Topic**: [Legacy Service](../../../topics/services/legacy.md)

Every key below is loaded in the `Legacy: LegacySettings{` block of
`settings/settings.go`. That block is the only thing that makes a setting
configurable, and the default shown here is the one it passes. A struct tag in
`settings/legacy_settings.go` is documentation, not wiring: a field with a tag
and no loader line arrives as its zero value whatever an operator writes.

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| WorkingDir | string | "../../data" | legacy_workingDir | Directory for legacy peer data, resolved to an absolute path and created at startup. Holds the address book when SavePeers is true. Read from the config directly, not through the settings struct |
| ListenAddresses | []string | [] | legacy_listen_addresses | Pipe-separated addresses to accept wire-protocol peers on. Empty falls back to the node's outbound interface IP and the network's default port |
| ConnectPeers | []string | [] | legacy_connect_peers | Pipe-separated peers to dial. A non-empty list puts the node in connect-only mode |
| OrphanEvictionDuration | time.Duration | 10m | legacy_orphanEvictionDuration | How long an orphan transaction is held. On eviction it gets one last validation attempt |
| MaxOrphanTxs | int | 100 | legacy_maxOrphanTxs | Cap on orphan transactions held in memory. Inserting past the cap evicts the oldest by insertion time. 0 leaves the pool unbounded |
| StoreBatcherSize | int | 1024 | legacy_storeBatcherSize | Multiplied by StoreBatcherConcurrency to give the concurrent-request limit on UTXO creates while a block is stored |
| StoreBatcherConcurrency | int | 32 | legacy_storeBatcherConcurrency | The other factor in that UTXO create limit (1024 x 32 = 32768 concurrent requests at the defaults) |
| SpendBatcherSize | int | 1024 | legacy_spendBatcherSize | Multiplied by SpendBatcherConcurrency to give the concurrent-request limit on UTXO spends |
| SpendBatcherConcurrency | int | 4 | legacy_spendBatcherConcurrency | The other factor in that UTXO spend limit |
| OutpointBatcherSize | int | 1024 | legacy_outpointBatcherSize | Goroutine limit for populating transaction inputs in extendTransactions |
| OutpointBatcherConcurrency | int | 32 | legacy_outpointBatcherConcurrency | Loaded but read by no code. Changing it has no effect |
| PrintInvMessages | bool | false | legacy_printInvMessages | Log every inventory message sent and received |
| GRPCAddress | string | "" | legacy_grpcAddress | Address other services dial to reach the legacy service. Client creation fails when empty |
| AllowBlockPriority | bool | true | legacy_allowBlockPriority | Negotiate the BSV multistream BlockPriority policy, which carries block traffic on its own TCP stream. False also refuses an inbound createstream |
| GRPCListenAddress | string | "" | legacy_grpcListenAddress | Bind address for the legacy gRPC server |
| SavePeers | bool | false | legacy_savePeers | Persist the address book. When false the address manager is given no directory and keeps nothing across a restart |
| AllowSyncCandidateFromLocalPeers | bool | false | legacy_allowSyncCandidateFromLocalPeers | Regtest only. False admits only 127.0.0.1 and localhost peers as sync candidates. On every other network the setting is not read |
| TempStore | *url.URL | "file://./data/tempstore" | temp_store | Blob store for temporary data. The out-of-order block park writes here and needs a file:// URL |
| PeerIdleTimeout | time.Duration | 125s | legacy_peerIdleTimeout | Disconnect a peer after this long with no message. A multistream association with recent traffic on another stream resets the timer. Half this value bounds the streaming pipeline's admission wait |
| MaxAddnodePeers | int | 8 | legacy_maxAddnodePeers | Ceiling on addnode peers, budgeted separately from MaxPeers and enforced on both the startup list and the runtime RPC |
| ReplenishInterval | time.Duration | 2s | legacy_replenishInterval | How often the connection manager dials to close its outbound deficit. 0 restores the one-minute ticker and disables the event-driven wake |
| MaxFeelerPeers | int | 1 | legacy_maxFeelerPeers | Peer slots reserved for short-lived feeler probes, and the cap on probes at once. 0 disables feelers and the reservation together |
| FeelerInterval | time.Duration | 120s | legacy_feelerInterval | Mean of the randomised gap between feeler probes. Not a disable lever: a non-positive value falls back to 120s with a warning |
| FeelerHandshakeTimeout | time.Duration | 25s | legacy_feelerHandshakeTimeout | How long a feeler waits for a version message. Must stay under the 30s peer negotiate timeout |
| PeerProcessingTimeout | time.Duration | 3m | legacy_peerProcessingTimeout | Per-message processing watchdog. Not armed for block messages while prefetch ingestion is active. Also the pre-admission deadline on an inbound peer, and part of the block-failure map TTL |
| BlockFailureBackoffBase | time.Duration | 5s | legacy_blockFailureBackoffBase | Base per-block backoff after a transient storage or service failure, multiplied by the consecutive failure count. 0 disables the backoff |
| BlockFailureBackoffMaxDuration | time.Duration | 150s | legacy_blockFailureBackoffMaxDuration | Cap on that backoff window, and with PeerProcessingTimeout the TTL of the failure-tracking map. 0 or less disables the backoff entirely |
| BlockFailureAttemptCeiling | int | 20 | legacy_blockFailureAttemptCeiling | Consecutive failures after which a block is given up on for the life of the process. 0 disables the ceiling |
| BlockDownloadTimeoutBasePercent | int64 | 100 | legacy_blockDownloadTimeoutBasePercent | Ceiling on one block download at the chain tip, as a percentage of the target block interval. Floored at 30 minutes, so values at or below 300 change nothing on a 10-minute chain |
| BlockDownloadTimeoutBaseIBDPercent | int64 | 600 | legacy_blockDownloadTimeoutBaseIBDPercent | The same ceiling while catching up. Also floored at 30 minutes, which the 600 default clears on a 10-minute chain |
| BlockDownloadTimeoutPerPeerPercent | int64 | 50 | legacy_blockDownloadTimeoutPerPeerPercent | Extra ceiling per other peer with a block download outstanding. The total is floored at 30 minutes, so this only adds patience |
| BlockPrefetchBufferBytes | int64 | 268435456 | legacy_blockPrefetchBufferBytes | On/off switch for asynchronous block admission. 0 disables it. The byte value itself is used only when the block park is off; with the park on the budget is a count of block slots |
| MaxBlockParallelFetch | int | 2 | legacy_maxBlockParallelFetch | Loaded but read by no code. The frontier racer it configured was deleted; re-asking a quiet peer's block is now part of every assignment pass |
| BlockSlowFetchTimeout | time.Duration | 20s | legacy_blockSlowFetchTimeout | Loaded but read by no code, for the same reason as MaxBlockParallelFetch |
| MultiPeerBlockDownload | bool | true | legacy_multiPeerBlockDownload | Spread block requests over every eligible peer. False assigns them all to the sync peer, disconnects a stalled sync peer instead of demoting it, and ignores notfound |
| MaxBlocksInTransitPerPeer | int | 16 | legacy_maxBlocksInTransitPerPeer | Block bodies one peer may owe at once. The block-size ladder lowers it further for large blocks. Also sizes the pipeline's admission budget, at four slots per peer |
| BlockDownloadWindow | int | 1024 | legacy_blockDownloadWindow | Block bodies the whole node may have outstanding, counting every peer together. A count, not svnode's per-peer height range |
| BlockDownloadLowerWindow | int | 128 | legacy_blockDownloadLowerWindow | How far above the committed tip a block may be asked for, scaled down by the block-size ladder and clamped to BlockDownloadWindow. 0 leaves BlockDownloadWindow as the only bound |
| ParkOutOfOrderBlocks | bool | true | legacy_parkOutOfOrderBlocks | Write a block whose parent is not committed yet to the temp store and commit it when the parent lands, instead of discarding it |
| ParkStoreTimeout | time.Duration | 10s | legacy_parkStoreTimeout | Deadline on each park blob store operation, bounding the wait for the file store's shared permits. Values below 1s are raised to 1s |
| ParkWorkers | int | 2 | legacy_parkWorkers | Workers that check and write parked blocks, keeping that work off the in-order commit goroutine. 0 or less becomes 1 |
| PeerRegistryEnabled | bool | true | legacy_peerRegistryEnabled | Mirror connected legacy peers into the centralized peer registry so the dashboard can show them |
| PeerRegistrySyncInterval | time.Duration | 10s | legacy_peerRegistrySyncInterval | How often that mirror reconciles connected legacy peers into the registry |

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

### Batch Processing Performance

- `StoreBatcherSize` multiplied by `StoreBatcherConcurrency` is the limit on
  concurrent UTXO create requests while a block is stored. `SpendBatcherSize` and
  `SpendBatcherConcurrency` bound spends the same way.
- `OutpointBatcherSize` bounds the goroutines that populate transaction inputs.
  `OutpointBatcherConcurrency` is read by nothing.

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

- Two bounds act on every assignment pass, and they measure different things.
  `BlockDownloadWindow` and `MaxBlocksInTransitPerPeer` bound how many block
  bodies are outstanding. `BlockDownloadLowerWindow` bounds how far above the
  committed tip they reach, which is what decides how much disk the park needs.
- `BlockDownloadLowerWindow` is scaled by the block-size ladder the node derives
  from its rolling average block size, so a configured 128 means fewer blocks in a
  large-block era. It is clamped to `BlockDownloadWindow`, because a limit looser
  than the node-wide window could never bind.
- With `BlockDownloadLowerWindow` at 0 there is no read-ahead limit, and the range
  of blocks the node names is bounded by `BlockDownloadWindow` instead.
- Blocks are chosen by a pass over that range: drop what is already on disk, in
  the park, given up on, inside its failure backoff or already owed, then hand the
  rest to peers with budget. There is no header list and no download cursor, so
  nothing carries a position between passes.
- A block whose owner has gone quiet for longer than the 60-second retry window is
  offered to another peer on the next pass. That is the general rule, which is why
  `MaxBlockParallelFetch` and `BlockSlowFetchTimeout` no longer do anything.
- `MultiPeerBlockDownload` set to false keeps the pass but gives every block to
  the sync peer, bounded by the block-size ladder alone.

### Block Prefetch

- `BlockPrefetchBufferBytes` at 0 disables asynchronous admission: one block is in
  flight at a time and the per-message watchdog is armed for block messages again.
  Prefetch is also never active on regtest, whatever the value.
- With the block park enabled, which is the default, the value in bytes is not
  used. The block's bytes have already been streamed to files by the time
  admission runs, so each block charges one slot against a budget sized as
  `MaxBlocksInTransitPerPeer` times four. At the defaults that is 64 blocks.
- With the park disabled the budget is bytes, bounding the total serialized size
  of received-but-not-yet-processed blocks across all peers. A block at least as
  large as the whole budget is admitted alone, giving no overlap; to get overlap
  on large blocks the budget has to hold more than one of them.

### Out-of-Order Block Park

- The park needs a temp store it can enumerate after a restart, so it turns itself
  off with a warning on any `temp_store` URL that is not a `file://` store, and on
  a node with no temp store at all.
- `ParkWorkers` is a memory decision. A worker holds a block for the length of its
  write, so more workers mean more blocks in flight at once.
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
| ParkOutOfOrderBlocks | Needs a file:// temp store that can be scanned on restart | The park switches itself off with a warning and out-of-order blocks are discarded | During sync manager construction |
| ParkStoreTimeout | Raised to 1s if lower | A zero deadline would fail every store operation instantly | During sync manager construction |
| ParkWorkers | Raised to 1 if 0 or less | The pool always has at least one worker | During sync manager construction |
| BlockDownloadLowerWindow | Clamped to BlockDownloadWindow; 0 or less means no read-ahead limit | Decides how far above the committed tip blocks are fetched | On every assignment pass |
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

### Performance Tuning

```text
legacy_storeBatcherSize = 2048
legacy_storeBatcherConcurrency = 64
legacy_spendBatcherSize = 2048
legacy_spendBatcherConcurrency = 64
```

### Bounding Read-Ahead

```text
legacy_blockDownloadLowerWindow = 32
legacy_maxBlocksInTransitPerPeer = 8
```

Lower values keep the park small and the node's memory footprint down, at the
cost of a sync more exposed to one slow peer.
