# Legacy Service Settings

**Related Topic**: [Legacy Service](../../../topics/services/legacy.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| WorkingDir | string | "../../data" | legacy_workingDir | Data storage directory |
| ListenAddresses | []string | [] | legacy_listen_addresses | **CRITICAL** - Network interfaces for peer connections |
| ConnectPeers | []string | [] | legacy_connect_peers | Forced peer connections |
| OrphanEvictionDuration | time.Duration | 10m | legacy_orphanEvictionDuration | Orphan transaction retention |
| StoreBatcherSize | int | 1024 | legacy_storeBatcherSize | **CRITICAL** - Store operation batch size |
| StoreBatcherConcurrency | int | 32 | legacy_storeBatcherConcurrency | **CRITICAL** - Store operation parallelism |
| SpendBatcherSize | int | 1024 | legacy_spendBatcherSize | **CRITICAL** - Spend operation batch size |
| SpendBatcherConcurrency | int | 32 | legacy_spendBatcherConcurrency | **CRITICAL** - Spend operation parallelism |
| OutpointBatcherSize | int | 1024 | legacy_outpointBatcherSize | **CRITICAL** - Outpoint operation batch size |
| OutpointBatcherConcurrency | int | 32 | legacy_outpointBatcherConcurrency | Outpoint operation parallelism |
| PrintInvMessages | bool | false | legacy_printInvMessages | Debug logging for inventory messages |
| GRPCAddress | string | "" | legacy_grpcAddress | **CRITICAL** - gRPC client connections (required for client, returns error if empty) |
| AllowBlockPriority | bool | false | legacy_allowBlockPriority | Block priority handling |
| GRPCListenAddress | string | "" | legacy_grpcListenAddress | gRPC server binding |
| SavePeers | bool | false | legacy_savePeers | Peer information persistence |
| AllowSyncCandidateFromLocalPeers | bool | false | legacy_allowSyncCandidateFromLocalPeers | **CRITICAL** - Local peer sync candidate selection |
| TempStore | *url.URL | "file://./data/tempstore" | temp_store | **CRITICAL** - Temporary storage location |
| PeerIdleTimeout | time.Duration | 125s | legacy_peerIdleTimeout | **CRITICAL** - Peer inactivity timeout |
| PeerProcessingTimeout | time.Duration | 3m | legacy_peerProcessingTimeout | **CRITICAL** - Message processing timeout |
| BlockFailureBackoffBase | time.Duration | 5s | legacy_blockFailureBackoffBase | Base per-block backoff after a transient storage/service failure (0 disables) |
| BlockFailureBackoffMaxDuration | time.Duration | 150s | legacy_blockFailureBackoffMaxDuration | Cap on the per-block backoff window and the failure-tracking map TTL, kept below the 180s sync-peer stall window (0 disables) |
| BlockPrefetchBufferBytes | int64 | 268435456 | legacy_blockPrefetchBufferBytes | Byte budget for blocks downloaded ahead of processing during sync (0 disables prefetch) |
| Upnp | bool | false | legacy_upnp | Enable UPnP for automatic port mapping |
| MaxFeelerPeers | int | 1 | legacy_maxFeelerPeers | Peer slots reserved for short-lived feeler probes (0 disables feelers and the reservation together) |
| FeelerInterval | time.Duration | 120s | legacy_feelerInterval | Mean of the randomised gap between feeler probes (not a disable lever; a non-positive value falls back to the default) |
| FeelerHandshakeTimeout | time.Duration | 25s | legacy_feelerHandshakeTimeout | How long a feeler waits for a version message; must stay under the 30s peer negotiate timeout |
| PeerRegistryEnabled | bool | true | legacy_peerRegistryEnabled | Mirror connected legacy peers into the centralized peer registry so the dashboard can show them (false is a kill switch) |
| PeerRegistrySyncInterval | time.Duration | 10s | legacy_peerRegistrySyncInterval | How often the mirror reconciles connected legacy peers into the registry |

## Configuration Dependencies

### Peer Connection Management

- `ListenAddresses` controls incoming connections (falls back to external IP:8333 if empty)
- `ConnectPeers` forces outgoing connections to specific peers
- When `ConnectPeers` is set, `MaxPeers` automatically set to match count (exclusive mode)
- `ConnectPeers` disables DNS seeding
- `SavePeers` controls peer information persistence to disk

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

- Batch sizes and concurrency settings work together for memory and performance control
- `StoreBatcherSize` * `StoreBatcherConcurrency` limits concurrent requests

### Peer Timeout Management

- `PeerIdleTimeout` set to 125s to accommodate 2-minute ping/pong intervals
- `PeerProcessingTimeout` set to 3m for block processing (largest operations)

### Sync Candidate Selection

- When `AllowSyncCandidateFromLocalPeers = false`, only non-localhost peers can be sync candidates
- Prevents local peers from being selected as blockchain sync source

### Block Priority

- `AllowBlockPriority = true`: Enables block priority messages via connection streaming
- Sent via Protoconf message during peer handshake

### Block Prefetch

- `BlockPrefetchBufferBytes` bounds the bytes of received-but-not-yet-processed blocks so download overlaps validation during sync; `0` disables prefetch (synchronous ingestion).
- Big-block era: a block at least as large as the whole budget is admitted alone (weight clamped), giving zero overlap — identical to pre-prefetch behaviour. To get overlap on large blocks, set the budget to at least ~2× the typical block size.

### Peer Registry Mirror

**PeerRegistryEnabled** and **PeerRegistrySyncInterval** control the mirror that
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

**Requirements**

- The blockchain service must be reachable, since it hosts the registry. When
  the registry client is unavailable the mirror does not start and the legacy
  service runs normally without dashboard peer visibility.

**Recommendations**

- Keep **PeerRegistryEnabled** at true for operator visibility. Set it to false
  only if the extra registry traffic is unwelcome on a node carrying very many
  legacy connections.
- The default 10s interval sits well below the legacy two-minute ping interval.
  Values under one second are pointless, because the underlying peer statistics
  do not change that fast.

## Service Dependencies

| Dependency | Interface | Usage |
|------------|-----------|-------|
| SubtreeStore | blob.Store | **CRITICAL** - Merkle subtree storage and verification |
| TempStore | blob.Store | **CRITICAL** - Temporary data storage during processing |
| UTXOStore | utxo.Store | **CRITICAL** - UTXO operations |
| BlockchainClient | blockchain.ClientI | **CRITICAL** - Blockchain operations and state queries |
| ValidatorClient | validator.Interface | **CRITICAL** - Transaction validation |
| SubtreeValidationClient | subtreevalidation.ClientI | **CRITICAL** - Subtree validation |
| BlockValidationClient | blockvalidation.ClientI | **CRITICAL** - Block validation |
| BlockAssemblyClient | blockassembly.ClientI | **CRITICAL** - Block assembly operations |

## Validation Rules

| Setting | Validation | Impact | When Checked |
|---------|------------|--------|-------------|
| GRPCAddress | Must not be empty | Client creation fails | During client initialization |
| ListenAddresses | Falls back to external IP:8333 if empty | Network connectivity | During server start |
| PeerIdleTimeout | Must accommodate ping/pong intervals | Peer stability | During peer connection |
| PeerProcessingTimeout | Must allow for block processing time | Message handling | During message processing |

## Configuration Examples

### Basic Configuration

```text
legacy_listen_addresses = "0.0.0.0:8333"
legacy_savePeers = false
```

### Forced Peer Connections

```text
legacy_connect_peers = "peer1.example.com:8333|peer2.example.com:8333"
legacy_allowSyncCandidateFromLocalPeers = false
```

### Performance Tuning

```text
legacy_storeBatcherSize = 2048
legacy_storeBatcherConcurrency = 64
legacy_spendBatcherSize = 2048
legacy_spendBatcherConcurrency = 64
```
