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
| Upnp | bool | false | legacy_upnp | Enable UPnP for automatic port mapping |

## Configuration Dependencies

### Peer Connection Management

- `ListenAddresses` controls incoming connections (falls back to external IP:8333 if empty)
- `ConnectPeers` forces outgoing connections to specific peers
- When `ConnectPeers` is set, `MaxPeers` automatically set to match count (exclusive mode)
- `ConnectPeers` disables DNS seeding
- `SavePeers` controls peer information persistence to disk

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
