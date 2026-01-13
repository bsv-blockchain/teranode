# P2P Service Settings

**Related Topic**: [P2P Service](../../../topics/services/p2p.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| BootstrapAddresses | []string | [] | p2p_bootstrapAddresses | DHT bootstrapping entry points |
| GRPCAddress | string | "" | p2p_grpcAddress | gRPC client connections |
| GRPCListenAddress | string | ":9906" | p2p_grpcListenAddress | **CRITICAL** - gRPC server binding |
| HTTPAddress | string | "localhost:9906" | p2p_httpAddress | HTTP client connections |
| HTTPListenAddress | string | "" | p2p_httpListenAddress | HTTP server binding |
| ListenAddresses | []string | [] | p2p_listen_addresses | P2P network interfaces |
| AdvertiseAddresses | []string | [] | p2p_advertise_addresses | Address advertisement to peers |
| ListenMode | string | "full" | listen_mode | Node operation mode |
| PeerID | string | "" | p2p_peer_id | Peer network identifier |
| Port | int | 9906 | p2p_port | Default P2P communication port |
| PrivateKey | string | "" | p2p_private_key | **CRITICAL** - Cryptographic peer identity |
| BlockTopic | string | "" | p2p_block_topic | Block propagation topic |
| NodeStatusTopic | string | "" | p2p_node_status_topic | Node status communication topic |
| RejectedTxTopic | string | "" | p2p_rejected_tx_topic | Rejected transaction topic |
| SubtreeTopic | string | "" | p2p_subtree_topic | Subtree propagation topic |
| StaticPeers | []string | [] | p2p_static_peers | Forced peer connections |
| RelayPeers | []string | [] | p2p_relay_peers | NAT traversal relay peers |
| PeerCacheDir | string | "" | p2p_peer_cache_dir | Peer cache directory |
| BanThreshold | int | 100 | p2p_ban_threshold | Peer banning threshold |
| BanDuration | time.Duration | 24h | p2p_ban_duration | Ban duration |
| ForceSyncPeer | string | "" | p2p_force_sync_peer | **CRITICAL** - Forced sync peer override |
| SharePrivateAddresses | bool | true | p2p_share_private_addresses | Private address advertisement |
| AllowPrunedNodeFallback | bool | true | p2p_allow_pruned_node_fallback | **CRITICAL** - Pruned node fallback behavior |

## Configuration Dependencies

### Forced Sync Peer Selection
- `ForceSyncPeer` overrides automatic peer selection
- `AllowPrunedNodeFallback` affects fallback behavior when forced peer unavailable

### Network Address Management
- `ListenAddresses` and `AdvertiseAddresses` control network presence
- `Port` used as fallback when addresses don't specify port
- `SharePrivateAddresses` controls address advertisement behavior

### Peer Connection Management
- `StaticPeers` ensures persistent connections
- `BootstrapAddresses` for initial network discovery
- `RelayPeers` for NAT traversal
- `PeerCacheDir` for peer persistence

## Service Dependencies

| Dependency | Interface | Usage |
|------------|-----------|-------|
| BlockchainClient | blockchain.ClientI | **CRITICAL** - Blockchain operations and block retrieval |
| BlockValidationClient | blockvalidation.Interface | **CRITICAL** - Block validation operations |
| BlockAssemblyClient | blockassembly.ClientI | **CRITICAL** - Block assembly operations |
| RejectedTxKafkaProducer | kafka.KafkaAsyncProducerI | **CRITICAL** - Rejected transaction messaging |
| BlocksKafkaProducer | kafka.KafkaAsyncProducerI | **CRITICAL** - Block messaging |
| SubtreeKafkaProducer | kafka.KafkaAsyncProducerI | **CRITICAL** - Subtree messaging |

## Validation Rules

| Setting | Validation | Impact |
|---------|------------|--------|
| GRPCListenAddress | Used for gRPC server binding | Service communication |
| ForceSyncPeer | Overrides automatic peer selection | Sync behavior |
| PeerHealthCheckInterval | Must be positive for health checks | Peer monitoring |

## Configuration Examples

### Basic Configuration

```text
p2p_grpcListenAddress = ":9906"
p2p_port = 9906
listen_mode = "full"
```

### Peer Health Monitoring

```text
p2p_health_check_interval = 30s
p2p_health_http_timeout = 5s
p2p_health_remove_after_failures = 3
```

### Forced Sync Configuration

```text
p2p_force_sync_peer = "peer-id-12345"
p2p_allow_pruned_node_fallback = true
```
