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
| DHTMode | string | "server" | p2p_dht_mode | DHT operation mode ("server" or "client") |
| DHTCleanupInterval | time.Duration | 24h | p2p_dht_cleanup_interval | DHT provider record cleanup interval |
| PeerMapMaxSize | int | 100000 | p2p_peer_map_max_size | Maximum entries in peer maps |
| PeerMapTTL | time.Duration | 30m | p2p_peer_map_ttl | Time-to-live for peer map entries |
| PeerMapCleanupInterval | time.Duration | 5m | p2p_peer_map_cleanup_interval | Peer map cleanup interval |
| EnableNAT | bool | false | p2p_enable_nat | Enable UPnP/NAT-PMP port mapping |
| EnableMDNS | bool | false | p2p_enable_mdns | Enable mDNS peer discovery |
| AllowPrivateIPs | bool | false | p2p_allow_private_ips | Allow connections to private IP addresses |
| SyncCoordinatorPeriodicEvaluationInterval | time.Duration | - | p2p_sync_coordinator_periodic_evaluation_interval | Sync coordinator evaluation interval |

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

### DHT Configuration

The DHT (Distributed Hash Table) can operate in two modes:

```text
# Server mode (default) - advertises on DHT and stores provider records
p2p_dht_mode = "server"
p2p_dht_cleanup_interval = 24h

# Client mode - query-only, no provider storage (reduces network overhead)
p2p_dht_mode = "client"
```

**When to use client mode:**

- Nodes that don't need to be discoverable by others
- Reduced network overhead and storage requirements
- Behind restrictive NAT/firewall

### Peer Registry Configuration

The peer registry persists peer reputation data across restarts:

```text
# Directory for peer cache file (default: binary directory)
p2p_peer_cache_dir = "/var/lib/teranode/p2p"

# Peer map memory management
p2p_peer_map_max_size = 100000
p2p_peer_map_ttl = 30m
p2p_peer_map_cleanup_interval = 5m
```

### Network Security Configuration

**IMPORTANT**: These settings can trigger network scanning alerts on shared hosting.

```text
# Enable only on private/local networks
p2p_enable_nat = false      # UPnP/NAT-PMP port mapping
p2p_enable_mdns = false     # mDNS peer discovery
p2p_allow_private_ips = false  # RFC1918 private networks
```

### Peer Selection and Reputation

For details on how peer selection and reputation scoring work, see [Peer Registry and Reputation System](../../../topics/features/peer_registry_reputation.md).

Key settings affecting peer selection:

- `p2p_force_sync_peer` - Override automatic selection with specific peer
- `p2p_allow_pruned_node_fallback` - Whether to fall back to pruned nodes
- `p2p_peer_cache_dir` - Where peer reputation data is persisted
