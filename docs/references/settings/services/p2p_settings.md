# P2P Service Settings

**Related Topic**: [P2P Service](../../../topics/services/p2p.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| BootstrapPeers | []string | [] (settings.conf ships with `/dnsaddr/${network}.bootstrap.teranode.bsvb.tech`) | p2p_bootstrap_peers | Peer discovery entry points (required for dht_mode "off" and "client") |
| GRPCAddress | string | "" | p2p_grpcAddress | gRPC client connections |
| GRPCListenAddress | string | "localhost:9906" (Go default; overridden to `localhost:9904` by `settings.conf` via `P2P_GRPC_PORT`, and widened to `:9904` in the `docker.m`, `docker.ss` and `operator` contexts, plus the generated split-mode compose contexts) | p2p_grpcListenAddress | **CRITICAL** - gRPC server binding; loopback by default |
| HTTPAddress | string | "localhost:9906" | p2p_httpAddress | HTTP client connections |
| HTTPListenAddress | string | "" | p2p_httpListenAddress | HTTP server binding |
| ListenAddresses | []string | [] | p2p_listen_addresses | P2P network interfaces |
| AdvertiseAddresses | []string | [] | p2p_advertise_addresses | Address advertisement to peers |
| ListenMode | string | "full" | listen_mode | Node operation mode ("full" or "listen_only") |
| PeerID | string | "" | p2p_peer_id | Peer network identifier |
| Port | int | 9905 | p2p_port | Default P2P communication port (multiaddrs in ListenAddresses/AdvertiseAddresses are the source of truth) |
| PrivateKey | string | "" | p2p_private_key | **CRITICAL** - Cryptographic peer identity |
| BlockTopic | string | "" | p2p_block_topic | Block propagation topic |
| NodeStatusTopic | string | "" | p2p_node_status_topic | Node status communication topic |
| RejectedTxTopic | string | "" | p2p_rejected_tx_topic | Rejected transaction topic |
| SubtreeTopic | string | "" | p2p_subtree_topic | Subtree propagation topic |
| StaticPeers | []string | [] | p2p_static_peers | Forced peer connections |
| PeerCacheDir | string | "" | p2p_peer_cache_dir | Peer cache directory |
| BanThreshold | int | 100 | p2p_ban_threshold | Peer banning threshold |
| BanDuration | time.Duration | 24h | p2p_ban_duration | Ban duration |
| ForceSyncPeer | string | "" | p2p_force_sync_peer | **CRITICAL** - Forced sync peer override |
| SharePrivateAddresses | bool | true | p2p_share_private_addresses | Private address advertisement |
| AllowPrunedNodeFallback | bool | true | p2p_allow_pruned_node_fallback | **CRITICAL** - Pruned node fallback behavior |
| DHTMode | string | "server" (Code default; settings.conf ships with "off") | p2p_dht_mode | DHT operation mode ("server", "client", or "off") |
| DHTCleanupInterval | time.Duration | 24h | p2p_dht_cleanup_interval | DHT provider record cleanup interval |
| EnableNAT | bool | false | p2p_enable_nat | **CRITICAL** - UPnP/NAT-PMP port mapping (triggers network scanning) |
| EnableMDNS | bool | false | p2p_enable_mdns | **CRITICAL** - mDNS peer discovery (triggers network scanning) |
| AllowPrivateIPs | bool | false | p2p_allow_private_ips | **CRITICAL** - Allow RFC1918 private IP connections |
| EnablePeerScoring | bool | true | p2p_enable_peer_scoring | **CRITICAL** - GossipSub peer scoring (Sybil mesh protection); static/bootstrap peers exempt |
| EnablePeerExchange | bool | true | p2p_enable_peer_exchange | GossipSub peer exchange (PX); requires EnablePeerScoring (startup error otherwise); inbound PX records currently refused |
| PeerScoreIPColocationThreshold | int | 10 | p2p_peer_score_ip_colocation_threshold | Peers allowed per exact IP before the colocation penalty applies |
| SyncCoordinatorPeriodicEvaluationInterval | time.Duration | 30s | p2p_sync_coordinator_periodic_evaluation_interval | Sync coordinator evaluation interval |
| HealthCheckEnabled | bool | true | p2p_health_check_enabled | Enable HTTP availability checking during peer selection |
| PeerMapMaxSize | int | 10000 | p2p_peer_map_max_size | Maximum entries in peer maps |
| PeerMapTTL | time.Duration | 10m | p2p_peer_map_ttl | Peer map entry time-to-live |
| PeerMapCleanupInterval | time.Duration | 1m | p2p_peer_map_cleanup_interval | Peer map cleanup frequency |
| PeerRegistryBatchInterval | time.Duration | 1s | p2p_peer_registry_batch_interval | Flush interval for batched peer-registry updates from gossip handlers |
| GossipHandlerConcurrency | int | 4 | p2p_gossip_handler_concurrency | Concurrent gossip handler workers per pubsub topic |
| WebSocketMaxConnections | int | 1000 | p2p_websocket_max_connections | Maximum concurrent /p2p-ws websocket connections (0 disables the cap) |
| WebSocketMaxConnectionsPerSource | int | 0 | p2p_websocket_max_connections_per_source | Per-source /p2p-ws cap: 0 = auto (max(4, cap/20)), -1 disables (needed behind a proxy/NAT) |
| WebSocketAllowedOrigins | []string | (empty) | p2p_websocket_allowed_origins | Allowed browser origins for /p2p-ws upgrades and HTTP CORS (empty allows all) |
| WebSocketTrustedSourceCIDRs | []string | 127.0.0.1/32\|::1/128 | p2p_websocket_trusted_source_cidrs | Source CIDRs exempt from the /p2p-ws connection caps; loopback only by design - broader trust would void the caps behind an L7 ingress or NAT (see longdesc). Sentinel `none` disables the bypass (empty falls back to the default) |

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
- `PeerCacheDir` for peer persistence
- When peer scoring is enabled, static and bootstrap peers become GossipSub *direct peers*: exempt from scoring, protected from connection-manager trimming, never grafted into the mesh (messages flow to them outside it). Static peer lists should be reciprocal (both sides list each other) or messages from the unlisted side arrive only via gossip pull.

### GossipSub Mesh Protection

- `EnablePeerScoring` (default true) applies penalty-only scoring: an IP-colocation penalty, a behaviour penalty (GRAFT/PRUNE flooding, broken IWANT promises), and a PX acceptance gate. It raises the cost of Sybil mesh capture; it does not award positive score.
- The IP-colocation penalty is applied **by each remote peer** that holds more than `PeerScoreIPColocationThreshold` (default 10) connections from the same source IP. An operator running more nodes than that behind one public IP (NAT, single cloud egress) is penalized by those remote peers, and **no local setting on the operator's nodes changes that** - disabling scoring locally only stops this node scoring others. Real mitigations: distinct public IPs, or reciprocal `StaticPeers` entries with the specific peers involved (direct peers are scoring-exempt). Note the exposure is per-observer: a remote peer holding only a few connections into the colocated set applies no penalty.
- `PeerScoreIPColocationThreshold` tunes the local penalty without disabling scoring (lower it on networks with no legitimately colocated operators).
- With `AllowPrivateIPs` true, loopback, RFC1918, RFC6598, link-local, and IPv6 ULA ranges are whitelisted from the colocation penalty, so local/test multi-node clusters are unaffected.
- `EnablePeerExchange` (default true) controls emitting PX records in PRUNE messages, and **requires** `EnablePeerScoring` (gossipsub v1.1 pairs PX with scoring) - the service refuses to start with PX on and scoring off. Inbound PX records are currently refused regardless (`AcceptPXThreshold` is set above the maximum attainable score): penalty-only scoring caps every score at 0, so accepting 0-scored records would let an attacker get itself dialed, and dialed peers register as outbound - bypassing gossipsub's Dhi graft refusal and Dout quota. This will be relaxed when positive (per-topic delivery) scoring exists.

### Peer Map Management

- `PeerMapMaxSize` limits memory usage for block/subtree peer tracking (10000 entries, enforced at insert)
- `PeerMapTTL` controls peer map entry expiration (10 minutes)
- `PeerMapCleanupInterval` sets cleanup frequency (1 minute)
- These settings have sensible defaults but can be overridden via environment variables

### Network Scanning Prevention

- `EnableNAT` triggers UPnP gateway scans (disable on shared hosting)
- `EnableMDNS` triggers LAN broadcasts (disable on shared hosting)
- `AllowPrivateIPs` allows RFC1918 connections (disable for production)

## Service Dependencies

| Dependency | Interface | Usage |
|------------|-----------|-------|
| BlockchainClient | blockchain.ClientI | **CRITICAL** - Blockchain operations and block retrieval |
| BlockAssemblyClient | blockassembly.ClientI | **CRITICAL** - Block assembly operations |
| RejectedTxKafkaConsumer | kafka.KafkaConsumerGroupI | **CRITICAL** - Consuming rejected transaction notifications |
| InvalidBlocksKafkaConsumer | kafka.KafkaConsumerGroupI | **CRITICAL** - Consuming invalid block notifications |
| InvalidSubtreeKafkaConsumer | kafka.KafkaConsumerGroupI | **CRITICAL** - Consuming invalid subtree notifications |
| BlocksKafkaProducer | kafka.KafkaAsyncProducerI | **CRITICAL** - Publishing block announcements |
| SubtreeKafkaProducer | kafka.KafkaAsyncProducerI | **CRITICAL** - Publishing subtree announcements |

## Validation Rules

| Setting | Validation | Impact | When Checked |
|---------|------------|--------|-------------|
| ListenMode | Must be "full" or "listen_only" | Service fails to start if invalid | During server initialization |
| GRPCListenAddress | Used for gRPC server binding | Service communication | During server start |
| ForceSyncPeer | Overrides automatic peer selection | Sync behavior | During sync coordinator initialization |
| EnableNAT | Triggers network scanning on shared hosting | Security alerts | During libp2p host initialization |
| EnableMDNS | Triggers network scanning on shared hosting | Security alerts | During libp2p host initialization |

## Configuration Examples

### Basic Configuration

```bash
# Note: settings.conf sets P2P_GRPC_PORT=9904, overriding the Go default port of 9906.
# The bind is loopback by default; widen it only when the P2P service is reached from
# another container or pod, and set a strong grpc_admin_api_key when you do.
p2p_grpcListenAddress=localhost:9904
p2p_port=9905
listen_mode=full
```

### Forced Sync Configuration

```bash
p2p_force_sync_peer=peer-id-12345
p2p_allow_pruned_node_fallback=true
p2p_sync_coordinator_periodic_evaluation_interval=30s
```

### DHT Configuration

The DHT (Distributed Hash Table) can operate in three modes:

```bash
# Server mode - full DHT participation: advertises on DHT, stores provider
# records, routes queries for other peers
p2p_dht_mode=server
p2p_dht_cleanup_interval=24h

# Client mode - query-only, no provider storage (reduces network overhead)
p2p_dht_mode=client

# Off - no DHT at all; topic-only network (settings.conf default)
p2p_dht_mode=off
```

**When to use server mode:**

- Publicly dialable nodes that should strengthen the network with direct connections
- Required for inclusion in the BSV Association DNS bootstrap pool (contact BSVA)
- Highest resource usage; pair with explicit `p2p_advertise_addresses` — see [How to Expose the P2P Service Publicly (Kubernetes)](../../../howto/miners/kubernetes/minersHowToExposeP2P.md)

**When to use client mode:**

- Nodes that don't need to be discoverable by others
- Reduced network overhead and storage requirements
- Behind restrictive NAT/firewall

**When to use off:**

- Most lightweight: no DHT crawling, no random connections; peer addresses come from bootstrap servers
- **Recommended for abuse-sensitive hosting providers (e.g. Hetzner, OVH)** — `server` and `client` modes connect to 100+ semi-random IPs, which these providers may flag as network scanning and answer with abuse warnings or suspension
- The node still participates fully in block and transaction propagation via topics

### Peer Registry Configuration

The peer registry persists peer reputation data across restarts:

```bash
# Directory for peer cache file (default: binary directory)
p2p_peer_cache_dir=/var/lib/teranode/p2p
```

### Network Security Configuration

**IMPORTANT**: These settings can trigger network scanning alerts on shared hosting.

```bash
# Enable only on private/local networks
p2p_enable_nat=false      # UPnP/NAT-PMP port mapping
p2p_enable_mdns=false     # mDNS peer discovery
p2p_allow_private_ips=false  # RFC1918 private networks
```

`p2p_allow_private_ips` also governs the static SSRF check on peer-supplied DataHub URLs:
with `true` that check is skipped entirely, so an announced URL naming a private, loopback or
link-local address is accepted into the peer registry.

It does **not** affect the connection-time guard. Every outbound request to a peer-supplied
URL - availability probes and block/subtree fetches alike - refuses loopback (127.0.0.0/8,
::1), link-local (169.254.0.0/16, fe80::/10) and unspecified addresses regardless of this
setting, including when a peer hostname only resolves to one. Accepting such a URL therefore
does not make it reachable. Private ranges are permitted at connection time on both paths,
since peer fetches legitimately traverse private networks; the probe deliberately applies the
same policy as the fetch path, so it never rejects a peer that catchup could have used.

One caveat: if `HTTP_PROXY`/`HTTPS_PROXY` is set, outbound requests are dialled to the proxy
and the proxy fetches the peer-supplied target on the node's behalf, which the address check
cannot see. Deployments that need these checks to hold must not route peer fetches through a
forward proxy.

### Peer Selection and Reputation

For details on how peer selection and reputation scoring work, see [Peer Registry and Reputation System](../../../topics/features/peer_registry_reputation.md).

Key settings affecting peer selection:

- `p2p_force_sync_peer` - Override automatic selection with specific peer
- `p2p_allow_pruned_node_fallback` - Whether to fall back to pruned nodes
- `p2p_peer_cache_dir` - Where peer reputation data is persisted
