# P2P Service Settings

**Related Topic**: [P2P Service](../../../topics/services/p2p.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| AllowedPublisherIDs | []string | [] | p2p_allowed_publisher_ids | Opt-in allowlist of peer IDs whose pubsub messages this node acts on; empty (default) accepts all. Bare peer IDs, not multiaddrs. Applies to every subscribed topic, so filtered peers also stop refreshing the peer registry and the monitoring feed |
| BootstrapPeers | []string | [] (settings.conf ships with `/dnsaddr/${network}.bootstrap.teranode.bsvb.tech`) | p2p_bootstrap_peers | Peer discovery entry points (required for dht_mode "off" and "client") |
| GRPCAddress | string | "" | p2p_grpcAddress | gRPC client connections |
| GRPCListenAddress | string | "localhost:9906" (Go default; overridden to `localhost:9904` by `settings.conf` via `P2P_GRPC_PORT`, and widened to `:9904` in the `docker.m`, `docker.ss` and `operator` contexts, plus the generated split-mode compose contexts) | p2p_grpcListenAddress | **CRITICAL** - gRPC server binding; loopback by default |
| HTTPAddress | string | "localhost:9906" | p2p_httpAddress | HTTP client connections |
| HTTPListenAddress | string | "" | p2p_httpListenAddress | HTTP server binding |
| ListenAddresses | []string | [] | p2p_listen_addresses | **REQUIRED** - Must describe the wildcard bind the bus performs (`0.0.0.0`, `::`, or `/ip4/0.0.0.0/tcp/<p2p_port>`); narrower values fail startup |
| AdvertiseAddresses | []string | [] | p2p_advertise_addresses | Address advertisement to peers |
| ListenMode | string | "full" | listen_mode | Node operation mode ("full", "listen_only" or "silent"; silent still binds p2p_port) |
| PeerID | string | "" | p2p_peer_id | Peer network identifier |
| Port | int | 9905 | p2p_port | TCP port libp2p binds on 0.0.0.0 and :: (always, independent of AdvertiseAddresses) |
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
| SharePrivateAddresses | bool | true | p2p_share_private_addresses | Currently no effect: libp2p advertises bound interface addresses (private included) whenever AdvertiseAddresses is empty |
| AllowPrunedNodeFallback | bool | true | p2p_allow_pruned_node_fallback | **CRITICAL** - Pruned node fallback behavior |
| DHTMode | string | "server" (Code default; settings.conf ships with "off") | p2p_dht_mode | DHT operation mode ("server", "client", or "off") |
| DHTCleanupInterval | time.Duration | 24h | p2p_dht_cleanup_interval | DHT provider record cleanup interval |
| EnableNAT | bool | false | p2p_enable_nat | **CRITICAL** - UPnP/NAT-PMP port mapping (triggers network scanning) |
| EnableMDNS | bool | false | p2p_enable_mdns | **CRITICAL** - mDNS peer discovery (triggers network scanning) |
| AllowPrivateIPs | bool | false | p2p_allow_private_ips | **CRITICAL** - Allow RFC1918 private IP connections |
| EnablePeerScoring | bool | true | p2p_enable_peer_scoring | **CRITICAL** - GossipSub peer scoring (Sybil mesh protection); static/bootstrap peers exempt |
| EnablePeerExchange | bool | true | p2p_enable_peer_exchange | GossipSub peer exchange (PX); requires EnablePeerScoring (startup error otherwise); inbound PX records currently refused |
| PeerScoreIPColocationThreshold | int | 10 | p2p_peer_score_ip_colocation_threshold | Peers allowed per exact IP before the colocation penalty applies |
| MaxConnections | int | 0 | p2p_max_connections | Connection manager high water mark; 0 keeps the library default (35) |
| MinConnections | int | 0 | p2p_min_connections | Connection manager low water mark; 0 keeps the library default (25) |
| ConnectionGracePeriod | time.Duration | 0s | p2p_connection_grace_period | How long new connections are protected from pruning; 0 keeps the library default (20s) |
| RcmgrConns | int | 0 | p2p_rcmgr_conns | Resource manager system-wide connection cap; 0 keeps the auto-scaled default |
| RcmgrConnsInbound | int | 0 | p2p_rcmgr_conns_inbound | Resource manager system-wide inbound connection cap; 0 keeps the auto-scaled default |
| RcmgrConnsOutbound | int | 0 | p2p_rcmgr_conns_outbound | Resource manager system-wide outbound connection cap; 0 keeps the auto-scaled default |
| RcmgrStreams | int | 0 | p2p_rcmgr_streams | Resource manager system-wide stream cap; 0 keeps the auto-scaled default |
| RcmgrStreamsInbound | int | 0 | p2p_rcmgr_streams_inbound | Resource manager system-wide inbound stream cap; 0 keeps the auto-scaled default |
| RcmgrStreamsOutbound | int | 0 | p2p_rcmgr_streams_outbound | Resource manager system-wide outbound stream cap; 0 keeps the auto-scaled default |
| RcmgrFD | int | 0 | p2p_rcmgr_fd | Resource manager file-descriptor cap for connections; 0 keeps the auto-scaled default |
| RcmgrPeerConns | int | 0 | p2p_rcmgr_peer_conns | Connections to or from any single peer; 0 keeps the default |
| RcmgrPeerConnsInbound | int | 0 | p2p_rcmgr_peer_conns_inbound | Inbound connections from any single peer; 0 keeps the default |
| RcmgrPeerStreamsInbound | int | 0 | p2p_rcmgr_peer_streams_inbound | Inbound streams from any single peer; 0 keeps the default |
| RcmgrIPv4SubnetLimits | []string | (empty) | p2p_rcmgr_ipv4_subnet_limits | Per-IPv4-subnet connection caps as pipe-separated `prefix:conns` entries (e.g. `32:8\|24:32`); empty keeps the default of 8 per address |
| RcmgrIPv6SubnetLimits | []string | (empty) | p2p_rcmgr_ipv6_subnet_limits | Per-IPv6-subnet connection caps as pipe-separated `prefix:conns` entries (e.g. `56:8\|48:64`); empty keeps the defaults of 8 per /56 and 64 per /48 |
| SyncCoordinatorPeriodicEvaluationInterval | time.Duration | 30s | p2p_sync_coordinator_periodic_evaluation_interval | Sync coordinator evaluation interval |
| HealthCheckEnabled | bool | true | p2p_health_check_enabled | Enable HTTP availability checking during peer selection |
| PeerMapMaxSize | int | 10000 | p2p_peer_map_max_size | Maximum entries in the peer attribution maps and the per-peer reputation cache (bounded at insert) |
| PeerMapTTL | time.Duration | 10m | p2p_peer_map_ttl | Peer map entry time-to-live |
| PeerMapCleanupInterval | time.Duration | 1m | p2p_peer_map_cleanup_interval | Peer map cleanup frequency |
| SeenHashMaxSize | int | 10000 | p2p_seen_hash_max_size | Maximum entries in each per-topic seen-hash announcement dedup cache |
| SeenHashTTL | time.Duration | 2m | p2p_seen_hash_ttl | Accounting window for the seen-hash announcement dedup |
| SeenHashMaxPublishers | int | 3 | p2p_seen_hash_max_publishers | Distinct announcers of one hash forwarded to Kafka per publish window |
| PeerRegistryBatchInterval | time.Duration | 1s | p2p_peer_registry_batch_interval | Flush interval for batched peer-registry updates from gossip handlers |
| GossipHandlerConcurrency | int | 4 | p2p_gossip_handler_concurrency | Concurrent gossip handler workers per pubsub topic |
| WebSocketMaxConnections | int | 1000 | p2p_websocket_max_connections | Maximum concurrent /p2p-ws websocket connections (0 disables the cap) |
| WebSocketMaxConnectionsPerSource | int | 0 | p2p_websocket_max_connections_per_source | Per-source /p2p-ws cap: 0 = auto (max(4, cap/20)), -1 disables (needed behind a proxy/NAT) |
| WebSocketAllowedOrigins | []string | (empty) | p2p_websocket_allowed_origins | Allowed browser origins for /p2p-ws upgrades and HTTP CORS (empty allows all) |
| WebSocketTrustedSourceCIDRs | []string | 127.0.0.1/32\|::1/128 | p2p_websocket_trusted_source_cidrs | Source CIDRs exempt from the /p2p-ws connection caps; loopback only by design - broader trust would void the caps behind an L7 ingress or NAT (see longdesc). Sentinel `none` disables the bypass (empty falls back to the default) |
| HTTPRateLimit | int | 100 | p2p_httpRateLimit | Per-source req/s cap on the P2P HTTP server (/health, /p2p-ws); 0 or negative disables it |

## Configuration Dependencies

### Forced Sync Peer Selection

- `ForceSyncPeer` overrides automatic peer selection
- `AllowPrunedNodeFallback` affects fallback behavior when forced peer unavailable

### Network Address Management

- `Port` is the libp2p listen port; it is always bound on all interfaces
- `ListenAddresses` can only confirm that wildcard bind (the message bus has no listen-address option); anything narrower is rejected at startup
- `AdvertiseAddresses` overrides what libp2p announces to peers; when empty libp2p announces its bound interface addresses and the public address observed via Identify
- `SharePrivateAddresses` no longer feeds `ListenAddresses` into the announce set (a wildcard is not dialable); announcement is left to libp2p either way

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

### Connection Limits

All connection-limit settings default to 0 (or empty), which keeps the library default; a node that sets none of them runs with exactly the limits it had before they existed. Leave them unset unless you have a reason to change them.

- `MaxConnections` / `MinConnections` / `ConnectionGracePeriod` configure the libp2p connection manager and govern how many peers the node keeps: above the high water mark it prunes low-value connections (DHT routing peers first; topic mesh, static and bootstrap peers are protected) down to the low water mark. The library defaults are 35, 25 and 20s. Each watermark defaults on its own, so moving one past the other's default (e.g. `MaxConnections` below 25) requires setting both, or the service refuses to start.
- The `Rcmgr*` settings override the libp2p resource manager, which enforces hard caps on connections, streams and file descriptors. Its defaults are auto-scaled to host memory and the process file-descriptor limit; the effective system limits are logged at startup (`Resource manager system limits: conns=... inbound=... fd=...`) and exported as the `libp2p_rcmgr_*` metrics.
- An inbound or outbound limit must not exceed its total, whether the total is set or auto-scaled. Raising `RcmgrConnsInbound` above the auto-scaled total needs `RcmgrConns` set too; otherwise the P2P service fails to start with an error saying so. `RcmgrConns` must not be below `MaxConnections`.
- `RcmgrIPv4SubnetLimits` / `RcmgrIPv6SubnetLimits` take pipe-separated `<prefix length>:<connections>` entries matched against the source address. A non-empty list replaces the default for that address family as a whole, so list every prefix you want limited. Loopback is never limited. A malformed entry fails startup with an error naming the setting.

**Bootstrap and DHT server nodes** (`p2p_dht_mode=server`) take heavy inbound churn and can exhaust the auto-scaled inbound connection limit. Raise `RcmgrConnsInbound` (with `RcmgrConns`), and raise `RcmgrFD` with it, since every TCP connection holds a file descriptor (keep it below the process `ulimit -n`). The connection manager watermarks still govern how many peers are kept once connected.

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
| ListenMode | Must be "full", "listen_only" or "silent" | Service fails to start if invalid | During server initialization |
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

### Connection Limits for a Bootstrap / DHT Server Node

Illustrative values only; size them to the host. Check the `Resource manager system limits` startup log line for the auto-scaled defaults before raising anything.

```bash
p2p_dht_mode=server
# Keep more peers once connected (connection manager watermarks)
p2p_max_connections=200
p2p_min_connections=150
# Accept more inbound connections; the total must cover the inbound limit
p2p_rcmgr_conns=4096
p2p_rcmgr_conns_inbound=3072
# Every TCP connection holds a file descriptor; stay below ulimit -n
p2p_rcmgr_fd=8192
```

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
with `true` the address checks are skipped, so an announced URL naming a private, loopback or
link-local address is accepted into the peer registry. The URL must still be a clean base URL
(http or https, no credentials, query or fragment) either way, because fetch paths are joined
onto it.

At connection time, every outbound request to a peer-supplied URL - availability probes and
block/subtree fetches alike - resolves the hostname and checks each address:

- Loopback (127.0.0.0/8, ::1), link-local (169.254.0.0/16, fe80::/10) and unspecified
  addresses are refused regardless of this setting. Accepting such a URL therefore does not
  make it reachable.
- Private-network addresses (RFC1918, IPv6 ULA fc00::/7 and shared address space
  100.64.0.0/10) are refused unless this setting is `true`. With the default `false`, a
  hostname that resolves to a private address is treated exactly like a private IP literal.
  Nodes that peer over a private network, such as Kubernetes clusters using
  `svc.cluster.local` DataHub URLs or multi-node compose stacks, must set it to `true`.
- With the setting `true`, a hostname that resolved to a public address within the last ten
  minutes may not resolve to a private one, and an answer mixing public and private addresses
  is refused. This stops a peer's DNS rebinding a public name onto an internal service
  between two requests.

A node started with the default `false` logs one warning naming this setting, because a
deployment whose peers are reachable only over a private network will find no sync peer:
every availability probe is refused, those peers are dropped from selection, and the node
stops catching up. Each refused probe is logged at warning level as well, so the cause is
visible in the log rather than only in a stalled height.

Redirects from a peer may not leave the origin of the requested URL (an http to https upgrade
on the same host is allowed), and a POST is never redirected. The probe applies the same
policy as the fetch path, so it never rejects a peer that catchup could have used.

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
