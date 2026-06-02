# Asset Service Settings

**Related Topic**: [Asset Service](../../../topics/services/assetServer.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| APIPrefix | string | "/api/v1" | asset_apiPrefix | URL prefix for API endpoints |
| CentrifugeListenAddress | string | ":8892" | asset_centrifugeListenAddress | WebSocket server binding address |
| CentrifugeDisable | bool | false | asset_centrifuge_disable | Disables WebSocket server |
| HTTPAddress | string | "`http://localhost:8090/api/v1`" | asset_httpAddress | **Required when Centrifuge enabled** - Must be non-empty and valid URL format |
| HTTPListenAddress | string | ":8090" | asset_httpListenAddress | **CRITICAL** - HTTP server binding (fails during Init() if empty) |
| HTTPPort | int | 8090 | ASSET_HTTP_PORT | **UNUSED** - HTTPListenAddress is used instead |
| HTTPPublicAddress | string | "" | asset_httpPublicAddress | **UNUSED** - Reserved for future use |
| SignHTTPResponses | bool | false | asset_sign_http_responses | Adds X-Signature header (requires P2P.PrivateKey, non-fatal if invalid) |
| EchoDebug | bool | false | ECHO_DEBUG | Enables verbose logging and request middleware |
| PropagationPublicURL | string | "" | asset_propagation_public_url | Public-facing URL for propagation service |

## Concurrency Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| ConcurrencyGetTransaction | int | 0 | asset_concurrency_get_transaction | Rate limit for GetTransaction (0=unlimited, -1=NumCPU, >0=exact) |
| ConcurrencyGetTransactionMeta | int | 0 | asset_concurrency_get_transaction_meta | Rate limit for GetTransactionMeta |
| ConcurrencyGetSubtreeData | int | 2 | asset_concurrency_get_subtree_data | Rate limit for GetSubtreeData |
| ConcurrencyGetSubtreeDataReader | int | 4 | asset_concurrency_get_subtree_data_reader | Rate limit for GetSubtreeDataReader |
| ConcurrencyGetSubtreeTransactions | int | 2 | asset_concurrency_get_subtree_transactions | Rate limit for GetSubtreeTransactions |
| ConcurrencyGetSubtreeExists | int | 0 | asset_concurrency_get_subtree_exists | Rate limit for GetSubtreeExists |
| ConcurrencyGetSubtreeHead | int | 0 | asset_concurrency_get_subtree_head | Rate limit for GetSubtreeHead |
| ConcurrencyGetUtxo | int | 0 | asset_concurrency_get_utxo | Rate limit for GetUtxo |
| ConcurrencyGetLegacyBlockReader | int | -1 | asset_concurrency_get_legacy_block_reader | Rate limit for GetLegacyBlockReader (default: NumCPU) |
| SubtreeDataStreamingChunkSize | int | 10000 | asset_subtreeDataStreamingChunkSize | Records per subtree data streaming chunk |
| SubtreeDataStreamingConcurrency | int | 2 | asset_subtreeDataStreamingConcurrency | Parallel workers for subtree data streaming |

**Concurrency Control:**

- `0` = Unlimited concurrency (no rate limiting)
- `-1` = Dynamic limit based on runtime.NumCPU()
- `>0` = Exact concurrency limit

## HTTP Rate Limit and Peer Authentication Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| HTTPRateLimit | int | 1024 | asset_httpRateLimit | Per-IP req/s for unverified clients (0 disables) |
| HTTPHeavyRateLimit | int | 10 | asset_httpHeavyRateLimit | Per-IP req/s on heavy endpoints (blocks, subtrees, batch txs) |
| HTTPPeerRateMultiplier | int | 5 | asset_httpPeerRateMultiplier | Authenticated peers get base × this rate |
| HTTPMinerRateLimit | int | 0 | asset_httpMinerRateLimit | Per-peer req/s cap for miner tier; 0 = fully exempt |
| HTTPBodyLimit | string | "100MB" | asset_httpBodyLimit | Max request body size (Echo BodyLimit, returns 413) |
| TrustedProxyCIDRs | string | "" | asset_trustedProxyCIDRs | Pipe-separated CIDRs trusted for X-Forwarded-For extraction (fails Init if non-empty but unparseable) |
| PeerAuthAllowlist | string | "" | asset_peerAuthAllowlist | Pipe-separated libp2p peer IDs eligible for tier elevation; empty = no peer is elevated |
| PeerMinerReputationThreshold | float64 | 50.0 | asset_peerMinerReputationThreshold | Min reputation score for tierMiner classification |

**Rate-limit tiers:**

- **Unverified** (no signature or signature fails verification): bucketed by source IP, IPv6 normalised to `/64`. Held in a bounded LRU (50k entries) so an attacker rotating addresses cannot grow the map without limit.
- **Peer** (valid signature + peer ID in allowlist + not classified as miner): bucketed by libp2p peer ID at `HTTPRateLimit × HTTPPeerRateMultiplier`.
- **Miner** (valid signature + peer ID in allowlist + `BlocksReceived > 0` + `ReputationScore ≥ PeerMinerReputationThreshold`): fully exempt if `HTTPMinerRateLimit = 0`, otherwise bucketed by peer ID at `HTTPMinerRateLimit`.

When `PeerAuthAllowlist` is empty (the default), signatures are still verified (replay cache + body digest + ±10s freshness window all apply) but no tier elevation is granted — every authenticated peer is treated as unverified for rate-limit purposes. This is the safe opt-in default; operators must explicitly list trusted peers.

## Global Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| SecurityLevelHTTP | int | 0 | securityLevelHTTP | 0=HTTP, non-zero=HTTPS |
| ServerCertFile | string | "" | server_certFile | TLS certificate file (required for HTTPS) |
| ServerKeyFile | string | "" | server_keyFile | TLS key file (required for HTTPS) |
| StatsPrefix | string | "gocore" | stats_prefix | URL prefix for gocore stats endpoints |
| Dashboard.Enabled | bool | false | N/A | Enables dashboard UI and authentication |
| P2P.PrivateKey | string | "" | p2p_private_key | Hex-encoded Ed25519 key for response signing |

## Configuration Dependencies

### Centrifuge WebSocket Server

- `CentrifugeDisable = false` enables WebSocket server
- `CentrifugeListenAddress` must be non-empty
- `HTTPAddress` must be non-empty and valid URL format (validated via url.Parse(), fails Init() if invalid)

### HTTP Response Signing

- `SignHTTPResponses = true` enables signing
- Requires `P2P.PrivateKey` (hex-encoded Ed25519 format)
- Invalid P2P.PrivateKey logs error but service continues without signing (non-fatal)
- Adds `X-Signature` header to responses

### HTTPS Support

- `SecurityLevelHTTP != 0` enables HTTPS
- Both `ServerCertFile` and `ServerKeyFile` required
- Validated during Start(), fails if missing

## Service Dependencies

| Dependency | Interface | Usage |
|------------|-----------|-------|
| UTXOStore | utxo.Store | UTXO-related API endpoints |
| TxStore | blob.Store | Transaction data access |
| SubtreeStore | blob.Store | Subtree data access |
| BlockPersisterStore | blob.Store | Block data access |
| BlockchainClient | blockchain.ClientI | Blockchain operations, FSM state, waits for FSM transition from IDLE |
| BlockvalidationClient | blockvalidation.Interface | Block invalidation/revalidation endpoints |
| P2PClient | p2p.ClientI | Peer registry and catchup status endpoints |

## Validation Rules

| Setting | Validation | Error | When Checked |
|---------|------------|-------|-------------|
| HTTPListenAddress | Must not be empty | "no asset_httpListenAddress setting found" | During Init() |
| HTTPAddress | Must be valid URL when Centrifuge enabled | "asset_httpAddress not found in config" | During Init() |
| ServerCertFile | Must exist when SecurityLevelHTTP != 0 | "server_certFile is required for HTTPS" | During Start() |
| ServerKeyFile | Must exist when SecurityLevelHTTP != 0 | "server_keyFile is required for HTTPS" | During Start() |

## Configuration Examples

### HTTP Configuration

```bash
asset_httpListenAddress=:8090
asset_apiPrefix=/api/v1
```

### HTTPS Configuration

```bash
securityLevelHTTP=1
server_certFile=/path/to/cert.pem
server_keyFile=/path/to/key.pem
asset_httpListenAddress=:8090
```

### Centrifuge WebSocket

```bash
asset_centrifuge_disable=false
asset_centrifugeListenAddress=:8892
asset_httpAddress=http://localhost:8090/api/v1
```

### HTTP Response Signing

```bash
asset_sign_http_responses=true
p2p_private_key=<hex-encoded-ed25519-private-key>
```

### Stats Endpoints

```bash
stats_prefix=/stats/
# Registers: /stats/stats, /stats/reset, /stats/*
```
