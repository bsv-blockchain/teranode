# Asset Service Settings

**Related Topic**: [Asset Service](../../../topics/services/assetServer.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| APIPrefix | string | "/api/v1" | asset_apiPrefix | URL prefix for API endpoints |
| CentrifugeListenAddress | string | ":8892" | asset_centrifugeListenAddress | WebSocket server binding address |
| CentrifugeDisable | bool | false | asset_centrifuge_disable | Disables WebSocket server |
| HTTPAddress | string | "http://localhost:8090/api/v1" | asset_httpAddress | **Required when Centrifuge enabled** - Must be valid URL |
| HTTPListenAddress | string | ":8090" | asset_httpListenAddress | **CRITICAL** - HTTP server binding (fails if empty) |
| SignHTTPResponses | bool | false | asset_sign_http_responses | Adds X-Signature header (requires P2P.PrivateKey) |
| EchoDebug | bool | false | ECHO_DEBUG | Enables verbose logging and request middleware |

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
- `HTTPAddress` must be valid URL (fails during Init() if invalid)

### HTTP Response Signing

- `SignHTTPResponses = true` enables signing
- Requires `P2P.PrivateKey` (hex-encoded Ed25519 format)
- Invalid key logs error but continues without signing (non-fatal)
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
