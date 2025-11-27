# Asset Service Settings

**Related Topic**: [Asset Service](../../../topics/services/assetServer.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| APIPrefix | string | "/api/v1" | asset_apiPrefix | API route grouping |
| CentrifugeListenAddress | string | ":8892" | asset_centrifugeListenAddress | WebSocket server address |
| CentrifugeDisable | bool | false | asset_centrifuge_disable | **CRITICAL** - Disables WebSocket functionality |
| HTTPAddress | string | "http://localhost:8090/api/v1" | asset_httpAddress | **CRITICAL for Centrifuge** - Base URL |
| HTTPPublicAddress | string | "" | asset_httpPublicAddress | Configuration placeholder |
| HTTPListenAddress | string | ":8090" | asset_httpListenAddress | **CRITICAL** - HTTP server binding |
| HTTPPort | int | 8090 | ASSET_HTTP_PORT | Configuration placeholder |
| SignHTTPResponses | bool | false | asset_sign_http_responses | HTTP response signing |
| EchoDebug | bool | false | ECHO_DEBUG | Echo framework debug mode |

## Global Security Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| SecurityLevelHTTP | int | 0 | securityLevelHTTP | HTTP (0) vs HTTPS (non-zero) |
| ServerCertFile | string | "" | server_certFile | **Required for HTTPS** |
| ServerKeyFile | string | "" | server_keyFile | **Required for HTTPS** |

## Configuration Dependencies

### Centrifuge WebSocket Server
- Requires `CentrifugeDisable = false`
- Requires valid `CentrifugeListenAddress`
- Requires valid `HTTPAddress` for base URL

### HTTP Response Signing
- Requires `SignHTTPResponses = true`
- Requires valid `P2P.PrivateKey` (Ed25519 format)

### HTTPS Support
- Requires `SecurityLevelHTTP != 0`
- Requires valid `ServerCertFile` and `ServerKeyFile`

## Service Dependencies

| Dependency | Interface | Usage |
|------------|-----------|-------|
| UTXOStore | utxo.Store | UTXO-related API endpoints |
| TxStore | blob.Store | Transaction data access |
| SubtreeStore | blob.Store | Subtree data access |
| BlockPersisterStore | blob.Store | Block data access |
| BlockchainClient | blockchain.ClientI | Blockchain operations, FSM state |

## Validation Rules

| Setting | Validation | Error |
|---------|------------|-------|
| HTTPListenAddress | Must not be empty | "no asset_httpListenAddress setting found" |
| HTTPAddress | Required when Centrifuge enabled | "asset_httpAddress not found in config" |
| ServerCertFile | Required when HTTPS enabled | "server_certFile is required for HTTPS" |
| ServerKeyFile | Required when HTTPS enabled | "server_keyFile is required for HTTPS" |

## Configuration Examples

### HTTP Configuration

```text
asset_httpListenAddress = ":8090"
asset_apiPrefix = "/api/v1"
```

### HTTPS Configuration

```text
securityLevelHTTP = 1
server_certFile = "/path/to/cert.pem"
server_keyFile = "/path/to/key.pem"
```

### Centrifuge WebSocket

```text
asset_centrifuge_disable = false
asset_centrifugeListenAddress = ":8892"
asset_httpAddress = "http://localhost:8090/api/v1"
```
