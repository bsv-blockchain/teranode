# RPC Service Settings

**Related Topic**: [RPC Service](../../../topics/services/rpc.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| RPCUser | string | "" | rpc_user | Full access authentication username |
| RPCPass | string | "" | rpc_pass | Full access authentication password |
| RPCLimitUser | string | "" | rpc_limit_user | Limited access authentication username |
| RPCLimitPass | string | "" | rpc_limit_pass | Limited access authentication password |
| RPCMaxClients | int | 1 | rpc_max_clients | Maximum concurrent RPC connections |
| RPCQuirks | bool | true | rpc_quirks | Legacy client compatibility behavior |
| RPCListenerURL | *url.URL | "" | rpc_listener_url | **CRITICAL** - RPC server network binding |
| CacheEnabled | bool | true | rpc_cache_enabled | **CRITICAL** - Response caching for performance |
| RPCTimeout | time.Duration | 30s | rpc_timeout | **CRITICAL** - RPC call execution timeout |
| ClientCallTimeout | time.Duration | 5s | rpc_client_call_timeout | **CRITICAL** - Service client call timeout |

## Configuration Dependencies

### Authentication System
- `RPCUser` and `RPCPass` provide full access to all RPC commands
- `RPCLimitUser` and `RPCLimitPass` provide limited access to subset of commands
- Two-tier authentication system with different access levels

### Response Caching
- When `CacheEnabled = true`, caches responses for getbestblockhash, getpeerinfo, getblockchaininfo, getinfo, and getchaintips
- Improves performance for frequently accessed data

### Timeout Management
- `RPCTimeout` controls overall RPC call duration with context timeout
- `ClientCallTimeout` controls calls to P2P and Legacy services
- Prevents hung requests and service calls

### Network Binding
- `RPCListenerURL` determines server binding interface and port
- `RPCMaxClients` limits concurrent connections

## Service Dependencies

| Dependency | Interface | Usage |
|------------|-----------|-------|
| UTXOStore | utxo.Store | **CRITICAL** - UTXO operations and transaction validation |
| TxStore | blob.Store | **CRITICAL** - Transaction data access |
| BlockchainClient | blockchain.ClientI | **CRITICAL** - Blockchain operations and data retrieval |
| BlockValidationClient | blockvalidation.Interface | **CRITICAL** - Block validation operations |
| BlockAssemblyClient | blockassembly.ClientI | **CRITICAL** - Mining operations and block templates |
| P2PClient | p2p.ClientI | **CRITICAL** - Peer information and network operations |
| LegacyPeerClient | peer.ClientI | **CRITICAL** - Legacy peer information |
| ValidatorClient | validator.ClientI | **CRITICAL** - Transaction validation |

## Validation Rules

| Setting | Validation | Impact | When Checked |
|---------|------------|--------|-------------|
| RPCListenerURL | Must be valid URL format | Server binding | During service initialization |
| CacheEnabled | Controls response caching behavior | Performance | During RPC method execution |
| RPCTimeout | Must be positive duration | Request handling | During RPC call execution |
| ClientCallTimeout | Must be positive duration | Service calls | During client service calls |

## Configuration Examples

### Basic Configuration

```text
rpc_listener_url = "http://localhost:8332"
rpc_max_clients = 10
```

### Authentication Setup

```text
rpc_user = "admin"
rpc_pass = "secure_password"
rpc_limit_user = "readonly"
rpc_limit_pass = "readonly_password"
```

### Performance Tuning

```text
rpc_cache_enabled = true
rpc_timeout = 60s
rpc_client_call_timeout = 10s
```
