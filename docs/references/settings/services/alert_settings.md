# Alert Service Settings

**Related Topic**: [Alert Service](../../../topics/services/alert.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| GenesisKeys | []string | [] | alert_genesis_keys | **CRITICAL** - Service fails during Init() if empty (pipe-delimited) |
| P2PPrivateKey | string | "" | alert_p2p_private_key | PEM-format private key. Auto-generates if empty |
| ProtocolID | string | "/bitcoin/alert-system/1.0.0" | alert_protocol_id | libp2p protocol identifier |
| StoreURL | *url.URL | "sqlite:///alert" | alert_store | Database connection URL |
| TopicName | string | "bitcoin_alert_system" | alert_topic_name | P2P pubsub topic (auto-suffixed for testnet/regtest) |
| P2PPort | int | 9908 | ALERT_P2P_PORT | P2P listening port (string length >= 2 after PORT_PREFIX applied) |

## Network-Specific Behavior

**Topic Name Suffixing:**

- Mainnet: Uses configured `TopicName` as-is
- Testnet: Auto-suffixed as `{TopicName}_testnet`
- Regtest: Auto-suffixed as `{TopicName}_regtest`

This behavior cannot be overridden.

**PORT_PREFIX Support:**

The global `PORT_PREFIX` environment variable is prepended to `P2PPort` if set.

Example: `PORT_PREFIX=1` with `ALERT_P2P_PORT=9908` results in port 19908.

## Auto-Generation Behavior

**P2P Private Key:**

- Triggers only if `P2PPrivateKey` is empty
- Creates directory: `$HOME/.alert-system/` (permissions: 0750)
- Creates file: `private_key.pem`
- Sets internal `PrivateKeyPath` (does not modify Teranode settings)

## Database Configuration

| Scheme | URL Format | Pool Settings | Notes |
|--------|------------|---------------|-------|
| SQLite | `sqlite:///database_name` | Max Idle: 1, Max Open: 1 | File created: `{DataFolder}/{database_name}.db` |
| SQLite Memory | `sqlitememory:///database_name` | Max Idle: 1, Max Open: 1 | In-memory only |
| PostgreSQL | `postgres://user:pass@host:port/db?sslmode=require` | Max Idle: 2, Max Open: 5, Timeout: 20s | Supports `sslmode` query param |
| MySQL | `mysql://user:pass@host:port/db?sslmode=require` | Max Idle: 2, Max Open: 5, Timeout: 20s | Supports `sslmode` query param |

## Internal Configuration (Hardcoded)

| Setting | Value | Usage |
|---------|-------|-------|
| AlertProcessingInterval | 5 minutes | Alert processing frequency |
| RequestLogging | true | HTTP request logging |
| AutoMigrate | true | Database schema migration on startup |
| DHTMode | "client" | DHT client mode |
| P2P.IP | "0.0.0.0" | P2P listening address (all interfaces) |
| PeerBanDuration | 100 years | Duration for peer bans (effectively permanent) |
| DisableRPCVerification | false | Validates blockchain client connection at startup |

## Service Dependencies

| Dependency | Interface | Usage |
|------------|-----------|-------|
| BlockchainClient | blockchain.ClientI | Block operations, RPC verification |
| UTXOStore | utxo.Store | UTXO freezing/unfreezing |
| BlockAssemblyClient | *blockassembly.Client | Block assembly operations |
| PeerClient | peer.ClientI | Legacy peer banning |
| P2PClient | p2pservice.ClientI | P2P peer operations |

## Validation Rules

| Setting | Validation | Error | When Checked |
|---------|------------|-------|-------------|
| GenesisKeys | Must not be empty | `config.ErrNoGenesisKeys` | During `Init()` |
| P2P.IP | Length >= 5 characters | `config.ErrNoP2PIP` | During `Init()` |
| P2P.Port | String length >= 2 characters | `config.ErrNoP2PPort` | During `Init()` |
| StoreURL | Supported scheme (sqlite/sqlitememory/postgres/mysql) | `ErrDatastoreUnsupported` | During `Init()` |


## Configuration Examples

### Production Configuration

```bash
alert_store=postgres://user:pass@host:5432/alert_db?sslmode=require
alert_genesis_keys=key1|key2|key3
alert_p2p_port=4001
alert_protocol_id=/bitcoin/alert-system/1.0.0
alert_topic_name=bitcoin_alert_system
```

### Development Configuration

```bash
alert_store=sqlite:///alert
alert_genesis_keys=devkey1
```

### Auto-Generated Private Key

```bash
# Leave empty to auto-generate
alert_p2p_private_key=
# File will be created at: $HOME/.alert-system/private_key.pem
```

