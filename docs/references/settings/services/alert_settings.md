# Alert Service Settings

**Related Topic**: [Alert Service](../../../topics/services/alert.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| GenesisKeys | []string | [] | alert_genesis_keys | **CRITICAL** - Service fails without valid keys |
| P2PPrivateKey | string | "" | alert_p2p_private_key | Auto-generates if empty |
| ProtocolID | string | "/bitcoin/alert-system/1.0.0" | alert_protocol_id | P2P protocol identification |
| StoreURL | *url.URL | "sqlite:///alert" | alert_store | Database connection |
| TopicName | string | "bitcoin_alert_system" | alert_topic_name | P2P topic (network-prefixed) |
| P2PPort | int | 9908 | ALERT_P2P_PORT | P2P listening port |

## Network-Specific Behavior

**Topic Name Prefixing:**
- Mainnet: Uses configured name as-is
- Testnet: Prefixed as `bitcoin_alert_system_testnet`
- Regtest: Prefixed as `bitcoin_alert_system_regtest`

## Auto-Generation Behavior

**P2P Private Key:**
- If empty, creates `$HOME/.alert-system/private_key.pem`
- Creates directory with 0750 permissions
- Sets `PrivateKeyPath` in internal config

## Database Configuration

| Scheme | URL Format | Pool Settings |
|--------|------------|---------------|
| SQLite | `sqlite:///database_name` | Max Idle: 1, Max Open: 1 |
| SQLite Memory | `sqlitememory:///database_name` | Max Idle: 1, Max Open: 1 |
| PostgreSQL | `postgres://user:pass@host:port/db` | Max Idle: 2, Max Open: 5 |
| MySQL | `mysql://user:pass@host:port/db` | Max Idle: 2, Max Open: 5 |

## Internal Configuration

| Setting | Value | Usage |
|---------|-------|-------|
| AlertProcessingInterval | 5 minutes | Alert processing frequency |
| RequestLogging | true | HTTP request logging |
| AutoMigrate | true | Database schema migration |
| DHTMode | "client" | DHT client mode |
| P2P.IP | "0.0.0.0" | P2P listening address |

## Service Dependencies

| Dependency | Interface | Usage |
|------------|-----------|-------|
| BlockchainClient | blockchain.ClientI | Block operations, RPC verification |
| UTXOStore | utxo.Store | UTXO freezing/unfreezing |
| BlockAssemblyClient | *blockassembly.Client | Block assembly operations |
| PeerClient | peer.ClientI | Legacy peer banning |
| P2PClient | p2pservice.ClientI | P2P peer operations |

## Validation Rules

| Setting | Validation | Error |
|---------|------------|-------|
| GenesisKeys | Must not be empty | `config.ErrNoGenesisKeys` |
| P2P.IP | Length >= 5 characters | `config.ErrNoP2PIP` |
| P2P.Port | Length >= 2 characters | `config.ErrNoP2PPort` |
| StoreURL | Supported scheme | `ErrDatastoreUnsupported` |


## Configuration Examples

### Production Configuration

```text
alert_store = postgres://user:pass@host:5432/alert_db
alert_genesis_keys = "key1|key2"
alert_p2p_port = 4001
```

### Development Configuration

```text
alert_store = sqlite:///alert
alert_genesis_keys = "key1"
```

