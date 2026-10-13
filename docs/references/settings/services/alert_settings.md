# Alert Service Settings

**Related Topic**: [Alert Service](../../../topics/services/alert.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| GenesisKeys | []string | per network (see below) | alert_genesis_keys | Public keys allowed to sign alerts (pipe-delimited). Empty uses the network default |
| P2PPrivateKey | string | "" | alert_p2p_private_key | Hex-encoded Ed25519 private key (64-byte seed‖pubkey). Auto-generates if empty |
| ProtocolID | string | per network (see below) | alert_protocol_id | libp2p protocol identifier. Empty uses the network default |
| StoreURL | *url.URL | "sqlite:///alert" | alert_store | Database connection URL |
| TopicName | string | per network (see below) | alert_topic_name | P2P pubsub topic. Empty uses the network default |
| P2PPort | int | 9908 | ALERT_P2P_PORT | P2P listening port (string length >= 2 after PORT_PREFIX applied) |
| P2PBootstrapPeer | string | "" | alert_p2p_bootstrap_peer | Extra libp2p bootstrap multiaddr for private alert networks (public IPFS peers are still tried) |
| P2PAllowPrivateIPs | bool | false | alert_p2p_allow_private_ips | Allow dialing/accepting/advertising RFC1918 addresses on the alert network |
| P2PPeerDiscoveryInterval | time.Duration | 10m | alert_p2p_peer_discovery_interval | Peer discovery and alert sync cadence |
| P2PDHTMode | string | "client" | alert_p2p_dht_mode | Kademlia DHT mode: `client` or `server` |

## Network-Specific Behavior

**Alert Network Defaults:**

`GenesisKeys`, `ProtocolID` and `TopicName` are empty in `settings.conf`. Each empty setting is
filled from the configured `network` when the service starts; a non-empty value is an explicit
override and is used as-is. The defaults come from go-alert-system's embedded network configs
(`config.NetworkDefaults`), resolved in `services/alert/network_defaults.go`.

| Network | ProtocolID | TopicName | GenesisKeys |
|---------|------------|-----------|-------------|
| mainnet | `/bitcoin/alert-system/1.0.0` | `bitcoin_alert_system` | mainnet key set |
| testnet | `/bitcoin-testnet/alert-system/0.0.1` | `bitcoin_alert_system_testnet` | testnet key set |
| stn | `/bitcoin-stn/alert-system/0.0.1` | `bitcoin_alert_system_stn` | testnet key set |
| others (regtest, teratestnet, tstn) | `/bitcoin/alert-system/1.0.0` | `bitcoin_alert_system_{network}` | mainnet key set |

Networks without a public alert network fall back to the mainnet keys and protocol on a
network-suffixed topic, and the service logs a warning. Set all three for a private alert network.

**PORT_PREFIX Support:**

The global `PORT_PREFIX` environment variable is prepended to `P2PPort` if set.

Example: `PORT_PREFIX=1` with `ALERT_P2P_PORT=9908` results in port 19908.

## Auto-Generation Behavior

**P2P Private Key:**

- Triggers only if `P2PPrivateKey` is empty
- Creates directory: `$HOME/.bitcoin/` (permissions: 0750)
- Sets internal `PrivateKeyPath` to `$HOME/.bitcoin/alert_system_private_key` (does not modify Teranode settings)
- Generates a key into that file (permissions: 0600) only if the file is missing or empty; otherwise
  loads it, so the peer identity is stable across restarts. An unreadable or invalid file fails startup
  rather than being overwritten

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
| GenesisKeys | Must not be empty after network defaults are applied | `config.ErrNoGenesisKeys` | During `Init()` |
| P2P.IP | Length >= 5 characters | `config.ErrNoP2PIP` | During `Init()` |
| P2P.Port | String length >= 2 characters | `config.ErrNoP2PPort` | During `Init()` |
| StoreURL | Supported scheme (sqlite/sqlitememory/postgres/mysql) | `ErrDatastoreUnsupported` | During `Init()` |

## Configuration Examples

### Production Configuration

```bash
alert_store=postgres://user:pass@host:5432/alert_db?sslmode=require
ALERT_P2P_PORT=4001
# genesis keys, protocol ID and topic follow `network`; no need to set them
```

### Private Alert Network

```bash
alert_genesis_keys=key1|key2|key3
alert_protocol_id=/my-net/alert-system/0.0.1
alert_topic_name=my_net_alert_system
```

### Development Configuration

```bash
alert_store=sqlite:///alert
```

### Auto-Generated Private Key

```bash
# Leave empty to auto-generate
alert_p2p_private_key=
# Generated once into $HOME/.bitcoin/alert_system_private_key and reused on restart
```
