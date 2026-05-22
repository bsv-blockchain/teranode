# Coinbase Service Settings

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| DB | string | "" | coinbaseDB | Legacy database connection string for coordinated mining |
| UserPwd | string | "" | coinbaseDBUserPwd | Database user password |
| ArbitraryText | string | "" | coinbase_arbitrary_text | Custom text embedded in coinbase transaction scriptSig |
| GRPCAddress | string | "" | coinbase_grpcAddress | gRPC client connection address for coinbase coordination |
| GRPCListenAddress | string | "" | coinbase_grpcListenAddress | gRPC server binding address for coinbase coordination |
| NotificationThreshold | int | 0 | coinbase_notification_threshold | Minimum reward (satoshis) that triggers Slack notifications; 0 = notify always |
| P2PPeerID | string | "" | coinbase_p2p_peer_id | Peer identifier in the coinbase coordination network; auto-derived from private key if empty |
| P2PPrivateKey | string | "" | coinbase_p2p_private_key | **CRITICAL** - Private key for coinbase P2P network authentication |
| P2PStaticPeers | []string | [] | coinbase_p2p_static_peers | Trusted peer addresses for reward distribution (pipe-separated multiaddrs) |
| ShouldWait | bool | false | coinbase_should_wait | Whether block assembly waits for coinbase coordination before finalizing blocks |
| Store | *url.URL | "" | coinbase_store | Database URL for coinbase coordination state storage |
| StoreDBTimeoutMillis | int | 0 | coinbase_store_dbTimeoutMillis | Database operation timeout in milliseconds; 0 = no timeout |
| WaitForPeers | bool | false | coinbase_wait_for_peers | Whether to wait for P2P peer connections before starting coinbase operations |
| WalletPrivateKey | string | "" | coinbase_wallet_private_key | **CRITICAL** - WIF private key that receives mining rewards |
| PeerStatusTimeout | time.Duration | 30s | peerStatus_timeout | Timeout for peer status checks in the coordination network |
| SlackChannel | string | "" | slack_channel | Slack channel ID or name for mining notifications |
| SlackToken | string | "" | slack_token | **CRITICAL** - Slack API authentication token |
| TestMode | bool | false | coinbase_test_mode | Enable test mode (simulates rewards without real fund transfers; never enable in production) |
| P2PPort | int | 9906 | p2p_port_coinbase | TCP port for coinbase coordination P2P network |
| DistributorFailureTolerance | int | 0 | distributor_failure_tolerance | Consecutive distribution failures tolerated before alerting; 0 = alert immediately |
| DistributorTimeout | time.Duration | 30s | distributor_timeout | Maximum time for reward distribution operations to complete |

## Configuration Dependencies

### Coordinated Mining Pool Setup

When deploying a mining pool with coordinated reward distribution:

- Set `ShouldWait = true` to hold block assembly until coordination is confirmed
- Set `WaitForPeers = true` to ensure the coordination network is established before mining
- Configure `P2PStaticPeers` with all coordinator node addresses
- Set `Store` to a PostgreSQL URL for production pool state persistence
- Set `DistributorFailureTolerance` and `DistributorTimeout` appropriate for your network

### Solo Mining

For solo mining, no coinbase coordination is needed:

- Leave `ShouldWait = false` (default) and `WaitForPeers = false` (default)
- Configure `WalletPrivateKey` to receive block rewards

### Notifications

Slack notifications require both `SlackToken` and `SlackChannel` to be set. Use `NotificationThreshold` to suppress notifications for small or test rewards.

## Database Configuration

The `Store` URL supports the following schemes:

| Scheme | URL Format | Notes |
|--------|------------|-------|
| SQLite | `sqlite:///path/to/db` | File-based; suitable for testing |
| SQLite Memory | `sqlitememory:///name` | In-memory only |
| PostgreSQL | `postgres://user:pass@host:port/db` | Recommended for production pools |

## Security Notes

- `WalletPrivateKey`: Back up securely. Losing it means losing all mining rewards.
- `P2PPrivateKey`: Compromise allows impersonation in reward distribution.
- `SlackToken`: Provides access to post to your Slack workspace.
- `coinbaseDBUserPwd`: Grants access to mining reward distribution data.

Use environment variables or a secrets manager for all sensitive keys. Never commit them to version control.

## Configuration Examples

### Solo Mining

```bash
coinbase_wallet_private_key=<WIF_private_key>
coinbase_arbitrary_text=MyMiner/1.0
```

### Mining Pool

```bash
coinbase_should_wait=true
coinbase_wait_for_peers=true
coinbase_store=postgres://user:pass@host:5432/coinbase_db
coinbase_grpcListenAddress=:9907
coinbase_p2p_static_peers=/ip4/192.168.1.100/tcp/9906/p2p/QmPeerId1|/ip4/192.168.1.101/tcp/9906/p2p/QmPeerId2
coinbase_wallet_private_key=<WIF_private_key>
slack_token=xoxb-...
slack_channel=#mining-alerts
```

### Development / Testing

```bash
coinbase_test_mode=true
coinbase_store=sqlitememory:///coinbase
```
