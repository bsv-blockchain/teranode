# Kafka Settings

**Related Topic**: [Kafka](../../topics/kafka/kafka.md)

Kafka configuration in Teranode is primarily specified through URLs. Each Kafka topic has its own URL with parameters that control its behavior. The URL format supports both production Kafka and in-memory testing.

## Kafka URL Format

### Production Kafka URL Format

```text
kafka://host1,host2,.../topic?param1=value1&param2=value2&...
```

Components of the URL:

- **Scheme**: Always `kafka://`
- **Hosts**: Comma-separated list of Kafka brokers (e.g., `localhost:9092,kafka2:9092`)
- **Topic**: The Kafka topic name (specified as the path component)
- **Parameters**: Query parameters that configure specific behavior

Example:

```text
kafka://localhost:9092/blocks?partitions=4&consumer_ratio=2&replication=3
```

### In-Memory Kafka URL Format (Testing)

```text
memory://topic?param1=value1&param2=value2&...
```

Components of the URL:

- **Scheme**: Always `memory://`
- **Topic**: The in-memory topic name (specified as the path component)
- **Parameters**: Same query parameters as production Kafka

Example:

```text
memory://test_blocks?partitions=2&consumer_ratio=1
```

**Usage**: Automatically enabled for dev/test contexts (`KAFKA_SCHEMA.dev = memory` in settings.conf). Eliminates need for running Kafka cluster during development. For Docker-based Kafka, override with `KAFKA_SCHEMA.dev = kafka` in `settings_local.conf`.

## URL Parameters

### Consumer Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `partitions` | int | 1 | Number of topic partitions |
| `replay` | int | 1 | Start from beginning (1) or latest (0) for new consumer groups |
| `offsetReset` | string | "" | Offset reset strategy: "latest", "earliest", or "" (uses replay). Overrides replay setting |
| `maxProcessingTime` | int | 100 | Max time (ms) to process a message before Sarama stops fetching. Must exceed actual processing time |
| `sessionTimeout` | int | 10000 | Time (ms) broker waits for heartbeat before considering consumer dead. Must be >= 3 * heartbeatInterval |
| `heartbeatInterval` | int | 3000 | Frequency (ms) of heartbeats sent to broker |
| `rebalanceTimeout` | int | 60000 | Max time (ms) for all consumers to join rebalance |
| `channelBufferSize` | int | 256 | Number of messages buffered in internal channels |
| `consumerTimeout` | int | 90000 | Watchdog timeout (ms). Triggers recovery if no messages received and Setup() not called |

### Producer Parameters (Async)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `partitions` | int | 1 | Number of topic partitions |
| `replication` | int | 1 | Replication factor |
| `retention` | string | "600000" | Message retention period in milliseconds (10 minutes default) |
| `segment_bytes` | string | "1073741824" | Maximum size of a single log segment file (1GB default) |
| `flush_bytes` | int | 1048576 | Bytes to accumulate before flushing (1MB default) |
| `flush_messages` | int | 50000 | Messages to accumulate before flushing |
| `flush_frequency` | duration | "10s" | Maximum time between flushes |

### Producer Parameters (Sync)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `partitions` | int | 1 | Number of topic partitions |
| `replication` | int | 1 | Replication factor |
| `retention` | string | "600000" | Message retention period in milliseconds (10 minutes default) |
| `segment_bytes` | string | "1073741824" | Maximum size of a single log segment file (1GB default) |
| `flush_bytes` | int | 1024 | Bytes to accumulate before flushing |

**Example Consumer URL:**

```text
kafka://localhost:9092/subtrees?partitions=8&sessionTimeout=15000&heartbeatInterval=5000&maxProcessingTime=30000
```

**Example Async Producer URL:**

```text
kafka://localhost:9092/blocks?partitions=4&replication=3&retention=3600000&flush_frequency=5s&flush_messages=100000
```

## Individual Settings

### Topic Names

| Setting | Default | Environment Variable | Usage |
|---------|---------|---------------------|-------|
| Blocks | "blocks" | KAFKA_BLOCKS | Block data messages |
| BlocksFinal | "blocks-final" | KAFKA_BLOCKS_FINAL | Finalized block announcements |
| InvalidBlocks | "invalid-blocks" | KAFKA_INVALID_BLOCKS | Invalid block notifications |
| InvalidSubtrees | "invalid-subtrees" | KAFKA_INVALID_SUBTREES | Invalid subtree notifications |
| LegacyInv | "legacy-inv" | KAFKA_LEGACY_INV | Legacy inventory messages |
| RejectedTx | "rejectedtx" | KAFKA_REJECTEDTX | Rejected transaction notifications |
| Subtrees | "subtrees" | KAFKA_SUBTREES | Subtree data messages |
| TxMeta | "txmeta" | KAFKA_TXMETA | Transaction metadata |
| UnitTest | "unittest" | KAFKA_UNITTEST | Unit testing |

### Connection Settings

| Setting | Default | Environment Variable | Usage |
|---------|---------|---------------------|-------|
| Hosts | "localhost:9092" | KAFKA_HOSTS | Comma-separated broker addresses |
| Port | 9092 | KAFKA_PORT | Default port when not in hosts |
| Partitions | 1 | KAFKA_PARTITIONS | Default partition count |
| ReplicationFactor | 1 | KAFKA_REPLICATION_FACTOR | Default replication factor |

### TLS Settings

| Setting | Default | Environment Variable | Usage |
|---------|---------|---------------------|-------|
| EnableTLS | false | KAFKA_ENABLE_TLS | Enable TLS encryption |
| TLSSkipVerify | false | KAFKA_TLS_SKIP_VERIFY | Skip certificate verification (testing only) |
| TLSCAFile | "" | KAFKA_TLS_CA_FILE | CA certificate file path (optional for custom CA) |
| TLSCertFile | "" | KAFKA_TLS_CERT_FILE | Client certificate file path (required with TLSKeyFile for mutual TLS) |
| TLSKeyFile | "" | KAFKA_TLS_KEY_FILE | Client key file path (required with TLSCertFile for mutual TLS) |

**TLS Requirements:**

- `TLSCertFile` and `TLSKeyFile` must both be provided together for mutual TLS authentication
- `TLSSkipVerify=true` should only be used in development/testing environments
- When `EnableTLS=true`, all Kafka connections use TLS encryption

### Debug Settings

| Setting | Default | Environment Variable | Usage |
|---------|---------|---------------------|-------|
| EnableDebugLogging | false | kafka_enable_debug_logging | Verbose Sarama logging |

## URL-Based Configuration

### Config URL Settings

| Setting | Environment Variable | Usage |
|---------|---------------------|-------|
| ValidatorTxsConfig | kafka_validatortxsConfig | Validator transaction messages |
| TxMetaConfig | kafka_txmetaConfig | Transaction metadata |
| LegacyInvConfig | kafka_legacyInvConfig | Legacy inventory messages |
| BlocksFinalConfig | kafka_blocksFinalConfig | Finalized blocks |
| RejectedTxConfig | kafka_rejectedTxConfig | Rejected transactions |
| InvalidBlocksConfig | kafka_invalidBlocksConfig | Invalid blocks |
| InvalidSubtreesConfig | kafka_invalidSubtreesConfig | Invalid subtrees |
| SubtreesConfig | kafka_subtreesConfig | Subtrees |
| BlocksConfig | kafka_blocksConfig | Blocks |

## Configuration Priority

URL-based configuration overrides individual settings when provided:

1. **URL Config** (e.g., `InvalidBlocksConfig`) - highest priority
2. **Individual Settings** (e.g., `InvalidBlocks`, `Hosts`, `Port`) - fallback

## Consumer Timeout Constraints

**Critical Validation Rule:**
```
sessionTimeout >= 3 * heartbeatInterval
```

This constraint is enforced by Sarama. Consumer creation will fail if violated.

**Example Valid Configuration:**

- `heartbeatInterval=5000` (5s)
- `sessionTimeout=15000` (15s) ✓ Valid: 15000 >= 3 * 5000

**Example Invalid Configuration:**

- `heartbeatInterval=5000` (5s)  
- `sessionTimeout=10000` (10s) ✗ Invalid: 10000 < 15000

## Consumer Watchdog

The consumer watchdog monitors for stuck consumers and automatically recovers by recreating the consumer group.

**Behavior:**

- Checks every 30 seconds
- Triggers recovery if `Consume()` is stuck for longer than `consumerTimeout` (default 90s)
- Detects RefreshMetadata hangs and offset-related issues
- Automatically recreates consumer group on recovery

**Configuration:**

- `consumerTimeout` URL parameter (default: 90000ms)

## Service Usage

### Block Assembly Service
- **Producer**: `BlocksConfig` - publishes blocks
- **Producer**: `SubtreesConfig` - publishes subtrees

### Block Validation Service
- **Consumer**: `BlocksConfig` - consumes blocks for validation
- **Producer**: `InvalidBlocksConfig` - publishes invalid blocks (optional)

### Blockchain Service
- **Producer**: `BlocksFinalConfig` - publishes finalized blocks

### Subtree Validation Service
- **Consumer**: `SubtreesConfig` - consumes subtrees for validation
- **Producer**: `InvalidSubtreesConfig` - publishes invalid subtrees (optional)

### Validator Service
- **Consumer**: `ValidatorTxsConfig` - consumes transactions for validation (optional)
- **Producer**: `ValidatorTxsConfig` - publishes validation results (optional)
- **Producer**: `RejectedTxConfig` - publishes rejected transactions

### Propagation Service
- **Consumer**: `RejectedTxConfig` - consumes rejected transactions

### Block Persister Service
- **Consumer**: `TxMetaConfig` - consumes transaction metadata (with random consumer group suffix)

### Legacy Service
- **Producer**: `LegacyInvConfig` - publishes legacy inventory messages
- **Consumer**: `BlocksFinalConfig` - consumes finalized blocks
- **Consumer**: `TxMetaConfig` - consumes transaction metadata

### P2P Service

- Uses `InvalidBlocksConfig` or constructs URL from `InvalidBlocks`, `Hosts`, `Port`
- Applies TLS settings from KafkaSettings
- Consumer group: `{topic}-consumer`

### Legacy Service

- Uses `LegacyInvConfig`, `BlocksFinalConfig`, `TxMetaConfig`
- Applies TLS settings from KafkaSettings

### Blockchain Service

- Uses async producer for block notifications
- Applies TLS settings from KafkaSettings

## Configuration Examples

### High-Throughput Producer

```text
kafka://localhost:9092/blocks?partitions=8&replication=3&retention=3600000&flush_bytes=10485760&flush_messages=100000&flush_frequency=5s
```

### Slow Processing Consumer

```text
kafka://localhost:9092/subtrees?partitions=4&maxProcessingTime=30000&sessionTimeout=60000&heartbeatInterval=20000&consumerTimeout=120000
```

### Low-Latency Producer

```text
kafka://localhost:9092/txmeta?partitions=4&flush_bytes=524288&flush_messages=1000&flush_frequency=1s
```

### Offset Reset Consumer

```text
kafka://localhost:9092/blocks?partitions=2&offsetReset=latest&replay=0
```

### TLS-Enabled Configuration

Environment variables:
```bash
KAFKA_ENABLE_TLS=true
KAFKA_TLS_CA_FILE=/path/to/ca.pem
KAFKA_TLS_CERT_FILE=/path/to/client-cert.pem
KAFKA_TLS_KEY_FILE=/path/to/client-key.pem
kafka_blocksConfig=kafka://broker1:9093,broker2:9093/blocks?partitions=4
```

### Memory Testing

```text
memory://test_blocks?partitions=2&replay=1
```
