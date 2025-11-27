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

**Usage**: The memory scheme is automatically detected by the Kafka utilities and enables in-memory message passing for unit tests and development environments. This eliminates the need for a running Kafka cluster during testing.

## URL Parameters

### Consumer Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `partitions` | int | 1 | Number of topic partitions |
| `replay` | int | 1 | Start from beginning (1) or latest (0) |
| `maxProcessingTime` | int | 100 | Max message processing time (ms) |
| `sessionTimeout` | int | 10000 | Session timeout (ms) |
| `heartbeatInterval` | int | 3000 | Heartbeat interval (ms) |
| `rebalanceTimeout` | int | 60000 | Rebalance timeout (ms) |
| `channelBufferSize` | int | 256 | Internal buffer size |
| `consumerTimeout` | int | 90000 | Watchdog timeout (ms) |
| `offsetReset` | string | "latest" | "latest", "earliest", or "" |

**Example Consumer URL:**

```text
kafka://localhost:9092/transactions?partitions=4&replay=0&sessionTimeout=15000
```

### Producer Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `partitions` | int | 1 | Number of topic partitions |
| `replication` | int | 1 | Replication factor |
| `retention` | string | "600000" | Retention period (ms) |
| `segment_bytes` | string | "1073741824" | Segment size in bytes |
| `flush_bytes` | int | 1048576 | Flush threshold in bytes |
| `flush_messages` | int | 50000 | Messages before flush |
| `flush_frequency` | string | "10s" | Flush frequency |

**Example Producer URL:**

```text
kafka://localhost:9092/blocks?partitions=2&replication=3&retention=3600000&flush_frequency=5s
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
| TLSSkipVerify | false | KAFKA_TLS_SKIP_VERIFY | Skip certificate verification |
| TLSCAFile | "" | KAFKA_TLS_CA_FILE | CA certificate file path |
| TLSCertFile | "" | KAFKA_TLS_CERT_FILE | Client certificate file path |
| TLSKeyFile | "" | KAFKA_TLS_KEY_FILE | Client key file path |

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

## Timeout Validation

Consumer timeout parameters must satisfy: `sessionTimeout >= 3 * heartbeatInterval`

## Service Usage

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

### Producer Configuration

```text
kafka://localhost:9092/blocks?partitions=4&replication=3&retention=3600000&flush_frequency=5s
```

### Consumer Configuration

```text
kafka://localhost:9092/subtrees?partitions=8&sessionTimeout=15000&heartbeatInterval=5000
```

### Memory Testing

```text
memory://test_blocks?partitions=2&replay=1
```
