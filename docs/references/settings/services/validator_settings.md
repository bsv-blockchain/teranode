# Validator Service Settings

**Related Topic**: [Validator Service](../../../topics/services/validator.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| GRPCAddress | string | "localhost:8081" | validator_grpcAddress | gRPC client connections |
| GRPCListenAddress | string | ":8081" | validator_grpcListenAddress | **CRITICAL** - gRPC server binding, service only starts if not empty |
| KafkaWorkers | int | 0 | validator_kafkaWorkers | Kafka worker thread configuration |
| SendBatchSize | int | 100 | validator_sendBatchSize | Batch processing size |
| SendBatchTimeout | int | 2 | validator_sendBatchTimeout | Batch processing timeout |
| SendBatchWorkers | int | 10 | validator_sendBatchWorkers | Batch worker thread count |
| BlockValidationDelay | int | 0 | validator_blockvalidation_delay | Block validation delay |
| BlockValidationMaxRetries | int | 5 | validator_blockvalidation_maxRetries | Block validation retry limit |
| BlockValidationRetrySleep | string | "2s" | validator_blockvalidation_retrySleep | Block validation retry delay |
| VerboseDebug | bool | false | validator_verbose_debug | **CRITICAL** - Debug logging control for validation operations |
| HTTPListenAddress | string | "" | validator_httpListenAddress | **CRITICAL** - HTTP server binding, health checks only run if not empty |
| HTTPAddress | *url.URL | "" | validator_httpAddress | HTTP client connections |
| HTTPRateLimit | int | 1024 | validator_httpRateLimit | **CRITICAL** - HTTP request rate limiting |
| KafkaMaxMessageBytes | int | 1048576 | validator_kafka_maxMessageBytes | Kafka message size limits |
| UseLocalValidator | bool | false | useLocalValidator | **CRITICAL** - Local vs remote validator deployment mode |
| TxMetaKafkaBatchSize | int | 1024 | validator_txmeta_kafka_batchSize | Transaction metadata Kafka batch size |
| TxMetaKafkaBatchTimeoutMs | int | 5 | validator_txmeta_kafka_batchTimeoutMs | Transaction metadata Kafka batch timeout (ms) |

## Configuration Dependencies

### gRPC Server Management
- Service only starts if `GRPCListenAddress` is not empty
- Health checks are conditional on gRPC server configuration

### HTTP Server Management
- HTTP server only starts if `HTTPListenAddress` is not empty
- Rate limiting applied when HTTP server is enabled
- Uses `HTTPRateLimit` for middleware configuration

### Debug Logging
- When `VerboseDebug = true`, provides detailed logging for validation operations
- Controls logging in block assembly interactions

### Batch Processing
- `SendBatchSize`, `SendBatchTimeout`, and `SendBatchWorkers` work together
- Controls transaction batch processing performance

### Block Validation Retry Logic
- `BlockValidationMaxRetries`, `BlockValidationRetrySleep`, and `BlockValidationDelay` control resilience
- Manages block validation failure recovery

## Service Dependencies

| Dependency | Interface | Usage |
|------------|-----------|-------|
| UTXOStore | utxo.Store | **CRITICAL** - Transaction input validation and double-spend prevention |
| BlockchainClient | blockchain.ClientI | **CRITICAL** - Blockchain operations and block data |
| BlockAssemblyClient | blockassembly.ClientI | **CRITICAL** - Sending validated transactions to mining |
| ValidatorTxsConfig | *url.URL | **CRITICAL** - Kafka-based transaction validation messaging |

## Validation Rules

| Setting | Validation | Impact | When Checked |
|---------|------------|--------|-------------|
| GRPCListenAddress | Service startup conditional | Service availability | During daemon startup |
| HTTPListenAddress | HTTP server startup conditional | API availability | During service initialization |
| VerboseDebug | Controls logging verbosity | Performance and diagnostics | During validation operations |
| HTTPRateLimit | Rate limiting enforcement | Resource protection | During HTTP middleware setup |

## Configuration Examples

### Basic Configuration

```text
validator_grpcListenAddress = ":8081"
validator_httpListenAddress = ":8080"
```

### Performance Tuning

```text
validator_sendBatchSize = 200
validator_sendBatchWorkers = 20
validator_httpRateLimit = 2048
```

### Debug Configuration

```text
validator_verbose_debug = true
validator_blockvalidation_maxRetries = 10
```
