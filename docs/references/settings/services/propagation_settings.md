# Propagation Service Settings

**Related Topic**: [Propagation Service](../../../topics/services/propagation.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| IPv6Addresses | string | "" | ipv6_addresses | IPv6 multicast addresses for transaction reception |
| IPv6Interface | string | "" | ipv6_interface | Network interface for IPv6 multicast (defaults to "en0") |
| GRPCMaxConnectionAge | time.Duration | 90s | propagation_grpcMaxConnectionAge | **CRITICAL** - gRPC connection lifecycle management |
| HTTPListenAddress | string | "" | propagation_httpListenAddress | **CRITICAL** - HTTP server binding, health checks only run if not empty |
| HTTPAddresses | []string | [] | propagation_httpAddresses | HTTP client connections |
| AlwaysUseHTTP | bool | false | propagation_alwaysUseHTTP | **CRITICAL** - Forces HTTP transport over gRPC |
| HTTPRateLimit | int | 1024 | propagation_httpRateLimit | **CRITICAL** - HTTP API rate limiting (requests/second) |
| SendBatchSize | int | 100 | propagation_sendBatchSize | Batch processing configuration |
| SendBatchTimeout | int | 5 | propagation_sendBatchTimeout | Batch timeout configuration (milliseconds) |
| GRPCAddresses | []string | [] | propagation_grpcAddresses | gRPC client connections |
| GRPCListenAddress | string | "" | propagation_grpcListenAddress | **CRITICAL** - gRPC server binding (service skipped if empty) |

## Configuration Dependencies

### HTTP Server Management
- When `HTTPListenAddress` is not empty, HTTP server starts
- `HTTPRateLimit` controls request rate limiting when HTTP server is active

### Service Startup

- Service skipped (not added to ServiceManager) if `GRPCListenAddress` is empty
- Service requires gRPC server to be configured for operation

### gRPC Server Management

- When `GRPCListenAddress` is not empty, gRPC server starts with connection age management
- `GRPCMaxConnectionAge` controls connection lifecycle

### Transport Selection
- `AlwaysUseHTTP` forces HTTP transport over gRPC for transaction operations
- Affects client-side transport selection in transaction processing

### IPv6 Multicast
- When `IPv6Addresses` is not empty, starts UDP6 listeners
- Uses `IPv6Interface` for network interface selection (defaults to "en0")

## Service Dependencies

| Dependency | Interface | Usage |
|------------|-----------|-------|
| TxStore | blob.Store | **CRITICAL** - Transaction storage and retrieval |
| ValidatorClient | validator.ClientI | **CRITICAL** - Transaction validation operations |
| BlockchainClient | blockchain.ClientI | **CRITICAL** - Blockchain state verification |
| ValidatorKafkaProducer | kafka.KafkaAsyncProducerI | **CRITICAL** - Validator messaging |

## Validation Rules

| Setting | Validation | Impact | When Checked |
|---------|------------|--------|-------------|
| GRPCListenAddress | Must not be empty | Service skipped if empty | During daemon startup |
| HTTPListenAddress | Optional for HTTP server | HTTP server not started if empty | During service initialization |
| IPv6Interface | Defaults to "en0" if empty | Network interface selection | During IPv6 listener setup |

## Configuration Examples

### Basic Configuration

```text
propagation_grpcListenAddress = ":9905"
propagation_httpListenAddress = ":8080"
```

### HTTP Rate Limiting

```text
propagation_httpRateLimit = 2048
propagation_alwaysUseHTTP = false
```

### IPv6 Multicast

```text
ipv6_addresses = "ff02::1"
ipv6_interface = "eth0"
```
