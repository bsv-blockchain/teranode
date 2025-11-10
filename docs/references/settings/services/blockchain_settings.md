# Blockchain Service Settings

**Related Topic**: [Blockchain Service](../../../topics/services/blockchain.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| GRPCAddress | string | "localhost:8087" | blockchain_grpcAddress | Client connection address |
| GRPCListenAddress | string | ":8087" | blockchain_grpcListenAddress | **CRITICAL** - gRPC server binding, health checks only run if not empty |
| HTTPListenAddress | string | ":8082" | blockchain_httpListenAddress | **CRITICAL** - HTTP server binding, service fails if empty |
| MaxRetries | int | 3 | blockchain_maxRetries | Retry attempts for operations |
| RetrySleep | int | 1000 | blockchain_retrySleep | Retry delay timing (milliseconds) |
| StoreURL | *url.URL | "sqlite:///blockchain" | blockchain_store | **CRITICAL** - Database connection |
| FSMStateRestore | bool | false | fsm_state_restore | FSM restore mode trigger |
| FSMStateChangeDelay | time.Duration | 0 | fsm_state_change_delay | **TESTING ONLY** - FSM state transition delay |
| StoreDBTimeoutMillis | int | 5000 | blockchain_store_dbTimeoutMillis | Configuration placeholder |
| InitializeNodeInState | string | "" | blockchain_initializeNodeInState | Initial FSM state for testing |

## Configuration Dependencies

### gRPC Server Health Checks
- Health checks only run if `GRPCListenAddress` is not empty
- `GRPCAddress` used for client connections

### HTTP API Server
- Service fails to start if `HTTPListenAddress` is empty
- Required for block invalidation/revalidation endpoints

### FSM State Management
- `FSMStateRestore` triggers restore mode in RPC service
- `FSMStateChangeDelay` used for test timing control
- `InitializeNodeInState` sets initial test state

### Database Configuration
- `StoreURL` determines database backend
- `StoreDBTimeoutMillis` is placeholder (not implemented)

## Service Dependencies

| Dependency | Interface | Usage |
|------------|-----------|-------|
| BlockchainStore | blockchain_store.Store | **CRITICAL** - Blockchain data persistence |
| KafkaProducer | kafka.KafkaAsyncProducerI | **CRITICAL** - Block publishing to downstream services |

## Validation Rules

| Setting | Validation | Error |
|---------|------------|-------|
| HTTPListenAddress | Must not be empty | "No blockchain_httpListenAddress specified" |
| GRPCListenAddress | Health checks only if not empty | Service monitoring disabled |
| StoreURL | Must be valid URL format | Database connection failure |

## Configuration Examples

### Basic Configuration

```text
blockchain_grpcListenAddress = ":8087"
blockchain_httpListenAddress = ":8082"
blockchain_store = "sqlite:///blockchain"
```

### PostgreSQL Configuration

```text
blockchain_store = "postgres://user:pass@host:5432/blockchain"
```

### Testing Configuration

```text
fsm_state_restore = true
fsm_state_change_delay = 1000
blockchain_initializeNodeInState = "IDLE"
```
