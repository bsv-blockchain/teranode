# Blockchain Service Settings

**Related Topic**: [Blockchain Service](../../../topics/services/blockchain.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| GRPCAddress | string | "localhost:8087" | blockchain_grpcAddress | Client connection address |
| GRPCListenAddress | string | ":8087" | blockchain_grpcListenAddress | gRPC server binding (optional, skips health checks if empty) |
| HTTPListenAddress | string | ":8082" | blockchain_httpListenAddress | **CRITICAL** - HTTP server binding (fails during Start() if empty) |
| MaxRetries | int | 3 | blockchain_maxRetries | Retry attempts for operations |
| RetrySleep | int | 1000 | blockchain_retrySleep | Retry delay timing (milliseconds) |
| StoreURL | *url.URL | "sqlite:///blockchain" | blockchain_store | **CRITICAL** - Database connection (fails during daemon startup if null) |
| FSMStateRestore | bool | false | fsm_state_restore | **UNUSED** - Previously triggered FSM restore via RPC service, implementation is currently disabled |
| FSMStateChangeDelay | time.Duration | 0 | fsm_state_change_delay | **TESTING ONLY** - Delays FSM state transitions |
| StoreDBTimeoutMillis | int | 5000 | blockchain_store_dbTimeoutMillis | Database operation timeout (store-level) |
| InitializeNodeInState | string | "" | blockchain_initializeNodeInState | **UNUSED** - Defined but not referenced in code |

## Configuration Dependencies

### gRPC Server

- `GRPCListenAddress` optional - when empty, gRPC server not started
- Health checks skipped if empty
- Service runs with HTTP API only when empty
- `GRPCAddress` used for client connections

### HTTP API Server

- `HTTPListenAddress` required - service fails during Start() if empty
- Provides block invalidation/revalidation endpoints

### FSM State Management

- `FSMStateRestore`: Currently unused. The implementation that sent a Restore event via RPC is disabled.
- On startup, the blockchain service restores the last persisted FSM state from the database store.
- `FSMStateChangeDelay` delays state transitions for test timing control.
- Initial FSM state set via `-localTestStartFromState` CLI argument.

### Database Configuration

- `StoreURL` determines database backend
- Service fails during daemon startup if null
- `StoreDBTimeoutMillis` passed to blockchain store for database timeout configuration

## Service Dependencies

| Dependency | Interface | Usage |
|------------|-----------|-------|
| BlockchainStore | blockchain_store.Store | **CRITICAL** - Blockchain data persistence |
| KafkaProducer | kafka.KafkaAsyncProducerI | **CRITICAL** - Block publishing to downstream services |

## Validation Rules

| Setting | Validation | Error | When Checked |
|---------|------------|-------|-------------|
| HTTPListenAddress | Must not be empty | "No blockchain_httpListenAddress specified" | During Start() |
| StoreURL | Must not be null | "blockchain store url not found" | During daemon startup |
| GRPCListenAddress | Optional | No error if empty, skips gRPC health checks | During Health() |

## Configuration Examples

### Basic Configuration

```bash
blockchain_grpcListenAddress=:8087
blockchain_httpListenAddress=:8082
blockchain_store=sqlite:///blockchain
```

### PostgreSQL Configuration

```bash
blockchain_store=postgres://user:pass@host:5432/blockchain
```

### Testing Configuration

```bash
fsm_state_change_delay=1s
# Set initial FSM state via CLI argument:
# -localTestStartFromState=IDLE
```
