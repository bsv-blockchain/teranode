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
| FSMStateChangeDelay | time.Duration | 0 | fsm_state_change_delay | **TESTING ONLY** - Delays FSM state transitions |

## Configuration Dependencies

### gRPC Server

- `GRPCListenAddress` optional - service runs without it
- Health checks skipped if empty
- `GRPCAddress` used for client connections

### HTTP API Server

- `HTTPListenAddress` required - service fails during Start() if empty
- Provides block invalidation/revalidation endpoints

### FSM State Management

- `FSMStateChangeDelay` delays state transitions for test timing control
- Initial FSM state set via `-localTestStartFromState` CLI argument
- FSM restore mode triggered by RPC service (see RPC settings)

### Database Configuration

- `StoreURL` determines database backend
- Service fails during daemon startup if null

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
