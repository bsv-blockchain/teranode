# Block Persister Service Settings

**Related Topic**: [Block Persister Service](../../../topics/services/blockPersister.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| PersisterStore | *url.URL | "file://./data/blockstore" | blockPersisterStore | **CRITICAL** - Block data storage location |
| StateFile | string | "file://./data/blockpersister_state.txt" | blockPersister_stateFile | **CRITICAL** - Tracks last persisted block for recovery |
| PersisterHTTPListenAddress | string | ":8083" | blockPersister_httpListenAddress | HTTP server for blob store access |
| BlockPersisterConcurrency | int | 8 | blockpersister_concurrency | **CRITICAL** - Parallel processing, reduced by half in all-in-one mode |
| BatchMissingTransactions | bool | true | blockpersister_batchMissingTransactions | Transaction processing batching |
| ProcessTxMetaUsingStoreBatchSize | int | 1024 | blockvalidation_processTxMetaUsingStore_BatchSize | Transaction metadata batch size |
| SkipUTXODelete | bool | false | blockpersister_skipUTXODelete | UTXO deletion behavior |
| BlockPersisterPersistAge | uint32 | 2 | blockpersister_persistAge | **CRITICAL** - Blocks behind tip to avoid reorgs |
| BlockPersisterPersistSleep | time.Duration | 1m | blockPersister_persistSleep | Sleep when no blocks available |
| BlockStore | *url.URL | "" | blockstore | Required when HTTP server enabled |

## Configuration Dependencies

### HTTP Server
- When `PersisterHTTPListenAddress` is not empty, HTTP server starts
- Requires valid `BlockStore` URL or returns configuration error

### Concurrency Management
- `BlockPersisterConcurrency` reduced by half when `IsAllInOneMode` is true
- Minimum concurrency of 1 enforced

### Block Processing Strategy
- `BlockPersisterPersistAge` determines safety margin from chain tip
- `BlockPersisterPersistSleep` controls polling frequency when idle

### Transaction Processing
- When `BatchMissingTransactions` is true, uses `ProcessTxMetaUsingStoreBatchSize`

## Service Dependencies

| Dependency | Interface | Usage |
|------------|-----------|-------|
| BlockStore | blob.Store | **CRITICAL** - Block data storage |
| SubtreeStore | blob.Store | **CRITICAL** - Subtree data storage |
| UTXOStore | utxo.Store | **CRITICAL** - UTXO operations |
| BlockchainClient | blockchain.ClientI | **CRITICAL** - Block retrieval and operations |

## Validation Rules

| Setting | Validation | Error |
|---------|------------|-------|
| BlockStore | Required when HTTP server enabled | "blockstore setting error" |
| StateFile | Must be valid file path | State initialization failure |
| PersisterStore | Must be valid URL format | Store creation failure |

## Configuration Examples

### Basic Configuration

```text
blockPersisterStore = "file://./data/blockstore"
blockPersister_stateFile = "file://./data/blockpersister_state.txt"
blockpersister_persistAge = 2
```

### High Performance Configuration

```text
blockpersister_concurrency = 16
blockpersister_batchMissingTransactions = true
blockvalidation_processTxMetaUsingStore_BatchSize = 2048
```

### HTTP Server Configuration

```text
blockPersister_httpListenAddress = ":8083"
blockstore = "file://./data/blockstore"
```
