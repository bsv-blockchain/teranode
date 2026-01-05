# Block Persister Service Settings

**Related Topic**: [Block Persister Service](../../../topics/services/blockPersister.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| PersisterStore | *url.URL | "file://./data/blockstore" | blockPersisterStore | **CRITICAL** - Block data storage location |
| StateFile | string | "" | blockPersister_stateFile | **CRITICAL** - Tracks last persisted block for recovery (auto-derived for file:// stores) |
| PersisterHTTPListenAddress | string | ":8083" | blockPersister_httpListenAddress | HTTP server for blob store access |
| BlockPersisterConcurrency | int | 8 | blockpersister_concurrency | **CRITICAL** - Parallel processing, reduced by half in all-in-one mode |
| BatchMissingTransactions | bool | true | blockpersister_batchMissingTransactions | Transaction processing batching |
| ProcessTxMetaUsingStoreBatchSize | int | 1024 | blockvalidation_processTxMetaUsingStore_BatchSize | **SHARED** - Transaction metadata batch size (shared with Block Validation service) |
| SkipUTXODelete | bool | false | blockpersister_skipUTXODelete | **UNUSED** - Not referenced in BlockPersister service |
| BlockPersisterProcessUTXOFiles | bool | true | blockpersister_processUTXOFiles | **POTENTIALLY UNUSED** - May control UTXO file processing |
| BlockPersisterPersistAge | uint32 | 2 | blockpersister_persistAge | **CRITICAL** - Blocks behind tip to avoid reorgs |
| BlockPersisterPersistSleep | time.Duration | 1m | blockPersister_persistSleep | Sleep when no blocks available |
| BlockPersisterEnableDefensiveReorgCheck | bool | true | blockpersister_enableDefensiveReorgCheck | **CRITICAL** - Enables defensive reorg detection checks |
| BlockStore | *url.URL | "file://./data/blockstore" | blockstore | Required when HTTP server enabled |

## Configuration Dependencies

### HTTP Server

- When `PersisterHTTPListenAddress` is not empty, HTTP server starts
- Requires valid `BlockStore` URL or returns configuration error

### State File Management

- For file:// stores, `StateFile` auto-derived from `PersisterStore` path if not explicitly set
- For non-file stores (S3, etc.), explicit `StateFile` required or configuration error occurs
- Auto-derived path: `{PersisterStore_path}/blockpersister_state.txt`

### Concurrency Management

- `BlockPersisterConcurrency` reduced by half when `IsAllInOneMode` is true
- Minimum concurrency of 1 enforced

### Block Processing Strategy

- `BlockPersisterPersistAge` determines safety margin from chain tip
- `BlockPersisterPersistSleep` controls polling frequency when idle

### Reorg Detection

- `BlockPersisterEnableDefensiveReorgCheck` enables defensive checks for blockchain reorganizations
- Validates last persisted block is still on current chain
- Validates parent hash consistency during block retrieval
- Can be disabled for testing or special scenarios

### Transaction Processing

- When `BatchMissingTransactions` is true, uses `ProcessTxMetaUsingStoreBatchSize`
- **Note**: `ProcessTxMetaUsingStoreBatchSize` uses the `blockvalidation_` prefix (not `blockpersister_`) as it's a shared setting with the Block Validation service. Both services use the same batch size for consistent transaction metadata processing.

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

```bash
blockPersisterStore=file://./data/blockstore
blockPersister_stateFile=file://./data/blockpersister_state.txt
blockpersister_persistAge=2
blockpersister_enableDefensiveReorgCheck=true
```

### High Performance Configuration

```bash
blockpersister_concurrency=16
blockpersister_batchMissingTransactions=true
blockvalidation_processTxMetaUsingStore_BatchSize=2048
```

### HTTP Server Configuration

```bash
blockPersister_httpListenAddress=:8083
blockstore=file://./data/blockstore
```
