# Block Persister Service Settings

**Related Topic**: [Block Persister Service](../../../topics/services/blockPersister.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| PersisterStore | *url.URL | "file://./data/blockstore" | blockPersisterStore | **CRITICAL** - Block data storage location |
| PersisterHTTPListenAddress | string | ":8083" | blockPersister_httpListenAddress | HTTP server for blob store access |
| BlockPersisterConcurrency | int | 8 | blockpersister_concurrency | **CRITICAL** - Parallel processing, reduced by half in all-in-one mode |
| BatchMissingTransactions | bool | true | blockpersister_batchMissingTransactions | Transaction processing batching |
| ProcessTxMetaUsingStoreBatchSize | int | 1024 | blockvalidation_processTxMetaUsingStore_BatchSize | **SHARED** - Transaction metadata batch size (shared with Block Validation service) |
| SkipUTXODelete | bool | false | blockpersister_skipUTXODelete | **UNUSED** - Not referenced in BlockPersister service |
| BlockPersisterProcessUTXOFiles | bool | true | blockpersister_processUTXOFiles | **POTENTIALLY UNUSED** - May control UTXO file processing |
| BlockPersisterPersistSleep | time.Duration | 1m | blockPersister_persistSleep | Sleep when no blocks available |
| BlockStore | *url.URL | "file://./data/blockstore" | blockstore | Required when HTTP server enabled |

## Configuration Dependencies

### HTTP Server

- When `PersisterHTTPListenAddress` is not empty, HTTP server starts
- Requires valid `BlockStore` URL or returns configuration error

### Concurrency Management

- `BlockPersisterConcurrency` reduced by half when `IsAllInOneMode` is true
- Minimum concurrency of 1 enforced

### Block Processing Strategy

- `BlockPersisterPersistSleep` controls polling frequency when idle
- Database `persisted_at` column tracks which blocks have been persisted

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
| PersisterStore | Must be valid URL format | Store creation failure |

## Configuration Examples

### Basic Configuration

```bash
blockPersisterStore=file://./data/blockstore
blockPersister_persistSleep=1m
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

## Migration Notes

The following settings have been **removed** in the current version as persistence state is now tracked in the database:

- `blockPersister_stateFile` - No longer needed, persistence tracked in database `persisted_at` column
- `blockpersister_persistAge` - No longer needed, blocks processed as soon as they're available
- `blockpersister_enableDefensiveReorgCheck` - No longer needed, reorg handling simplified

If you have these settings in your configuration files, they can be safely removed.
