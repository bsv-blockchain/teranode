# Block Persister Service Settings

**Related Topic**: [Block Persister Service](../../../topics/services/blockPersister.md)

## Configuration Settings

Settings are organized under the `BlockPersister` struct in `settings.Settings`.

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| Store | *url.URL | "file://./data/blockstore" | blockpersister_store | **CRITICAL** - Block data storage location |
| HTTPListenAddress | string | "127.0.0.1:8083" | blockpersister_httpListenAddress | HTTP server for blob store access |
| HTTPAuthToken | string | "" | blockpersister_httpAuthToken | **CRITICAL** - bearer token required for blob HTTP writes; unset means read-only |
| Concurrency | int | 8 | blockpersister_concurrency | **CRITICAL** - Parallel subtree processing, reduced by half in all-in-one mode |
| SkipUTXODelete | bool | false | blockpersister_skipUTXODelete | Skip UTXO deletion processing |
| PersistSleep | time.Duration | 10s | blockpersister_persistSleep | Sleep duration when no blocks available or after errors |
| ProcessUTXOFiles | bool | true | blockpersister_processUTXOFiles | Enable UTXO additions/deletions file generation |

### Related Settings (from Block struct)

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| ProcessTxMetaUsingStoreBatchSize | int | 1024 | blockvalidation_processTxMetaUsingStore_BatchSize | **SHARED** - Transaction metadata batch size (shared with Block Validation service) |
| BlockStore | *url.URL | "file://./data/blockstore" | blockstore | Required when HTTP server enabled |

## Configuration Dependencies

### HTTP Server

- When `HTTPListenAddress` is not empty, HTTP server starts
- Requires valid `Block.BlockStore` URL or returns configuration error
- The blob API it exposes requires `Authorization: Bearer <HTTPAuthToken>` on POST, PATCH and
  DELETE. With `HTTPAuthToken` unset the API serves GET and HEAD only and refuses every
  mutating request with 401
- The endpoint is plain HTTP with no TLS, so the token crosses the wire in the clear. It
  binds to loopback by default for that reason; publish it only on a trusted segment
- Clients configure the same value with `blob_httpAuthToken`, never in the store URL — store
  URLs are logged verbatim

### Concurrency Management

- `Concurrency` reduced by half when `IsAllInOneMode` is true
- Minimum concurrency of 1 enforced

### Block Processing Strategy

- `PersistSleep` controls polling frequency when idle and after errors
- Database `persisted_at` column tracks which blocks have been persisted

### Transaction Processing

- Transaction metadata is always fetched in batches, sized by `ProcessTxMetaUsingStoreBatchSize`
- **Note**: `ProcessTxMetaUsingStoreBatchSize` uses the `blockvalidation_` prefix (not `blockpersister_`) as it's a shared setting with the Block Validation service. Both services use the same batch size for consistent transaction metadata processing.

### UTXO File Processing

- When `ProcessUTXOFiles` is true (default), generates `.utxo-additions` and `.utxo-deletions` files for each block
- These files are used by the UTXO Persister service to maintain UTXO sets
- Set to false to disable UTXO file generation for performance in scenarios where UTXO sets are not needed

## Service Dependencies

| Dependency | Interface | Usage |
|------------|-----------|-------|
| BlockStore | blob.Store | **CRITICAL** - Block data storage |
| SubtreeStore | blob.Store | **CRITICAL** - Subtree data storage |
| UTXOStore | utxo.Store | **CRITICAL** - UTXO operations and transaction metadata |
| BlockchainClient | blockchain.ClientI | **CRITICAL** - Block retrieval and operations |

## Validation Rules

| Setting | Validation | Error |
|---------|------------|-------|
| Block.BlockStore | Required when HTTP server enabled | "blockstore setting error" |
| Store | Must be valid URL format | Store creation failure |
| HTTPAuthToken | Required for any blob write over HTTP; unset leaves the API read-only | HTTP 401 Unauthorized |

## Configuration Examples

### Basic Configuration

```bash
blockpersister_store=file://./data/blockstore
blockpersister_persistSleep=10s
```

### High Performance Configuration

```bash
blockpersister_concurrency=16
blockvalidation_processTxMetaUsingStore_BatchSize=2048
```

### HTTP Server Configuration

```bash
blockpersister_httpListenAddress=127.0.0.1:8083
blockstore=file://./data/blockstore

# Required for writes. Set this in settings_local.conf or the environment, never in the
# committed settings.conf, and configure the same value on the client side.
blockpersister_httpAuthToken=<shared-secret>
blob_httpAuthToken=<shared-secret>
```

### Disable UTXO File Processing

```bash
blockpersister_processUTXOFiles=false
```

## Migration Notes

### Listen Address Default

`blockpersister_httpListenAddress` now defaults to `127.0.0.1:8083` instead of `:8083`. The
endpoint is plain HTTP and its blob API accepts writes, so it no longer binds every interface
by default. A deployment that reaches this endpoint from another host must set the address
explicitly.

### Settings Reorganization

The Block Persister settings have been reorganized into a dedicated `BlockPersisterSettings` struct. The following environment variable names have changed:

| Old Variable | New Variable |
|--------------|--------------|
| blockPersisterStore | blockpersister_store |
| blockPersister_httpListenAddress | blockpersister_httpListenAddress |
| blockPersister_persistSleep | blockpersister_persistSleep |

### Removed Settings

The following settings have been **removed** in the current version as persistence state is now tracked in the database:

- `blockPersister_stateFile` - No longer needed, persistence tracked in database `persisted_at` column
- `blockpersister_persistAge` - No longer needed, blocks processed as soon as they're available
- `blockpersister_enableDefensiveReorgCheck` - No longer needed, reorg handling simplified

If you have these settings in your configuration files, they can be safely removed.
