# Blob Deletion API

This document describes the three approaches for completing blob deletions in the blockchain service.

## Overview

The blob deletion system allows the pruner service to delete blobs at specific blockchain heights. The blockchain service manages the scheduling queue, and provides three different APIs for completing deletions based on your use case.

## API Options

### Option 1: Individual Calls (Original API)

**Use Case**: Simple scenarios, testing, backward compatibility

**Methods**:
- `GetPendingBlobDeletions(height, limit)` - Get deletions ready for processing
- `RemoveBlobDeletion(deletion_id)` - Remove one completed deletion
- `IncrementBlobDeletionRetry(deletion_id, max_retries)` - Increment retry for one failed deletion

**Pros**:
- Simple to understand
- Fine-grained control
- Good for testing

**Cons**:
- **N+1 gRPC calls**: For 1000 deletions = 1001 calls (1 get + 1000 removes)
- High network overhead
- Slower performance

**Example**:
```go
// Get pending deletions
deletions, err := blockchainClient.GetPendingBlobDeletions(ctx, currentHeight, 100)

// Process each deletion
for _, deletion := range deletions {
    if err := deleteBlobFromStore(deletion); err != nil {
        // Increment retry
        shouldRemove, _, _ := blockchainClient.IncrementBlobDeletionRetry(ctx, deletion.Id, maxRetries)
        if shouldRemove {
            blockchainClient.RemoveBlobDeletion(ctx, deletion.Id)
        }
    } else {
        // Remove completed
        blockchainClient.RemoveBlobDeletion(ctx, deletion.Id)
    }
}
```

---

### Option 2: Batch Completion API

**Use Case**: Production deployments, high-throughput scenarios

**Methods**:
- `GetPendingBlobDeletions(height, limit)` - Get deletions ready for processing
- `CompleteBlobDeletions(completed_ids[], failed_ids[], max_retries)` - Complete all in one call

**Pros**:
- **2 gRPC calls total**: 1 get + 1 batch complete
- Much faster than individual calls
- Simple to use
- No state management required

**Cons**:
- No automatic locking (client decides when to fetch)
- Multiple pruners might fetch same items (they'll just skip locked rows)

**Example**:
```go
// Get pending deletions
deletions, err := blockchainClient.GetPendingBlobDeletions(ctx, currentHeight, 1000)

completedIDs := []int64{}
failedIDs := []int64{}

// Process all deletions
for _, deletion := range deletions {
    if err := deleteBlobFromStore(deletion); err != nil {
        failedIDs = append(failedIDs, deletion.Id)
    } else {
        completedIDs = append(completedIDs, deletion.Id)
    }
}

// Complete batch in one call
removedCount, retryIncrementedCount, err := blockchainClient.CompleteBlobDeletions(
    ctx, completedIDs, failedIDs, maxRetries)

log.Infof("Batch completed: %d removed, %d retries incremented", removedCount, retryIncrementedCount)
```

**Performance**:
- For 1000 deletions: **2 gRPC calls** (vs 1001 with individual API)
- ~500x fewer network round trips

---

### Option 3: Batch Acquisition API with Locking

**Use Case**: Multiple pruner instances, distributed processing, guaranteed no duplicates

**Methods**:
- `AcquireBlobDeletionBatch(height, limit, lock_timeout)` - Get batch with lock token
- `CompleteBlobDeletionBatch(batch_token, completed_ids[], failed_ids[], max_retries)` - Complete batch

**Pros**:
- **2 gRPC calls total**: 1 acquire + 1 complete
- **Uses `SELECT...FOR UPDATE SKIP LOCKED`**: Guarantees no duplicate processing
- Supports multiple pruner instances safely
- Token-based validation
- Automatic token expiry (default: 5 minutes)

**Cons**:
- Slightly more complex (need to track token)
- Token state held in memory (lost on blockchain service restart)
- Must complete within timeout or batch is released

**Example**:
```go
// Acquire batch with lock
batchToken, deletions, err := blockchainClient.AcquireBlobDeletionBatch(
    ctx, currentHeight, 1000, 300) // 300s = 5min timeout

if batchToken == "" {
    log.Info("No deletions available")
    return
}

completedIDs := []int64{}
failedIDs := []int64{}

// Process all deletions
for _, deletion := range deletions {
    if err := deleteBlobFromStore(deletion); err != nil {
        failedIDs = append(failedIDs, deletion.Id)
    } else {
        completedIDs = append(completedIDs, deletion.Id)
    }
}

// Complete batch with token
err = blockchainClient.CompleteBlobDeletionBatch(
    ctx, batchToken, completedIDs, failedIDs, maxRetries)

if err != nil {
    log.Errorf("Failed to complete batch: %v", err)
    // Batch will be released when token expires
}
```

**Locking Behavior**:
1. `AcquireBlobDeletionBatch` runs: `SELECT...FOR UPDATE SKIP LOCKED`
2. Rows are locked for the duration of the transaction
3. Other pruners calling `AcquireBlobDeletionBatch` skip locked rows
4. Token is generated and stored with deletion IDs
5. Client processes deletions
6. `CompleteBlobDeletionBatch` validates token and completes batch
7. Token is deleted (single-use)

**Token Expiry**:
- Default: 300 seconds (5 minutes)
- Configurable via `lock_timeout_seconds` parameter
- Expired tokens are cleaned up every 60 seconds
- If client doesn't complete before expiry, batch is released (no-op)

---

## Comparison Table

| Feature | Individual API | Batch Completion | Batch Acquisition |
|---------|---------------|------------------|-------------------|
| gRPC calls (1000 items) | 1001 | 2 | 2 |
| Network overhead | Very High | Low | Low |
| Multi-pruner safe | No* | No* | **Yes** |
| State management | None | None | Token (in-memory) |
| Complexity | Simple | Simple | Medium |
| Performance | Slow | Fast | Fast |
| Use case | Testing | Production | Multi-node production |

*Can work with multiple pruners if they process at different times or different height ranges

## Recommended Approach

**For Most Use Cases**: Use **Option 2 (Batch Completion API)**
- 500x fewer network calls
- No state management complexity
- Works well with single or multiple pruners
- Simple implementation

**For Guaranteed No-Duplicate Processing**: Use **Option 3 (Batch Acquisition API)**
- Essential when multiple pruners run simultaneously
- Lock guarantees no row is processed twice
- Slightly more complex but still only 2 gRPC calls

**For Testing/Development**: Use **Option 1 (Individual API)**
- Easier to debug individual operations
- Better for understanding the flow

## Implementation Notes

### SELECT...FOR UPDATE SKIP LOCKED

Both `GetPendingBlobDeletions` and `AcquireBlobDeletionBatch` use `LIMIT` with the locking:

```sql
SELECT id, blob_key, file_type, store_type, delete_at_height, retry_count
FROM scheduled_blob_deletions
WHERE delete_at_height <= $1
ORDER BY delete_at_height ASC, id ASC
LIMIT $2
FOR UPDATE SKIP LOCKED
```

**Key behaviors**:
- `LIMIT` is applied BEFORE locking
- Only the returned rows are locked
- Other rows remain available for other pruners
- `SKIP LOCKED` means: if a row is already locked, skip it and move to the next

This allows efficient parallel processing with multiple pruner instances.

### Token Management

Batch tokens are stored in-memory in the blockchain service:

```go
type blobDeletionBatchToken struct {
    token         string
    acquiredAt    time.Time
    expiresAt     time.Time
    deletionIDs   []int64
    deletionCount int
}
```

- Tokens are single-use (deleted after `CompleteBlobDeletionBatch`)
- Expired tokens cleaned up every 60 seconds
- Tokens are lost if blockchain service restarts (deletions will be reprocessed)

### Database Transaction Handling

All batch operations run in a single transaction:

```go
func CompleteBlobDeletions(ctx, completedIDs, failedIDs, maxRetries) {
    tx := db.BeginTx()
    defer tx.Rollback()
    
    // DELETE completed
    // UPDATE/DELETE failed
    
    tx.Commit()
}
```

This ensures atomicity - either all updates succeed or none do.

## Migration Path

1. **Start with Individual API** (already implemented in pruner)
2. **Migrate to Batch Completion API** for production (simple change)
3. **Consider Batch Acquisition API** if running multiple pruner instances

All three APIs coexist - you can use any combination based on your needs.
