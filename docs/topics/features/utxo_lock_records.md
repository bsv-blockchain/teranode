# UTXO Lock Record Pattern for Multi-Record Transactions

## Index

1. [Overview](#1-overview)
2. [Purpose and Benefits](#2-purpose-and-benefits)
3. [Architecture](#3-architecture)
    - [3.1. Lock Record Structure](#31-lock-record-structure)
    - [3.2. Creating Flag](#32-creating-flag)
    - [3.3. Record Layout](#33-record-layout)
4. [Two-Phase Commit Protocol](#4-two-phase-commit-protocol)
    - [4.1. Phase 1: Record Creation](#41-phase-1-record-creation)
    - [4.2. Phase 2: Flag Clearing](#42-phase-2-flag-clearing)
    - [4.3. Atomicity Guarantees](#43-atomicity-guarantees)
5. [Error Handling and Recovery](#5-error-handling-and-recovery)
    - [5.1. Partial Failure Scenarios](#51-partial-failure-scenarios)
    - [5.2. Auto-Recovery Mechanisms](#52-auto-recovery-mechanisms)
    - [5.3. StorageError Usage](#53-storageerror-usage)
6. [TTL and Resource Management](#6-ttl-and-resource-management)
7. [Integration with Block Processing](#7-integration-with-block-processing)
8. [Configuration Options](#8-configuration-options)
9. [Monitoring and Debugging](#9-monitoring-and-debugging)
10. [Related Documentation](#10-related-documentation)

## 1. Overview

The Lock Record Pattern is a distributed consistency mechanism used by Teranode's UTXO store to safely handle transactions with more than 20,000 outputs. When a transaction exceeds the Aerospike record size limit, it must be split across multiple records. The lock record pattern ensures these multi-record operations complete atomically, preventing data corruption from partial writes or concurrent access.

The pattern uses two key mechanisms:

1. **Lock Records**: Temporary Aerospike records that prevent concurrent creation attempts for the same transaction
2. **Creating Flag**: A per-record flag that prevents UTXO spending until all records are fully committed

This architecture ensures that even in failure scenarios, UTXOs cannot be spent prematurely, and the system self-heals through automatic recovery.

## 2. Purpose and Benefits

The Lock Record Pattern addresses several critical challenges in handling large transactions:

### Atomic Multi-Record Operations

- **Record Size Limits**: Aerospike limits individual records to ~1MB; large transactions must span multiple records
- **Consistency Guarantee**: All records for a transaction either exist completely or not at all (from a spendability perspective)
- **No Partial Spending**: UTXOs cannot be spent until the entire transaction is committed

### Concurrent Access Protection

- **Duplicate Prevention**: Lock record prevents multiple processes from creating the same transaction simultaneously
- **Race Condition Safety**: Lock acquisition is atomic via CREATE_ONLY policy
- **Clear Ownership**: Lock records include process ID and hostname for debugging

### Failure Recovery

- **Self-Healing**: System automatically recovers from partial failures without manual intervention
- **No Data Loss**: Worst case is temporary inability to spend (not lost funds)
- **Multiple Recovery Paths**: Recovery can occur through retry, re-encounter, or mining operations

### Performance Optimization

- **External Storage**: Large transaction data stored in blob storage, reducing Aerospike load
- **Batch Operations**: Multiple records created in single batch for efficiency
- **TTL-Based Cleanup**: Lock records automatically expire, preventing resource leaks

## 3. Architecture

### 3.1. Lock Record Structure

Lock records are special Aerospike records identified by a unique index (`0xFFFFFFFF`) that cannot conflict with actual sub-records:

```go
const LockRecordIndex = uint32(0xFFFFFFFF)
```

**Lock Record Bins:**

| Bin Name | Type | Description |
|----------|------|-------------|
| `created_at` | `int64` | Unix timestamp of lock creation |
| `lock_type` | `string` | Always "tx_creation" |
| `process_id` | `int` | OS process ID that holds the lock |
| `hostname` | `string` | Host where lock was acquired |
| `expected_recs` | `int` | Number of records to be created |

### 3.2. Creating Flag

The `creating` flag is a boolean bin present on each transaction record during the two-phase commit:

- **True**: Record exists but is part of an incomplete multi-record transaction
- **False/Absent**: Record is fully committed and UTXOs are spendable

The Lua UDF script checks this flag before allowing UTXO spending:

```lua
-- From teranode.lua (spend operation)
if record[creating] then
    return error("UTXO_LOCKED")
end
```

### 3.3. Record Layout

For a transaction with >20,000 outputs, records are organized as:

```text
Transaction with N batches:

┌─────────────────────┐
│   Lock Record       │  Index: 0xFFFFFFFF (temporary)
│   TTL: 30-300s      │
└─────────────────────┘

┌─────────────────────┐
│   Master Record     │  Index: 0
│   - Metadata        │  - TxID, version, fees, etc.
│   - UTXOs 0-19999   │  - First batch of outputs
│   - TotalExtraRecs  │  - Count of additional records
│   - Creating flag   │
└─────────────────────┘

┌─────────────────────┐
│   Child Record 1    │  Index: 1
│   - UTXOs 20000+    │  - Second batch of outputs
│   - Creating flag   │
└─────────────────────┘

┌─────────────────────┐
│   Child Record N-1  │  Index: N-1
│   - Final UTXOs     │  - Last batch of outputs
│   - Creating flag   │
└─────────────────────┘
```

## 4. Two-Phase Commit Protocol

### 4.1. Phase 1: Record Creation

The first phase creates all transaction records with the `creating` flag set to `true`:

1. **Acquire Lock**

    - Create lock record with CREATE_ONLY policy
    - If lock exists, return `TxExistsError` (another process is creating)
    - Calculate dynamic TTL based on number of records

2. **Store External Data**

    - Write transaction bytes to blob storage (S3/filesystem)
    - Use atomic write with existence check

3. **Create Aerospike Records**

    - Prepare all record keys upfront (fail fast on key errors)
    - Add `creating=true` to all bins
    - Execute batch write with CREATE_ONLY policy
    - Handle KEY_EXISTS_ERROR as recovery case

4. **Release Lock**

    - Delete lock record (always, even on partial failure)
    - Partial records remain for next attempt to complete

### 4.2. Phase 2: Flag Clearing

The second phase removes the `creating` flag in a specific order:

1. **Clear Child Records First** (indices 1, 2, ..., N-1)

    - Batch operation with expression filter
    - Only updates records where `creating` bin exists
    - Use UPDATE_ONLY policy

2. **Clear Master Record Last** (index 0)

    - Single record operation
    - Master's flag absence = atomic completion indicator

This ordering ensures:

- If Phase 2 fails midway, master still has flag (incomplete)
- Checking only master is sufficient to determine completion
- Recovery can identify incomplete transactions by master's flag

### 4.3. Atomicity Guarantees

The protocol provides the following guarantees:

| Scenario | State | UTXOs Spendable | Recovery |
|----------|-------|-----------------|----------|
| Phase 1 incomplete | Lock held, partial records | No | Next attempt completes |
| Phase 1 complete, Phase 2 not started | All records with `creating=true` | No | Auto-recovery on retry |
| Phase 2 incomplete | Children cleared, master has flag | No | Master flag checked |
| Phase 2 complete | No `creating` flags | Yes | N/A |

## 5. Error Handling and Recovery

### 5.1. Partial Failure Scenarios

**Lock Acquisition Failure:**

- Another process holds the lock
- Return `TxExistsError` immediately
- No cleanup needed

**Blob Storage Failure:**

- Release lock
- No Aerospike records created
- Clean retry on next attempt

**Partial Record Creation:**

- Some records created, some failed
- Release lock
- Return error but do NOT delete partial records
- Next attempt will find existing records and complete them

**Phase 2 Failure:**

- All records exist with `creating=true`
- Return success (transaction is persisted)
- Log error for monitoring
- Auto-recovery will clear flags

### 5.2. Auto-Recovery Mechanisms

The system self-heals through multiple paths:

1. **Retry Path**

    - When transaction is re-submitted
    - Finds all records exist (KEY_EXISTS_ERROR)
    - Attempts Phase 2 to clear creating flags

2. **Re-Encounter Path**

    - When transaction appears in block or subtree
    - `processTxMetaUsingStore.go` checks for `creating` flag
    - Triggers re-processing to complete commit

3. **Mining Path**

    - When block is mined containing the transaction
    - `SetMined` operation clears creating flags
    - Normal mining flow completes the commit

4. **TTL-Based Lock Release**

    - Lock records automatically expire (30-300 seconds)
    - Prevents permanent lock on process crash
    - Allows other processes to retry

### 5.3. StorageError Usage

The `StorageError` type is used specifically for external storage failures:

```go
errors.NewStorageError("[sendStoreBatch] error writing transaction to external store [%s]", txHash.String())
```

This error type:

- Indicates recoverable storage failures
- Distinguishes from processing errors
- Used by callers to decide retry strategy

## 6. TTL and Resource Management

Lock record TTL is calculated dynamically based on transaction complexity:

```go
TTL = BaseTTL + (PerRecordTTL * NumRecords)
```

**Constants:**

| Constant | Value | Description |
|----------|-------|-------------|
| `LockRecordBaseTTL` | 30 seconds | Minimum lock duration |
| `LockRecordPerRecordTTL` | 2 seconds | Additional time per record |
| `LockRecordMaxTTL` | 300 seconds | Maximum lock duration (5 minutes) |

**Example Calculations:**

- 1 record: 30 + (2 × 1) = 32 seconds
- 10 records: 30 + (2 × 10) = 50 seconds
- 100 records: 30 + (2 × 100) = 230 seconds
- 200+ records: Capped at 300 seconds

The TTL ensures:

- Sufficient time for batch operations to complete
- Automatic cleanup on process crash
- No indefinite locks from abandoned operations

## 7. Integration with Block Processing

### Block Validation Flow

When a block is validated containing a large transaction:

1. Check if transaction exists in UTXO store
2. If `creating` flag is set, transaction is incomplete
3. Re-process transaction to complete Phase 2
4. Continue with block validation

### SetMined Operation

The `SetMined` operation (called when block is accepted) includes:

1. Update block IDs and heights on all records
2. Clear `creating` flag if present
3. Set `UnminedSince` to 0

This provides a final recovery path for any transactions that failed Phase 2.

### Subtree Validation

Subtree validation checks the `creating` flag:

```go
// From processTxMetaUsingStore.go
if txMeta.Creating {
    // Re-process to complete the two-phase commit
    return processTxMetaWithRetry(...)
}
```

## 8. Configuration Options

The lock record pattern uses these configuration settings:

| Setting | Default | Description |
|---------|---------|-------------|
| `utxo_store_batch_size` | 20000 | UTXOs per record (triggers multi-record) |
| `utxo_store_externalize_all_transactions` | false | Force external storage for all transactions |
| `utxo_store_max_tx_size_in_store` | 1MB | Size threshold for external storage |

**Batch Size Impact:**

- Smaller batch = More records = Longer TTL
- Larger batch = Fewer records = Risk of hitting size limits

## 9. Monitoring and Debugging

### Prometheus Metrics

- `utxo_create_batch_size`: Distribution of batch sizes
- `utxo_create_external`: Duration of external storage writes
- `utxo_store_errors`: Error counts by type

### Log Messages

Key log patterns for debugging:

```text
[StoreTransactionExternally] Record N already exists for tx HASH (completing previous attempt)
[StoreTransactionExternally] Transaction HASH created but creating flag not cleared
[clearCreatingFlag] Failed to clear creating flag for child record N
```

### Lock Record Inspection

Lock records can be queried directly in Aerospike:

```sql
SELECT * FROM teranode.utxos WHERE PK = calculateLockKey(txHash)
```

The lock record contains debugging information:

- `process_id`: Which process holds/held the lock
- `hostname`: Which host created the lock
- `expected_recs`: How many records should exist
- `created_at`: When the lock was acquired

## 10. Related Documentation

- [Two-Phase Transaction Commit Process](two_phase_commit.md) - Related two-phase commit for transaction processing
- [UTXO Store Documentation](../stores/utxo.md) - Main UTXO store documentation
- [UTXO Data Model](../datamodel/utxo_data_model.md) - Data structures and fields
- [UTXO Store Reference](../../references/stores/utxo_reference.md) - API reference
- [Error Handling Reference](../../references/errorHandling.md) - StorageError and other error types
