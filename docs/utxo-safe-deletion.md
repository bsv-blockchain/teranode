# UTXO Safe Deletion: Delete-At-Height (DAH) Evolution

## Overview

This document describes the evolution of the "delete at height" mechanism for parent transaction cleanup in the UTXO store. Through production experience and analysis, we've refined the approach to provide comprehensive verification of all spending children before parent deletion, ensuring no child transaction can be orphaned.

## The Problem

When a parent transaction becomes fully spent (all its outputs have been consumed by child transactions), we want to delete it to free up storage. However, we must ensure we never delete a parent transaction while its spending children are still unstable - either unmined or subject to reorganization.

### Risk scenario 1: Unmined child

1. Parent TX is fully spent by Child TX (in block assembly)
2. Parent reaches DAH and is deleted from UTXO store
3. Child TX tries to validate, but Parent TX is missing → **validation fails**

### Risk scenario 2: Recently mined child (reorganization)

1. Parent TX is fully spent by Child TX at block height 1000
2. Parent reaches DAH at height 1288 and is deleted
3. Chain reorganization at height 1300 orphans Child TX
4. Child TX gets re-validated, but Parent TX is missing → **validation fails**

All scenarios require: **atomic verification at deletion time, not separate coordination**.

## Design Philosophy: Fail-Safe Deletion

**Core principle:** When designing deletion logic, safety takes priority. Temporary storage bloat is recoverable; premature deletion of data that child transactions depend on is not.

### Learnings from Production

Production deployment revealed edge cases where parent transactions were being deleted while their spending children were not yet stable:

- Some children remained in block assembly longer than expected
- Chain reorganizations affecting recently-mined children
- These cases led to validation failures when children attempted to re-validate

### Current Approach: Comprehensive Verification

Based on these learnings, the refined approach implements:

1. **Positive verification required**: Deletion proceeds only after confirming ALL children are stable
2. **Conservative on uncertainty**: Missing or ambiguous data → skip deletion, retry later
3. **Independent checks**: Multiple verification layers; any failure safely aborts deletion
4. **Predictable failure mode**: Issues manifest as observable storage retention, not data loss

This makes the system self-correcting - parents that should be deleted will be picked up in future cleanup passes once conditions are met.

## Initial Approach

### Mechanism

The deletion mechanism used time-based retention:

```lua
local newDeleteHeight = currentBlockHeight + blockHeightRetention
-- With blockHeightRetention=288, DAH = current + 288 blocks (~2 days)
```

The cleanup service would delete records when `current_height >= deleteAtHeight`.

### Observations from Production

Through production deployment, we discovered edge cases:

1. **Variable mining times**: Some transactions remained in block assembly longer than the 288-block window
2. **Chain reorganizations**: Recently-mined children could be reorganized out
3. **No child verification**: The initial implementation didn't verify child transaction stability before parent deletion

**Example scenario:**

```text
Block 1000: Parent fully spent by Child (in block assembly)
Block 1288: DAH reached → Parent deleted
Block 1300: Child attempts validation → validation failure (parent missing)
```

### Evolution: Adding Parent Preservation

To address these production issues, a preservation mechanism was added:

**Function:** `PreserveParentsOfOldUnminedTransactions()` (in `stores/utxo/cleanup_unmined.go`)

**Approach:**

- Ran periodically via ticker in Block Assembly service
- Identified old unmined transactions
- Set `preserve_until` on their parent transactions to prevent premature deletion
- Provided safety net for long-running unmined transactions

**Characteristics:**

1. **Reactive solution**: Addressed issue after transactions became old
2. **Additional complexity**: Required periodic scanning and parent identification
3. **Focused scope**: Primarily protected parents of very old unmined transactions
4. **Production stabilization**: Successfully prevented validation failures in deployment

This intermediate solution provided production stability while we analyzed the comprehensive fix.

## Current Approach: Comprehensive Child Verification

### Two-Layer Safety Mechanism

The refined implementation uses **two independent safety checks** working together:

#### Layer 1: DAH Eligibility (Lua Script)

```lua
-- Set DAH at normal retention period
local deleteHeight = currentBlockHeight + blockHeightRetention
-- With blockHeightRetention=288, DAH = current + 288 blocks (~2 days)
```

**No additional tracking needed** - spending children are already stored:

```lua
-- When an output is spent, spendMulti() stores the child TX hash in spending_data:
-- UTXO format: [32 bytes utxo_hash][36 bytes spending_data]
--   spending_data = [32 bytes child_tx_hash][4 bytes vin]
--
-- All spending children are implicitly tracked in the UTXOs bin
-- Cleanup extracts all unique children from spent UTXOs
```

**Why verify ALL children?**

- A parent with 100 outputs might be spent by 100 different child transactions
- We must verify **every single child** is stable before deleting the parent
- If even one child is unmined or recently mined, the parent must be kept
- The spending_data already contains all this information - we just extract and verify it

#### Layer 2: Child Stability Verification (Cleanup Service)

When cleanup runs and finds a parent eligible for deletion (DAH reached), it performs an **additional safety check**:

**Aerospike cleanup:**

```go
const safetyWindow = 288

// Batch verify ALL spending children for this parent
allSpendingChildren := getSpendingChildrenSet(parent)
safetyMap := batchVerifyChildrenSafety(allSpendingChildren, currentBlockHeight)

// For each parent:
for _, childHash := range allSpendingChildren {
    if !safetyMap[childHash] {
        // At least one child not stable - SKIP DELETION (fail-safe)
        // ALL children must be stable before parent can be deleted
        return nil
    }
}

// ALL children are stable (mined >= 288 blocks ago) - safe to delete parent
```

**Fail-safe verification logic:**

```go
// Conservative edge case handling in batchVerifyChildrenSafety:

if record.Err != nil {
    // ANY error (including child not found) → BE CONSERVATIVE
    // We require POSITIVE verification, not assumptions
    safetyMap[hexHash] = false
    continue
}

if unminedSince != nil {
    // Child is unmined → NOT SAFE
    safetyMap[hexHash] = false
    continue
}

if !hasBlockHeights || len(blockHeightsList) == 0 {
    // Missing block height data → BE CONSERVATIVE
    safetyMap[hexHash] = false
    continue
}

// Only mark safe after EXPLICIT confirmation of stability
if currentBlockHeight >= maxChildBlockHeight + safetyWindow {
    safetyMap[hexHash] = true  // Positive verification
} else {
    safetyMap[hexHash] = false  // Not yet stable
}
```

**SQL cleanup:**

```sql
DELETE FROM transactions
WHERE id IN (
  SELECT t.id
  FROM transactions t
  WHERE t.delete_at_height IS NOT NULL
    AND t.delete_at_height <= current_height
    AND NOT EXISTS (
      -- Find ANY unstable child - if found, parent cannot be deleted
      -- This ensures ALL children must be stable before parent deletion
      SELECT 1
      FROM outputs o
      WHERE o.transaction_id = t.id
        AND o.spending_data IS NOT NULL
        AND (
          -- Extract child TX hash from spending_data (first 32 bytes)
          -- Check if this child is NOT stable
          NOT EXISTS (
            SELECT 1
            FROM transactions child
            INNER JOIN block_ids child_blocks ON child.id = child_blocks.transaction_id
            WHERE child.hash = substr(o.spending_data, 1, 32)
              AND child.unmined_since IS NULL  -- Child must be mined
              AND child_blocks.block_height <= (current_height - 288)  -- Child must be stable
          )
        )
    )
)
```

**Logic:** Parent can only be deleted if there are NO outputs with unstable children. Even one unstable child blocks deletion.

### How It Works: Timeline Example

**Scenario:** Parent fully spent at block 1000, child mined at block 1001

| Block Height | Event | Previous Approach | Current Approach |
|--------------|-------|-------------------|------------------|
| 1000 | Parent fully spent | DAH = 1288 | DAH = 1288, children in spending_data |
| 1001 | Child mined | - | Child @ height 1001 |
| 1288 | Cleanup runs | Delete (time-based) | Extract and check all children |
| 1288 | Child stability check | - | 1288 - 1001 = 287 < 288 → Skip (wait) |
| 1289 | Cleanup runs | Already deleted | Extract and check all children |
| 1289 | Child stability check | - | 1289 - 1001 = 288 ≥ 288 → Delete safely! |

**Key Improvement:**

- **Previous**: Deletion based solely on time elapsed
- **Current**: Deletion based on verified child stability
- **Benefit**: Handles variable mining times and reorganizations gracefully

**Edge Cases Discovered in Production:**

### Scenario A: Long-running block assembly

| Block Height | Event | Observed Behavior | Impact |
|--------------|-------|-------------------|--------|
| 1000 | Parent fully spent by Child (in block assembly) | DAH = 1288 | - |
| 1200 | Cleanup runs (child still unmined) | ⏳ Not yet time | - |
| 1288 | Cleanup runs | Parent deleted | Child validation fails |
| 1300 | Child attempts validation | Parent missing | Service degradation |

### Scenario B: Chain reorganization

| Block Height | Event | Observed Behavior | Impact |
|--------------|-------|-------------------|--------|
| 1000 | Parent fully spent by Child | DAH = 1288 | - |
| 1100 | Child mined | Child @ height 1100 | - |
| 1288 | Cleanup runs | Parent deleted | - |
| 1300 | Chain reorg orphans child | Child needs re-validation | Validation fails |

These production observations informed the comprehensive verification approach.

## Safety Guarantees

### Edge Case 1: Child Never Mined

**Scenario:** Parent fully spent at block 1000, child remains in block assembly

| Block Height | Event | New System Behavior |
|--------------|-------|---------------------|
| 1000 | Parent fully spent (child in block assembly) | DAH = 1288, children in spending_data |
| 1288 | Cleanup runs | Child unmined → ❌ Skip (keeps parent) |
| 1500 | Cleanup runs | Child still unmined → ❌ Skip (keeps parent) |
| 2000 | Cleanup runs | Child still unmined → ❌ Skip (keeps parent) |

**Result:** Parent is never deleted as long as child remains unmined. This is correct behavior - we cannot delete a parent whose child might still validate.

### Edge Case 2: Child Mined Very Late

**Scenario:** Parent fully spent at block 1000, child not mined until block 1500

| Block Height | Event | New System Behavior |
|--------------|-------|---------------------|
| 1000 | Parent fully spent | DAH = 1288, children in spending_data |
| 1288 | Cleanup runs | Child not mined yet → ❌ Skip |
| 1500 | Child finally mined | Child @ height 1500 |
| 1788 | Cleanup runs | 1788 - 1500 = 288 → ✅ Delete safely! |

**Result:** The parent waits until the child is actually mined AND stable, regardless of how long that takes.

### Edge Case 3: Chain Reorganization

**Scenario:** Chain reorg orphans the child

| Block Height | Event | System Behavior |
|--------------|-------|-----------------|
| 1000 | Parent fully spent by Child | DAH = 1288, children in spending_data |
| 1100 | Child mined | Child @ height 1100 |
| 1200 | Chain reorg orphans Child | Child moved to unmined state |
| 1288 | Cleanup runs | Child unmined → ❌ Skip (keeps parent) |
| 1300 | Child re-mined at new height | Child @ height 1300 |
| 1588 | Cleanup runs | 1588 - 1300 = 288 → ✅ Delete safely! |

**Result:** The parent is preserved through the reorg and only deleted after child stability is re-established at the new height.

### Edge Case 4: Service Restart Race Condition (Previous Approach)

**Scenario:** System restarts while parents have unmined children

| Event | Previous Approach Behavior | Vulnerability |
|-------|---------------------------|---------------|
| Block 1000: Parent spent by unmined child | DAH = 1288 | - |
| Block 1400: System shutdown | preserve_until not set (preservation hasn't run yet) | - |
| Block 1500: System restart | - | - |
| Block 1500: Cleanup service starts | Sees parent with DAH=1288, preserve_until=NULL | - |
| Block 1500: Cleanup runs BEFORE preservation ticker | Parent deleted (no preservation yet) | **Parent deleted prematurely** |
| Block 1500: Preservation ticker finally starts | Tries to preserve parent (too late) | Child orphaned |

**Architectural issue:** Previous approach relied on coordination between two independent processes:

- Cleanup service (continuous)
- Preservation ticker (periodic)

After restart, cleanup could run before preservation caught up, creating a race window.

**Current approach eliminates this race:**

- Verification happens atomically during cleanup
- No separate preservation process needed
- No coordination required
- Robust to restarts, delays, or timing issues

## Implementation Details

### Removed: Periodic Parent Preservation Workaround

**Deleted files:**

- `stores/utxo/cleanup_unmined.go` (126 lines)
- `stores/utxo/tests/cleanup_unmined_test.go` (415 lines)
- `test/e2e/daemon/wip/unmined_tx_cleanup_e2e_test.go` (488 lines)

**Deleted function:** `PreserveParentsOfOldUnminedTransactions()`

This periodic preservation workaround is no longer needed because:

- **Old approach**: React to old unmined transactions by preserving their parents
- **New approach**: Proactively verify child stability before any deletion
- The new last_spender verification provides comprehensive protection, making the band-aid unnecessary

### Lua Script Changes (teranode.lua)

**Modified:** `setDeleteAtHeight()` function - No longer uses excessive buffer

```lua
-- DAH now set at normal retention (same as old system)
local conservativeDeleteHeight = currentBlockHeight + blockHeightRetention
```

**Key insight:** No separate tracking needed!

```lua
-- We do NOT track spending children separately
-- They are already embedded in UTXO spending_data (bytes 32-64 of each spent UTXO)
-- Cleanup service extracts ALL children from spent UTXOs for verification
```

This is more robust because:

- Cannot miss any children (all are in spending_data)
- No risk of tracking only "last" child
- Simpler code - no special tracking logic needed

### Aerospike Cleanup Service Changes

**Modified:** `processRecordChunk()` function

- Extracts ALL unique spending children from UTXO spending_data
- Scans every spent UTXO to find all child TX hashes
- Builds map of parent -> all children

**Enhanced:** `batchVerifyChildrenSafety()` function

- Batch reads ALL unique spending children in a chunk
- Checks each child's mined status and block height
- Returns safety map: `childHash -> isSafe`

**Modified:** `processRecordCleanupWithSafetyMap()`

- Verifies ALL children are stable (not just one)
- If ANY child is unstable, skips deletion
- Parent will be reconsidered in future cleanup passes

### SQL Cleanup Service Changes

**Modified:** `deleteTombstoned()` query

- Uses `NOT EXISTS` to find ANY unstable child
- Extracts ALL children from `outputs.spending_data`
- Verifies ALL children are mined and stable
- If ANY child fails verification, parent is kept
- Comprehensive query that checks every spending child

### Database Schema

**No new fields required!**

The spending children information is already present in existing data:

- **Aerospike**: Embedded in UTXO `spending_data` (bytes 32-64 of each spent UTXO)
- **SQL**: Stored in `outputs.spending_data` column (bytes 1-32 contain child TX hash)

This approach is superior because:

- No redundant data storage
- Cannot get out of sync
- Automatically tracks ALL children
- Simpler schema

## Benefits

### 1. Addresses Production Edge Cases

- Observed cases where children remained in block assembly past retention window
- Chain reorganizations affecting recently-mined transactions
- Verification ensures children are both mined AND stable before parent deletion
- Gracefully handles variable mining times

### 2. Fail-Safe Operational Characteristics

- Verification uncertainties lead to retention rather than deletion
- Observable in storage metrics
- Self-correcting through retry mechanism
- Allows investigation before any service impact

### 3. Maintains Retention Efficiency

- Uses same 288-block (~2 day) retention window
- Adds verification layer without extending base retention
- Optimal case (immediate mining): similar deletion timing
- Edge cases (delayed mining): waits appropriately for stability

### 4. Robust Safety Properties

- Verifies all spending children, regardless of count
- Multiple independent verification layers
- Handles variable mining times and reorganizations
- Conservative defaults on ambiguous cases

### 5. Database Operation Trade-offs

**Previous approach (main + preservation):**

Case 1: Parent whose child mines normally (no preservation)

- Spend: 1 read (parent), 1 write (set DAH)
- Cleanup: 1 read (parent), 1 delete, M writes (mark child as deleted in M parent TXs via deletedChildren map)
- **Total: 2 reads, 1+M writes**

Case 2: Parent whose child remains unmined (triggers preservation)

- Spend: 1 read (parent), 1 write (set DAH)
- Preservation: 1 index scan, 1 read (child TxInpoints), 1 write (set preserve_until on parents)
- Cleanup skips: K reads (while preserved)
- Expiration: 1 read (batch), 1 write (clear preserve, set DAH)
- Final cleanup: 1 read (parent), 1 delete, M writes (deletedChildren)
- **Total: 3+K reads, 3+M writes**

**Current approach (all cases):**

- Spend: 1 read (parent), 1 write (set DAH)
- Cleanup: 1 read (parent + UTXOs), N reads (verify N children via BatchGet), 1 delete, M writes (deletedChildren)
- **Total: 2+N reads, 1+M writes**

Where:

- N = unique children for this parent (typically 2-3, up to 100+)
- M = number of inputs (parents to update with deletedChildren) - same in both approaches
- K = number of cleanup skip attempts in previous preservation case

**Analysis:**

The current approach **always performs child verification reads** (N reads):

- **Best case**: Parent with 2 children → 2+2 = 4 reads (vs 2 reads if no preservation needed)
- **Typical case**: Parent with 2-3 children → 4-5 reads (vs 2-5 reads depending on preservation)
- **Worst case**: Parent with 100 unique children → 100+ reads (vs 2-5 reads)

**Trade-off evaluation:**

- Previous approach: Variable overhead (only on problematic cases)
- Current approach: Consistent overhead (on ALL parents)
- Current does more work in common case, less in problematic case
- Current is heavier on database but provides comprehensive verification

**Honest assessment:** The current approach increases database load by adding child verification reads to every parent deletion. This overhead is the cost of comprehensive safety verification - we're choosing correctness over efficiency.

### 6. Simplified Architecture

- Replaces periodic preservation with direct verification
- Removed `cleanup_unmined.go` and associated tests (1000+ lines)
- Single verification approach replaces multi-stage process
- Uses existing data (spending_data) rather than separate tracking
- Easier to reason about and maintain

### 7. Clearer Intent

- Code explicitly documents safety mechanism
- Easy to understand and verify correctness
- Separates concerns (eligibility vs. verification)
- Fail-safe approach makes debugging straightforward

## Backward Compatibility

### Schema Migration

**No schema changes required!**

The new implementation uses existing data:

- **Aerospike**: Reads `utxos` bin (already exists) to extract spending children
- **SQL**: Queries `outputs.spending_data` column (already exists)

**Migration:**

- Zero schema changes needed
- No database migrations
- Works immediately on existing data
- Backward compatible with all existing records

### Handling All Records (New and Old)

The cleanup logic works identically for all records:

1. Query records where `delete_at_height <= current_height`
2. For each record, extract ALL spending children from spending_data
3. Verify ALL children are mined and stable
4. Only delete if ALL children pass verification

**This works for:**

- New records (created with new code)
- Old records (created before this change)
- Conflicting transactions (no spending children)
- Edge cases (missing data → conservative, skip deletion)

## Conclusion

The safe deletion mechanism has evolved through three stages, each addressing observed challenges in production.

### Evolution Timeline

#### Stage 1: Initial Implementation

- Time-based deletion using DAH
- Simple and predictable
- Worked well for typical cases

#### Stage 2: Production Hardening

- Added periodic parent preservation
- Addressed edge cases with long-running transactions
- Provided operational stability

#### Stage 3: Comprehensive Solution (This Branch)

- Direct verification of all children
- Consolidates previous multi-stage approach
- More efficient and complete

### Key Characteristics

#### Comprehensive Verification

- Checks ALL spending children, not just one representative
- Handles parents with many outputs spent by different children
- Prevents edge case where early children could be orphaned

#### Architectural Simplification

- Eliminated multi-process coordination (cleanup + preservation)
- Removed 1000+ lines of workaround code
- Single atomic verification at deletion time
- No race conditions between independent processes

#### Production-Safe Design

- Verification issues manifest as observable retention
- Self-correcting through retry mechanism
- Clear operational signals for troubleshooting
- Storage metrics indicate when children block deletion

### Summary

The evolution of the deletion mechanism reflects iterative refinement based on production experience. Each stage addressed observed challenges:

1. **Initial**: Time-based deletion (simple, predictable)
2. **Intermediate**: Added preservation for old unmined transactions (production stabilization)
3. **Current**: Comprehensive child verification (complete solution)

**Architecture evolution:**

- Consolidated multi-stage process into single atomic verification
- Leverages existing data (spending_data) rather than additional tracking
- Reduced code complexity (1000+ lines removed)
- Eliminated race conditions and process coordination complexity
- Trade-off: More database reads during cleanup for comprehensive verification

#### Key design principle: Verify ALL children

For parents with multiple outputs spent by different children:

**Multi-child example:**

```text
Block 100: Child A spends parent output 0
Block 200: Child B spends parent output 1 (makes parent fully spent)
Block 488: Cleanup must verify BOTH Child A and Child B are stable
Block 500: If Child A reorganized but only Child B was checked → validation failure
```

**Implementation approach:**

- Extract ALL spending children from spending_data (already embedded in UTXOs)
- Verify EVERY child is mined and stable
- If ANY child unstable → parent kept
- Ensures no child can be orphaned

**Technical details:**

- **Aerospike**: Scans all spent UTXOs to extract children (bytes 32-64 of each UTXO)
- **SQL**: Query checks all `outputs.spending_data` (bytes 1-32 of spending_data)
- **Both**: Single batch verification call for all unique children in a chunk
- Efficient due to batching and data locality
