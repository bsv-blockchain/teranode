# 🔍 Block Validation Service

## Index

1. [Description](#1-description)
2. [Functionality](#2-functionality)
    - [2.1. Receiving blocks for validation](#21-receiving-blocks-for-validation)
    - [2.2. Validating blocks](#22-validating-blocks)
    - [2.2.1. Overview](#221-overview)
    - [2.2.2. Catching up after a parent block is not found](#222-catching-up-after-a-parent-block-is-not-found)
    - [2.2.3. Quick Validation for Checkpointed Blocks](#223-quick-validation-for-checkpointed-blocks)
    - [2.2.4. Validating the Subtrees](#224-validating-the-subtrees)
    - [2.2.5. Block Data Validation](#225-block-data-validation)
    - [2.2.6. Transaction Re-presentation Detection](#226-transaction-re-presentation-detection)
    - [2.3. Marking Txs as mined](#23-marking-txs-as-mined)
3. [gRPC Protobuf Definitions](#3-grpc-protobuf-definitions)
4. [Data Model](#4-data-model)
5. [Technology](#5-technology)
6. [Directory Structure and Main Files](#6-directory-structure-and-main-files)
7. [How to run](#7-how-to-run)
8. [Configuration options (settings flags)](#8-configuration-options-settings-flags)
9. [Other Resources](#9-other-resources)

## 1. Description

The Block Validator is responsible for ensuring the integrity and consistency of each block before it is added to the blockchain. It performs several key functions:

1. **Validation of Block Structure**: Verifies that each block adheres to the defined structure and format, and that their subtrees are known and valid.

2. **Merkle Root Verification**: Confirms that the Merkle root in the block header correctly represents the subtrees in the block, ensuring data integrity.

3. **Block Header Verification**: Validates the block header, including the proof of work , timestamp, and reference to the previous block, maintaining the blockchain's unbroken chain.

![Block_Validation_Service_Container_Diagram.png](img/Block_Validation_Service_Container_Diagram.png)

The Block Validation Service:

- Receives new blocks from the Legacy Service. The Legacy Service has received them from other nodes on the network.
- Validates the blocks, after fetching them from the remote asset server.
- Updates stores, and notifies the blockchain service of the new block.

The Legacy Service communicates with the Block Validation over the gRPC protocol.

![Block_Validation_Service_Component_Diagram.png](img/Block_Validation_Service_Component_Diagram.png)

### Detailed Component Diagram

The detailed component diagram below shows the internal architecture of the Block Validation Service:

![Block_Validation_Component](img/plantuml/blockvalidation/Block_Validation_Component.svg)

> **Note**: For information about how the Block Validation service is initialized during daemon startup and how it interacts with other services, see the [Teranode Daemon Reference](../../references/teranodeDaemonReference.md#service-initialization-flow).

Finally, note that the Block Validation service benefits of the use of Lustre Fs (filesystem). Lustre is a type of parallel distributed file system, primarily used for large-scale cluster computing. This filesystem is designed to support high-performance, large-scale data storage and workloads.
Specifically for Teranode, these volumes are meant to be temporary holding locations for short-lived file-based data that needs to be shared quickly between various services
Teranode microservices make use of the Lustre file system in order to share subtree and tx data, eliminating the need for redundant propagation of subtrees over grpc or message queues. The services sharing Subtree data through this system can be seen here:

![lustre_fs.svg](img/plantuml/lustre_fs.svg)

## 2. Functionality

The block validator is a service that validates blocks. After validating them, it will update the relevant stores and blockchain accordingly.

### 2.1. Receiving blocks for validation

![block_validation_p2p_block_found.svg](img/plantuml/blockvalidation/block_validation_p2p_block_found.svg)

- The Legacy Service is responsible for receiving new blocks from the network. When a new block is found, it will notify the block validation service via the `BlockFound()` gRPC endpoint.
- The block validation service will then check if the block is already known. If not, it will start the validation process.
- The block is added to a channel for processing. The channel is used to ensure that the block validation process is asynchronous and non-blocking.

### 2.2. Validating blocks

#### 2.2.1. Overview

![block_validation_p2p_block_validation.svg](img/plantuml/blockvalidation/block_validation_p2p_block_validation.svg)

- As seen in the section 2.1, a new block is queued for validation in the blockFoundCh. The block validation server will pick it up and start the validation process.
- The server will request the block data from the remote node (`DoHTTPRequest()`).
- If the parent block is not known, it will be added to the catchupCh channel for processing. We stop at this point, as we can no longer proceed. The catchup process will be explained in the next section (section 2.2.2).
- If the parent is known, the block will be validated.
    - First, the service validates all the block subtrees.
        - For each subtree, we check if it is known. If not, we kick off a subtree validation process (see section 2.2.3 for more details).
    - The validator retrieves the last 100 block headers, which are used to validate the block data. We can see more about this specific step in the section 2.2.4.
    - The validator stores the coinbase Tx in the UTXO Store and the Tx Store.
    - The validator adds the block to the Blockchain.
    - Subtrees retain their finite DAH (Delete-At-Height) from assembly/validation. The Block Persister will promote them to permanent (DAH=0) when the block is confirmed on the main chain.
    - For each Tx for each Subtree, we set the Tx as mined in the UTXO Store. This allows the UTXO Store to know which block(s) the Tx is in.
    - Should an error occur during the validation process, the block will be invalidated and removed from the blockchain.

##### Optimistic Mining Mode

The `optimisticMining` setting provides an alternative validation strategy that prioritizes block propagation speed over immediate validation completion. This experimental feature reverses the normal validate-then-add sequence.

**Normal Mode (Default):**

```text
1. Validate block (subtrees, transactions, proof of work)
2. If valid → Add to blockchain
3. Update stores and caches
4. Notify other services
```

**Optimistic Mining Mode:**

```text
1. Add block to blockchain immediately (before full validation)
2. Mark block as existing in cache
3. Validate in background goroutine:
   a. Fetch block headers for validation context
   b. Validate subtrees and transactions
   c. Update stores (subtree DAH, tx metadata)
   d. Mark transactions as mined
4. If validation fails → Queue for revalidation
```

**Technical Implementation:**

The optimistic path is implemented in `ValidateBlock()` (services/blockvalidation/BlockValidation.go:1187-1220):

1. **Immediate Block Addition**: Calls `blockchainClient.AddBlock()` before subtree validation completes
2. **Block Exists Cache**: Sets block as existing to prevent reprocessing
3. **Background Validation**: Spawns goroutine with `optimisticMiningWg.Add(1)`
4. **Decoupled Context**: Uses `tracing.DecoupleTracingSpan()` to prevent context cancellation from blocking background work
5. **Rollback on Failure**: Calls `ReValidateBlock()` if background validation fails

**Configuration:**

- **Setting**: `blockvalidation_optimisticMining` (default: `false`)
- **Runtime Override**: Can be disabled per-block via `ValidateBlockOptions.DisableOptimisticMining`
- **Automatic Disable**: Always disabled during catchup mode for better reliability

**Performance Benefits:**

- **Faster Block Propagation**: Block becomes available to Block Assembly ~100-200ms sooner
- **Reduced Latency**: Miners can build on new blocks before validation completes
- **Higher Throughput**: Decouples block acceptance from validation time

**Risks and Tradeoffs:**

1. **Temporary Invalid Chain State**:

    - Node may temporarily have an invalid block in its chain
    - If validation fails in background, chain state becomes inconsistent until revalidation
    - Other services (Block Assembly, Validator) may work with temporarily invalid state

2. **Fork Risk**:

    - If optimistic block turns out invalid, creates temporary fork
    - Miners building on invalid block waste hash power
    - Requires chain reorganization to remove invalid block

3. **Resource Waste**:

    - Background goroutines consume memory and CPU
    - May process transactions that will later be rejected
    - Stores updated with data that may need rollback

4. **Revalidation Complexity**:

    - Failed blocks queued to `revalidateBlockChan`
    - Revalidation retries up to 3 times
    - After retries exhausted, block marked permanently invalid

**When to Use Optimistic Mining:**

- **Performance Testing**: Measure maximum throughput without validation bottleneck
- **Low-Risk Environments**: Test networks where temporary forks acceptable
- **High-Trust Scenarios**: When block sources are highly trusted (e.g., own miners)

**NOT Recommended For:**

- **Production Networks**: Risk of temporary chain corruption unacceptable
- **Public Nodes**: May propagate invalid blocks to network
- **Financial Applications**: Temporary invalid state could affect transaction processing

**Future Improvements:**

The optimistic mining feature is experimental and currently disabled by default. Future enhancements may include:

- Better rollback mechanisms
- Invalid block notifications to dependent services
- Configurable risk levels (optimistic vs pessimistic thresholds)

For implementation details, see `BlockValidation.go:ValidateBlock()` (lines 1146-1220).

#### 2.2.2. Catching up after a parent block is not found

![block_validation_p2p_block_catchup.svg](img/plantuml/blockvalidation/block_validation_p2p_block_catchup.svg)

When a block parent is not found in the local blockchain, the node initiates a comprehensive catchup process to synchronize with the peer's chain. This process is orchestrated through 11 distinct steps to ensure safe and validated synchronization while protecting against malicious peers.

##### Catchup Process Overview

The catchup orchestration follows this step-by-step sequence (implemented in `catchup()` in `services/blockvalidation/catchup.go`):

1. **Acquire catchup lock** - Prevents concurrent catchups using atomic `isCatchingUp` flag
2. **Fetch headers from peer** - Retrieves block headers using block locator pattern (`headers_from_common_ancestor` endpoint)
3. **Find and validate common ancestor** - Locates the newest block that exists in both chains using O(log n) search
4. **Check coinbase maturity constraints** - Validates fork depth doesn't exceed coinbase maturity (default: 100 blocks)
5. **Detect secret mining attempts** - Checks if common ancestor is too far behind (threshold: default 10 blocks)
6. **Filter headers to process** - Extracts only headers after the common ancestor
7. **Build header chain cache** - Pre-computes validation headers to reduce database queries during validation
8. **Verify chain continuity** - Ensures first block properly connects to local chain
9. **Verify checkpoints** - Validates checkpoint hashes to confirm correct chain before proceeding
10. **Fetch and validate blocks** - Coordinates concurrent fetching with sequential validation
11. **Clean up resources** - Clears header chain cache and releases locks

##### FSM State Transitions During Catchup

The Block Validation service manages FSM (Finite State Machine) state transitions during catchup to coordinate with other services:

**State Transition Flow:**

```text
RUNNING → CATCHINGBLOCKS → RUNNING
```

**When State Changes:**

- **Transition to CATCHINGBLOCKS**: Triggered when new blocks extend the current main chain
    - Calls `blockchainClient.CatchUpBlocks(ctx)` before fetching blocks
    - Prevents Block Assembly from mining during catchup
    - Disables transaction propagation to conserve resources
- **Transition back to RUNNING**: Triggered after catchup completes (success or failure)
    - Calls `blockchainClient.Run(ctx)` to restore normal operations
    - Re-enables Block Assembly and transaction processing

For more information on FSM states and their effects on services, see the [State Management](../architecture/stateManagement.md) documentation.

##### Header Chain Cache

To optimize block validation performance during catchup, the service builds a header chain cache before fetching full blocks:

**Cache Building Process:**

1. Takes all headers that need to be processed (headers after common ancestor)
2. Calls `headerChainCache.BuildFromHeaders(blockHeaders, previousBlockHeaderCount)`
3. Pre-computes validation headers for each block (typically last 11 headers for MTP calculation)
4. Stores in memory for O(1) lookup during validation

**Performance Benefits:**

- Eliminates repeated database queries for previous headers during validation
- Reduces validation latency during catchup
- Particularly effective when catching up on hundreds or thousands of blocks

##### Checkpoint Verification

Before fetching full block data, the catchup process verifies checkpoints to ensure the peer is on the correct chain:

**Verification Steps:**

1. **Get Configured Checkpoints**: Reads checkpoints from `ChainCfgParams.Checkpoints`
2. **Determine Header Range**: Calculates which checkpoints fall within the headers to be processed
3. **Verify Each Checkpoint**: For each checkpoint in range:

    - Maps checkpoint height to header index
    - Compares checkpoint hash with actual header hash
    - Fails catchup immediately if any checkpoint doesn't match
4. **Record Results**: Logs number of checkpoints verified

**Checkpoint-Based Quick Validation:**

- If all checkpoints verify and blocks are below highest checkpoint, quick validation can be used
- Quick validation creates UTXOs and spends transactions in parallel (2-phase process)
- Approximately 10x faster than full validation for historical blocks
- Currently disabled pending additional testing (`useQuickValidation = false`)

**Safety Guarantees:**

Checkpoint verification ensures:

- Cannot be tricked onto a fake chain with valid proof of work
- Detects chain forks before downloading gigabytes of block data
- Provides early exit for catchup from malicious peers

##### Peer Quality Tracking

The catchup process maintains detailed metrics on peer behavior to detect and mitigate malicious activity:

**Metrics Tracked Per Peer:**

- **Malicious Attempts**: Count of detected malicious behaviors (secret mining, invalid blocks)
- **Catchup Successes/Failures**: Success rate for catchup operations from this peer
- **Validation Failures**: Number of blocks from peer that failed consensus validation
- **Response Times**: How quickly peer responds to header/block requests

**Malicious Behavior Detection:**

The system detects and records:

1. **Secret Mining**: Common ancestor more than `secret_mining_threshold` blocks behind (default: 10)
2. **Coinbase Maturity Violation**: Fork depth exceeds coinbase maturity (default: 100 blocks)
3. **Invalid Block Propagation**: Blocks that fail consensus validation (PoW, merkle root, timestamps)
4. **Checkpoint Mismatch**: Peer provides chain with incorrect checkpoint hashes

**Metrics Recording:**

```go
peerMetric := u.peerMetrics.GetOrCreatePeerMetrics(peerID)
peerMetric.RecordMaliciousAttempt()
```

These metrics feed into the circuit breaker pattern (see Section 2.2.9) to temporarily or permanently avoid problematic peers.

##### Error Handling and Peer Quality

**Invalid Block Classification:**

During catchup, blocks are only marked as invalid (`invalid=true`) for consensus violations:

- Proof of work failures (hash doesn't meet target)
- Merkle root mismatches
- Timestamp violations
- Transaction ordering or uniqueness violations

**Recoverable Errors:**

Blocks are NOT marked invalid for:

- Missing subtrees or transaction data (can be recovered)
- Network timeouts or temporary storage failures
- Processing errors that may succeed on retry

**Peer Banning (Logged, Not Implemented):**

When malicious behavior is detected, the system logs:

```text
SECURITY: Peer <peerID> attempted secret mining - should be banned (banning not yet implemented)
```

This provides an audit trail for operators and prepares for future automatic banning.

##### Concurrent Fetch + Sequential Validation

The catchup process optimizes performance by fetching blocks concurrently while validating them sequentially:

**Coordination Pattern:**

1. **Error Group**: Creates `errgroup.WithContext()` to manage two concurrent goroutines
2. **Fetch Goroutine**: Fetches blocks from peer and sends to `validateBlocksChan`
    - Fetches blocks concurrently (respects rate limits)
    - Sends blocks to channel in order
    - Closes channel when all blocks fetched
3. **Validation Goroutine**: Reads from channel and validates blocks sequentially
    - Maintains chain order by validating in sequence
    - Uses cached headers for performance
    - Updates atomic counter after each block
4. **Error Handling**: Either goroutine error terminates both via shared context

**Benefits:**

- Maximizes network and CPU utilization
- Reduces total catchup time by ~50-60%
- Maintains chain integrity through sequential validation
- Provides early termination on first error

**Progress Tracking:**

The system logs progress every 100 blocks:

```text
[catchup] validating block <hash> 100/1000
[catchup] 900 blocks remaining
```

For implementation details, see `catchup.go:fetchAndValidateBlocks()` (lines 554-612).

**Sync Coordination and Peer Selection:**

The catchup process integrates with the P2P service's peer registry and reputation system to select optimal peers for block retrieval:

1. **Peer Selection**: The sync coordinator uses `SelectSyncPeer` to choose the best peer based on:
    - Reputation score (minimum 20.0 threshold)
    - Storage mode (full nodes preferred over pruned)
    - Blockchain height (must be ahead of local node)
    - Response time history
    - Recent interaction success rate

2. **Reputation Updates**: Peer reputation is updated based on catchup results:
    - Successful block retrieval increases reputation
    - Invalid blocks result in severe reputation penalty (malicious marking)
    - Timeouts and failures decrease reputation

3. **Peer Rotation**: If a peer consistently fails during catchup:
    - System automatically rotates to the next best peer
    - Failed peer enters cooldown period before retry
    - Exponential backoff for repeated failures

**Performance Optimizations:**

The catchup process includes several performance optimizations:

- **Concurrent Header Fetching**: Block headers are fetched in parallel before full block retrieval
- **Batch Block Processing**: Multiple blocks are processed in configurable batch sizes
- **Adaptive Concurrency**: Processing parallelism adjusts based on system load
- **Smart Peer Selection**: Preferentially uses peers with lowest latency and highest success rates

For configuration of catchup performance settings, see the [Block Validation Settings Reference](../../references/settings/services/blockvalidation_settings.md).

For details on the peer reputation system, see [Peer Registry and Reputation System](../features/peer_registry_reputation.md).

#### 2.2.3. Quick Validation for Checkpointed Blocks

For blocks that are below known checkpoints in the blockchain, the Block Validation service employs an optimized quick validation path that significantly improves synchronization performance. This mechanism is particularly effective during initial blockchain synchronization.

![quick_validation.svg](img/plantuml/blockvalidation/quick_validation.svg)

##### Quick Validation Process

The quick validation system operates in two distinct phases implemented in `quickValidateBlock()` in `services/blockvalidation/quick_validate.go`:

**Phase 1: UTXO Creation (`createAllUTXOs()`)**

All UTXOs for the block's transactions are created in parallel before validation:

1. **BlockID Stability**: Before creating UTXOs, checks if first transaction already exists with a BlockID
    - On retry scenarios, reuses existing BlockID to maintain consistency
    - Prevents BlockID conflicts when recovering from failures
2. **Synchronous First Transaction**: Creates first transaction synchronously to establish BlockID anchor
    - Ensures BlockID is committed before parallel operations
    - Provides stable reference point for retry detection
3. **Parallel UTXO Creation**: Remaining transactions processed concurrently
    - Concurrency limited by `UtxoStore.StoreBatcherSize * 2`
    - Each UTXO created with `locked=true` flag (two-phase commit)
    - UTXOs created with block ID and mined status pre-set
    - Gracefully handles `ErrTxExists` for recovery scenarios

**Phase 2: Transaction Spending (`spendAllTransactions()`)**

Validates transactions by spending their inputs in parallel:

1. **Parallel Spending**: All transactions validated concurrently
    - Concurrency limited by `SpendBatcherSize * SpendBatcherConcurrency`
    - Uses `IgnoreLocked: true` flag to spend from UTXOs created in Phase 1
2. **Validation Optimization**: Skips expensive script validation for checkpointed blocks
    - Checkpoints guarantee these blocks are valid
    - Focuses on UTXO state consistency rather than script execution

**Final Steps:**

1. **Add Block to Blockchain**: Block added with `SubtreesSet=true` and `MinedSet=true`
2. **Unlock UTXOs**: Calls `SetLocked(txHashes, false)` to complete two-phase commit
3. **Mark Block Exists**: Updates local cache to prevent reprocessing

##### Checkpoint-Based Optimization

The quick validation path is only applied to blocks below verified checkpoints:

- **Checkpoint Verification**: During catchup, checkpoints are verified in header chain (see Section 2.2.2)
- **Eligibility Check**: `block.Height <= highestCheckpointHeight` determines if quick validation can be used
- **Trust Assumption**: Blocks below checkpoints are known to be valid, allowing optimized processing
- **Current Status**: Quick validation currently disabled (`useQuickValidation = false`) pending additional testing

##### Performance Benefits

Quick validation provides substantial performance improvements:

- **10x Faster**: Approximately 10x faster processing for historical blocks below checkpoints
- **Parallel Operations**: UTXO creation and spending both use maximum concurrency
- **Skip Script Validation**: Eliminates most expensive validation step for known-valid blocks
- **Reduced Memory**: On-demand transaction extension minimizes memory footprint

##### Subtree and Transaction Processing

During quick validation, the system reconstructs subtree files and extends transactions on-demand (implemented in `getBlockTransactions()`):

**Subtree File Generation:**

For each subtree in the block, the system:

1. **Reads `.subtreeToCheck`**: Fetches subtree structure with transaction IDs
2. **Reads `.subtreeData`**: Fetches raw transaction data
3. **Validates Coinbase**: Ensures first transaction in first subtree is coinbase
4. **Reconstructs `.subtree`**: If missing, creates full subtree with fee and size information
    - Adds each transaction node with metadata from transaction
    - Stores complete subtree for future use
5. **Generates `.subtreeMeta`**: If missing, creates metadata file with transaction inpoints
    - Stores input relationships for efficient lookups
    - Used by subtree validation service

**Transaction Extension:**

Transactions in standard Bitcoin format are extended in-memory for validation:

1. **Extension Detection**: Checks each transaction with `tx.IsExtended()`
2. **Parent Lookup**: For non-extended transactions:

    - Searches for parent transactions within same block first
    - Falls back to UTXO store for external parents
    - Waits for parent extension if it's being processed concurrently

3. **Input Population**: Sets `PreviousTxSatoshis` and `PreviousTxScript` for each input
4. **Concurrency Control**: Uses errgroup with configurable limits (`GetBlockTransactionsConcurrency`)

**Merkle Root Verification:**

After processing all transactions, the system verifies:

```go
if err := block.CheckMerkleRoot(ctx); err != nil {
    return errors.NewProcessingError("merkle root mismatch")
}
```

This ensures the transactions match the block header before proceeding.

**Error Handling:**

If quick validation encounters any errors:

- Removes `.subtree` files to force reprocessing
- Falls back to normal validation automatically
- Normal validation re-creates UTXOs and validates with full script execution

For implementation details, see `quick_validate.go` in `services/blockvalidation/`.

#### 2.2.4. Validating the Subtrees

Should the validation process for a block encounter a subtree it does not know about, it can request its processing off the Subtree Validation service.

![block_validation_subtree_validation_request.svg](img/plantuml/subtreevalidation/block_validation_subtree_validation_request.svg)

If any transaction under the subtree is also missing, the subtree validation process will kick off a recovery process for those transactions.

#### 2.2.5. Block Data Validation

As part of the overall block validation, the service will validate the block data, ensuring the format and integrity of the data, as well as confirming that coinbase tx, subtrees and transactions are valid. This is done in the `Valid()` method under the `Block` struct.

![block_data_validation.svg](img/plantuml/blockvalidation/block_data_validation.svg)

Effectively, the following validations are performed:

- The hash of the previous block must be known and valid. Teranode must always build a block on a previous block that it recognizes as the longest chain.

- The Proof of Work of a block must satisfy the difficulty target (Proof of Work higher than nBits in block header).

- The Merkle root of all transactions in a block must match the value of the Merkle root in the block header.

- A block must include at least one transaction, which is the Coinbase transaction.

- A block timestamp must not be too far in the past or the future.

    - The block time specified in the header must be larger than the Median-Time-Past (MTP) calculated from the previous block index. MTP is calculated by taking the timestamps of the last 11 blocks and finding the median (More details in BIP113).
    - The block time specified in the header must not be larger than the adjusted current time plus two hours ("maximum future block time").

- The first transaction in a block must be Coinbase. The transaction is Coinbase if the following requirements are satisfied:

    - The Coinbase transaction has exactly one input.
    - The input is null, meaning that the input's previous hash is 0000…0000 and the input's previous index is 0xFFFFFFFF.
    - The Coinbase transaction must start with the serialized block height, to ensure block and transaction uniqueness.

- The Coinbase transaction amount may not exceed block subsidy and all transaction fees (block reward).

#### 2.2.6. Transaction Re-presentation Detection

The Block Validation service implements a robust mechanism for detecting re-presented transactions using bloom filters. This mechanism, implemented in the `validOrderAndBlessed` function, is critical for preventing double-spending and ensuring transaction integrity in the blockchain.

![bloom_filter_validation.svg](img/plantuml/blockvalidation/bloom_filter_validation.svg)

##### Bloom Filter Implementation

Teranode maintains bloom filters for recent blocks to efficiently detect re-presented transactions:

- **Creation**: Each validated block generates a bloom filter containing all of its transaction hashes
- **Storage**: Bloom filters are stored in both memory (for active validation) and in the subtree store (for persistence)
- **Retention**: Filters are maintained for a configurable number of recent blocks (`blockvalidation_bloom_filter_retention_size`)
- **TTL Ordering**: The system enforces a strict TTL (Time-To-Live) ordering: txmetacache < utxo store < bloom filter
    - This ensures that even if a transaction is pruned from txmetacache, the bloom filter can still detect its re-presentation
    - The longer retention period for bloom filters provides an extended window for detecting re-presented transactions

##### The validOrderAndBlessed Mechanism

The `validOrderAndBlessed` function performs several critical validations during block processing:

1. **Transaction Ordering Validation**:

    - Ensures child transactions appear after their parent transactions within the same block
    - For each transaction, verifies that all of its parent transactions either appear earlier in the same block or exist in a previous block on the current chain

2. **Re-presented Transaction Detection**:

    - Efficiently checks if transactions have already been mined in the current chain using bloom filters
    - For potential matches in the bloom filter (which may include false positives), performs definitive verification against the txMetaStore
    - Rejects blocks containing transactions that have already been mined in the current chain

3. **Duplicate Input Prevention**:

    - Tracks all inputs being spent within the block to detect duplicate spends
    - If a transaction is found to be already mined in another block on the same chain, the new block is marked as invalid
    - Ensures no two transactions in the block spend the same input

4. **Orphaned Transaction Prevention**:

    - Verifies that parent transactions of each transaction either exist in the current block (before the child) or in a previous block on the current chain
    - Prevents situations where transactions depend on parents that don't exist or aren't accessible

This comprehensive validation mechanism operates with high concurrency (configurable via `block_validOrderAndBlessedConcurrency`) to maintain performance while ensuring the integrity of the blockchain by preventing double-spends and transaction re-presentations.

#### 2.2.7. Fork Management and Chain Reorganization

The Block Validation service implements a sophisticated fork manager to track and manage parallel chain branches that may occur during blockchain operation. This system is critical for handling chain reorganizations and ensuring the node selects and maintains the correct chain.

##### Fork States and Lifecycle

The fork manager tracks forks through four distinct states:

- **Active**: Fork is currently being processed and built upon
- **Stale**: Fork has had no activity for an extended period (default: 1 hour)
- **Resolved**: Fork has been resolved to the main chain or abandoned
- **Orphaned**: Fork is too far behind the main chain (beyond coinbase maturity)

##### Fork Registration and Tracking

When a block arrives whose parent is not on the current chain tip, the fork manager:

1. **Determines Fork ID**: Uses `DetermineForkID()` to check if the block belongs to an existing fork or creates a new one
2. **Registers Fork**: Creates a new `ForkBranch` with base hash (common ancestor), base height, and creation timestamp
3. **Adds Blocks**: Tracks all blocks belonging to each fork in sequential order
4. **Updates Metrics**: Records fork creation, depth, and lifetime metrics

##### Parallel Fork Processing

The fork manager enforces limits on concurrent fork processing to prevent resource exhaustion:

- **maxParallelForks**: Maximum number of forks that can be processed simultaneously (default: 4, configurable via `blockvalidation_maxParallelForks`)
- **maxTrackedForks**: Maximum total number of forks to track (default: 1000, configurable via `blockvalidation_maxTrackedForks`)
- **BlockProcessingGuard**: Automatic cleanup mechanism using Go finalizers to ensure processing locks are always released

The guard pattern ensures that even if a panic occurs, the processing lock is released:

```go
guard, err := forkManager.MarkBlockProcessing(blockHash)
if err != nil {
    // Handle error
}
defer guard.Release()
// Process block
```

##### Fork Resolution

Forks are resolved when:

1. **Main Chain Updated**: A fork block becomes the new main chain tip
2. **Checkpoint Verification**: Fork passes through a known checkpoint
3. **Manual Resolution**: Operator triggers reconsideration

When a fork is resolved, the fork manager:

- Marks fork state as `Resolved`
- Notifies registered callbacks with resolution details
- Records resolution metrics (depth, lifetime, outcome)
- Schedules cleanup after grace period (5 minutes)

##### Fork Cleanup

The fork manager runs an automatic cleanup routine (configurable interval, default: 5 minutes) that:

- Removes resolved forks after grace period
- Abandons stale forks with no activity
- Orphans forks too far behind main chain
- Applies retention policy to limit memory usage
- Updates aggregate metrics (average depth, longest fork, etc.)

##### Integration with Block Priority Queue

The fork manager coordinates with the block priority queue to:

- Signal when processing slots become available (`priorityQueue.Signal()`)
- Track which blocks are currently being processed per fork
- Prevent concurrent processing of blocks from the same fork
- Requeue blocks from abandoned forks for retry with alternative sources

For implementation details, see `fork_manager.go` in `services/blockvalidation/`.

#### 2.2.8. Block Processing Queue and Priority System

The Block Validation service uses a sophisticated priority queue system to manage the order in which blocks are validated. This ensures that blocks are processed efficiently while handling retries and alternative sources when validation fails.

##### Priority Levels

Blocks are assigned one of three priority levels:

- **PriorityHigh**: Blocks on the current main chain or from trusted sources
- **PriorityMedium**: Blocks that are 1-2 blocks ahead of current tip
- **PriorityLow**: Blocks further ahead or on side chains

The priority determines the order in which blocks are selected for validation when multiple blocks are waiting.

##### Queue Operations

**Adding Blocks:**

When a new block is discovered via `BlockFound()`:

1. Block is assigned a priority based on its height and relationship to current chain
2. Added to the priority queue with retry count initialized to 0
3. If queue is full or limits exceeded, block may be rejected

**Retrieving Blocks:**

The `WaitForBlock()` method:

1. Checks fork manager to ensure processing limits not exceeded
2. Selects highest priority block that can be processed
3. Returns block to worker for validation
4. Blocks waiting if no blocks available (with timeout)

##### Retry Mechanism

When block validation fails due to transient errors:

1. **RequeueForRetry()**: Adds block back to queue with incremented retry count
2. **Exponential Backoff**: Each retry waits longer before reprocessing
3. **Retry Limits**: Maximum retries configurable (default: 3)
4. **Alternative Sources**: After failures, attempts to fetch from different peers

##### Alternative Source Handling

The `GetAlternativeSource()` method:

- Tracks which peer provided the block
- On validation failure, marks peer as potentially problematic
- Searches for alternative peers that may have the same block
- Fetches block data from alternative source if available
- Updates peer quality metrics via circuit breaker

##### Coordination with Fork Manager

The priority queue integrates tightly with the fork manager:

- Checks `CanProcessBlock()` before returning blocks to workers
- Respects parallel fork processing limits
- Receives signals when processing slots become available
- Maintains block-to-fork mappings for efficient lookups

For implementation details, see `block_priority.go` in `services/blockvalidation/`.

#### 2.2.9. Malicious Peer Detection and Tracking

The Block Validation service implements comprehensive peer quality tracking to detect and mitigate malicious behavior during block synchronization and catchup operations.

##### Detection Mechanisms

**Secret Mining Detection:**

The service detects potential secret mining attacks during catchup by checking if:

- Common ancestor is more than `blockvalidation_secret_mining_threshold` blocks behind current tip (default: 10)
- Peer withheld blocks to build a secret chain
- Fork depth exceeds expected values for honest mining

When detected:

```text
1. Log security warning with peer ID and reason
2. Record malicious attempt in peer metrics
3. Update circuit breaker for that peer
4. Reject the catchup operation
5. Ban peer (when banning implemented)
```

**Coinbase Maturity Violation:**

During catchup, the service validates that:

- Fork depth does not exceed coinbase maturity (default: 100 blocks)
- Prevents chain reorganizations that could invalidate spent coinbase outputs
- Protects against attacks attempting to reverse deep transactions

**Invalid Block Propagation:**

The service tracks peers that provide:

- Blocks that fail consensus validation
- Blocks with invalid proof of work
- Blocks that violate checkpoint hashes
- Blocks that fail merkle root verification

##### Peer Quality Metrics

The `PeerCircuitBreakers` and `CatchupMetrics` systems track:

- **Malicious Attempts**: Count of detected malicious behaviors
- **Invalid Blocks**: Number of invalid blocks from this peer
- **Failed Validations**: Transient failures vs consensus violations
- **Response Times**: How quickly peer responds to requests
- **Success Rate**: Percentage of successful validations from this peer

##### Circuit Breaker Pattern

When a peer's quality score falls below threshold:

1. **Circuit Opens**: Requests to this peer are temporarily blocked
2. **Retry Delay**: Exponential backoff before retrying the peer
3. **Alternative Sources**: System uses other peers for block fetching
4. **Circuit Half-Open**: After delay, allows limited test requests
5. **Circuit Closes**: If peer recovers, full access restored

##### Peer Banning (Future Feature)

The system logs when peers should be banned but doesn't yet implement automatic banning:

```text
SECURITY: Peer <ID> attempted secret mining - should be banned (banning not yet implemented)
```

This provides an audit trail for operators to manually ban malicious peers and prepares for future automatic banning implementation.

For implementation details, see `malicious_peer_handling_test.go` and peer tracking code in `catchup.go`.

#### 2.2.10. Invalid Block Tracking and Notifications

The Block Validation service provides comprehensive tracking and notification when blocks fail validation, distinguishing between consensus violations and recoverable errors.

##### Invalid Block Classification

**Consensus Violations (Marked Invalid):**

Blocks are marked as invalid and stored in the blockchain with `invalid=true` when they violate:

- Proof of work requirements (nBits incorrect or hash doesn't meet target)
- Merkle root mismatch
- Timestamp constraints (too far in past/future, MTP violation)
- Coinbase structure rules
- Block size limits
- Transaction ordering or uniqueness rules
- Re-presentation of already-mined transactions

**Recoverable Errors (Not Marked Invalid):**

Blocks are NOT marked invalid for:

- Missing subtrees or transaction data (can be recovered)
- Temporary storage failures
- Network timeouts
- Processing errors that may succeed on retry
- Missing parent blocks (catchup can resolve)

##### Kafka Notification System

When a block is marked invalid due to consensus violation:

1. **Create Message**: `KafkaInvalidBlockTopicMessage` with block hash and reason
2. **Async Publishing**: Message published to Kafka topic in background
3. **Topic Configuration**: Controlled by `kafka_InvalidBlocks` setting
4. **Message Format**:

    ```protobuf
    message KafkaInvalidBlockTopicMessage {
        string block_hash = 1;
        string reason = 2;
    }
    ```

5. **Subscribers**: Other services or monitoring systems can subscribe to track invalid blocks

The Kafka producer is initialized at service startup if the topic is configured:

- Non-blocking async publishing ensures validation performance not impacted
- Producer has message buffer (default: 100 messages)
- Failed publishes are logged but don't block validation

##### Block Revalidation Process

For blocks that fail validation due to recoverable errors, the service queues them for revalidation:

**Revalidation Trigger:**

When `ValidateBlock()` encounters non-consensus errors:

```go
u.ReValidateBlock(block, baseURL)
```

This queues the block to the `revalidateBlockChan` channel.

**Revalidation Worker:**

A background goroutine processes the revalidation queue:

1. **Fetch Fresh Headers**: Gets latest block headers (may have changed)
2. **Re-validate Subtrees**: Attempts subtree validation again
3. **Full Validation**: Runs complete block validation with fresh state
4. **Retry Logic**: Up to 3 retry attempts with increasing delays
5. **Final Outcome**:

    - Success: Block accepted into blockchain
    - Failure after retries: Block marked invalid

**Retry Limits:**

- Maximum retries: 3 (hardcoded in revalidation worker)
- Delay between retries: Managed by revalidation channel
- After exhausting retries: Block marked invalid to prevent infinite retry loops

##### Integration with Block Invalidation

The `markBlockAsInvalid()` function:

1. Logs invalidation event with block hash and reason
2. Calls `kafkaNotifyBlockInvalid()` to publish event
3. Calls `blockchainClient.InvalidateBlock()` to mark in database
4. Updates metrics for tracking

For implementation details, see `InvalidBlocksKafkaProducer_test.go` and `BlockValidation.go` (`markBlockAsInvalid()`, `kafkaNotifyBlockInvalid()`, `reValidateBlock()`).

#### 2.2.11. Subtree Deduplication

The Block Validation service implements a deduplication system to prevent redundant processing of subtrees that may arrive from multiple sources simultaneously.

##### Purpose

During block propagation and validation:

- Multiple peers may send the same subtree
- Subtree validation requests may be duplicated
- Parallel block processing may request same subtrees
- Network retries may resend subtree data

The `DeDuplicator` prevents wasted processing by ensuring each subtree is validated only once.

##### Implementation

**TTL-Based Tracking:**

The deduplicator maintains a time-based cache of recently processed subtrees:

- Each subtree hash is recorded when processing begins
- Entries expire after `blockHeightRetention` period
- Memory automatically cleaned up as entries age out

**Concurrency Safety:**

The deduplicator is thread-safe for concurrent access:

- Multiple validation workers can query simultaneously
- Lock-free reads for checking if subtree is being processed
- Atomic operations for adding new subtree entries

##### Integration with Subtree Validation

When the Block Validation service needs to validate a subtree:

1. **Check Deduplicator**: Query if subtree is already being processed
2. **If Already Processing**: Skip validation, wait for result from first processor
3. **If New Subtree**: Mark as processing in deduplicator, proceed with validation
4. **After Completion**: Entry remains in cache with TTL to prevent duplicate work

This coordination ensures:

- No redundant subtree validation computations
- Reduced load on UTXO store and transaction validators
- Lower memory and CPU usage during high block arrival rates

For implementation details, see `deduplicator.go` in `services/blockvalidation/`.

### 2.3. Marking Txs as mined

When a block is validated and added to the blockchain, the transactions within that block must be marked as mined in the UTXO store. This process is the second phase of the two-phase transaction commit system and is triggered through an event-driven notification mechanism.

#### Trigger Points and Sequence

The process to mark transactions as mined follows a two-step sequence with specific trigger points:

**Step 1: Setting Block Subtrees as Ready (`SetBlockSubtreesSet`)**

This step is triggered at different points depending on who mined the block:

- **Remotely Mined Blocks**: `Block Validation` service triggers this AFTER successfully validating the block
    - Trigger point: Immediately after `blockchainClient.AddBlock()` succeeds in `ValidateBlock()`
    - This ensures the block is fully validated before marking subtrees as set
- **Locally Mined Blocks**: `Block Assembly` service triggers this AFTER successfully assembling and mining a block
    - Trigger point: Immediately after mining the block and adding it to the blockchain
    - Allows local transactions to be marked as mined without waiting for validation

When triggered, the service calls `blockchainClient.SetBlockSubtreesSet(blockHash)` which:

1. Marks the block's subtrees as "set" in the blockchain store
2. Publishes `NotificationType_BlockSubtreesSet` event to all subscribers
3. Allows the system to proceed with marking transactions as mined

![blockchain_setblocksubtreesset.svg](img/plantuml/blockchain/blockchain_setblocksubtreesset.svg)

**Step 2: Marking Transactions as Mined (`SetMinedMulti`)**

This step is triggered automatically via event subscription:

- **Trigger**: `NotificationType_BlockSubtreesSet` event from Blockchain service
- **Subscriber**: Block Validation service subscribes to blockchain notifications during initialization
- **Handler**: `OnBlockchainNotification()` receives the event and processes it

When the event is received, Block Validation:

1. Extracts all transaction hashes from the block's subtrees
2. Calls `utxoStore.SetMinedMulti(txHashes, blockInfo)` to batch-update all transactions
3. Each transaction is updated with:

    - `blockID`: The internal block identifier
    - `blockHeight`: The block's height in the chain
    - `subtreeIdx`: Index of the subtree containing the transaction
    - `locked=false`: Completes the two-phase commit by unsetting the locked flag

![block_validation_set_tx_mined.svg](img/plantuml/blockvalidation/block_validation_set_tx_mined.svg)

#### Integration with Two-Phase Commit

The marking of transactions as mined serves as the **second phase** of the two-phase commit process:

**Phase 1 (Transaction Validation):**

- Validator service creates UTXOs with `locked=true`
- Transactions forwarded to Block Assembly for inclusion in blocks
- Locked flag prevents double-spending before block confirmation

**Phase 2 (Block Validation):**

- Block Validation marks transactions as mined
- `SetMinedMulti` automatically unsets `locked=false` for all transactions
- Transactions now permanently associated with their mined block
- UTXOs become fully spendable in subsequent transactions

#### Responsibility and Exclusivity

The Block Validation service has **exclusive responsibility** for marking transactions as mined, regardless of block source:

- **Remotely Mined Blocks**: Block Validation validates and marks transactions as mined
- **Locally Mined Blocks**: Block Assembly triggers the event, but Block Validation still performs the actual marking
- **Quick Validated Blocks**: During catchup, quick validation calls `SetLocked(txHashes, false)` directly as a batch operation (see Section 2.2.3)

This centralized responsibility ensures consistency in how transactions are marked across all scenarios.

#### Performance Optimization

The `SetMinedMulti` operation is highly optimized for batch processing:

- **Batch Updates**: All transactions in a block updated in single database operation
- **Parallel Processing**: For blocks with many subtrees, subtree processing can be parallelized
- **Lua Script Execution**: Aerospike UTXO store uses atomic Lua scripts for metadata updates
- **Automatic Unlocking**: Locked flag automatically unset as part of mined metadata update

> **For a comprehensive explanation of the two-phase commit process across the entire system, including how Block Validation plays a role in the second phase, see the [Two-Phase Transaction Commit Process](../features/two_phase_commit.md) documentation.**
>
## 3. gRPC Protobuf Definitions

The Block Validation Service uses gRPC for communication between nodes. The protobuf definitions used for defining the service methods and message formats can be seen in the [Block Validation protobuf documentation](../../references/protobuf_docs/blockvalidationProto.md).

## 4. Data Model

- [Block Data Model](../datamodel/block_data_model.md): Contain lists of subtree identifiers.
- [Subtree Data Model](../datamodel/subtree_data_model.md): Contain lists of transaction IDs and their Merkle root.
- [Transaction Data Model](../datamodel/transaction_data_model.md): Comprehensive documentation covering both standard Bitcoin format and Extended Format (BIP-239), including automatic format conversion during block validation.
- [UTXO Data Model](../datamodel/utxo_data_model.md): UTXO and UTXO Metadata data models for managing unspent transaction outputs.

## 5. Technology

1. **Go Programming Language (Golang)**.

2. **gRPC (Google Remote Procedure Call)**:

    - Used for implementing server-client communication. gRPC is a high-performance, open-source framework that supports efficient communication between services.

3. **Blockchain Data Stores**:

    - Integration with various stores such as UTXO (Unspent Transaction Output) store, blob store, and transaction metadata store.

4. **Caching Mechanisms (ttlcache)**:

    - Uses `ttlcache`, a Go library for in-memory caching with time-to-live settings, to avoid redundant processing and improve performance.

5. **Configuration Management (gocore)**:

    - Uses `gocore` for configuration management, allowing dynamic configuration of service parameters.

6. **Networking and Protocol Buffers**:

    - Handles network communications and serializes structured data using Protocol Buffers, a language-neutral, platform-neutral, extensible mechanism for serializing structured data.

7. **Synchronization Primitives (sync)**:

    - Utilizes Go's `sync` package for synchronization primitives like mutexes, aiding in managing concurrent access to shared resources.

## 6. Directory Structure and Main Files

```text
./services/blockvalidation
│
├── BlockValidation.go                      - Core block validation logic and state management.
├── BlockValidation_test.go                 - Unit tests for BlockValidation core logic.
├── BlockValidation_difficulty_test.go      - Difficulty validation tests.
├── BlockValidation_error_test.go           - Error handling tests for block validation.
├── block_priority.go                       - Block priority queue management with retry logic.
├── block_priority_test.go                  - Tests for priority queue functionality.
├── block_priority_get_test.go              - Tests for priority queue retrieval operations.
├── block_priority_coverage_test.go         - Coverage tests for priority queue.
├── block_processing_retry_test.go          - Tests for block processing retry mechanisms.
├── catchup.go                              - Main catchup orchestration with 11-step process.
├── catchup_test.go                         - Comprehensive catchup tests.
├── catchup_consolidated_test.go            - Consolidated catchup scenario tests.
├── catchup_fork_handling_test.go           - Fork handling during catchup tests.
├── catchup_get_block_headers.go            - Block header fetching for catchup.
├── catchup_helpers_test.go                 - Helper functions for catchup tests.
├── catchup_malicious_peer_test.go          - Malicious peer detection tests.
├── catchup_network_resilience_test.go      - Network resilience during catchup tests.
├── catchup_quickvalidation_test.go         - Quick validation during catchup tests.
├── catchup_resource_exhaustion_test.go     - Resource exhaustion scenario tests.
├── catchup_test_suite.go                   - Catchup test suite infrastructure.
├── catchup.md                              - Catchup process documentation.
├── catchup/                                - Catchup implementation package.
│   └── (catchup helper functions)
├── Client.go                               - Client-side API for block validation service.
├── Client_test.go                          - Client tests.
├── deduplicator.go                         - Subtree deduplication to prevent redundant processing.
├── deduplicator_test.go                    - Deduplication tests.
├── fork_manager.go                         - Fork tracking and chain reorganization management.
├── fork_manager_test.go                    - Fork manager tests.
├── fork_manager_atomic_counter_test.go     - Atomic counter tests for fork tracking.
├── fork_manager_coverage_test.go           - Coverage tests for fork manager.
├── fork_manager_metrics_test.go            - Fork manager metrics tests.
├── get_blocks.go                           - Block retrieval and fetching logic.
├── get_blocks_test.go                      - Block retrieval tests.
├── integration_retry_test.go               - Integration tests for retry logic.
├── Interface.go                            - Block validation service interface definition.
├── InvalidBlocksKafkaProducer_test.go      - Tests for Kafka invalid block notifications.
├── malicious_peer_handling_test.go         - Malicious peer tracking and banning tests.
├── metrics.go                              - Prometheus metrics for monitoring.
├── mock.go                                 - Mock implementations for testing.
├── quick_validate.go                       - Optimized validation for checkpointed blocks.
├── quick_validate_test.go                  - Quick validation tests.
├── Server.go                               - Server-side implementation of block validation.
├── Server_test.go                          - Server tests.
├── blockvalidation_api/
│   ├── blockvalidation_api.pb.go           - Auto-generated protobuf Go bindings.
│   ├── blockvalidation_api.proto           - Protocol Buffers API definition.
│   └── blockvalidation_api_grpc.pb.go      - gRPC-specific generated code.
├── testdata/                               - Test fixtures and data.
├── testhelpers/                            - Test helper utilities.
├── ttl_queue.go                            - TTL queue for caching with time-based expiration.
├── txmetacache.go                          - Transaction metadata cache for performance.
└── txmetacache_test.go                     - Transaction metadata cache tests.
```

## 7. How to run

To run the Block Validation Service locally, you can execute the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run . -blockvalidation=1
```

Please refer to the [Locally Running Services Documentation](../../howto/locallyRunningServices.md) document for more information on running the Block Validation Service locally.

## 8. Configuration options (settings flags)

For comprehensive configuration documentation including all settings, defaults, and interactions, see the [block Validation Settings Reference](../../references/settings/services/blockvalidation_settings.md).

## 9. Other Resources

[Block Validation Reference](../../references/services/blockvalidation_reference.md)
