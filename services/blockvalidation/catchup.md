# Blockvalidation Catchup: Design and Lifecycle

This document explains the catchup logic in `services/blockvalidation/` for Teranode. It is written for developers maintaining or extending the blockvalidation service, and intentionally omits code examples.

## Purpose
Catchup brings a node that is behind back to chain tip by efficiently discovering divergence, validating headers, and fetching/validating full blocks while protecting against malicious peers and resource pressure.

## High-Level Flow
The catchup lifecycle is orchestrated by the server in `services/blockvalidation/catchup.go` and proceeds in well-defined steps:

1. **Acquire exclusive catchup lock**
   - Ensures only one catchup runs at a time.
   - Exposes activity via Prometheus gauge.

2. **Fetch headers from peer**
   - Uses a single “headers from common ancestor” style request with a block locator to retrieve headers from oldest to newest.
   - Implemented in `services/blockvalidation/catchup_get_block_headers.go`.
   - Returns a structured result and the peer’s best known header for context and metrics.

3. **Find common ancestor**
   - Computes the best common ancestor between local and remote chains using the header list and locator.
   - Implemented by `services/blockvalidation/catchup/common_ancestor.go` and `common_ancestor_finder.go`.
   - Also determines fork depth.

4. **Validate fork depth**
   - Rejects deep reorganizations beyond coinbase maturity to avoid invalidating immature coinbase spends.

5. **Detect secret mining**
   - Checks if the peer likely withheld blocks to mount a secret-mining attack; flags the peer and records metrics.

6. **Filter headers to process**
   - Keeps only headers after the common ancestor and removes those already present locally to avoid redundant work.

7. **Build header chain cache**
   - Pre-computes and caches header context to reduce database lookups during block validation.
   - Implemented by `services/blockvalidation/catchup/header_chain_cache.go`.

8. **Verify chain continuity**
   - Ensures the first new block connects to a locally known parent (should be the common ancestor).

9. **Fetch and validate blocks**
   - Concurrently fetches full blocks in batches while a validator consumes them in order.
   - Fetch pipeline is defined in `services/blockvalidation/get_blocks.go` with worker pools and ordered delivery for validation.
   - The orchestrator runs fetch and validate in parallel and aggregates errors.
   - When appropriate, the server temporarily moves its FSM into a dedicated catching state and restores it afterwards.

10. **Cleanup**
    - Clears header caches and releases the exclusive lock.

## Design Rationale
- **Single header request from common ancestor**: Minimizes round trips and simplifies control flow by leveraging a block locator pattern and streaming headers oldest→newest.
- **O(log n) ancestor search**: Using locator-based ancestry reduces the complexity of finding the merge point between chains.
- **Header chain cache**: Reduces repeated reads during validation by caching the required context for recent headers.
- **Concurrent fetch, sequential validate**: Parallel network I/O yields throughput; ordered validation preserves chain correctness.
- **Defensive peer handling**: Circuit breakers, per-peer reputation, and explicit secret-mining checks isolate or penalize unreliable peers.

## Key Components
- **Orchestration**: `services/blockvalidation/catchup.go`
  - Step sequencing, exclusive lock, FSM transitions, metrics, logging.
- **Header fetcher**: `services/blockvalidation/catchup_get_block_headers.go`
  - Locator-based header retrieval, per-iteration peer checks, and result aggregation.
- **Common ancestor**: `services/blockvalidation/catchup/common_ancestor.go`, `common_ancestor_finder.go`
  - Common ancestor resolution and chain connection checks.
- **Header cache**: `services/blockvalidation/catchup/header_chain_cache.go`
  - Builds an in-memory cache to accelerate subsequent validation.
- **Header validation**: `services/blockvalidation/catchup/header_validation.go`
  - Basic header checks (proof-of-work, merkle root shape, timestamp) and optional checkpoint validation.
- **Full block fetch pipeline**: `services/blockvalidation/get_blocks.go`
  - High-throughput batch fetches, worker pools for subtree data, ordered delivery to validation.
- **Circuit breaker**: `services/blockvalidation/catchup/circuit_breaker.go`
  - Closed/open/half-open states to prevent cascades; protects the system from bad peers or transient failures.
- **Peer reputation metrics**: `services/blockvalidation/catchup/metrics.go`
  - `PeerCatchupMetrics` track per-peer success/failure counts, response times, reputation, and breaker state.
- **Per-peer breakers**: `services/blockvalidation/catchup/peer_circuit_breakers.go`
  - Manages the circuit breaker per peer.
- **Catchup results**: `services/blockvalidation/catchup/result.go`
  - Aggregated view of a catchup attempt for logging, testing, and introspection.
- **Catchup helpers & errors**: `services/blockvalidation/catchup/helpers.go`, `services/blockvalidation/catchup/errors.go`
  - Helper utilities and standard error values used by the catchup path.

## Concurrency Model
- **Exclusive catchup lock**: Enforces a single catchup at a time across the service.
- **Two primary goroutines during block phase**: One fetcher and one validator coordinated via a channel; an error group waits on both.
- **Worker pools**: The fetcher uses internal worker pools to retrieve block subtree data in parallel while maintaining ordered output to the validator.
- **FSM integration**: During active catching, the server may switch into a catching state and later restore the prior state to minimize interference with normal operation.

## Validation Responsibilities
- **Proof-of-work, merkle root, and timestamp checks**: Basic header-level checks applied before deeper processing.
- **Checkpoint enforcement**: Optional comparison against configured chain checkpoints.
- **Chain continuity**: First new block must connect to an existing local parent.

## Error Handling and Peer Protection
- **Standardized catchup errors** (see `services/blockvalidation/catchup/errors.go`):
  - Circuit open
  - No common ancestor
  - Secret mining detected
  - Invalid header chain
  - Timeout
- **Circuit breakers**: Failures increment breaker state; open breakers short-circuit further requests to problematic peers until half-open retries.
- **Malicious behavior tracking**: Secret-mining and other policy violations are recorded to peer metrics and error counters.
- **Graceful degradation**: Header filtering and continuity checks avoid wasted fetch/validation when the peer’s chain cannot connect.

## Metrics and Observability
- **Prometheus (service level)**: Defined in `services/blockvalidation/metrics.go`.
  - `catchup_active` (gauge): Indicates active catchup (0/1).
  - `catchup_duration_seconds` (histogram with labels: peer, success): End-to-end catchup timing.
  - `catchup_headers_fetched_total` (counter with label: peer): Number of headers retrieved during catchup.
  - `catchup_blocks_fetched_total` (counter with label: peer): Number of full blocks fetched.
  - `catchup_errors_total` (counter with labels: peer, error_type): Error counts; examples include coinbase-maturity violations, validation failures, secret-mining detections.
- **Per-peer reputation**: `PeerCatchupMetrics` in `services/blockvalidation/catchup/metrics.go` track peer-specific behavior for selection and trust decisions.
- **Structured logging**: All steps log with block hash context and peer URL for traceability.

## Integration Points
- **Block processing**: The catchup path integrates with normal block processing in `services/blockvalidation/Server.go`. When parents are missing or the node lags behind, the server triggers catchup and then resumes normal validation once the pipeline is filled.
- **Configuration**: Behavior depends on chain parameters (e.g., coinbase maturity) and blockvalidation settings (e.g., header cache size). See `settings` usage within the catchup orchestrator and helpers.

## Operational Notes
- **Single-run semantics**: Only one catchup should be in progress; concurrent attempts are rejected by the lock.
- **Ordering guarantees**: Validation preserves block order to maintain chain correctness, even though fetch happens concurrently.
- **Cleanup discipline**: Header caches are cleared and FSM is restored regardless of success or failure.

## Failure Modes to Watch
- **No common ancestor**: Misconfigured peers or extreme divergence.
- **Excessive fork depth**: Violates coinbase maturity constraints.
- **Secret mining**: Peer withholds blocks; flagged and recorded.
- **Invalid header chain**: Discontinuity or malformed headers.
- **Timeouts / network instability**: Managed by circuit breakers and error propagation.

This overview should provide enough context to navigate the catchup implementation and extend it safely. For deeper inspection, start at `services/blockvalidation/catchup.go` and follow the step-wise functions in order.

## Protocol Details and Test Implementation Guidelines

### Headers From Common Ancestor Endpoint Protocol

The `headers_from_common_ancestor` endpoint is central to the catchup process. Understanding its behavior is critical for correct implementation and testing:

1. **Request Format**: The endpoint receives a block locator (list of block hashes) and returns headers starting from the common ancestor.

2. **Response Format**: 
   - The response MUST include the common ancestor as the first header
   - Followed by headers AFTER the common ancestor in oldest-to-newest order
   - This allows the receiver to verify the chain connection

3. **Common Ancestor Finding**:
   - The `FindCommonAncestor` function expects to find a match between the locator hashes and the returned headers
   - The common ancestor MUST be present in the returned headers for the algorithm to work
   - After finding the common ancestor, headers are filtered to remove those already in the local chain

### Test Setup Requirements

When writing tests for catchup functionality, the following principles must be followed:

1. **Chain Continuity**:
   - The parent of the first new block must exist in the local blockchain
   - For tests, this often means adding the genesis or parent block to the `blockExists` cache:
     ```go
     server.blockValidation.SetBlockExists(parentBlockHash)
     ```

2. **Mock Setup Order**:
   - Set up `GetBlockExists` mocks BEFORE the HTTP handlers are called
   - Mocks added inside HTTP handlers may not be registered in time
   - Use `.Maybe()` for mocks that might be called multiple times

3. **Header Response Structure**:
   - Always include the common ancestor as the first header in the response
   - For multi-iteration tests, each iteration should include its common ancestor (the last header from the previous iteration)

4. **Block Fetching**:
   - When mocking block fetches, return properly serialized blocks with at least a header and transaction count
   - Empty blocks need: header (80 bytes) + varint for 0 transactions (1 byte)

### Common Test Failures and Solutions

1. **"No common ancestor found"**:
   - Cause: The common ancestor is not included in the returned headers
   - Solution: Ensure the first header in the response is the common ancestor

2. **"Parent block not found - cannot establish chain connection"**:
   - Cause: The parent of the first new block doesn't exist in the local chain
   - Solution: Add the parent block to the `blockExists` cache

3. **"Expected N blocks, got 0"**:
   - Cause: Block parsing fails due to incorrect serialization
   - Solution: Use proper block serialization or mock at a higher level

### Implementation Notes

- The catchup process uses filtered headers (only new ones) for building the header cache and fetching blocks
- The common ancestor finding uses the unfiltered headers from the peer
- This separation can cause confusion in tests if not properly understood
- The `FilterNewHeaders` function removes headers that already exist locally, which may include the common ancestor
