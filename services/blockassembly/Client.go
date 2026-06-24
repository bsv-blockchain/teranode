// Package blockassembly provides functionality for assembling Bitcoin blocks in Teranode.
package blockassembly

import (
	"context"
	"net/http"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/batchermetrics"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// batchItem represents an item in a transaction batch.
// This structure encapsulates a transaction request along with a channel
// for signaling completion, enabling asynchronous batch processing while
// still allowing the caller to wait for individual transaction results.

type batchItem struct {
	// req contains the transaction request
	req *blockassembly_api.AddTxRequest

	// done signals completion of batch processing
	done chan error
}

// Client implements the ClientI interface for block assembly operations.
// It provides a high-level API for interacting with the block assembly service,
// handling communication details, request formatting, and response processing.
//
// This client includes built-in batching support for transaction submission to improve
// performance when processing large numbers of transactions. It also provides methods
// for mining operations, service status checks, and block assembly management.

type Client struct {
	// client is the gRPC client for block assembly API
	client blockassembly_api.BlockAssemblyAPIClient

	// logger provides logging functionality
	logger ulogger.Logger

	// settings contains configuration parameters
	settings *settings.Settings

	// batchSize determines the size of transaction batches
	batchSize int

	// batchCh handles batch processing
	batchCh chan []*batchItem

	// batcher manages transaction batching
	batcher *batcher.Batcher[batchItem]
}

// NewClient creates a new block assembly client.
//
// Parameters:
//   - ctx: Context for cancellation
//   - logger: Logger for operations
//   - tSettings: Teranode settings configuration
//
// Returns:
//   - *Client: New client instance
//   - error: Any error encountered during creation
func NewClient(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings) (*Client, error) {
	blockAssemblyGrpcAddress := tSettings.BlockAssembly.GRPCAddress
	if blockAssemblyGrpcAddress == "" {
		return nil, errors.NewConfigurationError("no blockassembly_grpcAddress setting found")
	}

	maxRetries := tSettings.BlockAssembly.GRPCMaxRetries

	retryBackoff := tSettings.BlockAssembly.GRPCRetryBackoff
	if retryBackoff == 0 {
		return nil, errors.NewConfigurationError("blockassembly_grpcRetryBackoff setting error")
	}

	baConn, err := util.GetGRPCClient(
		ctx,
		blockAssemblyGrpcAddress,
		&util.ConnectionOptions{
			MaxRetries:   maxRetries,
			RetryBackoff: retryBackoff,
			CallerName:   "blockassembly",
		}, tSettings,
	)
	if err != nil {
		return nil, errors.NewServiceError("failed to connect to block assembly", err)
	}

	batchSize := tSettings.BlockAssembly.SendBatchSize
	sendBatchTimeout := tSettings.BlockAssembly.SendBatchTimeout

	if batchSize > 0 {
		logger.Infof("Using batch mode to send transactions to block assembly, batches: %d, timeout: %d", batchSize, sendBatchTimeout)
	}

	duration := time.Duration(sendBatchTimeout) * time.Millisecond

	client := &Client{
		client:    blockassembly_api.NewBlockAssemblyAPIClient(baConn),
		logger:    logger,
		settings:  tSettings,
		batchSize: batchSize,
		batchCh:   make(chan []*batchItem),
	}

	sendBatch := func(batch []*batchItem) {
		client.sendBatchToBlockAssembly(ctx, batch)
	}
	b := batcher.NewWithPool(batchSize, duration, sendBatch, tSettings.BatcherBackground,
		batcher.WithName("blockassembly_client"),
		batcher.WithLogger(logger),
		batcher.WithMetrics(batchermetrics.Provider()),
		batcher.WithTracer(tracing.Tracer("blockassembly").OTelTracer()),
	)
	if tSettings.BatcherDrainMode {
		b.SetDrainMode(true)
	}
	// Tick interval after drain: go-batcher's drain-wins guard no-ops + warns under
	// drain. Default 0 = disabled.
	if ms := tSettings.BlockAssembly.SendBatchTickerIntervalMillis; ms > 0 {
		b.SetTickInterval(time.Duration(ms) * time.Millisecond)
	}
	if tSettings.BlockAssembly.SendBatchMaxConcurrent > 0 {
		b.SetMaxConcurrent(tSettings.BlockAssembly.SendBatchMaxConcurrent)
		logger.Infof("Block assembly batch max concurrent: %d", tSettings.BlockAssembly.SendBatchMaxConcurrent)
	}
	client.batcher = b

	return client, nil
}

// NewClientWithAddress creates a new block assembly client with a specific address.
//
// Parameters:
//   - ctx: Context for cancellation
//   - logger: Logger for operations
//   - tSettings: Teranode settings configuration
//   - blockAssemblyGrpcAddress: Specific gRPC address for block assembly
//
// Returns:
//   - *Client: New client instance
//   - error: Any error encountered during creation
func NewClientWithAddress(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, blockAssemblyGrpcAddress string) (*Client, error) {
	baConn, err := util.GetGRPCClient(ctx, blockAssemblyGrpcAddress, &util.ConnectionOptions{
		MaxRetries:   tSettings.GRPCMaxRetries,
		RetryBackoff: tSettings.GRPCRetryBackoff,
		CallerName:   "blockassembly",
	}, tSettings)
	if err != nil {
		return nil, errors.NewServiceError("failed to connect to block assembly", err)
	}

	batchSize := tSettings.BlockAssembly.SendBatchSize
	sendBatchTimeout := tSettings.BlockAssembly.SendBatchTimeout

	if batchSize > 0 {
		logger.Infof("Using batch mode to send transactions to block assembly, batches: %d, timeout: %dms", batchSize, sendBatchTimeout)
	}

	duration := time.Duration(sendBatchTimeout) * time.Millisecond

	client := &Client{
		client:    blockassembly_api.NewBlockAssemblyAPIClient(baConn),
		logger:    logger,
		settings:  tSettings,
		batchSize: batchSize,
		batchCh:   make(chan []*batchItem),
	}

	sendBatch := func(batch []*batchItem) {
		client.sendBatchToBlockAssembly(ctx, batch)
	}
	b := batcher.NewWithPool(batchSize, duration, sendBatch, tSettings.BatcherBackground,
		batcher.WithName("blockassembly_client"),
		batcher.WithLogger(logger),
		batcher.WithMetrics(batchermetrics.Provider()),
		batcher.WithTracer(tracing.Tracer("blockassembly").OTelTracer()),
	)
	if tSettings.BatcherDrainMode {
		b.SetDrainMode(true)
	}
	// Tick interval after drain: go-batcher's drain-wins guard no-ops + warns under
	// drain. Default 0 = disabled.
	if ms := tSettings.BlockAssembly.SendBatchTickerIntervalMillis; ms > 0 {
		b.SetTickInterval(time.Duration(ms) * time.Millisecond)
	}
	if tSettings.BlockAssembly.SendBatchMaxConcurrent > 0 {
		b.SetMaxConcurrent(tSettings.BlockAssembly.SendBatchMaxConcurrent)
		logger.Infof("Block assembly batch max concurrent: %d", tSettings.BlockAssembly.SendBatchMaxConcurrent)
	}
	client.batcher = b

	return client, nil
}

// Health checks the health status of the block assembly service.
//
// Parameters:
//   - ctx: Context for cancellation
//   - checkLiveness: Whether to perform liveness check
//
// Returns:
//   - int: HTTP status code indicating health state
//   - string: Health status message
//   - error: Any error encountered during health check
func (s *Client) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	resp, err := s.client.HealthGRPC(ctx, &blockassembly_api.EmptyMessage{})
	if err != nil {
		return http.StatusFailedDependency, "", errors.UnwrapGRPC(err)
	}

	if !resp.GetOk() {
		details := ""
		if resp != nil {
			details = resp.GetDetails()
		}
		return http.StatusFailedDependency, details, errors.NewServiceError("health check failed: %s", details)
	}

	return http.StatusOK, "OK", nil
}

// Store stores a transaction in block assembly.
//
// Parameters:
//   - ctx: Context for cancellation
//   - hash: Transaction hash
//   - fee: Transaction fee in satoshis
//   - size: Transaction size in bytes
//
// Returns:
//   - bool: True if storage was successful
//   - error: Any error encountered during storage
func (s *Client) Store(ctx context.Context, hash *chainhash.Hash, fee, size uint64, txInpoints subtree.TxInpoints) (bool, error) {
	txInpointsBytes, err := txInpoints.Serialize()
	if err != nil {
		return false, err
	}

	req := &blockassembly_api.AddTxRequest{
		Txid:       hash[:],
		Fee:        fee,
		Size:       size,
		TxInpoints: txInpointsBytes,
	}

	if s.batchSize == 0 {
		if _, err := s.client.AddTx(ctx, req); err != nil {
			return false, errors.UnwrapGRPC(err)
		}
	} else {
		/* batch mode */
		done := make(chan error)
		s.batcher.PutCtx(ctx, &batchItem{
			req:  req,
			done: done,
		})

		err := <-done
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

// RemoveTx removes a transaction from block assembly.
//
// Parameters:
//   - ctx: Context for cancellation
//   - hash: Hash of transaction to remove
//
// Returns:
//   - error: Any error encountered during removal
func (s *Client) RemoveTx(ctx context.Context, hash *chainhash.Hash) error {
	_, err := s.client.RemoveTx(ctx, &blockassembly_api.RemoveTxRequest{
		Txid: hash[:],
	})

	unwrappedErr := errors.UnwrapGRPC(err)
	if unwrappedErr == nil {
		return nil
	}

	return unwrappedErr
}

// GetMiningCandidate retrieves a candidate block for mining.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - *model.MiningCandidate: Mining candidate block
//   - error: Any error encountered during retrieval
func (s *Client) GetMiningCandidate(ctx context.Context, includeSubtreeHashes ...bool) (*model.MiningCandidate, error) {
	includeSubtrees := false
	if len(includeSubtreeHashes) > 0 {
		includeSubtrees = includeSubtreeHashes[0]
	}

	req := &blockassembly_api.GetMiningCandidateRequest{
		IncludeSubtrees: includeSubtrees,
	}

	res, err := s.client.GetMiningCandidate(ctx, req)
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	return res, nil
}

// GetCurrentDifficulty retrieves the current mining difficulty.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - float64: Current difficulty value
//   - error: Any error encountered during retrieval
func (s *Client) GetCurrentDifficulty(ctx context.Context) (float64, error) {
	req := &blockassembly_api.EmptyMessage{}

	res, err := s.client.GetCurrentDifficulty(ctx, req)
	if err != nil {
		return 0, errors.UnwrapGRPC(err)
	}

	return res.Difficulty, nil
}

// SubmitMiningSolution submits a solution for a mined block.
//
// Parameters:
//   - ctx: Context for cancellation
//   - solution: Mining solution to submit
//
// Returns:
//   - error: Any error encountered during submission
func (s *Client) SubmitMiningSolution(ctx context.Context, solution *model.MiningSolution) error {
	_, err := s.client.SubmitMiningSolution(ctx, &blockassembly_api.SubmitMiningSolutionRequest{
		Id:         solution.Id,
		Nonce:      solution.Nonce,
		CoinbaseTx: solution.Coinbase,
		Time:       solution.Time,
		Version:    solution.Version,
	})

	if retErr := errors.UnwrapGRPC(err); retErr != nil {
		return retErr
	}

	return nil
}

// GenerateBlocks generates a specified number of blocks.
//
// Parameters:
//   - ctx: Context for cancellation
//   - req: Block generation request parameters
//
// Returns:
//   - error: Any error encountered during generation
func (s *Client) GenerateBlocks(ctx context.Context, req *blockassembly_api.GenerateBlocksRequest) error {
	_, err := s.client.GenerateBlocks(ctx, req)

	unwrappedErr := errors.UnwrapGRPC(err)
	if unwrappedErr == nil {
		return nil
	}

	return unwrappedErr
}

// GetCandidateBlock retrieves block metadata for an existing mining candidate.
// Returns the 80-byte header, coinbase tx, subtree hashes, and tx count.
func (s *Client) GetCandidateBlock(ctx context.Context, candidateID []byte) (*blockassembly_api.GetCandidateBlockResponse, error) {
	res, err := s.client.GetCandidateBlock(ctx, &blockassembly_api.GetCandidateBlockRequest{
		Id: candidateID,
	})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	return res, nil
}

// sendBatchToBlockAssembly sends a batch of transactions to block assembly.
// Uses columnar format if enabled in settings, otherwise uses traditional row format.
//
// The columnar format reduces CPU by 15-20% and GC pressure by 50% at high throughput (>2M tx/sec)
// by storing transaction data in column-oriented arrays instead of row-oriented structures.
//
// Parameters:
//   - ctx: Context for cancellation
//   - batch: Batch of transactions to send
func (s *Client) sendBatchToBlockAssembly(ctx context.Context, batch []*batchItem) {
	if s.settings.BlockAssembly.UseColumnarBatch {
		s.sendBatchColumnar(ctx, batch)
	} else {
		s.sendBatchRowOriented(ctx, batch)
	}
}

// sendBatchRowOriented sends batch using traditional row-oriented format (existing implementation).
// This is the backward-compatible format that works with all block assembly versions.
func (s *Client) sendBatchRowOriented(ctx context.Context, batch []*batchItem) {
	txRequests := make([]*blockassembly_api.AddTxRequest, len(batch))
	for i, item := range batch {
		txRequests[i] = item.req
	}

	txBatch := &blockassembly_api.AddTxBatchRequest{
		TxRequests: txRequests,
	}

	_, err := s.client.AddTxBatch(ctx, txBatch)
	if err != nil {
		s.logger.Errorf("%v", err)

		for _, item := range batch {
			item.done <- errors.UnwrapGRPC(err)
		}

		return
	}

	for _, item := range batch {
		item.done <- nil
	}
}

// sendBatchColumnar sends batch using optimized columnar format.
// Provides 15-20% CPU reduction and 50% GC pressure reduction compared to row-oriented format.
func (s *Client) sendBatchColumnar(ctx context.Context, batch []*batchItem) {
	columnarReq, err := s.convertToColumnarFormat(batch)
	if err != nil {
		s.logger.Errorf("failed to convert batch to columnar format: %v", err)
		for _, item := range batch {
			item.done <- err
		}
		return
	}

	_, err = s.client.AddTxBatchColumnar(ctx, columnarReq)
	if err != nil {
		// Peer server predates PR #889 and doesn't implement the columnar
		// RPC. Fall back to the row-oriented path for this call so a
		// rolling-deploy mismatch doesn't stall tx ingestion. No
		// connection-level stickiness: each batch re-tries columnar
		// independently. Costs one wasted RPC per batch against a
		// persistently-old server; acceptable for the simpler code path.
		if status.Code(err) == codes.Unimplemented {
			s.logger.Debugf("[blockassembly] columnar AddTxBatch unimplemented on peer; falling back to row-oriented batch (rolling deploy?): %v", err)
			s.sendBatchRowOriented(ctx, batch)
			return
		}

		s.logger.Errorf("%v", err)

		for _, item := range batch {
			item.done <- errors.UnwrapGRPC(err)
		}

		return
	}

	for _, item := range batch {
		item.done <- nil
	}
}

// convertToColumnarFormat converts a batch of items to columnar format.
// Single allocation strategy: pre-allocate all arrays based on batch size to minimize allocations.
//
// OPTIMIZATION: This version deserializes TxInpoints on the Client (which runs on multiple machines)
// and sends them in columnar format. This shifts deserialization work away from the single-machine Server.
//
// The columnar format packs all data into contiguous arrays:
//   - txids_packed: All 32-byte TXIDs concatenated
//   - fees: All fees in a single array
//   - sizes: All sizes in a single array
//   - parent_tx_hashes_packed: All parent tx hashes (from TxInpoints) concatenated
//   - parent_tx_offsets: Offset table for parent hashes per transaction
//   - vout_idxs_packed: Count-prefixed packed vouts in the exact shape the
//     server's TxInpoints.voutIdxs stores internally. For each parent, one
//     uint32 count word followed by that many vout-value words, concatenated
//     across all transactions.
//   - vout_idxs_tx_offsets: Per-tx offsets into vout_idxs_packed.
//
// This pushes all TxInpoints layout work to the Client (horizontally scalable
// validators) so the single-instance block-assembly Server can construct
// TxInpoints from a per-tx slice with zero allocation
// (subtree.NewTxInpointsFromPacked).
func (s *Client) convertToColumnarFormat(batch []*batchItem) (*blockassembly_api.AddTxBatchColumnarRequest, error) {
	batchSize := len(batch)
	if batchSize == 0 {
		return nil, errors.NewInvalidArgumentError("empty batch")
	}

	// Pre-allocate with exact/estimated sizes (minimize allocations)
	// These use exact capacity to avoid reallocation
	txidsPacked := make([]byte, batchSize*32)
	fees := make([]uint64, batchSize)
	sizes := make([]uint64, batchSize)
	parentTxOffsets := make([]uint32, batchSize+1)
	voutIdxsTxOffsets := make([]uint32, batchSize+1)

	// For variable-length fields, estimate capacity based on typical usage
	// Estimate: avg 3 parent hashes per tx
	estimatedParentHashes := batchSize * 3
	parentTxHashesPacked := make([]byte, 0, estimatedParentHashes*32)

	// Estimate: avg 2 vout indices per parent hash, plus a count word per
	// parent. Sized as estimatedParentHashes * 3 ≈ count word + 2 values.
	voutIdxsPacked := make([]uint32, 0, estimatedParentHashes*3)

	// Start with offset 0
	parentTxOffsets[0] = 0
	voutIdxsTxOffsets[0] = 0

	currentParentHashCount := uint32(0)

	for i, item := range batch {
		req := item.req

		// Validate TXID length
		if len(req.Txid) != 32 {
			return nil, errors.NewInvalidArgumentError("invalid txid length at index %d: %d", i, len(req.Txid))
		}

		// Pack basic columns - use copy for fixed-size arrays to avoid bounds checks
		copy(txidsPacked[i*32:(i+1)*32], req.Txid)
		fees[i] = req.Fee
		sizes[i] = req.Size

		// Deserialize TxInpoints on the Client side
		txInpoints, err := subtree.NewTxInpointsFromBytes(req.TxInpoints)
		if err != nil {
			return nil, errors.NewInvalidArgumentError("failed to deserialize TxInpoints at index %d: %v", i, err)
		}

		// Pack parent transaction hashes
		parentHashes := txInpoints.GetParentTxHashes()
		for j := range parentHashes {
			parentTxHashesPacked = append(parentTxHashesPacked, parentHashes[j][:]...)
			currentParentHashCount++
		}
		parentTxOffsets[i+1] = currentParentHashCount

		// Pack vouts in count-prefixed layout, one parent at a time. The
		// resulting slice is byte-identical to TxInpoints.voutIdxs on the
		// Server side, so it can be aliased directly with no decoding.
		for j := range parentHashes {
			vouts, err := txInpoints.GetParentVoutsAtIndex(j)
			if err != nil {
				return nil, errors.NewInvalidArgumentError("failed to read vouts at parent %d of tx %d: %v", j, i, err)
			}

			voutIdxsPacked = append(voutIdxsPacked, uint32(len(vouts)))
			voutIdxsPacked = append(voutIdxsPacked, vouts...)
		}

		voutIdxsTxOffsets[i+1] = uint32(len(voutIdxsPacked))
	}

	return &blockassembly_api.AddTxBatchColumnarRequest{
		TxidsPacked:          txidsPacked,
		Fees:                 fees,
		Sizes:                sizes,
		ParentTxHashesPacked: parentTxHashesPacked,
		ParentTxOffsets:      parentTxOffsets,
		VoutIdxsPacked:       voutIdxsPacked,
		VoutIdxsTxOffsets:    voutIdxsTxOffsets,
	}, nil
}

// ResetBlockAssembly triggers a reset of the block assembly state.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - error: Any error encountered during reset
func (s *Client) ResetBlockAssembly(ctx context.Context) error {
	_, err := s.client.ResetBlockAssembly(ctx, &blockassembly_api.EmptyMessage{})

	unwrappedErr := errors.UnwrapGRPC(err)
	if unwrappedErr == nil {
		return nil
	}

	return unwrappedErr
}

// ResetBlockAssemblyFully triggers a full reset of the block assembly state.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - error: Any error encountered during reset
func (s *Client) ResetBlockAssemblyFully(ctx context.Context) error {
	_, err := s.client.ResetBlockAssemblyFully(ctx, &blockassembly_api.EmptyMessage{})

	unwrappedErr := errors.UnwrapGRPC(err)
	if unwrappedErr == nil {
		return nil
	}

	return unwrappedErr
}

// ResetBlockAssemblyValidateInputs performs a full reset with UTXO input validation.
// For each unmined transaction, verifies inputs are still spent by this tx.
// If an input is spent by a different tx, marks the tx as conflicting and excludes it.
func (s *Client) ResetBlockAssemblyValidateInputs(ctx context.Context) error {
	_, err := s.client.ResetBlockAssemblyValidateInputs(ctx, &blockassembly_api.EmptyMessage{})

	unwrappedErr := errors.UnwrapGRPC(err)
	if unwrappedErr == nil {
		return nil
	}

	return unwrappedErr
}

// CheckBlockAssemblyValidateInputs checks unmined tx inputs for validity without modifying state.
func (s *Client) CheckBlockAssemblyValidateInputs(ctx context.Context) error {
	_, err := s.client.CheckBlockAssemblyValidateInputs(ctx, &blockassembly_api.EmptyMessage{})
	return errors.UnwrapGRPC(err)
}

// GetBlockAssemblyState retrieves the current state of block assembly.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - *blockassembly_api.StateMessage: Current state
//   - error: Any error encountered during retrieval
func (s *Client) GetBlockAssemblyState(ctx context.Context) (*blockassembly_api.StateMessage, error) {
	state, err := s.client.GetBlockAssemblyState(ctx, &blockassembly_api.EmptyMessage{})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	return state, nil
}

// BlockAssemblyAPIClient returns the underlying gRPC client for block assembly API.
//
// Returns:
//   - blockassembly_api.BlockAssemblyAPIClient: The gRPC client instance
func (s *Client) BlockAssemblyAPIClient() blockassembly_api.BlockAssemblyAPIClient {
	return s.client
}

// GetBlockAssemblyBlockCandidate retrieves the current block candidate in block assembly.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - *model.Block: Block candidate
//   - error: Any error encountered during retrieval
func (s *Client) GetBlockAssemblyBlockCandidate(ctx context.Context) (*model.Block, error) {
	resp, err := s.client.GetBlockAssemblyBlockCandidate(ctx, &blockassembly_api.EmptyMessage{})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	block, err := model.NewBlockFromBytes(resp.Block)
	if err != nil {
		return nil, errors.NewServiceError("failed to create block from bytes", err)
	}

	return block, nil
}

// GetTransactionHashes retrieves all transaction hashes in block assembly.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - []string: List of transaction hashes
//   - error: Any error encountered during retrieval
func (s *Client) GetTransactionHashes(ctx context.Context) ([]string, error) {
	resp, err := s.client.GetBlockAssemblyTxs(ctx, &blockassembly_api.EmptyMessage{})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	return resp.Txs, nil
}
