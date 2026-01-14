// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
//
// The subtreevalidation service is a core component of the Teranode blockchain node that manages
// the validation of transaction subtrees. Unlike traditional Bitcoin implementations that use a
// mempool, Teranode uses a subtree-based approach for transaction management and validation.
//
// Key Features:
//   - Subtree validation and processing for blockchain transactions
//   - Transaction metadata caching and retrieval
//   - Integration with validator services for transaction validation
//   - Concurrent processing with proper locking mechanisms
//   - Metrics collection and monitoring support
//   - gRPC API for external service integration
//
// Architecture:
// The service operates as both a gRPC server and client, providing validation services to other
// components while consuming blockchain and validator services. It maintains transaction metadata
// in various storage backends and implements sophisticated caching strategies for performance.
//
// Integration:
// This service integrates with:
//   - Blockchain service for block and transaction data
//   - Validator service for transaction validation logic
//   - Blob storage for persistent transaction metadata
//   - P2P service for network communication
//   - Metrics collection systems for monitoring
//
// Concurrency:
// The service is designed for high-concurrency operations with proper synchronization mechanisms
// to handle multiple validation requests simultaneously while maintaining data consistency.
package subtreevalidation

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

// missingTx represents a transaction that needs to be retrieved and its position in the subtree.
//
// This structure pairs a transaction with its index in the original subtree transaction list,
// allowing the validation process to maintain the correct ordering and relationship of transactions
// even after retrieval and processing operations that might otherwise lose this context.
//
// The structure is primarily used during the transaction retrieval and validation phase
// to track which transactions were missing from local storage and needed to be fetched
// from external sources.
type missingTx struct {
	// tx is the actual transaction data that was retrieved
	tx *bt.Tx

	// idx is the original position of this transaction in the subtree's transaction list
	idx int
}

// SetSubtreeExists marks a subtree as existing in the local storage.
//
// This method is intended to track which subtrees have been processed and stored locally
// to avoid redundant processing. Currently, this is a placeholder implementation that
// always returns success without performing any actual storage operations.
//
// Parameters:
//   - subtreeHash: The hash identifier of the subtree to mark as existing
//
// Returns:
//   - error: Always nil in the current implementation
//
// TODO: Implement actual local storage tracking for subtree existence.
func (u *Server) SetSubtreeExists(_ *chainhash.Hash) error {
	// TODO: implement for local storage
	return nil
}

// GetSubtreeExists checks if a subtree exists in the local storage.
//
// This method queries the local storage to determine whether a specific subtree
// has already been processed and stored. It's used to optimize processing by
// avoiding duplicate work on subtrees that have already been validated.
//
// Parameters:
//   - ctx: Context for cancellation and request-scoped values
//   - subtreeHash: The hash identifier of the subtree to check
//
// Returns:
//   - bool: Always false in the current implementation
//   - error: Always nil in the current implementation
//
// TODO: Implement actual local storage lookup for subtree existence.
func (u *Server) GetSubtreeExists(_ context.Context, _ *chainhash.Hash) (bool, error) {
	return false, nil
}

// txMetaCacheOps defines the interface for transaction metadata cache operations.
//
// This interface abstracts the caching operations to enable mocking during testing
// and to provide a clean separation between the validation logic and the underlying
// storage implementation. It allows the subtree validation service to work with
// different cache implementations while maintaining consistent behavior.
//
// The interface is typically implemented by UTXO stores that support caching
// functionality, enabling efficient storage and retrieval of transaction metadata
// during the validation process.
type txMetaCacheOps interface {
	// Delete removes transaction metadata from the cache for the specified hash.
	// Returns an error if the deletion operation fails.
	Delete(ctx context.Context, hash *chainhash.Hash) error

	// SetCacheFromBytes stores raw transaction metadata bytes in the cache using the provided key.
	// This method allows direct storage of pre-serialized metadata for performance optimization.
	// Returns an error if the cache operation fails.
	SetCacheFromBytes(key, txMetaBytes []byte) error
}

// SetTxMetaCacheFromBytes stores raw transaction metadata bytes in the cache.
//
// This method provides a direct way to store pre-serialized transaction metadata
// in the cache without requiring deserialization and re-serialization. It's used
// for performance optimization when metadata is already in byte format.
//
// The method checks if the underlying UTXO store supports caching operations
// through the txMetaCacheOps interface. If caching is not supported, the method
// returns successfully without performing any operation.
//
// Parameters:
//   - ctx: Context for cancellation and request-scoped values (currently unused)
//   - key: The cache key for storing the metadata
//   - txMetaBytes: The serialized transaction metadata to store
//
// Returns:
//   - error: Error from the cache operation, or nil if successful or caching not supported
func (u *Server) SetTxMetaCacheFromBytes(_ context.Context, key, txMetaBytes []byte) error {
	if cache, ok := u.utxoStore.(txMetaCacheOps); ok {
		return cache.SetCacheFromBytes(key, txMetaBytes)
	}

	return nil
}

// DelTxMetaCache removes transaction metadata from the cache if caching is enabled.
//
// This method removes cached transaction metadata for the specified transaction hash.
// It includes distributed tracing support to monitor cache deletion operations and
// only performs the deletion if the underlying UTXO store supports caching.
//
// The method is typically called during cleanup operations or when transaction
// metadata needs to be invalidated due to blockchain reorganizations or other
// state changes.
//
// Parameters:
//   - ctx: Context for cancellation, tracing, and request-scoped values
//   - hash: The transaction hash whose metadata should be removed from cache
//
// Returns:
//   - error: Error from the cache deletion operation, or nil if successful or caching not supported
func (u *Server) DelTxMetaCache(ctx context.Context, hash *chainhash.Hash) error {
	if cache, ok := u.utxoStore.(txMetaCacheOps); ok {
		ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "SubtreeValidation:DelTxMetaCache")
		defer deferFn()

		return cache.Delete(ctx, hash)
	}

	return nil
}

// DelTxMetaCacheMulti removes multiple transaction metadata entries from the cache.
func (u *Server) DelTxMetaCacheMulti(ctx context.Context, hash *chainhash.Hash) error {
	if cache, ok := u.utxoStore.(*txmetacache.TxMetaCache); ok {
		ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "SubtreeValidation:DelTxMetaCacheMulti")
		defer deferFn()

		return cache.Delete(ctx, hash)
	}

	return nil
}

// getMissingTransactionsBatch retrieves a batch of transactions from the network.
// Note: The returned transactions may not be in the same order as the input hashes.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - subtreeHash: Hash of the subtree containing the transactions
//   - txHashes: Slice of transaction hashes to retrieve
//   - baseURL: URL of the network source for transactions
//
// Returns:
//   - []*bt.Tx: Slice of retrieved transactions
//   - error: Any error encountered during retrieval
func (u *Server) getMissingTransactionsBatch(ctx context.Context, subtreeHash chainhash.Hash, txHashes []utxo.UnresolvedMetaData, baseURL string) ([]*bt.Tx, error) {
	// Validate that baseURL is a proper HTTP/HTTPS URL and not a peer ID
	parsedURL, err := url.Parse(baseURL)
	if err != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") {
		u.logger.Errorf("[getMissingTransactionsBatch][%s] Invalid baseURL '%s' - must be valid http/https URL, not peer ID",
			subtreeHash.String(), baseURL)
		return nil, errors.NewExternalError("[getMissingTransactionsBatch][%s] invalid baseURL - not a valid http/https URL", subtreeHash.String())
	}

	log := false

	utxoStoreURL := u.settings.UtxoStore.UtxoStore
	if strings.Contains(utxoStoreURL.String(), "logging=true") {
		// we are logging every utxostore create/spend/delete so we need to log every tx request here too for easier debugging
		log = true
	}

	txIDBytes := make([]byte, 32*len(txHashes))

	for idx, txHash := range txHashes {
		if log {
			u.logger.Debugf("[getMissingTransactionsBatch][%s][%s] adding tx hash %d to request", subtreeHash.String(), txHash.Hash.String(), idx)
		}

		copy(txIDBytes[idx*32:(idx+1)*32], txHash.Hash[:])
	}

	// do a POST http request to baseUrl + subtree hash + txs endpoint
	url := fmt.Sprintf("%s/subtree/%s/txs", baseURL, subtreeHash.String())
	u.logger.Debugf("[getMissingTransactionsBatch][%s] getting %d txs from peer %s", subtreeHash.String(), len(txHashes), url)

	body, err := util.DoHTTPRequestBodyReader(ctx, url, txIDBytes)
	if err != nil {
		// Peer cannot provide requested transactions - report as invalid subtree
		u.publishInvalidSubtree(ctx, subtreeHash.String(), baseURL, "peer_cannot_provide_transactions")
		return nil, errors.NewExternalError("[getMissingTransactionsBatch][%s] failed to do http request", subtreeHash.String(), err)
	}

	defer body.Close()

	// read the body into transactions using go-bt
	missingTxs := make([]*bt.Tx, 0, len(txHashes))

	var tx *bt.Tx

	for {
		tx, err = u.readTxFromReader(body)
		if err != nil || tx == nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// Malformed transaction data from peer - report as invalid subtree
			u.publishInvalidSubtree(ctx, subtreeHash.String(), baseURL, "malformed_transaction_data")
			// Not recoverable, returning processing error
			return nil, errors.NewProcessingError("[getMissingTransactionsBatch][%s] failed to read transaction from body", subtreeHash.String(), err)
		}

		missingTxs = append(missingTxs, tx)
	}

	if len(missingTxs) != len(txHashes) {
		// Peer sent wrong number of transactions - report as invalid subtree
		u.publishInvalidSubtree(ctx, subtreeHash.String(), baseURL, "transaction_count_mismatch")
		return nil, errors.NewProcessingError("[getMissingTransactionsBatch][%s] missing tx count mismatch: missing=%d, txHashes=%d", subtreeHash.String(), len(missingTxs), len(txHashes))
	}

	return missingTxs, nil
}

// readTxFromReader reads and validates a single transaction from an io.ReadCloser.
// It includes panic recovery for handling potential runtime errors from the go-bt library.
//
// Parameters:
//   - body: ReadCloser containing the transaction data
//
// Returns:
//   - *bt.Tx: The parsed transaction
//   - error: Any error encountered during reading or validation
func (u *Server) readTxFromReader(body io.ReadCloser) (tx *bt.Tx, err error) {
	defer func() {
		// there is a bug in go-bt, that does not check input and throws a runtime error in
		// github.com/bsv-blockchain/go-bt/v2@v2.2.2/input.go:76 +0x16b
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.NewUnknownError(x)
			case error:
				err = x
			default:
				err = errors.NewError("unknown panic: %v", r)
			}
		}
	}()

	tx = &bt.Tx{}

	_, err = tx.ReadFrom(body)
	if err != nil {
		return nil, err
	}

	return tx, nil
}

// blessMissingTransaction validates a transaction and retrieves its metadata,
// performing the core consensus validation operations required for blockchain inclusion.
//
// This method applies full validation to a transaction, ensuring it adheres to all
// Bitcoin consensus rules and can be properly included in the blockchain. The validation
// includes:
// - Transaction format and structure validation
// - Input signature verification
// - Input UTXO availability and spending authorization
// - Fee calculation and policy enforcement
// - Script execution and validation
// - Double-spend prevention
//
// Upon successful validation, the transaction's metadata is calculated and stored,
// making it available for future reference and for validation of dependent transactions.
// The metadata includes critical information such as input references, output values,
// and transaction state.
//
// This method employs defensive validation techniques with proper error handling
// and logging to ensure robustness even with malformed or invalid transactions.
//
// Parameters:
//   - ctx: Context for cancellation, tracing, and timeouts
//   - subtreeHash: Hash of the subtree containing the transaction (for logging and reference)
//   - tx: The transaction to validate
//   - blockHeight: Height of the block containing the transaction
//   - blockIds: Map of block IDs to check if transactions in the subtree are already mined
//   - validationOptions: Additional options controlling validation behavior
//
// Returns:
//   - *meta.Data: Transaction metadata structure if validation succeeds, nil otherwise
//   - error: Detailed error information if validation fails for any reason
func (u *Server) blessMissingTransaction(ctx context.Context, blockHash chainhash.Hash, subtreeHash chainhash.Hash, tx *bt.Tx, blockHeight uint32,
	blockIds map[uint32]bool, validationOptions *validator.Options) (txMeta *meta.Data, err error) {
	start := time.Now()

	defer func() {
		prometheusSubtreeValidationBlessMissingTransaction.Observe(time.Since(start).Seconds())
	}()

	if tx == nil {
		return nil, errors.NewTxInvalidError("[blessMissingTransaction][%s/%s] tx is nil", blockHash.String(), subtreeHash.String())
	}

	if tx.IsCoinbase() {
		return nil, errors.NewTxInvalidError("[blessMissingTransaction][%s/%s][%s] transaction is coinbase", blockHash.String(), subtreeHash.String(), tx.TxID())
	}

	// validate the transaction in the validation service
	// this should spend utxos, create the tx meta and create new utxos
	txMeta, err = u.validatorClient.ValidateWithOptions(ctx, tx, blockHeight, validationOptions)
	if err != nil {
		if errors.Is(err, errors.ErrTxConflicting) {
			// conflicting transaction, which has been saved, but not spent
			u.logger.Warnf("[blessMissingTransaction][%s/%s][%s] transaction is conflicting", blockHash.String(), subtreeHash.String(), tx.TxID())
		} else {
			return nil, errors.NewProcessingError("[blessMissingTransaction][%s/%s][%s] failed to validate transaction", blockHash.String(), subtreeHash.String(), tx.TxID(), err)
		}
	}

	// Not recoverable, returning processing error
	if txMeta == nil {
		return nil, errors.NewProcessingError("[blessMissingTransaction][%s/%s][%s] tx meta is nil", blockHash.String(), subtreeHash.String(), tx.TxID())
	}

	// check whether this transaction was already mined on our chain by comparing the block ids
	if len(txMeta.BlockIDs) > 0 && len(blockIds) > 0 {
		for _, blockID := range txMeta.BlockIDs {
			if blockIds[blockID] {
				return nil, errors.NewTxInvalidError("[blessMissingTransaction][%s/%s][%s] transaction is already mined on our chain, in block %d", blockHash.String(), subtreeHash.String(), tx.TxID(), blockID)
			}
		}
	}

	if txMeta.Conflicting {
		if err = u.checkCounterConflictingOnCurrentChain(ctx, *tx.TxIDChainHash(), blockIds); err != nil {
			return nil, errors.NewProcessingError("[blessMissingTransaction][%s/%s][%s] failed to check counter conflicting tx on current chain", blockHash.String(), subtreeHash.String(), tx.TxID(), err)
		}
	}

	return txMeta, nil
}

// checkCounterConflictingOnCurrentChain checks if the counter-conflicting transactions of a given transaction have
// already been mined on the current chain. If they have, it returns an error indicating that the transaction is invalid.
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - subtreeHash: Hash of the subtree containing the transaction
//   - txHash: Hash of the transaction to check
//   - blockIds: Map of block IDs to check if transactions in the subtree are already mined
//
// Returns:
//   - error: Any error encountered during the check
//   - nil: If the counter-conflicting transactions have not been mined on the current chain
func (u *Server) checkCounterConflictingOnCurrentChain(ctx context.Context, txHash chainhash.Hash, blockIds map[uint32]bool) error {
	// the tx is conflicting, check whether the counter-conflicting transactions have already been mined on our chain
	// first get the parent transactions and check if they were spent
	counterConflictingTxHashes, err := utxo.GetCounterConflictingTxHashes(ctx, u.utxoStore, txHash)
	if err != nil {
		return errors.NewProcessingError("[checkCounterConflictingOnCurrentChain][%s] failed to get counter conflicting tx hashes", txHash.String(), err)
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(128)

	counterConflictingTxMetas := make([]*meta.Data, len(counterConflictingTxHashes))

	for idx, counterConflictingTxHash := range counterConflictingTxHashes {
		g.Go(func() error {
			// if a transaction is frozen, the counter-transaction will be the same as the coinbase placeholder
			if counterConflictingTxHash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
				return errors.NewProcessingError("[checkCounterConflictingOnCurrentChain][%s] counter conflicting tx is frozen", txHash.String())
			}

			counterConflictingTxMeta := &meta.Data{}
			if err := u.utxoStore.GetMeta(gCtx, &counterConflictingTxHash, counterConflictingTxMeta); err != nil {
				return errors.NewProcessingError("[checkCounterConflictingOnCurrentChain][%s] failed to get counter conflicting tx meta", txHash.String(), err)
			}

			counterConflictingTxMetas[idx] = counterConflictingTxMeta

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return errors.NewProcessingError("[checkCounterConflictingOnCurrentChain][%s] failed to get counter conflicting tx meta", txHash.String(), err)
	}

	// check whether the child transactions of the counter-conflicting transactions are frozen
	for _, counterConflictingTxHash := range counterConflictingTxHashes {
		childTransactionHashes, err := utxo.GetConflictingChildren(ctx, u.utxoStore, counterConflictingTxHash)
		if err != nil {
			return errors.NewProcessingError("[checkCounterConflictingOnCurrentChain][%s] failed to get child transactions", txHash.String(), err)
		}

		for _, childTransactionHash := range childTransactionHashes {
			if childTransactionHash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
				return errors.NewProcessingError("[checkCounterConflictingOnCurrentChain][%s] child transaction is frozen", txHash.String())
			}
		}
	}

	// check whether the counter-conflicting transactions have already been mined on our chain
	for _, counterConflictingTxMeta := range counterConflictingTxMetas {
		for _, blockID := range counterConflictingTxMeta.BlockIDs {
			if blockIds[blockID] {
				return errors.NewTxInvalidError("[checkCounterConflictingOnCurrentChain][%s] transaction is already mined on our chain", txHash.String())
			}
		}
	}

	return nil
}

// ValidateSubtree encapsulates all the necessary information required to validate a transaction subtree.
//
// This structure provides a clean interface for the validation methods, containing the identifying
// information for the subtree, source location for retrieving missing transactions, and configuration
// options for the validation process. It serves as the primary input parameter for subtree validation
// operations throughout the service.
type ValidateSubtree struct {
	// SubtreeHash is the unique identifier hash of the subtree to be validated
	SubtreeHash chainhash.Hash

	// PeerID is the ID of the peer from which we received the subtree
	PeerID string

	// BaseURL is the source URL for retrieving missing transactions if needed
	BaseURL string

	// TxHashes contains the list of transaction hashes in the subtree
	// This may be empty if the subtree transactions need to be fetched from the store
	TxHashes []chainhash.Hash

	// AllowFailFast enables early termination of validation when an error is encountered.
	// When true, validation stops at the first error for quick failure detection.
	// When false, validation attempts to process all transactions to collect comprehensive error information.
	AllowFailFast bool
}

type metaSliceItem struct {
	fee         uint64
	sizeInBytes uint64
	coinbase    bool
	conflicting bool
	creating    bool
	isSet       bool
	txInpoints  subtreepkg.TxInpoints
}

// ValidateSubtreeInternal performs the actual validation of a subtree.
//
// This is the core method of the subtree validation service, responsible for the
// complete validation process of a transaction subtree. It handles the complex task
// of verifying that all transactions in a subtree are valid both individually and
// collectively, ensuring they can be safely added to the blockchain.
//
// The validation process includes several key steps:
// 1. Retrieving the subtree structure and transaction list
// 2. Identifying which transactions need validation (missing metadata)
// 3. Retrieving missing transactions from appropriate sources
// 4. Validating transaction dependencies and ordering
// 5. Applying consensus rules to each transaction
// 6. Managing transaction metadata storage and updates
// 7. Handling any conflicts or validation failures
//
// The method employs several optimization techniques:
// - Batch processing of transaction validations where possible
// - Caching of transaction metadata to avoid redundant validation
// - Parallel processing of independent transaction validations
// - Early termination for invalid subtrees (when AllowFailFast is true)
// - Efficient retrieval of missing transactions in batches
//
// The method includes comprehensive error handling and logging to ensure
// problems can be diagnosed and resolved effectively.
//
// Parameters:
//   - ctx: Context for cancellation, tracing, and request-scoped values
//   - v: ValidateSubtree struct containing the subtree hash, base URL, and configuration
//   - blockHeight: The height of the block containing the subtree
//   - blockIds: Map of block IDs to check if transactions are already mined
//   - validationOptions: Additional options controlling validation behavior
//
// Returns:
//   - error: Detailed error information if validation fails, nil on success
//
// This method is typically called by higher-level API handlers after performing
// necessary authorization and parameter validation.
func (u *Server) ValidateSubtreeInternal(ctx context.Context, v ValidateSubtree, blockHeight uint32,
	blockIds map[uint32]bool, validationOptions ...validator.Option) (subtree *subtreepkg.Subtree, err error) {
	stat := gocore.NewStat("ValidateSubtreeInternal")
	startTotal := time.Now()

	ctx, _, endSpan := tracing.Tracer("subtreevalidation").Start(ctx, "ValidateSubtreeInternal",
		tracing.WithHistogram(prometheusSubtreeValidationValidateSubtree),
		tracing.WithDebugLogMessage(u.logger, "[ValidateSubtreeInternal][%s] called", v.SubtreeHash.String()),
	)

	defer func() {
		endSpan(err)
	}()

	start := gocore.CurrentTime()

	// Get the subtree hashes if they were passed in
	txHashes := v.TxHashes

	if txHashes == nil {
		subtreeExists, err := u.GetSubtreeExists(ctx, &v.SubtreeHash)

		stat.NewStat("1. subtreeExists").AddTime(start)

		if err != nil {
			return nil, errors.NewStorageError("[ValidateSubtreeInternal][%s] failed to check if subtree exists in store", v.SubtreeHash.String(), err)
		}

		if subtreeExists {
			// If the subtree is already in the store, it means it is already validated.
			// Therefore, we finish processing of the subtree.
			return nil, nil
		}

		txHashes, err = u.getSubtreeTxHashes(ctx, stat, &v.SubtreeHash, v.BaseURL)
		if err != nil {
			return nil, errors.NewServiceError("[ValidateSubtreeInternal][%s] failed to get subtree from network", v.SubtreeHash.String(), err)
		}
	}

	// create the empty subtree
	height := math.Ceil(math.Log2(float64(len(txHashes))))

	subtree, err = subtreepkg.NewTree(int(height))
	if err != nil {
		return nil, errors.NewProcessingError("failed to create new subtree", err)
	}

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)

	failFastValidation := u.settings.Block.FailFastValidation
	abandonTxThreshold := u.settings.BlockValidation.SubtreeValidationAbandonThreshold
	maxRetries := u.settings.BlockValidation.ValidationMaxRetries

	retrySleepDuration := u.settings.BlockValidation.RetrySleep

	// TODO document, what does this do?
	subtreeWarmupCount := u.settings.BlockValidation.ValidationWarmupCount

	subtreeWarmupCountInt32, err := safeconversion.IntToInt32(subtreeWarmupCount)
	if err != nil {
		return nil, err
	}

	// TODO document, what is the logic here?
	failFast := v.AllowFailFast && failFastValidation && u.subtreeCount.Add(1) > subtreeWarmupCountInt32

	// txMetaSlice will be populated with the txMeta data for each txHash
	// in the retry attempts, only the tx hashes that are missing will be retried, not the whole subtree
	txMetaSlice := make([]metaSliceItem, len(txHashes))

	for attempt := 1; attempt <= maxRetries+1; attempt++ {
		prometheusSubtreeValidationValidateSubtreeRetry.Inc()

		var logMsg string

		switch {
		case u.isPrioritySubtreeCheckActive(v.SubtreeHash.String()):
			failFast = false
			logMsg = fmt.Sprintf("[ValidateSubtreeInternal][%s] [attempt #%d] Priority request (fail fast=%v) - final priority attempt to process subtree, this time with full checks enabled", v.SubtreeHash.String(), attempt, failFast)
		case attempt > maxRetries:
			failFast = false
			logMsg = fmt.Sprintf("[ValidateSubtreeInternal][%s] [attempt #%d] final attempt to process subtree, this time with full checks enabled", v.SubtreeHash.String(), attempt)
		default:
			logMsg = fmt.Sprintf("[ValidateSubtreeInternal][%s] [attempt #%d] (fail fast=%v) process %d txs from subtree", v.SubtreeHash.String(), attempt, failFast, len(txHashes))
		}

		u.logger.Debugf(logMsg)

		// unlike many other lists, this needs to be a pointer list, because a lot of values could be empty = nil

		// 1. First attempt to load the txMeta from the cache...
		missed, err := u.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, failFast)
		if err != nil {
			if errors.Is(err, errors.ErrThresholdExceeded) {
				u.logger.Warnf("[ValidateSubtreeInternal][%s] [attempt #%d] too many missing txmeta entries in cache (fail fast check only, will retry)", v.SubtreeHash.String(), attempt)
				select {
				case <-ctx.Done():
					break
				case <-time.After(retrySleepDuration):
					break
				case <-time.After(10 * time.Millisecond):
					if u.isPrioritySubtreeCheckActive(v.SubtreeHash.String()) {
						// break early - this is now a priority request. what the hell are we doing waiting around?
						break
					}
				}

				continue
			}

			// Don't wrap the error again, processTxMetaUsingCache returns the correctly formatted error.
			return nil, err
		}

		if failFast && abandonTxThreshold > 0 && missed > abandonTxThreshold {
			// Not recoverable, returning processing error
			return nil, errors.NewProcessingError("[ValidateSubtreeInternal][%s] [attempt #%d] failed to get tx meta from cache", v.SubtreeHash.String(), attempt, err)
		}

		if missed > 0 {
			batched := u.settings.SubtreeValidation.BatchMissingTransactions

			// 2. ...then attempt to load the txMeta from the store (i.e - aerospike in production)
			missed, err = u.processTxMetaUsingStore(ctx, txHashes, txMetaSlice, blockIds, batched, failFast)
			if err != nil {
				// Don't wrap the error again, processTxMetaUsingStore returns the correctly formatted error.
				return nil, err
			}
		}

		if missed > 0 {
			// 3. ...then attempt to load the txMeta from the network
			start, stat5, ctx5 := tracing.NewStatFromDefaultContext(ctx, "5. processMissingTransactions")
			// missingTxHashes is a slice if all txHashes in the subtree, but only the missing ones are not nil
			// this is done to make sure the order is preserved when getting them in parallel
			// compact the missingTxHashes to only a list of the missing ones
			missingTxHashesCompacted := make([]utxo.UnresolvedMetaData, 0, missed)

			for idx, txHash := range txHashes {
				if !txMetaSlice[idx].isSet && !txHash.IsEqual(subtreepkg.CoinbasePlaceholderHash) {
					missingTxHashesCompacted = append(missingTxHashesCompacted, utxo.UnresolvedMetaData{
						Hash: txHash,
						Idx:  idx,
					})
				}
			}

			u.logger.Debugf("[ValidateSubtreeInternal][%s] [attempt #%d] processing %d missing tx for subtree instance", v.SubtreeHash.String(), attempt, len(missingTxHashesCompacted))

			err = u.processMissingTransactions(
				ctx5,
				v.SubtreeHash,
				subtree,
				missingTxHashesCompacted,
				txHashes,
				v.BaseURL,
				txMetaSlice,
				blockHeight,
				blockIds,
				validationOptions...,
			)
			if err != nil {
				// u.logger.Errorf("SAO %s", err)
				// Don't wrap the error again, processMissingTransactions returns the correctly formatted error.
				return nil, err
			}

			stat5.AddTime(start)
		}

		break
	}

	start = gocore.CurrentTime()

	u.logger.Debugf("[ValidateSubtreeInternal][%s] adding %d nodes to subtree instance", v.SubtreeHash.String(), len(txHashes))

	for idx, txHash := range txHashes {
		// if placeholder just add it and continue
		if idx == 0 && txHash.Equal(*subtreepkg.CoinbasePlaceholderHash) {
			err = subtree.AddCoinbaseNode()
			if err != nil {
				return nil, errors.NewProcessingError("[ValidateSubtreeInternal][%s] failed to add coinbase placeholder node to subtree", v.SubtreeHash.String(), err)
			}

			continue
		}

		if !txMetaSlice[idx].isSet {
			return nil, errors.NewProcessingError("[ValidateSubtreeInternal][%s] tx meta not found in txMetaSlice at index %d: %s", v.SubtreeHash.String(), idx, txHash.String())
		}

		if txMetaSlice[idx].coinbase {
			// Not recoverable, returning TxInvalid error
			return nil, errors.NewTxInvalidError("[ValidateSubtreeInternal][%s] invalid subtree index for coinbase tx %d: %s", v.SubtreeHash.String(), idx, txHash.String())
		}

		// finally add the transaction hash and fee to the subtree
		if err = subtree.AddNode(txHash, txMetaSlice[idx].fee, txMetaSlice[idx].sizeInBytes); err != nil {
			return nil, errors.NewProcessingError("[ValidateSubtreeInternal][%s] failed to add node to subtree / subtreeMeta", v.SubtreeHash.String(), err)
		}

		// mark the transaction as conflicting if it is
		if txMetaSlice[idx].conflicting {
			if err = subtree.AddConflictingNode(txHash); err != nil {
				return nil, errors.NewProcessingError("[ValidateSubtreeInternal][%s] failed to add conflicting node to subtree", v.SubtreeHash.String(), err)
			}
		}

		// add the txMeta data we need for block validation
		subtreeIdx := subtree.Length() - 1

		if err = subtreeMeta.SetTxInpoints(subtreeIdx, txMetaSlice[idx].txInpoints); err != nil {
			return nil, errors.NewProcessingError("[ValidateSubtreeInternal][%s] failed to set parent tx hash in subtreeMeta", v.SubtreeHash.String(), err)
		}
	}

	stat.NewStat("6. addAllTxHashFeeSizesToSubtree").AddTime(start)

	if err = u.storeSubtreeFiles(ctx, stat, &v.SubtreeHash, subtree, subtreeMeta); err != nil {
		return nil, err
	}

	// only set this on no errors
	prometheusSubtreeValidationValidateSubtreeDuration.Observe(float64(time.Since(startTotal).Microseconds()) / 1_000_000)

	// Increase peer's reputation for providing a valid subtree
	if u.p2pClient != nil && v.PeerID != "" {
		if err := u.p2pClient.ReportValidSubtree(ctx, v.PeerID, v.SubtreeHash.String()); err != nil {
			u.logger.Warnf("[ValidateSubtreeInternal][%s] failed to report valid subtree to peer %s: %v", v.SubtreeHash.String(), v.PeerID, err)
		}
	}

	return subtree, nil
}

func (u *Server) storeSubtreeFiles(ctx context.Context, stat *gocore.Stat, subtreeHash *chainhash.Hash, subtree *subtreepkg.Subtree, subtreeMeta *subtreepkg.Meta) error {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "storeSubtreeFiles",
		tracing.WithParentStat(stat),
		tracing.WithDebugLogMessage(u.logger, "[storeSubtreeFiles][%s] called", subtreeHash.String()),
	)
	defer deferFn()

	// does the merkle tree give the correct root?
	merkleRoot := subtree.RootHash()
	if !merkleRoot.IsEqual(subtreeHash) {
		return errors.NewSubtreeInvalidError("subtree root hash does not match")
	}

	dah := u.utxoStore.GetBlockHeight() + u.settings.GetSubtreeValidationBlockHeightRetention()

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		//
		// store subtree meta in store
		//
		u.logger.Debugf("[ValidateSubtreeInternal][%s] serialize subtree meta", subtreeHash.String())

		completeSubtreeMetaBytes, err := subtreeMeta.Serialize()
		if err != nil {
			return errors.NewProcessingError("[ValidateSubtreeInternal][%s] failed to serialize subtree meta", subtreeHash.String(), err)
		}

		u.logger.Debugf("[ValidateSubtreeInternal][%s] store subtree meta", subtreeHash.String())

		if err = u.subtreeStore.Set(gCtx,
			merkleRoot[:],
			fileformat.FileTypeSubtreeMeta,
			completeSubtreeMetaBytes,
			options.WithDeleteAt(dah),
		); err != nil {
			if errors.Is(err, errors.ErrBlobAlreadyExists) {
				u.logger.Warnf("[ValidateSubtreeInternal][%s] subtree meta already exists in store", subtreeHash.String())
			} else {
				return errors.NewStorageError("[ValidateSubtreeInternal][%s] failed to store subtree meta", subtreeHash.String(), err)
			}
		}

		return nil
	})

	g.Go(func() error {
		//
		// store subtree in store
		//
		u.logger.Debugf("[ValidateSubtreeInternal][%s] serialize subtree", subtreeHash.String())

		completeSubtreeBytes, err := subtree.Serialize()
		if err != nil {
			return errors.NewProcessingError("[ValidateSubtreeInternal][%s] failed to serialize subtree", subtreeHash.String(), err)
		}

		u.logger.Debugf("[ValidateSubtreeInternal][%s] store subtree", subtreeHash.String())

		if err = u.subtreeStore.Set(gCtx,
			merkleRoot[:],
			fileformat.FileTypeSubtree,
			completeSubtreeBytes,
			options.WithDeleteAt(dah),
		); err != nil {
			if errors.Is(err, errors.ErrBlobAlreadyExists) {
				u.logger.Warnf("[ValidateSubtreeInternal][%s] subtree already exists in store", subtreeHash.String())
			} else {
				return errors.NewStorageError("[ValidateSubtreeInternal][%s] failed to store subtree", subtreeHash.String(), err)
			}
		}

		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}

	_ = u.SetSubtreeExists(subtreeHash)

	return nil
}

// getSubtreeTxHashes retrieves transaction hashes for a subtree from a remote source.
func (u *Server) getSubtreeTxHashes(spanCtx context.Context, stat *gocore.Stat, subtreeHash *chainhash.Hash, baseURL string) ([]chainhash.Hash, error) {
	if baseURL == "" {
		return nil, errors.NewInvalidArgumentError("[getSubtreeTxHashes][%s] baseUrl for subtree is empty", subtreeHash.String())
	}

	start := gocore.CurrentTime()

	txHashes := make([]chainhash.Hash, 0, u.settings.BlockAssembly.InitialMerkleItemsPerSubtree)

	// check whether we have a subtreeToCheck file and use that instead of doing a network request
	subtreeToCheckBytes, err := u.subtreeStore.Get(spanCtx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
	if err == nil && subtreeToCheckBytes != nil {
		u.logger.Debugf("[getSubtreeTxHashes][%s] found subtreeToCheck file in store, using it instead of network request", subtreeHash.String())

		subtree, err := subtreepkg.NewSubtreeFromBytes(subtreeToCheckBytes)
		if err != nil {
			return nil, errors.NewProcessingError("[getSubtreeTxHashes][%s] failed to create subtree from subtreeToCheck bytes", subtreeHash.String(), err)
		}

		// return the transaction hashes from the subtree
		for _, node := range subtree.Nodes {
			txHashes = append(txHashes, node.Hash)
		}

		return txHashes, nil
	}

	// do http request to baseUrl + subtreeHash.String()
	url := fmt.Sprintf("%s/subtree/%s", baseURL, subtreeHash.String())
	u.logger.Debugf("[getSubtreeTxHashes][%s] getting subtree from %s", subtreeHash.String(), url)

	// TODO add the metric for how long this takes
	// body, err := util.DoHTTPRequestBodyReader(spanCtx, url)
	subtreeBytes, err := util.DoHTTPRequest(spanCtx, url)
	if err != nil {
		// check whether this is a 404 error
		if errors.Is(err, errors.ErrNotFound) {
			// Peer cannot provide subtree data - report as invalid subtree
			u.publishInvalidSubtree(spanCtx, subtreeHash.String(), baseURL, "peer_cannot_provide_subtree")

			return nil, errors.NewSubtreeNotFoundError("[getSubtreeTxHashes][%s] subtree not found on host %s", subtreeHash.String(), baseURL, err)
		}

		return nil, errors.NewExternalError("[getSubtreeTxHashes][%s] failed to do http request on host %s", subtreeHash.String(), baseURL, err)
	}
	// defer body.Close()

	stat.NewStat("2. http fetch subtree").AddTime(start)

	start = gocore.CurrentTime()
	hashBuffer := make([]byte, chainhash.HashSize)

	// Use pooled bufio.Reader
	// bufferedReader := bufioReaderPool.Get().(*bufio.Reader)
	// bufferedReader.Reset(body)
	// defer func() {
	// 	bufferedReader.Reset(nil)
	// 	bufioReaderPool.Put(bufferedReader)
	// }()
	buf := bytes.NewReader(subtreeBytes)

	u.logger.Debugf("[getSubtreeTxHashes][%s] processing subtree response into tx hashes", subtreeHash.String())

	for {
		n, err := io.ReadFull(buf, hashBuffer)
		if n > 0 {
			txHashes = append(txHashes, chainhash.Hash(hashBuffer))
		}

		if err != nil {
			if err == io.EOF {
				break
			}
			// Not recoverable, returning processing error
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, errors.NewProcessingError("[getSubtreeTxHashes][%s] unexpected EOF: partial hash read", subtreeHash.String())
			}

			return nil, errors.NewProcessingError("[getSubtreeTxHashes][%s] error reading stream", subtreeHash.String(), err)
		}
	}

	stat.NewStat("3. createTxHashes").AddTime(start)

	u.logger.Debugf("[getSubtreeTxHashes][%s] done with subtree response", subtreeHash.String())

	// TODO: Report successful subtree fetch to improve peer reputation
	// Cannot call ReportValidSubtree here because we don't have peer ID, only baseURL (HTTP URL)
	// Need to track peer ID through the call chain if we want to enable this
	// if u.p2pClient != nil {
	// 	if err := u.p2pClient.ReportValidSubtree(spanCtx, peerID, subtreeHash.String()); err != nil {
	// 		u.logger.Warnf("[getSubtreeTxHashes][%s] failed to report valid subtree: %v", subtreeHash.String(), err)
	// 	}
	// }

	return txHashes, nil
}

// processMissingTransactions handles the retrieval and validation of missing transactions
// in a subtree, coordinating both the retrieval process and the validation workflow.
//
// This method is a critical part of the subtree validation process, responsible for:
// 1. Retrieving transactions that are referenced in the subtree but not available locally
// 2. Organizing transactions into dependency levels for ordered processing
// 3. Validating each transaction according to consensus rules
// 4. Managing parallel processing of independent transaction validations
// 5. Tracking validation results and updating transaction metadata
//
// The method supports both file-based and network-based transaction retrieval,
// with fallback mechanisms to ensure maximum resilience. It implements a level-based
// processing approach where transactions are grouped by dependency level and processed
// in order, ensuring that parent transactions are validated before their children.
//
// Performance optimization includes parallel processing of transactions within the same
// dependency level, which significantly improves validation throughput while maintaining
// correctness guarantees.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - subtreeHash: Hash of the subtree being validated
//   - subtree: Parsed subtree structure containing transaction relationships
//   - missingTxHashes: List of transaction hashes that need to be retrieved and validated
//   - allTxs: Complete list of all transaction hashes in the subtree
//   - baseURL: Source URL for retrieving missing transactions
//   - txMetaSlice: Pre-allocated slice to store transaction metadata results
//   - blockHeight: Height of the block containing the subtree
//   - blockIds: Map of block IDs to check if transactions are already mined
//   - validationOptions: Additional options for transaction validation behavior
//
// Returns:
//   - error: Any error encountered during retrieval or validation
func (u *Server) processMissingTransactions(ctx context.Context, subtreeHash chainhash.Hash, subtree *subtreepkg.Subtree,
	missingTxHashes []utxo.UnresolvedMetaData, allTxs []chainhash.Hash, baseURL string, txMetaSlice []metaSliceItem, blockHeight uint32,
	blockIds map[uint32]bool, validationOptions ...validator.Option) (err error) {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "SubtreeValidation:processMissingTransactions",
		tracing.WithDebugLogMessage(u.logger, "[processMissingTransactions][%s] processing %d missing txs", subtreeHash.String(), len(missingTxHashes)),
		tracing.WithNewRoot(), // decouple tracing from the parent context, otherwise it will explode with too many spans
	)

	defer func() {
		deferFn(err)
	}()

	isRunning, err := u.blockchainClient.IsFSMCurrentState(ctx, blockchain.FSMStateRUNNING)
	if err != nil {
		return errors.NewProcessingError("[validateSubtree][%s] failed to check if blockchain is running: %v", subtreeHash.String(), err)
	}

	missingTxs, err := u.getSubtreeMissingTxs(ctx, subtreeHash, subtree, missingTxHashes, allTxs, baseURL)
	if err != nil {
		return err
	}

	u.logger.Debugf("[validateSubtree][%s] blessing %d missing txs", subtreeHash.String(), len(missingTxs))

	var (
		mTx          missingTx
		missingCount atomic.Uint32
	)

	missed := make([]*chainhash.Hash, 0, len(txMetaSlice))
	missedMu := sync.Mutex{}

	// process the transactions in parallel, based on the number of parents in the list
	maxLevel, txsPerLevel, err := u.selectPrepareTxsPerLevel(ctx, missingTxs)
	if err != nil {
		return errors.NewProcessingError("[processMissingTransactions][%s] failed to prepare transactions per level: %v", subtreeHash.String(), err)
	}

	u.logger.Debugf("[processMissingTransactions][%s] maxLevel: %d", subtreeHash.String(), maxLevel)

	// pre-process the validation options into a struct
	processedValidatorOptions := validator.ProcessOptions(validationOptions...)

	var (
		errorsFound      = atomic.Uint64{}
		addedToOrphanage = atomic.Uint64{}
		firstError       error
		firstErrorOnce   sync.Once
	)

	for level := uint32(0); level <= maxLevel; level++ {
		g, gCtx := errgroup.WithContext(ctx)
		util.SafeSetLimit(g, u.settings.SubtreeValidation.SpendBatcherSize*2)

		u.logger.Debugf("[processMissingTransactions][%s] processing level %d/%d with %d transactions", subtreeHash.String(), level+1, maxLevel+1, len(txsPerLevel[level]))

		for _, mTx = range txsPerLevel[level] {
			tx := mTx.tx
			txIdx := mTx.idx

			if tx == nil {
				return errors.NewProcessingError("[validateSubtree][%s] missing transaction is nil", subtreeHash.String())
			}

			// process each transaction in the background, since the transactions are all batched into the utxo store
			g.Go(func() error {
				txMeta, err := u.blessMissingTransaction(gCtx, chainhash.Hash{}, subtreeHash, tx, blockHeight, blockIds, processedValidatorOptions)
				if err != nil {
					// Log the error, but do not return it, since we want to process all transactions in the subtree
					u.logger.Debugf("[validateSubtree][%s] failed to bless missing transaction: %s: %v", subtreeHash.String(), tx.TxIDChainHash().String(), err)
					errorsFound.Add(1)

					firstErrorOnce.Do(func() {
						firstError = errors.NewProcessingError("[validateSubtree][%s] failed to bless missing transaction: %s", subtreeHash.String(), tx.TxIDChainHash().String(), err)
					})

					// Check if this is a truly invalid transaction (not just policy error)
					if errors.Is(err, errors.ErrTxMissingParent) {
						// check whether we are in a running state, otherwise we can just ignore the missing parent transactions
						if isRunning {
							// add tx to the orphanage
							u.logger.Debugf("[validateSubtree][%s] transaction %s is missing parent, adding to orphanage", subtreeHash.String(), tx.TxIDChainHash().String())
							if u.orphanage.Set(*tx.TxIDChainHash(), tx) {
								addedToOrphanage.Add(1)
							} else {
								u.logger.Warnf("[validateSubtree][%s] Failed to add transaction %s to orphanage - orphanage is full", subtreeHash.String(), tx.TxIDChainHash().String())
							}
						}
					} else if errors.Is(err, errors.ErrTxInvalid) && !errors.Is(err, errors.ErrTxPolicy) {
						// Report invalid subtree - contains truly invalid transaction
						u.publishInvalidSubtree(gCtx, subtreeHash.String(), baseURL, "contains_invalid_transaction")

						// return the error, so that the caller can handle it
						if errors.Is(err, errors.ErrTxInvalid) {
							return err
						}
					} else {
						// If the error is not a policy error, we log it as a processing error
						u.logger.Errorf("[validateSubtree][%s] failed to bless missing transaction: %s: %v", subtreeHash.String(), tx.TxIDChainHash().String(), err)
					}

					return nil
				}

				if txMeta == nil {
					missingCount.Add(1)
					missedMu.Lock()
					missed = append(missed, tx.TxIDChainHash())
					missedMu.Unlock()
					u.logger.Infof("[validateSubtree][%s] tx meta is nil [%s]", subtreeHash.String(), tx.TxIDChainHash().String())
				} else {
					if txMetaSlice[txIdx].isSet {
						u.logger.Debugf("[validateSubtree][%s] tx meta already exists in txMetaSlice at index %d: %s", subtreeHash.String(), txIdx, tx.TxIDChainHash().String())
						errorsFound.Add(1)

						firstErrorOnce.Do(func() {
							firstError = errors.NewProcessingError("[validateSubtree][%s] tx meta already exists in txMetaSlice at index %d: %s", subtreeHash.String(), txIdx, tx.TxIDChainHash().String())
						})

						return nil
					}

					txMetaSlice[txIdx] = metaSliceItem{
						fee:         txMeta.Fee,
						sizeInBytes: txMeta.SizeInBytes,
						coinbase:    txMeta.IsCoinbase,
						conflicting: txMeta.Conflicting,
						creating:    txMeta.Creating,
						isSet:       true,
						txInpoints:  txMeta.TxInpoints,
					}
				}

				return nil
			})
		}

		// wait for each level to process separately
		if err = g.Wait(); err != nil {
			return err
		}
	}

	if errorsFound.Load() > 0 {
		// If there are errors found, we return here, so that the caller can handle it
		return errors.NewProcessingError("[validateSubtree][%s] found %d errors while processing subtree, added %d to orphanage", subtreeHash.String(), errorsFound.Load(), addedToOrphanage.Load(), firstError)
	}

	if missingCount.Load() > 0 {
		u.logger.Errorf("[validateSubtree][%s] %d missing entries in txMetaSlice (%d requested)", subtreeHash.String(), missingCount.Load(), len(txMetaSlice))

		for _, m := range missed {
			u.logger.Debugf("\t txid: %s", m)
		}
	}

	return nil
}

// getSubtreeMissingTxs retrieves transactions that are referenced in a subtree but not available locally.
//
// This method implements an intelligent retrieval strategy for missing transactions with
// optimizations for different scenarios. It first checks if a complete subtree data file exists
// locally, which would contain all transactions. If not available, it makes a decision based on
// the percentage of missing transactions:
//
//   - If a large percentage of transactions are missing (configurable threshold), it attempts to
//     fetch the entire subtree data file from the peer to optimize network usage.
//   - Otherwise, it retrieves only the specific missing transactions individually.
//
// The method employs fallback mechanisms to ensure maximum resilience, switching between
// file-based and network-based retrieval methods as needed. This approach balances efficiency
// with reliability, optimizing for both common and edge cases.
//
// Parameters:
//   - ctx: Context for cancellation, tracing, and timeout control
//   - subtreeHash: Hash of the subtree containing the transactions
//   - subtree: Parsed subtree structure for reference
//   - missingTxHashes: List of transaction hashes that need to be retrieved
//   - allTxs: Complete list of all transaction hashes in the subtree
//   - baseURL: Base URL for network-based transaction retrieval
//
// Returns:
//   - []missingTx: Slice of retrieved transactions paired with their indices
//   - error: Any error encountered during the retrieval process
func (u *Server) getSubtreeMissingTxs(ctx context.Context, subtreeHash chainhash.Hash, subtree *subtreepkg.Subtree,
	missingTxHashes []utxo.UnresolvedMetaData, allTxs []chainhash.Hash, baseURL string) ([]missingTx, error) {
	// first check whether we have the subtreeData file for this subtree and use that for the missing transactions
	subtreeDataExists, err := u.subtreeStore.Exists(ctx,
		subtreeHash[:],
		fileformat.FileTypeSubtreeData,
	)
	if err != nil {
		return nil, errors.NewProcessingError("[validateSubtree][%s] failed to check if subtreeData exists", subtreeHash.String(), err)
	}

	if !subtreeDataExists {
		subtreeSize := subtree.Size()
		missingTxLength := len(missingTxHashes)
		percentageMissing := 100 * float64(missingTxLength) / float64(subtreeSize)

		if percentageMissing > u.settings.SubtreeValidation.PercentageMissingGetFullData {
			// get the whole subtree from the other peer
			url := fmt.Sprintf("%s/subtree_data/%s", baseURL, subtreeHash.String())

			body, subtreeDataErr := util.DoHTTPRequestBodyReader(ctx, url)
			if subtreeDataErr != nil {
				// Peer cannot provide subtree data - report as invalid subtree
				u.publishInvalidSubtree(ctx, subtreeHash.String(), baseURL, "peer_cannot_provide_subtree_data")
				u.logger.Errorf("[validateSubtree][%s] failed to get subtree data from %s: %v", subtreeHash.String(), url, subtreeDataErr)
			} else {
				// Build subtree structure from allTxs for deserialization
				// We cannot use the empty 'subtree' parameter as it has no nodes yet
				subtreeForData, buildErr := subtreepkg.NewIncompleteTreeByLeafCount(len(allTxs))
				if buildErr != nil {
					u.logger.Errorf("[validateSubtree][%s] failed to create subtree for data: %v", subtreeHash.String(), buildErr)
					_ = body.Close()
				} else {
					// Add all transaction hashes to the subtree structure
					for _, txHash := range allTxs {
						if txHash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
							buildErr = subtreeForData.AddCoinbaseNode()
						} else {
							buildErr = subtreeForData.AddNode(txHash, 0, 0)
						}
						if buildErr != nil {
							u.logger.Errorf("[validateSubtree][%s] failed to add node to subtree: %v", subtreeHash.String(), buildErr)
							break
						}
					}

					if buildErr != nil {
						_ = body.Close()
					} else {
						// load the subtree data, making sure to validate it against the subtree txs
						// this is less efficient than reading straight to disk with SetFromReader, but we need to validate the
						// data before storing it on disk
						// Use buffered reader to reduce syscalls - each tx.ReadFrom() makes many small reads
						subtreeData, err := subtreepkg.NewSubtreeDataFromReader(subtreeForData, bufio.NewReaderSize(body, 1024*1024))
						_ = body.Close()
						if err != nil {
							u.logger.Errorf("[validateSubtree][%s] failed to create subtree data from reader: %v", subtreeHash.String(), err)
							// Can't proceed without valid subtree data, skip to next steps
						} else if subtreeData == nil || len(subtreeData.Txs) == 0 || subtreeData.Txs[len(subtreeData.Txs)-1] == nil {
							u.logger.Errorf("[validateSubtree][%s] subtree data is nil or empty", subtreeHash.String())
							// Invalid subtree data, skip to next steps
						} else if !subtreeForData.Nodes[len(subtreeForData.Nodes)-1].Hash.Equal(*subtreeData.Txs[len(subtreeData.Txs)-1].TxIDChainHash()) {
							return nil, errors.NewProcessingError("[validateSubtree][%s] subtree data does not match subtree", subtreeHash.String())
						} else {
							// Valid subtree data - proceed with serialization and storage
							subtreeDataBytes, err := subtreeData.Serialize()
							if err != nil {
								u.logger.Errorf("[validateSubtree][%s] failed to serialize subtree data: %v", subtreeHash.String(), err)
							} else {
								dah := u.utxoStore.GetBlockHeight() + u.settings.GetSubtreeValidationBlockHeightRetention()

								if subtreeDataErr = u.subtreeStore.Set(ctx,
									subtreeHash[:],
									fileformat.FileTypeSubtreeData,
									subtreeDataBytes,
									options.WithDeleteAt(dah),
								); subtreeDataErr != nil {
									u.logger.Errorf("[validateSubtree][%s] failed to store subtree data: %v", subtreeHash.String(), subtreeDataErr)
								} else {
									u.logger.Infof("[validateSubtree][%s] stored subtree data from %s", subtreeHash.String(), url)
									subtreeDataExists = true

									// TODO: Report successful subtree data fetch to improve peer reputation
									// Cannot call ReportValidSubtree here because we don't have peer ID, only baseURL (HTTP URL)
									// Need to track peer ID through the call chain if we want to enable this
									// if u.p2pClient != nil {
									// 	if err := u.p2pClient.ReportValidSubtree(ctx, peerID, subtreeHash.String()); err != nil {
									// 		u.logger.Warnf("[validateSubtree][%s] failed to report valid subtree: %v", subtreeHash.String(), err)
									// 	}
									// }
								}
							}
						}
					}
				}
			}
		}
	}

	var missingTxs []missingTx

	err = nil

	if subtreeDataExists {
		u.logger.Debugf("[validateSubtree][%s] fetching %d missing txs from subtreeData file", subtreeHash.String(), len(missingTxHashes))

		missingTxs, err = u.getMissingTransactionsFromFile(ctx, subtreeHash, missingTxHashes, allTxs)
	}

	if !subtreeDataExists || err != nil {
		u.logger.Debugf("[validateSubtree][%s] fetching %d missing txs", subtreeHash.String(), len(missingTxHashes))

		missingTxs, err = u.getMissingTransactionsFromPeer(ctx, subtreeHash, missingTxHashes, baseURL)
		if err != nil {
			return nil, errors.NewProcessingError("[validateSubtree][%s] failed to get missing transactions", subtreeHash.String(), err)
		}
	}

	return missingTxs, nil
}

// txMapWrapper contains transaction metadata used during validation processing.
//
// This structure wraps transaction data with additional metadata required for the validation
// process, particularly for tracking dependency relationships and block-level information.
// It serves as an internal data structure to maintain context during the complex validation
// workflow where transactions need to be processed in dependency order.
//
// The wrapper is used in the prepareTxsPerLevel method to organize transactions by their
// dependency levels, ensuring proper validation ordering while maintaining performance
// through parallel processing of independent transactions.
type txMapWrapper struct {
	// missingTx contains the transaction data and its original position in the subtree
	missingTx missingTx
	// someParentsInBlock indicates whether some of this transaction's parents are already in a block.
	// This flag is used to optimize validation by skipping certain checks for transactions
	// whose parents have already been validated and included in the blockchain.
	someParentsInBlock bool
	// childLevelInBlock represents the dependency level of this transaction within the block structure.
	// Lower levels indicate transactions with fewer dependencies, allowing for optimized
	// parallel processing during validation. Level 0 transactions have no dependencies
	// within the current subtree and can be validated first.
	childLevelInBlock uint32
}

// selectPrepareTxsPerLevel selects and executes the appropriate level preparation algorithm
// based on the UseOrderedLevelAlgorithm configuration setting.
func (u *Server) selectPrepareTxsPerLevel(ctx context.Context, transactions []missingTx) (uint32, [][]missingTx, error) {
	if u.settings.SubtreeValidation.UseOrderedLevelAlgorithm {
		return u.prepareTxsPerLevelOrdered(ctx, transactions)
	}
	return u.prepareTxsPerLevel(ctx, transactions)
}

// prepareTxsPerLevel organizes transactions by their dependency level for ordered processing.
//
// This method implements a topological sorting algorithm to organize transactions based on their
// dependency relationships. Transactions are grouped into levels, where each level contains
// transactions that can be processed in parallel without dependency conflicts.
//
// The level assignment follows these rules:
// - Level 0: Transactions with no parents in the current subtree
// - Level 1: Transactions with parents only in level 0
// - Level n: Transactions with parents in levels 0 through n-1
//
// This approach enables efficient parallel processing while maintaining correct validation order,
// ensuring that parent transactions are always validated before their children. The implementation
// is optimized for large subtrees with complex dependency graphs.
//
// Note: This code is conceptually similar to the transaction ordering logic in the legacy
// netsync/handle_block handler but is adapted for the subtree validation context and data structures.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - transactions: List of transactions to organize by level
//
// Returns:
//   - uint32: The maximum dependency level found
//   - map[uint32][]missingTx: Map of dependency levels to transactions at that level
//   - error: Any error encountered during the processing
func (u *Server) prepareTxsPerLevel(ctx context.Context, transactions []missingTx) (uint32, [][]missingTx, error) {
	_, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "prepareTxsPerLevel",
		tracing.WithDebugLogMessage(u.logger, "[prepareTxsPerLevel] preparing %d transactions per level", len(transactions)),
	)

	defer deferFn()

	// Build dependency graph with adjacency lists for efficient lookups
	txMap := make(map[chainhash.Hash]*txMapWrapper, len(transactions))
	maxLevel := uint32(0)
	sizePerLevel := make(map[uint32]uint64)

	// First pass: create all nodes and initialize structures
	for _, mTx := range transactions {
		if mTx.tx != nil && !mTx.tx.IsCoinbase() {
			hash := *mTx.tx.TxIDChainHash()
			txMap[hash] = &txMapWrapper{
				missingTx:         mTx,
				childLevelInBlock: 0,
			}
		}
	}

	// Second pass: calculate dependency levels using topological approach
	// Build dependency graph first
	dependencies := make(map[chainhash.Hash][]chainhash.Hash) // child -> parents
	childrenMap := make(map[chainhash.Hash][]chainhash.Hash)  // parent -> children

	for _, mTx := range transactions {
		if mTx.tx == nil || mTx.tx.IsCoinbase() {
			continue
		}

		txHash := *mTx.tx.TxIDChainHash()
		dependencies[txHash] = make([]chainhash.Hash, 0)

		// Check each input of the transaction to find its parents
		for _, input := range mTx.tx.Inputs {
			parentHash := *input.PreviousTxIDChainHash()

			// check if parentHash exists in the map, which means it is part of the subtree
			if _, exists := txMap[parentHash]; exists {
				dependencies[txHash] = append(dependencies[txHash], parentHash)

				if childrenMap[parentHash] == nil {
					childrenMap[parentHash] = make([]chainhash.Hash, 0)
				}
				childrenMap[parentHash] = append(childrenMap[parentHash], txHash)
			}
		}
	}

	// Calculate levels using iterative topological sort to avoid stack overflow
	// and detect circular dependencies
	levelCache := make(map[chainhash.Hash]uint32)

	// Find all transactions with no dependencies (level 0)
	for txHash, parents := range dependencies {
		if len(parents) == 0 {
			levelCache[txHash] = 0
		}
	}

	// Process remaining transactions level by level
	// Maximum iterations is len(dependencies) + 1 to handle all possible levels
	maxIterations := len(dependencies) + 1
	for iteration := 0; iteration < maxIterations; iteration++ {
		progress := false

		for txHash, parents := range dependencies {
			if _, exists := levelCache[txHash]; exists {
				continue
			}

			// Check if all parents have computed levels
			allParentsComputed := true
			maxParentLevel := uint32(0)
			for _, parentHash := range parents {
				parentLevel, exists := levelCache[parentHash]
				if !exists {
					allParentsComputed = false
					break
				}
				if parentLevel > maxParentLevel {
					maxParentLevel = parentLevel
				}
			}

			if allParentsComputed {
				levelCache[txHash] = maxParentLevel + 1
				progress = true
			}
		}

		if !progress {
			// No progress made - check if we're done or have a cycle
			if len(levelCache) < len(dependencies) {
				return 0, nil, errors.NewProcessingError("Circular dependency detected in transaction graph")
			}
			break
		}
	}

	// Update wrappers with calculated levels
	for _, mTx := range transactions {
		if mTx.tx == nil || mTx.tx.IsCoinbase() {
			continue
		}

		txHash := *mTx.tx.TxIDChainHash()
		wrapper := txMap[txHash]
		if wrapper == nil {
			continue
		}

		level, exists := levelCache[txHash]
		if !exists {
			// This shouldn't happen if the algorithm is correct
			return 0, nil, errors.NewProcessingError("Failed to calculate level for transaction")
		}

		wrapper.childLevelInBlock = level
		wrapper.someParentsInBlock = len(dependencies[txHash]) > 0

		sizePerLevel[level]++
		if level > maxLevel {
			maxLevel = level
		}
	}

	blocksPerLevelSlice := make([][]missingTx, maxLevel+1)

	// Build result map with pre-allocated slices
	for _, wrapper := range txMap {
		level := wrapper.childLevelInBlock
		if blocksPerLevelSlice[level] == nil {
			// Initialize the slice for this level if it doesn't exist
			blocksPerLevelSlice[level] = make([]missingTx, 0, sizePerLevel[level])
		}

		blocksPerLevelSlice[level] = append(blocksPerLevelSlice[level], wrapper.missingTx)
	}

	return maxLevel, blocksPerLevelSlice, nil
}

// prepareTxsPerLevelOrdered is an optimized version of prepareTxsPerLevel that assumes transactions
// are already in topological order (parents before children), as guaranteed by the Bitcoin protocol.
//
// ORDERING GUARANTEE: The Bitcoin protocol mandates that transactions within a block must be ordered
// such that parent transactions appear before their children. This is enforced during block construction
// and validated during block processing. All callers of this function provide transactions from:
//   - Block.Transactions() - guaranteed ordered by Bitcoin protocol
//   - Subtree.Txs - maintains block ordering when constructed
//   - processTransactionsInLevels - preserves original block/subtree ordering via indexed iteration
//
// This optimization reduces complexity from O(V*E + V²) to O(V*I) where:
//   - V = number of transactions
//   - E = number of dependencies
//   - I = average inputs per transaction
//
// SINGLE-PASS OPTIMIZATION: Calculates levels AND groups transactions simultaneously in ONE iteration.
// Eliminates: second pass, redundant hash calculations, and extra map lookups.
// Optimized for 1M+ transaction batches.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - transactions: List of transactions in topological order (parents before children)
//
// Returns:
//   - uint32: The maximum dependency level found
//   - [][]missingTx: Slice of dependency levels containing transactions at each level
//   - error: Any error encountered during processing
func (u *Server) prepareTxsPerLevelOrdered(ctx context.Context, transactions []missingTx) (uint32, [][]missingTx, error) {
	_, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "prepareTxsPerLevelOrdered",
		tracing.WithDebugLogMessage(u.logger, "[prepareTxsPerLevelOrdered] preparing %d transactions per level (optimized)", len(transactions)),
	)
	defer deferFn()

	// GC OPTIMIZATION: Use index-based approach to minimize heap allocations
	// Map stores hash -> transaction index (int is smaller than uint32 + reduces map overhead)
	// Levels stored in slice for fast array access instead of map lookups
	txIndex := make(map[chainhash.Hash]int, len(transactions))
	levels := make([]uint32, len(transactions))

	// Pre-allocate result slices with reasonable initial capacity
	// Most transactions are level 0 (no parents in block), so optimize for that case
	txsPerLevel := make([][]missingTx, 1, 16)                  // Start with level 0, capacity for 16 levels
	txsPerLevel[0] = make([]missingTx, 0, len(transactions)/2) // Level 0: assume ~50% of txs

	maxLevel := uint32(0)
	validTxCount := 0 // Track valid transactions for index mapping

	// SINGLE PASS: calculate levels AND append to result slices simultaneously
	for i, mTx := range transactions {
		if mTx.tx == nil || mTx.tx.IsCoinbase() {
			continue
		}

		// GC OPTIMIZATION: Get hash pointer once and reuse it
		// This avoids copying the 32-byte hash multiple times
		txHashPtr := mTx.tx.TxIDChainHash()
		txHash := *txHashPtr // Single dereference for map operations

		maxParentLevel := uint32(0)
		hasParentInBlock := false

		// Check each input to find the maximum parent level
		// GC OPTIMIZATION: Look up parent level in array instead of map
		for _, input := range mTx.tx.Inputs {
			parentHashPtr := input.PreviousTxIDChainHash()
			parentHash := *parentHashPtr // Single dereference

			// If parent exists in txIndex, it's part of this subtree/block
			if parentIdx, exists := txIndex[parentHash]; exists {
				hasParentInBlock = true
				// Array lookup is faster and more GC-friendly than map lookup
				parentLevel := levels[parentIdx]
				if parentLevel > maxParentLevel {
					maxParentLevel = parentLevel
				}
			}
		}

		// Calculate this transaction's level
		level := uint32(0)
		if hasParentInBlock {
			level = maxParentLevel + 1
		}

		// Store index mapping for children to reference
		// GC OPTIMIZATION: Store index (int) in map, level in array
		txIndex[txHash] = i
		levels[i] = level

		// Track max level and grow result slice if needed
		if level > maxLevel {
			maxLevel = level
			// Grow txsPerLevel slice to accommodate new level
			for uint32(len(txsPerLevel)) <= level {
				// GC OPTIMIZATION: Use more realistic capacity hints based on distribution
				// Level 0 is large, higher levels are progressively smaller
				capacity := 64
				if level == maxLevel && validTxCount > 1000 {
					// For new max level, estimate based on transaction count
					capacity = validTxCount / 100 // Heuristic: ~1% of txs at higher levels
					if capacity < 64 {
						capacity = 64
					}
				}
				txsPerLevel = append(txsPerLevel, make([]missingTx, 0, capacity))
			}
		}

		// Append directly to result slice (NO second pass!)
		txsPerLevel[level] = append(txsPerLevel[level], mTx)
		validTxCount++
	}

	return maxLevel, txsPerLevel, nil
}

// getMissingTransactionsFromPeer retrieves missing transactions from either the network or local store.
// It handles batching and parallel retrieval of transactions for improved performance.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - subtreeHash: Hash of the subtree containing the transactions
//   - missingTxHashes: Slice of transaction hashes to retrieve
//   - baseUrl: URL of the network source for transactions
//
// Returns:
//   - []missingTx: Slice of retrieved transactions with their indices
//   - error: Any error encountered during retrieval
func (u *Server) getMissingTransactionsFromFile(ctx context.Context, subtreeHash chainhash.Hash, missingTxHashes []utxo.UnresolvedMetaData,
	allTxs []chainhash.Hash) (missingTxs []missingTx, err error) {
	var subtree *subtreepkg.Subtree

	if len(allTxs) == 0 {
		// load the subtree
		subtreeReader, err := u.subtreeStore.GetIoReader(ctx,
			subtreeHash[:],
			fileformat.FileTypeSubtree,
		)
		if err != nil {
			// try getting the subtree from the store, marked as to be checked from the legacy service
			subtreeReader, err = u.subtreeStore.GetIoReader(ctx,
				subtreeHash[:],
				fileformat.FileTypeSubtreeToCheck,
			)
			if err != nil {
				return nil, errors.NewStorageError("[getMissingTransactionsFromFile] failed to get subtree from store", err)
			}
		}
		defer subtreeReader.Close()

		subtree = &subtreepkg.Subtree{}
		if err = subtree.DeserializeFromReader(subtreeReader); err != nil {
			return nil, err
		}
	} else {
		subtree, err = subtreepkg.NewIncompleteTreeByLeafCount(len(allTxs))
		if err != nil {
			return nil, errors.NewProcessingError("[getMissingTransactionsFromFile] failed to create new subtree from txs in memory", err)
		}

		for _, txHash := range allTxs {
			if txHash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
				if err = subtree.AddCoinbaseNode(); err != nil {
					return nil, errors.NewProcessingError("[getMissingTransactionsFromFile] failed to add coinbase placeholder node to subtree", err)
				}

				continue
			}

			if err = subtree.AddNode(txHash, 0, 0); err != nil {
				return nil, errors.NewProcessingError("[getMissingTransactionsFromFile] failed to add node to subtree", err)
			}
		}
	}

	// get the subtreeData
	subtreeDataReader, err := u.subtreeStore.GetIoReader(ctx,
		subtreeHash[:],
		fileformat.FileTypeSubtreeData,
	)
	if err != nil {
		return nil, errors.NewStorageError("[getMissingTransactionsFromFile] failed to get subtreeData from store", err)
	}
	defer subtreeDataReader.Close()

	// Use buffered reader to reduce syscalls - each tx.ReadFrom() makes many small reads
	subtreeData, err := subtreepkg.NewSubtreeDataFromReader(subtree, bufio.NewReaderSize(subtreeDataReader, 1024*1024))
	if err != nil {
		return nil, err
	}

	// Check that subtreeData is not nil or empty
	if subtreeData == nil || len(subtreeData.Txs) == 0 {
		return nil, errors.NewProcessingError("[getMissingTransactionsFromFile][%s] subtree data is nil or empty", subtreeHash.String())
	}

	// check that the last tx is the same, making sure we are not missing any transactions
	lastSubtreeDataTx := subtreeData.Txs[len(subtreeData.Txs)-1]
	if lastSubtreeDataTx == nil || !subtree.Nodes[len(subtree.Nodes)-1].Hash.Equal(*lastSubtreeDataTx.TxIDChainHash()) {
		return nil, errors.NewProcessingError("[validateSubtree][%s] subtree data does not match subtree", subtreeHash.String())
	}

	subtreeLookupMap, err := subtree.GetMap()
	if err != nil {
		return nil, err
	}

	// populate the missingTx slice with the tx data from the subtreeData
	missingTxs = make([]missingTx, 0, len(missingTxHashes))

	for _, mTx := range missingTxHashes {
		txIdx, ok := subtreeLookupMap.Get(mTx.Hash)
		if !ok {
			return nil, errors.NewProcessingError("[getMissingTransactionsFromFile] missing transaction [%s]", mTx.Hash.String())
		}

		tx := subtreeData.Txs[txIdx]
		if tx == nil {
			return nil, errors.NewProcessingError("[getMissingTransactionsFromFile] #2 missing transaction is nil [%s]", mTx.Hash.String())
		}

		missingTxs = append(missingTxs, missingTx{tx: tx, idx: mTx.Idx})
	}

	return missingTxs, nil
}

func (u *Server) getMissingTransactionsFromPeer(ctx context.Context, subtreeHash chainhash.Hash, missingTxHashes []utxo.UnresolvedMetaData,
	baseURL string) (missingTxs []missingTx, err error) {
	// transactions have to be returned in the same order as they were requested
	missingTxsMap := make(map[chainhash.Hash]*bt.Tx, len(missingTxHashes))
	missingTxsMu := sync.Mutex{}

	getMissingTransactionsConcurrency := u.settings.SubtreeValidation.GetMissingTransactions

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, getMissingTransactionsConcurrency) // keep 32 cores free for other tasks

	// get the transactions in batches of 500
	batchSize := u.settings.SubtreeValidation.MissingTransactionsBatchSize

	for i := 0; i < len(missingTxHashes); i += batchSize {
		missingTxHashesBatch := missingTxHashes[i:subtreepkg.Min(i+batchSize, len(missingTxHashes))]

		g.Go(func() error {
			missingTxsBatch, err := u.getMissingTransactionsBatch(gCtx, subtreeHash, missingTxHashesBatch, baseURL)
			if err != nil {
				return errors.NewProcessingError("[getMissingTransactionsFromPeer][%s] failed to get missing transactions batch", subtreeHash.String(), err)
			}

			missingTxsMu.Lock()
			for _, tx := range missingTxsBatch {
				if tx == nil {
					missingTxsMu.Unlock()
					return errors.NewProcessingError("[getMissingTransactionsFromPeer][%s] #1 missing transaction is nil", subtreeHash.String())
				}

				missingTxsMap[*tx.TxIDChainHash()] = tx
			}
			missingTxsMu.Unlock()

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return nil, errors.NewProcessingError("[getMissingTransaction][%s] failed to get all transactions", subtreeHash.String(), err)
	}

	// populate the missingTx slice with the tx data
	missingTxs = make([]missingTx, 0, len(missingTxHashes))

	for _, mTx := range missingTxHashes {
		tx, ok := missingTxsMap[mTx.Hash]
		if !ok {
			return nil, errors.NewProcessingError("[getMissingTransaction][%s] missing transaction [%s]", subtreeHash.String(), mTx.Hash.String())
		}

		if tx == nil {
			return nil, errors.NewProcessingError("[getMissingTransaction][%s] #3 missing transaction is nil [%s]", subtreeHash.String(), mTx.Hash.String())
		}

		missingTxs = append(missingTxs, missingTx{tx: tx, idx: mTx.Idx})
	}

	if len(missingTxs) != len(missingTxHashes) {
		return nil, errors.NewProcessingError("[getMissingTransaction][%s] missing tx count mismatch: missing=%d, txHashes=%d", subtreeHash.String(), len(missingTxs), len(missingTxHashes))
	}

	return missingTxs, nil
}

// isPrioritySubtreeCheckActive checks if a priority check is active for the given subtree hash.
func (u *Server) isPrioritySubtreeCheckActive(subtreeHash string) bool {
	u.prioritySubtreeCheckActiveMapLock.Lock()
	defer u.prioritySubtreeCheckActiveMapLock.Unlock()

	active, ok := u.prioritySubtreeCheckActiveMap[subtreeHash]

	return ok && active
}
