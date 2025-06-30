// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/pkg/go-safe-conversion"
	subtreepkg "github.com/bitcoin-sv/teranode/pkg/go-subtree"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/txmetacache"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
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
func (u *Server) SetSubtreeExists(_ *chainhash.Hash) error {
	// TODO: implement for local storage
	return nil
}

// GetSubtreeExists checks if a subtree exists in the local storage.
func (u *Server) GetSubtreeExists(_ context.Context, _ *chainhash.Hash) (bool, error) {
	return false, nil
}

// to help mocking the operations
type txMetaCacheOps interface {
	Delete(ctx context.Context, hash *chainhash.Hash) error
	SetCacheFromBytes(key, txMetaBytes []byte) error
}

// SetTxMetaCacheFromBytes stores raw transaction metadata bytes in the cache.
func (u *Server) SetTxMetaCacheFromBytes(_ context.Context, key, txMetaBytes []byte) error {
	if cache, ok := u.utxoStore.(txMetaCacheOps); ok {
		return cache.SetCacheFromBytes(key, txMetaBytes)
	}

	return nil
}

// DelTxMetaCache removes transaction metadata from the cache if caching is enabled.
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
			// Not recoverable, returning processing error
			return nil, errors.NewProcessingError("[getMissingTransactionsBatch][%s] failed to read transaction from body", subtreeHash.String(), err)
		}

		missingTxs = append(missingTxs, tx)
	}

	if len(missingTxs) != len(txHashes) {
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
		// github.com/libsv/go-bt/v2@v2.2.2/input.go:76 +0x16b
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

	if !tx.IsExtended() { // block height is not used
		return nil, errors.NewTxInvalidError("tx is not extended")
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
func (u *Server) blessMissingTransaction(ctx context.Context, subtreeHash chainhash.Hash, tx *bt.Tx, blockHeight uint32,
	blockIds map[uint32]bool, validationOptions *validator.Options) (txMeta *meta.Data, err error) {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "getMissingTransaction",
		tracing.WithHistogram(prometheusSubtreeValidationBlessMissingTransaction),
	)

	defer deferFn()

	if tx == nil {
		return nil, errors.NewTxInvalidError("[blessMissingTransaction][%s] tx is nil", subtreeHash.String())
	}

	if tx.IsCoinbase() {
		return nil, errors.NewTxInvalidError("[blessMissingTransaction][%s][%s] transaction is coinbase", subtreeHash.String(), tx.TxID())
	}

	// validate the transaction in the validation service
	// this should spend utxos, create the tx meta and create new utxos
	txMeta, err = u.validatorClient.ValidateWithOptions(ctx, tx, blockHeight, validationOptions)
	if err != nil {
		if errors.Is(err, errors.ErrTxConflicting) {
			// conflicting transaction, which has been saved, but not spent
			u.logger.Warnf("[blessMissingTransaction][%s][%s] transaction is conflicting", subtreeHash.String(), tx.TxID())
		} else {
			return nil, errors.NewServiceError("[blessMissingTransaction][%s][%s] failed to validate transaction", subtreeHash.String(), tx.TxID(), err)
		}
	}

	// Not recoverable, returning processing error
	if txMeta == nil {
		return nil, errors.NewProcessingError("[blessMissingTransaction][%s][%s] tx meta is nil", subtreeHash.String(), tx.TxID())
	}

	// check whether this transaction was already mined on our chain by comparing the block ids
	if len(txMeta.BlockIDs) > 0 && len(blockIds) > 0 {
		for _, blockID := range txMeta.BlockIDs {
			if blockIds[blockID] {
				return nil, errors.NewTxInvalidError("[blessMissingTransaction][%s][%s] transaction is already mined on our chain", subtreeHash.String(), tx.TxID())
			}
		}
	}

	if txMeta.Conflicting {
		if err = u.checkCounterConflictingOnCurrentChain(ctx, *tx.TxIDChainHash(), blockIds); err != nil {
			return nil, errors.NewProcessingError("[blessMissingTransaction][%s][%s] failed to check counter conflicting tx on current chain", subtreeHash.String(), tx.TxID(), err)
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

			counterConflictingTxMeta, err := u.utxoStore.GetMeta(gCtx, &counterConflictingTxHash)
			if err != nil {
				return errors.NewProcessingError("[checkCounterConflictingOnCurrentChain][%s] failed to get counter conflicting tx meta", txHash.String(), err)
			}

			if counterConflictingTxMeta == nil {
				return errors.NewProcessingError("[checkCounterConflictingOnCurrentChain][%s] counter conflicting tx meta is nil", txHash.String())
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

// ValidateSubtree contains the parameters needed for validating a subtree.
//
// This structure encapsulates all the necessary information required to validate
// a transaction subtree, providing a clean interface for the validation methods.
// It contains the identifying information for the subtree, source location for
// retrieving missing transactions, and configuration options for the validation process.
type ValidateSubtree struct {
	// SubtreeHash is the unique identifier hash of the subtree to be validated
	SubtreeHash chainhash.Hash

	// BaseURL is the source URL for retrieving missing transactions if needed
	BaseURL string

	// TxHashes contains the list of transaction hashes in the subtree
	// This may be empty if the subtree transactions need to be fetched from the store
	TxHashes []chainhash.Hash

	// AllowFailFast enables early termination of validation when an error is encountered
	// When true, validation stops at the first error; when false, attempts to validate all transactions
	// AllowFailFast enables quick failure for invalid subtrees
	AllowFailFast bool
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
	blockIds map[uint32]bool, validationOptions ...validator.Option) (err error) {

	stat := gocore.NewStat("ValidateSubtreeInternal")

	startTotal := time.Now()

	ctx, _, endSpan := tracing.Tracer("subtreevalidation").Start(ctx, "ValidateSubtreeInternal",
		tracing.WithHistogram(prometheusSubtreeValidationValidateSubtree),
	)

	defer func() {
		endSpan(err)
	}()

	u.logger.Infof("[ValidateSubtreeInternal][%s] called", v.SubtreeHash.String())

	start := gocore.CurrentTime()

	// Get the subtree hashes if they were passed in
	txHashes := v.TxHashes

	if txHashes == nil {
		subtreeExists, err := u.GetSubtreeExists(ctx, &v.SubtreeHash)

		stat.NewStat("1. subtreeExists").AddTime(start)

		if err != nil {
			return errors.NewStorageError("[ValidateSubtreeInternal][%s] failed to check if subtree exists in store", v.SubtreeHash.String(), err)
		}

		if subtreeExists {
			// If the subtree is already in the store, it means it is already validated.
			// Therefore, we finish processing of the subtree.
			return nil
		}

		// The function was called by BlockFound, and we had not already blessed the subtree, so we load the subtree from the store to get the hashes
		// get subtree from network over http using the baseUrl
		for retries := uint(0); retries < 3; retries++ {
			txHashes, err = u.getSubtreeTxHashes(ctx, stat, &v.SubtreeHash, v.BaseURL)
			if err != nil {
				if retries < 2 {
					backoff := time.Duration(1<<retries) * time.Second
					u.logger.Warnf("[ValidateSubtreeInternal][%s] failed to get subtree from network (try %d), will retry in %s", v.SubtreeHash.String(), retries, backoff.String())
					time.Sleep(backoff)
				} else {
					return errors.NewServiceError("[ValidateSubtreeInternal][%s] failed to get subtree from network", v.SubtreeHash.String(), err)
				}
			} else {
				break
			}
		}
	}

	// create the empty subtree
	height := math.Ceil(math.Log2(float64(len(txHashes))))

	subtree, err := subtreepkg.NewTree(int(height))
	if err != nil {
		return errors.NewProcessingError("failed to create new subtree", err)
	}

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)

	failFastValidation := u.settings.Block.FailFastValidation
	abandonTxThreshold := u.settings.BlockValidation.SubtreeValidationAbandonThreshold
	maxRetries := u.settings.BlockValidation.ValidationMaxRetries

	retrySleepDuration := u.settings.BlockValidation.RetrySleep

	// TODO document, what does this do?
	subtreeWarmupCount := u.settings.BlockValidation.ValidationWarmupCount

	subtreeWarmupCountInt32, err := safe.IntToInt32(subtreeWarmupCount)
	if err != nil {
		return err
	}

	// TODO document, what is the logic here?
	failFast := v.AllowFailFast && failFastValidation && u.subtreeCount.Add(1) > subtreeWarmupCountInt32

	// txMetaSlice will be populated with the txMeta data for each txHash
	// in the retry attempts, only the tx hashes that are missing will be retried, not the whole subtree
	txMetaSlice := make([]*meta.Data, len(txHashes))

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

		u.logger.Infof(logMsg)

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
			return err
		}

		if failFast && abandonTxThreshold > 0 && missed > abandonTxThreshold {
			// Not recoverable, returning processing error
			return errors.NewProcessingError("[ValidateSubtreeInternal][%s] [attempt #%d] failed to get tx meta from cache", v.SubtreeHash.String(), attempt, err)
		}

		if missed > 0 {
			batched := u.settings.SubtreeValidation.BatchMissingTransactions

			// 2. ...then attempt to load the txMeta from the store (i.e - aerospike in production)
			missed, err = u.processTxMetaUsingStore(ctx, txHashes, txMetaSlice, blockIds, batched, failFast)
			if err != nil {
				// Don't wrap the error again, processTxMetaUsingStore returns the correctly formated error.
				return err
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
				if txMetaSlice[idx] == nil && !txHash.IsEqual(subtreepkg.CoinbasePlaceholderHash) {
					missingTxHashesCompacted = append(missingTxHashesCompacted, utxo.UnresolvedMetaData{
						Hash: txHash,
						Idx:  idx,
					})
				}
			}

			u.logger.Infof("[ValidateSubtreeInternal][%s] [attempt #%d] processing %d missing tx for subtree instance", v.SubtreeHash.String(), attempt, len(missingTxHashesCompacted))

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
				return err
			}

			stat5.AddTime(start)
		}

		break
	}

	start = gocore.CurrentTime()

	var txMeta *meta.Data

	u.logger.Infof("[ValidateSubtreeInternal][%s] adding %d nodes to subtree instance", v.SubtreeHash.String(), len(txHashes))

	for idx, txHash := range txHashes {
		// if placeholder just add it and continue
		if idx == 0 && txHash.Equal(*subtreepkg.CoinbasePlaceholderHash) {
			err = subtree.AddCoinbaseNode()
			if err != nil {
				return errors.NewProcessingError("[ValidateSubtreeInternal][%s] failed to add coinbase placeholder node to subtree", v.SubtreeHash.String(), err)
			}

			continue
		}

		txMeta = txMetaSlice[idx]
		if txMeta == nil {
			return errors.NewProcessingError("[ValidateSubtreeInternal][%s] tx meta not found in txMetaSlice at index %d: %s", v.SubtreeHash.String(), idx, txHash.String())
		}

		if txMeta.IsCoinbase {
			// Not recoverable, returning TxInvalid error
			return errors.NewTxInvalidError("[ValidateSubtreeInternal][%s] invalid subtree index for coinbase tx %d", v.SubtreeHash.String(), idx, err)
		}

		// finally add the transaction hash and fee to the subtree
		if err = subtree.AddNode(txHash, txMeta.Fee, txMeta.SizeInBytes); err != nil {
			return errors.NewProcessingError("[ValidateSubtreeInternal][%s] failed to add node to subtree / subtreeMeta", v.SubtreeHash.String(), err)
		}

		// mark the transaction as conflicting if it is
		if txMeta.Conflicting {
			if err = subtree.AddConflictingNode(txHash); err != nil {
				return errors.NewProcessingError("[ValidateSubtreeInternal][%s] failed to add conflicting node to subtree", v.SubtreeHash.String(), err)
			}
		}

		// add the txMeta data we need for block validation
		subtreeIdx := subtree.Length() - 1

		if err = subtreeMeta.SetTxInpoints(subtreeIdx, txMeta.TxInpoints); err != nil {
			return errors.NewProcessingError("[ValidateSubtreeInternal][%s] failed to set parent tx hash in subtreeMeta", v.SubtreeHash.String(), err)
		}
	}

	stat.NewStat("6. addAllTxHashFeeSizesToSubtree").AddTime(start)

	// does the merkle tree give the correct root?
	merkleRoot := subtree.RootHash()
	if !merkleRoot.IsEqual(&v.SubtreeHash) {
		return errors.NewSubtreeInvalidError("subtree root hash does not match", err)
	}

	//
	// store subtree meta in store
	//
	u.logger.Debugf("[ValidateSubtreeInternal][%s] serialize subtree meta", v.SubtreeHash.String())

	completeSubtreeMetaBytes, err := subtreeMeta.Serialize()
	if err != nil {
		return errors.NewProcessingError("[ValidateSubtreeInternal][%s] failed to serialize subtree meta", v.SubtreeHash.String(), err)
	}

	start = gocore.CurrentTime()

	u.logger.Debugf("[ValidateSubtreeInternal][%s] store subtree meta", v.SubtreeHash.String())

	dah := u.utxoStore.GetBlockHeight() + u.settings.GlobalBlockHeightRetention

	err = u.subtreeStore.Set(ctx, merkleRoot[:], fileformat.FileTypeSubtreeMeta, completeSubtreeMetaBytes, options.WithDeleteAt(dah))

	stat.NewStat("7. storeSubtreeMeta").AddTime(start)

	if err != nil {
		if errors.Is(err, errors.ErrBlobAlreadyExists) {
			u.logger.Warnf("[ValidateSubtreeInternal][%s] subtree meta already exists in store", v.SubtreeHash.String())
		} else {
			return errors.NewStorageError("[ValidateSubtreeInternal][%s] failed to store subtree meta", v.SubtreeHash.String(), err)
		}
	}

	//
	// store subtree in store
	//
	u.logger.Debugf("[ValidateSubtreeInternal][%s] serialize subtree", v.SubtreeHash.String())

	completeSubtreeBytes, err := subtree.Serialize()
	if err != nil {
		return errors.NewProcessingError("[ValidateSubtreeInternal][%s] failed to serialize subtree", v.SubtreeHash.String(), err)
	}

	start = gocore.CurrentTime()

	u.logger.Debugf("[ValidateSubtreeInternal][%s] store subtree", v.SubtreeHash.String())

	err = u.subtreeStore.Set(ctx,
		merkleRoot[:],
		fileformat.FileTypeSubtree,
		completeSubtreeBytes,
		options.WithDeleteAt(dah),
	)

	stat.NewStat("8. storeSubtree").AddTime(start)

	if err != nil {
		if errors.Is(err, errors.ErrBlobAlreadyExists) {
			u.logger.Warnf("[ValidateSubtreeInternal][%s] subtree already exists in store", v.SubtreeHash.String())
		} else {
			return errors.NewStorageError("[ValidateSubtreeInternal][%s] failed to store subtree", v.SubtreeHash.String(), err)
		}
	}

	_ = u.SetSubtreeExists(&v.SubtreeHash)

	// only set this on no errors
	prometheusSubtreeValidationValidateSubtreeDuration.Observe(float64(time.Since(startTotal).Microseconds()) / 1_000_000)

	return nil
}

// getSubtreeTxHashes retrieves transaction hashes for a subtree from a remote source.
func (u *Server) getSubtreeTxHashes(spanCtx context.Context, stat *gocore.Stat, subtreeHash *chainhash.Hash, baseURL string) ([]chainhash.Hash, error) {
	if baseURL == "" {
		return nil, errors.NewInvalidArgumentError("[getSubtreeTxHashes][%s] baseUrl for subtree is empty", subtreeHash.String())
	}

	start := gocore.CurrentTime()

	// do http request to baseUrl + subtreeHash.String()
	url := fmt.Sprintf("%s/subtree/%s", baseURL, subtreeHash.String())
	u.logger.Infof("[getSubtreeTxHashes][%s] getting subtree from %s", subtreeHash.String(), url)

	// TODO add the metric for how long this takes
	body, err := util.DoHTTPRequestBodyReader(spanCtx, url)
	if err != nil {
		// check whether this is a 404 error
		if errors.Is(err, errors.ErrNotFound) {
			return nil, errors.NewSubtreeNotFoundError("[getSubtreeTxHashes][%s] subtree not found on host %s", subtreeHash.String(), baseURL, err)
		}

		return nil, errors.NewExternalError("[getSubtreeTxHashes][%s] failed to do http request on host %s", subtreeHash.String(), baseURL, err)
	}
	defer body.Close()

	stat.NewStat("2. http fetch subtree").AddTime(start)

	start = gocore.CurrentTime()
	txHashes := make([]chainhash.Hash, 0, u.settings.BlockAssembly.InitialMerkleItemsPerSubtree)
	buffer := make([]byte, chainhash.HashSize)
	bufferedReader := bufio.NewReaderSize(body, 1024*1024*4) // 4MB buffer

	u.logger.Debugf("[getSubtreeTxHashes][%s] processing subtree response into tx hashes", subtreeHash.String())

	for {
		n, err := io.ReadFull(bufferedReader, buffer)
		if n > 0 {
			txHashes = append(txHashes, chainhash.Hash(buffer))
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
	missingTxHashes []utxo.UnresolvedMetaData, allTxs []chainhash.Hash, baseURL string, txMetaSlice []*meta.Data, blockHeight uint32,
	blockIds map[uint32]bool, validationOptions ...validator.Option) (err error) {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "SubtreeValidation:processMissingTransactions",
		tracing.WithDebugLogMessage(u.logger, "[processMissingTransactions][%s] processing %d missing txs", subtreeHash.String(), len(missingTxHashes)),
	)

	defer func() {
		deferFn(err)
	}()

	missingTxs, err := u.getSubtreeMissingTxs(ctx, subtreeHash, subtree, missingTxHashes, allTxs, baseURL)
	if err != nil {
		return err
	}

	u.logger.Infof("[validateSubtree][%s] blessing %d missing txs", subtreeHash.String(), len(missingTxs))

	var (
		mTx          missingTx
		missingCount atomic.Uint32
	)

	missed := make([]*chainhash.Hash, 0, len(txMetaSlice))
	missedMu := sync.Mutex{}

	// process the transactions in parallel, based on the number of parents in the list
	maxLevel, txsPerLevel := u.prepareTxsPerLevel(ctx, missingTxs)

	u.logger.Debugf("[processMissingTransactions][%s] maxLevel: %d", subtreeHash.String(), maxLevel)

	// pre-process the validation options into a struct
	processedValidatorOptions := validator.ProcessOptions(validationOptions...)

	for level := uint32(0); level <= maxLevel; level++ {
		g, gCtx := errgroup.WithContext(ctx)
		util.SafeSetLimit(g, u.settings.SubtreeValidation.SpendBatcherSize*2)

		u.logger.Debugf("[processMissingTransactions][%s] processing level %d with %d transactions", subtreeHash.String(), level, len(txsPerLevel[level]))

		for _, mTx = range txsPerLevel[level] {
			tx := mTx.tx
			txIdx := mTx.idx

			if tx == nil {
				return errors.NewProcessingError("[validateSubtree][%s] missing transaction is nil", subtreeHash.String())
			}

			// process each transaction in the background, since the transactions are all batched into the utxo store
			g.Go(func() error {
				txMeta, err := u.blessMissingTransaction(gCtx, subtreeHash, tx, blockHeight, blockIds, processedValidatorOptions)
				if err != nil {
					return errors.NewProcessingError("[validateSubtree][%s] failed to bless missing transaction: %s", subtreeHash.String(), tx.TxIDChainHash().String(), err)
				}

				if txMeta == nil {
					missingCount.Add(1)
					missedMu.Lock()
					missed = append(missed, tx.TxIDChainHash())
					missedMu.Unlock()
					u.logger.Infof("[validateSubtree][%s] tx meta is nil [%s]", subtreeHash.String(), tx.TxIDChainHash().String())
				} else {
					if txMetaSlice[txIdx] != nil {
						return errors.NewProcessingError("[validateSubtree][%s] tx meta already exists in txMetaSlice at index %d: %s", subtreeHash.String(), txIdx, tx.TxIDChainHash().String())
					}

					txMetaSlice[txIdx] = txMeta
				}

				return nil
			})
		}

		// wait for each level to process separately
		if err = g.Wait(); err != nil {
			return err
		}
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
				u.logger.Errorf("[validateSubtree][%s] failed to get subtree data from %s: %v", subtreeHash.String(), url, subtreeDataErr)
			} else {
				if subtreeDataErr = u.subtreeStore.SetFromReader(ctx,
					subtreeHash[:],
					fileformat.FileTypeSubtreeData,
					body,
				); subtreeDataErr != nil {
					u.logger.Errorf("[validateSubtree][%s] failed to store subtree data: %v", subtreeHash.String(), subtreeDataErr)
				} else {
					u.logger.Infof("[validateSubtree][%s] stored subtree data from %s", subtreeHash.String(), url)

					subtreeDataExists = true
				}

				_ = body.Close()
			}
		}
	}

	var missingTxs []missingTx

	if subtreeDataExists {
		u.logger.Infof("[validateSubtree][%s] fetching %d missing txs from subtreeData file", subtreeHash.String(), len(missingTxHashes))

		missingTxs, err = u.getMissingTransactionsFromFile(ctx, subtreeHash, missingTxHashes, allTxs)
		if err != nil {
			return nil, errors.NewProcessingError("[validateSubtree][%s] failed to get missing transactions from subtreeData", subtreeHash.String(), err)
		}
	} else {
		u.logger.Infof("[validateSubtree][%s] fetching %d missing txs", subtreeHash.String(), len(missingTxHashes))

		missingTxs, err = u.getMissingTransactionsFromPeer(ctx, subtreeHash, missingTxHashes, baseURL)
		if err != nil {
			return nil, errors.NewProcessingError("[validateSubtree][%s] failed to get missing transactions", subtreeHash.String(), err)
		}
	}

	return missingTxs, nil
}

// txMapWrapper contains transaction metadata used during validation.
type txMapWrapper struct {
	missingTx          missingTx
	someParentsInBlock bool
	childLevelInBlock  uint32
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

func (u *Server) prepareTxsPerLevel(ctx context.Context, transactions []missingTx) (uint32, map[uint32][]missingTx) {
	_, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "prepareTxsPerLevel",
		tracing.WithDebugLogMessage(u.logger, "[prepareTxsPerLevel] preparing %d transactions per level", len(transactions)),
	)

	defer deferFn()

	// create a map of transactions for easy lookup when determining parents
	txMap := make(map[chainhash.Hash]*txMapWrapper)

	for _, mTx := range transactions {
		txHash := *mTx.tx.TxIDChainHash()
		// don't add the coinbase to the txMap, we cannot process it anyway
		if !mTx.tx.IsCoinbase() {
			txMap[txHash] = &txMapWrapper{missingTx: mTx}

			for _, input := range mTx.tx.Inputs {
				prevTxHash := *input.PreviousTxIDChainHash()
				if _, found := txMap[prevTxHash]; found {
					txMap[txHash].someParentsInBlock = true
				}
			}
		}
	}

	maxLevel := uint32(0)
	sizePerLevel := make(map[uint32]uint64)
	blockTxsPerLevel := make(map[uint32][]missingTx)

	// determine the level of each transaction in the block, based on the number of parents in the block
	for _, mTx := range transactions {
		txHash := *mTx.tx.TxIDChainHash()
		if _, found := txMap[txHash]; found {
			if txMap[txHash].someParentsInBlock {
				for _, input := range txMap[txHash].missingTx.tx.Inputs {
					parentTxHash := *input.PreviousTxIDChainHash()
					if parentTxWrapper, found := txMap[parentTxHash]; found {
						// if the parent from this input is at the same level or higher,
						// we need to increase the child level of this transaction
						if parentTxWrapper.childLevelInBlock >= txMap[txHash].childLevelInBlock {
							txMap[txHash].childLevelInBlock = parentTxWrapper.childLevelInBlock + 1
						}

						if txMap[txHash].childLevelInBlock > maxLevel {
							maxLevel = txMap[txHash].childLevelInBlock
						}
					}
				}
			}

			sizePerLevel[txMap[txHash].childLevelInBlock] += 1
		}
	}

	// pre-allocation of the blockTxsPerLevel map
	for i := uint32(0); i <= maxLevel; i++ {
		blockTxsPerLevel[i] = make([]missingTx, 0, sizePerLevel[i])
	}

	// put all transactions in a map per level for processing
	for _, txWrapper := range txMap {
		blockTxsPerLevel[txWrapper.childLevelInBlock] = append(blockTxsPerLevel[txWrapper.childLevelInBlock], txWrapper.missingTx)
	}

	return maxLevel, blockTxsPerLevel
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

	subtreeData, err := subtreepkg.NewSubtreeDataFromReader(subtree, subtreeDataReader)
	if err != nil {
		return nil, err
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
