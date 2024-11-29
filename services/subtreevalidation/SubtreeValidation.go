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

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

// missingTx represents a transaction that needs to be retrieved and its position in the subtree.
type missingTx struct {
	tx  *bt.Tx
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

// SetTxMetaCache stores transaction metadata in the cache if caching is enabled.
func (u *Server) SetTxMetaCache(ctx context.Context, hash *chainhash.Hash, txMeta *meta.Data) error {
	if cache, ok := u.utxoStore.(*txmetacache.TxMetaCache); ok {
		_, _, deferFn := tracing.StartTracing(ctx, "SubtreeValidation:SetTxMetaCache")
		defer deferFn()

		return cache.SetCache(hash, txMeta)
	}

	return nil
}

// SetTxMetaCacheFromBytes stores raw transaction metadata bytes in the cache.
func (u *Server) SetTxMetaCacheFromBytes(_ context.Context, key, txMetaBytes []byte) error {
	if cache, ok := u.utxoStore.(*txmetacache.TxMetaCache); ok {
		return cache.SetCacheFromBytes(key, txMetaBytes)
	}

	return nil
}

// DelTxMetaCache removes transaction metadata from the cache if caching is enabled.
func (u *Server) DelTxMetaCache(ctx context.Context, hash *chainhash.Hash) error {
	if cache, ok := u.utxoStore.(*txmetacache.TxMetaCache); ok {
		ctx, _, deferFn := tracing.StartTracing(ctx, "SubtreeValidation:DelTxMetaCache")
		defer deferFn()

		return cache.Delete(ctx, hash)
	}

	return nil
}

// DelTxMetaCacheMulti removes multiple transaction metadata entries from the cache.
func (u *Server) DelTxMetaCacheMulti(ctx context.Context, hash *chainhash.Hash) error {
	if cache, ok := u.utxoStore.(*txmetacache.TxMetaCache); ok {
		ctx, _, deferFn := tracing.StartTracing(ctx, "SubtreeValidation:DelTxMetaCacheMulti")
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
func (u *Server) getMissingTransactionsBatch(ctx context.Context, subtreeHash *chainhash.Hash, txHashes []utxo.UnresolvedMetaData, baseURL string) ([]*bt.Tx, error) {
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

	// do http request to baseUrl + txHash.String()
	u.logger.Debugf("[getMissingTransactionsBatch][%s] getting %d txs from other miner %s", subtreeHash.String(), len(txHashes), baseURL)
	url := fmt.Sprintf("%s/txs", baseURL)

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

	if !util.IsExtended(tx, 0) { // block height is not used
		return nil, errors.NewTxInvalidError("tx is not extended")
	}

	return tx, nil
}

// blessMissingTransaction validates a transaction and retrieves its metadata.
// The transaction is validated against the current blockchain state and its
// metadata is stored for future reference.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - subtreeHash: Hash of the subtree containing the transaction
//   - tx: Transaction to validate
//   - blockHeight: Height of the block containing the transaction
//
// Returns:
//   - *meta.Data: Transaction metadata if validation succeeds
//   - error: Any error encountered during validation
func (u *Server) blessMissingTransaction(ctx context.Context, subtreeHash *chainhash.Hash, tx *bt.Tx, blockHeight uint32) (txMeta *meta.Data, err error) {
	ctx, stat, deferFn := tracing.StartTracing(ctx, "getMissingTransaction",
		tracing.WithHistogram(prometheusSubtreeValidationBlessMissingTransaction),
	)
	defer deferFn()

	if tx == nil {
		return nil, errors.NewTxInvalidError("[blessMissingTransaction][%s] tx is nil", subtreeHash.String())
	}

	u.logger.Debugf("[blessMissingTransaction][%s][%s] called", subtreeHash.String(), tx.TxID())

	if tx.IsCoinbase() {
		return nil, errors.NewTxInvalidError("[blessMissingTransaction][%s][%s] transaction is coinbase", subtreeHash.String(), tx.TxID())
	}

	// validate the transaction in the validation service
	// this should spend utxos, create the tx meta and create new utxos
	// TODO return tx meta data
	// u.logger.Debugf("[blessMissingTransaction][%s] validating transaction (pq:)", tx.TxID())
	err = u.validatorClient.Validate(ctx, tx, blockHeight)
	if err != nil {
		// TODO what to do here? This could be a double spend and the transaction needs to be marked as conflicting
		return nil, errors.NewServiceError("[blessMissingTransaction][%s][%s] failed to validate transaction", subtreeHash.String(), tx.TxID(), err)
	}

	start := gocore.CurrentTime()
	txMeta, err = u.utxoStore.GetMeta(ctx, tx.TxIDChainHash())

	stat.NewStat("getTxMeta").AddTime(start)

	if err != nil {
		return nil, errors.NewServiceError("[blessMissingTransaction][%s][%s] failed to get tx meta", subtreeHash.String(), tx.TxID(), err)
	}

	// Not recoverable, returning processing error
	if txMeta == nil {
		return nil, errors.NewProcessingError("[blessMissingTransaction][%s][%s] tx meta is nil", subtreeHash.String(), tx.TxID())
	}

	return txMeta, nil
}

// ValidateSubtree contains the parameters needed for validating a subtree.
type ValidateSubtree struct {
	// SubtreeHash is the merkle root hash of the subtree to validate
	SubtreeHash chainhash.Hash
	// BaseURL is the source URL for fetching missing transactions
	BaseURL string
	// TxHashes contains the list of transaction hashes in the subtree
	TxHashes []chainhash.Hash
	// AllowFailFast enables quick failure for invalid subtrees
	AllowFailFast bool
}

// validateSubtreeInternal performs the actual validation of a subtree.
// It handles transaction validation, metadata management, and subtree storage.
func (u *Server) validateSubtreeInternal(ctx context.Context, v ValidateSubtree, blockHeight uint32) (err error) {
	startTotal := time.Now()
	ctx, stat, deferFn := tracing.StartTracing(ctx, "validateSubtreeInternal",
		tracing.WithHistogram(prometheusSubtreeValidationValidateSubtree),
	)

	defer func() {
		deferFn(err)
	}()

	u.logger.Infof("[validateSubtreeInternal][%s] called", v.SubtreeHash.String())

	start := gocore.CurrentTime()

	// Get the subtree hashes if they were passed in (SubtreeFound() passes them in, BlockFound does not)
	txHashes := v.TxHashes

	if txHashes == nil {
		subtreeExists, err := u.GetSubtreeExists(ctx, &v.SubtreeHash)

		stat.NewStat("1. subtreeExists").AddTime(start)

		if err != nil {
			return errors.NewStorageError("[validateSubtreeInternal][%s] failed to check if subtree exists in store", v.SubtreeHash.String(), err)
		}

		if subtreeExists {
			// If the subtree is already in the store, it means it is already validated.
			// Therefore, we finish processing of the subtree.
			return nil
		}

		// The function was called by BlockFound, and we had not already blessed the subtree, so we load the subtree from the store to get the hashes
		// get subtree from network over http using the baseUrl
		for retries := 0; retries < 3; retries++ {
			txHashes, err = u.getSubtreeTxHashes(ctx, stat, &v.SubtreeHash, v.BaseURL)
			if err != nil {
				// TODO: Unify retry logic with the helper function
				if retries < 2 {
					backoff := time.Duration(2^retries) * time.Second
					u.logger.Warnf("[validateSubtreeInternal][%s] failed to get subtree from network (try %d), will retry in %s", v.SubtreeHash.String(), retries, backoff.String())
					time.Sleep(backoff)
				} else {
					return errors.NewServiceError("[validateSubtreeInternal][%s] failed to get subtree from network", v.SubtreeHash.String(), err)
				}
			} else {
				break
			}
		}
	}

	// create the empty subtree
	height := math.Ceil(math.Log2(float64(len(txHashes))))

	subtree, err := util.NewTree(int(height))
	if err != nil {
		return errors.NewProcessingError("failed to create new subtree", err)
	}

	subtreeMeta := util.NewSubtreeMeta(subtree)

	failFastValidation := u.settings.Block.FailFastValidation
	abandonTxThreshold := u.settings.BlockValidation.SubtreeValidationAbandonThreshold
	maxRetries := u.settings.BlockValidation.ValidationMaxRetries

	retrySleepDuration := u.settings.BlockValidation.RetrySleep

	// TODO document, what does this do?
	subtreeWarmupCount := u.settings.BlockValidation.ValidationWarmupCount

	// TODO document, what is the logic here?
	failFast := v.AllowFailFast && failFastValidation && u.subtreeCount.Add(1) > int32(subtreeWarmupCount)

	// txMetaSlice will be populated with the txMeta data for each txHash
	// in the retry attempts, only the tx hashes that are missing will be retried, not the whole subtree
	txMetaSlice := make([]*meta.Data, len(txHashes))

	for attempt := 1; attempt <= maxRetries+1; attempt++ {
		prometheusSubtreeValidationValidateSubtreeRetry.Inc()

		if u.isPrioritySubtreeCheckActive(v.SubtreeHash.String()) {
			failFast = false
			u.logger.Infof("[validateSubtreeInternal][%s] [attempt #%d] Priority request (fail fast=%v) - final priority attempt to process subtree, this time with full checks enabled", v.SubtreeHash.String(), attempt, failFast)
		} else if attempt > maxRetries {
			failFast = false

			u.logger.Infof("[validateSubtreeInternal][%s] [attempt #%d] final attempt to process subtree, this time with full checks enabled", v.SubtreeHash.String(), attempt)
		} else {
			u.logger.Infof("[validateSubtreeInternal][%s] [attempt #%d] (fail fast=%v) process %d txs from subtree", v.SubtreeHash.String(), attempt, failFast, len(txHashes))
		}

		// unlike many other lists, this needs to be a pointer list, because a lot of values could be empty = nil

		// 1. First attempt to load the txMeta from the cache...
		missed, err := u.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, failFast)
		if err != nil {
			if errors.Is(err, errors.ErrThresholdExceeded) {
				u.logger.Warnf("[validateSubtreeInternal][%s] [attempt #%d] too many missing txmeta entries in cache (fail fast check only, will retry)", v.SubtreeHash.String(), attempt)
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
			return errors.NewProcessingError("[validateSubtreeInternal][%s] [attempt #%d] failed to get tx meta from cache", v.SubtreeHash.String(), attempt, err)
		}

		if missed > 0 {
			batched := u.settings.SubtreeValidation.BatchMissingTransactions

			// 2. ...then attempt to load the txMeta from the store (i.e - aerospike in production)
			missed, err = u.processTxMetaUsingStore(ctx, txHashes, txMetaSlice, batched, failFast)
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
				if txMetaSlice[idx] == nil && !txHash.IsEqual(util.CoinbasePlaceholderHash) {
					missingTxHashesCompacted = append(missingTxHashesCompacted, utxo.UnresolvedMetaData{
						Hash: txHash,
						Idx:  idx,
					})
				}
			}

			u.logger.Infof("[validateSubtreeInternal][%s] [attempt #%d] processing %d missing tx for subtree instance", v.SubtreeHash.String(), attempt, len(missingTxHashesCompacted))

			err = u.processMissingTransactions(ctx5, &v.SubtreeHash, missingTxHashesCompacted, v.BaseURL, txMetaSlice, blockHeight)
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

	u.logger.Infof("[validateSubtreeInternal][%s] adding %d nodes to subtree instance", v.SubtreeHash.String(), len(txHashes))

	for idx, txHash := range txHashes {
		// if placeholder just add it and continue
		if idx == 0 && txHash.Equal(*util.CoinbasePlaceholderHash) {
			err = subtree.AddCoinbaseNode()
			if err != nil {
				return errors.NewProcessingError("[validateSubtreeInternal][%s] failed to add coinbase placeholder node to subtree", v.SubtreeHash.String(), err)
			}

			continue
		}

		txMeta = txMetaSlice[idx]
		if txMeta == nil {
			return errors.NewProcessingError("[validateSubtreeInternal][%s] tx meta not found in txMetaSlice at index %d: %s", v.SubtreeHash.String(), idx, txHash.String())
		}

		if txMeta.IsCoinbase {
			// Not recoverable, returning TxInvalid error
			return errors.NewTxInvalidError("[validateSubtreeInternal][%s] invalid subtree index for coinbase tx %d", v.SubtreeHash.String(), idx, err)
		}

		// finally add the transaction hash and fee to the subtree
		err = subtree.AddNode(txHash, txMeta.Fee, txMeta.SizeInBytes)
		if err != nil {
			return errors.NewProcessingError("[validateSubtreeInternal][%s] failed to add node to subtree / subtreeMeta", v.SubtreeHash.String(), err)
		}

		// add the txMeta data we need for block validation
		subtreeIdx := subtree.Length() - 1

		err = subtreeMeta.SetParentTxHashes(subtreeIdx, txMeta.ParentTxHashes)
		if err != nil {
			return errors.NewProcessingError("[validateSubtreeInternal][%s] failed to set parent tx hash in subtreeMeta", v.SubtreeHash.String(), err)
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
	u.logger.Debugf("[validateSubtreeInternal][%s] serialize subtree meta", v.SubtreeHash.String())

	completeSubtreeMetaBytes, err := subtreeMeta.Serialize()
	if err != nil {
		return errors.NewProcessingError("[validateSubtreeInternal][%s] failed to serialize subtree meta", v.SubtreeHash.String(), err)
	}

	start = gocore.CurrentTime()

	u.logger.Debugf("[validateSubtreeInternal][%s] store subtree meta", v.SubtreeHash.String())

	err = u.subtreeStore.Set(ctx, merkleRoot[:], completeSubtreeMetaBytes, options.WithTTL(u.settings.BlockAssembly.SubtreeTTL), options.WithFileExtension("meta"))

	stat.NewStat("7. storeSubtreeMeta").AddTime(start)

	if err != nil {
		if errors.Is(err, errors.ErrBlobAlreadyExists) {
			u.logger.Infof("[validateSubtreeInternal][%s] subtree meta already exists in store", v.SubtreeHash.String())
			return nil
		}

		return errors.NewStorageError("[validateSubtreeInternal][%s] failed to store subtree meta", v.SubtreeHash.String(), err)
	}

	//
	// store subtree in store
	//
	u.logger.Debugf("[validateSubtreeInternal][%s] serialize subtree", v.SubtreeHash.String())

	completeSubtreeBytes, err := subtree.Serialize()
	if err != nil {
		return errors.NewProcessingError("[validateSubtreeInternal][%s] failed to serialize subtree", v.SubtreeHash.String(), err)
	}

	start = gocore.CurrentTime()

	u.logger.Debugf("[validateSubtreeInternal][%s] store subtree", v.SubtreeHash.String())

	err = u.subtreeStore.Set(ctx, merkleRoot[:], completeSubtreeBytes, options.WithTTL(u.settings.BlockAssembly.SubtreeTTL), options.WithFileExtension("subtree"))

	stat.NewStat("8. storeSubtree").AddTime(start)

	if err != nil {
		if errors.Is(err, errors.ErrBlobAlreadyExists) {
			u.logger.Infof("[validateSubtreeInternal][%s] subtree already exists in store", v.SubtreeHash.String())
			return nil
		}

		return errors.NewStorageError("[validateSubtreeInternal][%s] failed to store subtree", v.SubtreeHash.String(), err)
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
	u.logger.Infof("[getSubtreeTxHashes][%s] getting subtree from %s", subtreeHash.String(), baseURL)
	url := fmt.Sprintf("%s/subtree/%s", baseURL, subtreeHash.String())
	// TODO add the metric for how long this takes
	body, err := util.DoHTTPRequestBodyReader(spanCtx, url)
	if err != nil {
		return nil, errors.NewExternalError("[getSubtreeTxHashes][%s] failed to do http request", subtreeHash.String(), err)
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
// in a subtree. It supports both file-based and network-based transaction retrieval.
func (u *Server) processMissingTransactions(ctx context.Context, subtreeHash *chainhash.Hash, missingTxHashes []utxo.UnresolvedMetaData, baseURL string, txMetaSlice []*meta.Data, blockHeight uint32) (err error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "SubtreeValidation:processMissingTransactions")
	defer func() {
		deferFn(err)
	}()

	var (
		missingTxs        []missingTx
		subtreeDataExists bool
	)

	// first check whether we have the subtreeData file for this subtree and use that for the missing transactions
	subtreeDataExists, err = u.subtreeStore.Exists(ctx,
		subtreeHash[:],
		options.WithFileExtension("subtreeData"),
	)
	if err != nil {
		return errors.NewProcessingError("[validateSubtree][%s] failed to check if subtreeData exists", subtreeHash.String(), err)
	}

	if subtreeDataExists {
		u.logger.Infof("[validateSubtree][%s] fetching %d missing txs from subtreeData file", subtreeHash.String(), len(missingTxHashes))

		missingTxs, err = u.getMissingTransactionsFromFile(ctx, subtreeHash, missingTxHashes)
		if err != nil {
			return errors.NewProcessingError("[validateSubtree][%s] failed to get missing transactions from subtreeData", subtreeHash.String(), err)
		}
	} else {
		u.logger.Infof("[validateSubtree][%s] fetching %d missing txs", subtreeHash.String(), len(missingTxHashes))

		missingTxs, err = u.getMissingTransactions(ctx, subtreeHash, missingTxHashes, baseURL)
		if err != nil {
			return errors.NewProcessingError("[validateSubtree][%s] failed to get missing transactions", subtreeHash.String(), err)
		}
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

	for level := uint32(0); level <= maxLevel; level++ {
		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(u.settings.SubtreeValidation.SpendBatcherSize * 2)

		for _, mTx = range txsPerLevel[level] {
			mTx := mTx
			if mTx.tx == nil {
				return errors.NewProcessingError("[validateSubtree][%s] missing transaction is nil", subtreeHash.String())
			}

			g.Go(func() error {
				txMeta, err := u.blessMissingTransaction(gCtx, subtreeHash, mTx.tx, blockHeight)
				if err != nil {
					return errors.NewProcessingError("[validateSubtree][%s] failed to bless missing transaction: %s", subtreeHash.String(), mTx.tx.TxIDChainHash().String(), err)
				}

				if txMeta == nil {
					missingCount.Add(1)
					missedMu.Lock()
					missed = append(missed, mTx.tx.TxIDChainHash())
					missedMu.Unlock()
					u.logger.Infof("[validateSubtree][%s] tx meta is nil [%s]", subtreeHash.String(), mTx.tx.TxIDChainHash().String())
				} else {
					u.logger.Debugf("[validateSubtree][%s] adding missing tx to txMetaSlice: %s", subtreeHash.String(), mTx.tx.TxIDChainHash().String())

					if txMetaSlice[mTx.idx] != nil {
						return errors.NewProcessingError("[validateSubtree][%s] tx meta already exists in txMetaSlice at index %d: %s", subtreeHash.String(), mTx.idx, mTx.tx.TxIDChainHash().String())
					}
					txMetaSlice[mTx.idx] = txMeta
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

// txMapWrapper contains transaction metadata used during validation.
type txMapWrapper struct {
	missingTx          missingTx
	someParentsInBlock bool
	childLevelInBlock  uint32
}

// prepareTxsPerLevel organizes transactions by their dependency level for ordered processing.
// It returns the maximum level and a map of transactions per level.
// Levels are determined by the number of parents in the block
// this code is very similar to the one in the legacy netsync/handle_block handler, but works on different base tx data

func (u *Server) prepareTxsPerLevel(ctx context.Context, transactions []missingTx) (uint32, map[uint32][]missingTx) {
	_, _, deferFn := tracing.StartTracing(ctx, "prepareTxsPerLevel")
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

// getMissingTransactions retrieves missing transactions from either the network or local store.
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
func (u *Server) getMissingTransactionsFromFile(ctx context.Context, subtreeHash *chainhash.Hash, missingTxHashes []utxo.UnresolvedMetaData) (missingTxs []missingTx, err error) {
	// load the subtree
	subtreeReader, err := u.subtreeStore.GetIoReader(ctx,
		subtreeHash[:],
		options.WithFileExtension("subtree"),
	)
	if err != nil {
		return nil, errors.NewStorageError("[getMissingTransactionsFromFile] failed to get subtree from store", err)
	}
	defer subtreeReader.Close()

	subtree := &util.Subtree{}
	if err = subtree.DeserializeFromReader(subtreeReader); err != nil {
		return nil, err
	}

	// get the subtreeData
	subtreeDataReader, err := u.subtreeStore.GetIoReader(ctx,
		subtreeHash[:],
		options.WithFileExtension("subtreeData"),
	)
	if err != nil {
		return nil, errors.NewStorageError("[getMissingTransactionsFromFile] failed to get subtreeData from store", err)
	}
	defer subtreeDataReader.Close()

	subtreeData, err := util.NewSubtreeDataFromReader(subtree, subtreeDataReader)
	if err != nil {
		return nil, err
	}

	subtreeLookupMap := subtree.GetMap()

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

func (u *Server) getMissingTransactions(ctx context.Context, subtreeHash *chainhash.Hash, missingTxHashes []utxo.UnresolvedMetaData,
	baseUrl string) (missingTxs []missingTx, err error) {

	// transactions have to be returned in the same order as they were requested
	missingTxsMap := make(map[chainhash.Hash]*bt.Tx, len(missingTxHashes))
	missingTxsMu := sync.Mutex{}

	getMissingTransactionsConcurrency := u.settings.SubtreeValidation.GetMissingTransactions

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(getMissingTransactionsConcurrency) // keep 32 cores free for other tasks

	// get the transactions in batches of 500
	batchSize := u.settings.SubtreeValidation.MissingTransactionsBatchSize

	for i := 0; i < len(missingTxHashes); i += batchSize {
		missingTxHashesBatch := missingTxHashes[i:util.Min(i+batchSize, len(missingTxHashes))]

		g.Go(func() error {
			missingTxsBatch, err := u.getMissingTransactionsBatch(gCtx, subtreeHash, missingTxHashesBatch, baseUrl)
			if err != nil {
				return errors.NewProcessingError("[getMissingTransactions][%s] failed to get missing transactions batch", subtreeHash.String(), err)
			}

			missingTxsMu.Lock()
			for _, tx := range missingTxsBatch {
				if tx == nil {
					missingTxsMu.Unlock()
					return errors.NewProcessingError("[getMissingTransactions][%s] #1 missing transaction is nil", subtreeHash.String())
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
