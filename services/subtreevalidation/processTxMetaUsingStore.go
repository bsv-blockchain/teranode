// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/retry"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

const errProcessTxMetaContextDone = "[processTxMetaUsingStore] context done"

var TxMetaFieldsForDecorate = []fields.FieldName{fields.Fee, fields.SizeInBytes, fields.TxInpoints, fields.Conflicting, fields.BlockIDs, fields.Creating}

// txMetaFieldsForBlockValidation is TxMetaFieldsForDecorate without TxInpoints.
//
// Block validation never reads them: on that path txMetaSlice is consulted only
// for isSet, and the one reader of .txInpoints in this service is the
// peer-announced path, which builds the subtree meta file. TxInpoints is the
// largest field in the record — every parent hash, 32 bytes per input — so
// asking for it costs a bigger response to ship and parse and, on the store
// side, the ~7.5% of subtree-validator CPU that processInputsToTxInpoints
// takes to rebuild something nothing looks at.
//
// This set is only ever used on a read that also skips the cache. That pairing
// is the safety property, not a coincidence: a meta without TxInpoints must
// never reach the txmeta cache, because a later subtree-meta serialize rejects
// the inpoints-less node and the block wedges (see TxMetaCache.BatchDecorate).
// decorateReadPath is the only place the two are chosen, so a caller cannot take
// the cheap read and still poison the cache.
//
// One deliberate consequence: asking for TxInpoints is what routes an
// externally-stored tx through its blob, so dropping the field means block
// validation no longer notices a missing or corrupt external blob here. That
// detection was only ever incidental — it forced a revalidation that would hit
// the same broken blob — and the blob is still read wherever it is actually
// needed.
var txMetaFieldsForBlockValidation = []fields.FieldName{fields.Fee, fields.SizeInBytes, fields.Conflicting, fields.BlockIDs, fields.Creating}

// unresolvedMetaDataSlicePool reduces allocation pressure by reusing
// []*utxo.UnresolvedMetaData slices during batch tx metadata processing.
var unresolvedMetaDataSlicePool = sync.Pool{}

// getUnresolvedMetaDataSlice returns a slice from the pool or allocates a new one.
func getUnresolvedMetaDataSlice(capacity int) *[]*utxo.UnresolvedMetaData {
	if v := unresolvedMetaDataSlicePool.Get(); v != nil {
		s := v.(*[]*utxo.UnresolvedMetaData)
		if cap(*s) >= capacity {
			*s = (*s)[:0]
			return s
		}
	}
	s := make([]*utxo.UnresolvedMetaData, 0, capacity)
	return &s
}

// putUnresolvedMetaDataSlice returns a slice to the pool after clearing references.
func putUnresolvedMetaDataSlice(s *[]*utxo.UnresolvedMetaData) {
	if s == nil {
		return
	}
	// Clear references to allow GC of pointed-to objects
	for i := range *s {
		(*s)[i] = nil
	}
	*s = (*s)[:0]
	unresolvedMetaDataSlicePool.Put(s)
}

// batchDecorateRetryBackoff is the base unit for the linear backoff between
// BatchDecorate attempts. It is deliberately short: the failing call is itself
// bounded by the store client's own timeout, so that timeout — not this value —
// dominates the spacing between attempts.
const batchDecorateRetryBackoff = 100 * time.Millisecond

// storeRetryCount is the configured retry count, clamped to a non-negative
// value. retry.Retry reads -1 as "retry forever", so passing the setting through
// unclamped would turn an operator typo in
// blockvalidation_processTxMetaUsingStore_Retries into every batch of a block
// hammering a dead store until the context is cancelled.
func (u *Server) storeRetryCount() int {
	return max(0, u.settings.BlockValidation.ProcessTxMetaUsingStoreRetries)
}

// withStoreRetry runs a read against the UTXO store, retrying only transient
// failures (see errors.IsRetryableError: a storage or network fault, never a
// cancelled context or a malformed request).
//
// A transient store failure must not become a validation verdict on a block.
// These reads re-resolve their target in place, so replaying one is idempotent.
// Without this, a single client-side timeout in one of a block's many batches
// fails the whole block, and because nothing upstream distinguishes "the store
// stalled" from "the block is bad", the node can wedge behind a block it will
// never accept.
//
// Every failed attempt is logged at WARN — retry.Retry itself only logs the
// first five attempts at DEBUG, which is invisible in a production deployment
// and leaves no trace of a store that is quietly degrading.
func (u *Server) withStoreRetry(ctx context.Context, what string, read func() error) error {
	var lastErr error

	//nolint:errcheck // the attempt error is carried out via lastErr, which distinguishes "gave up" from "not retryable"
	_, _ = retry.Retry(ctx, u.logger, func() (struct{}, error) {
		lastErr = read()
		if lastErr == nil {
			return struct{}{}, nil
		}

		if !errors.IsRetryableError(lastErr) {
			u.logger.Warnf("[processTxMetaUsingStore] %s failed with a non-retryable error: %v", what, lastErr)

			// Returning nil stops retry.Retry; lastErr is what the caller sees.
			return struct{}{}, nil
		}

		u.logger.Warnf("[processTxMetaUsingStore] %s failed with a transient store error, retrying: %v", what, lastErr)

		return struct{}{}, lastErr
	},
		retry.WithMessage("[processTxMetaUsingStore] "+what),
		retry.WithRetryCount(u.storeRetryCount()),
		retry.WithBackoffDurationType(batchDecorateRetryBackoff),
	)

	// retry.Retry abandons its loop on a cancelled context, leaving lastErr
	// holding the store error from the attempt it gave up on. Returning that
	// would report a shutdown — or a sibling goroutine's failure cancelling the
	// errgroup — as a storage fault, which blockvalidation classifies as
	// recoverable and redelivers the block for.
	if lastErr != nil && ctx.Err() != nil {
		return errors.NewContextCanceledError(errProcessTxMetaContextDone, ctx.Err())
	}

	return lastErr
}

// batchDecorateWithRetry runs BatchDecorate against the UTXO store. See
// withStoreRetry for why a transient failure here must not fail the block.
func (u *Server) batchDecorateWithRetry(ctx context.Context, store utxo.Store, items []*utxo.UnresolvedMetaData, decorateFields []fields.FieldName) error {
	return u.withStoreRetry(ctx, fmt.Sprintf("batch decorate of %d txs", len(items)), func() error {
		return store.BatchDecorate(ctx, items, decorateFields...)
	})
}

// decorateReadPath resolves which store a decorate read goes through and which
// fields it asks for.
//
// The two are returned together on purpose. The reduced field set is safe only
// on a read that cannot populate the txmeta cache, so nothing outside this
// function gets to pick one without the other: a meta without TxInpoints that
// reaches the cache wedges a later subtree-meta serialize (see
// TxMetaCache.BatchDecorate). A caching store that cannot be unwrapped
// therefore keeps the full set rather than trusting the cache's own guard.
func (u *Server) decorateReadPath(skipCachePopulation bool) (utxo.Store, []fields.FieldName) {
	if !skipCachePopulation {
		return u.utxoStore, TxMetaFieldsForDecorate
	}

	if cache, ok := u.utxoStore.(*txmetacache.TxMetaCache); ok {
		// Unwrapped: the read goes straight to the origin, so it neither probes
		// nor populates the cache.
		return cache.UnderlyingStore(), txMetaFieldsForBlockValidation
	}

	if _, cached := u.utxoStore.(txMetaCacheOps); cached {
		// Something in front of the store caches tx meta but is not the cache we
		// know how to unwrap. The read still passes through it, so it has to ask
		// for everything the cache needs.
		return u.utxoStore, TxMetaFieldsForDecorate
	}

	// No tx meta cache in front of the store — every cache in this service is
	// reached through txMetaCacheOps — so the read is already cache-free.
	return u.utxoStore, txMetaFieldsForBlockValidation
}

// processTxMetaUsingStore attempts to retrieve transaction metadata from the underlying store
// for a batch of transactions. It supports both batched and individual transaction retrieval.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - txHashes: Slice of transaction hashes to process
//   - txMetaSlice: Pre-allocated slice to store retrieved metadata
//   - blockIds: Block IDs of the current chain, used to resolve conflicting txs
//   - batched: If true, uses batch operations for retrieval
//   - failFast: If true, fails quickly when missing transaction threshold is exceeded
//   - skipCachePopulation: If true, reads bypass the txmeta cache and ask for the
//     reduced block-validation field set (the two are inseparable — see
//     decorateReadPath)
//
// Returns:
//   - int: Number of transactions missing from store
//   - error: Any error encountered during processing
//
// The function uses BatchDecorate when batched is true, otherwise falls back to
// individual GetMeta calls. It will return a ThresholdExceededError if failFast
// is true and the number of missing transactions exceeds the configured threshold.
func (u *Server) processTxMetaUsingStore(ctx context.Context, txHashes []chainhash.Hash, txMetaSlice []metaSliceItem,
	blockIds map[uint32]bool, batched bool, failFast bool, skipCachePopulation bool) (int, error) {
	if len(txHashes) != len(txMetaSlice) {
		return 0, errors.NewInvalidArgumentError("txHashes and txMetaSlice must be the same length")
	}

	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "processTxMetaUsingStore")
	defer deferFn()

	batchSize := u.settings.BlockValidation.ProcessTxMetaUsingStoreBatchSize
	validateSubtreeInternalConcurrency := u.settings.BlockValidation.ProcessTxMetaUsingStoreConcurrency
	missingTxThreshold := u.settings.BlockValidation.ProcessTxMetaUsingStoreMissingTxThreshold

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(u.logger, g, validateSubtreeInternalConcurrency)

	// Resolve which store these reads go through and what they ask it for.
	// skipCachePopulation unwraps the txmeta cache and reads the origin directly,
	// so the reads neither probe nor populate it. Used by the block-validation
	// path, where a populated entry is never read back (see
	// TxMetaCache.UnderlyingStore).
	store, decorateFields := u.decorateReadPath(skipCachePopulation)

	var missed atomic.Int32

	if batched {
		for i := 0; i < len(txHashes); i += batchSize {
			i := i // capture range variable for goroutine

			g.Go(func() error {
				end := subtree.Min(i+batchSize, len(txHashes))

				missingTxHashesCompactedPtr := getUnresolvedMetaDataSlice(end - i)
				missingTxHashesCompacted := *missingTxHashesCompactedPtr
				defer func() {
					*missingTxHashesCompactedPtr = missingTxHashesCompacted
					putUnresolvedMetaDataSlice(missingTxHashesCompactedPtr)
				}()

				for j := 0; j < subtree.Min(batchSize, len(txHashes)-i); j++ {
					select {
					case <-gCtx.Done(): // Listen for cancellation signal
						// Return the error that caused the cancellation
						return errors.NewContextCanceledError(errProcessTxMetaContextDone, gCtx.Err())

					default:
						if txHashes[i+j].Equal(*subtree.CoinbasePlaceholderHash) {
							// coinbase placeholder is not in the store
							continue
						}

						if !txMetaSlice[i+j].isSet {
							missingTxHashesCompacted = append(missingTxHashesCompacted, &utxo.UnresolvedMetaData{
								Hash: txHashes[i+j],
								Idx:  i + j,
							})
						}
					}
				}

				if err := u.batchDecorateWithRetry(gCtx, store, missingTxHashesCompacted, decorateFields); err != nil {
					if gCtx.Err() != nil {
						// Cancelled, not faulty: don't hand upstream a storage
						// error it would redeliver the block for.
						return errors.NewContextCanceledError(errProcessTxMetaContextDone, gCtx.Err())
					}

					return errors.NewStorageError("error running batch decorate on utxo store for missing transactions", err)
				}

				select {
				case <-gCtx.Done(): // Listen for cancellation signal
					// Return the error that caused the cancellation
					return errors.NewContextCanceledError(errProcessTxMetaContextDone, gCtx.Err())
				default:
					missingTxThresholdInt32, err := safeconversion.IntToInt32(missingTxThreshold)
					if err != nil {
						return err
					}

					for _, data := range missingTxHashesCompacted {
						if data.Data == nil || data.Err != nil {
							newMissed := missed.Add(1)

							if failFast && missingTxThresholdInt32 > 0 && newMissed > missingTxThresholdInt32 {
								return errors.NewThresholdExceededError("threshold exceeded for missing txs: %d > %d", newMissed, missingTxThreshold)
							}

							continue
						}

						// Auto-recovery: if transaction is still being created (incomplete multi-record creation),
						// treat it as missing to trigger re-processing
						if data.Data.Creating {
							newMissed := missed.Add(1)

							if failFast && missingTxThresholdInt32 > 0 && newMissed > missingTxThresholdInt32 {
								return errors.NewThresholdExceededError("threshold exceeded for missing txs (incomplete): %d > %d", newMissed, missingTxThreshold)
							}

							continue
						}

						txMetaSlice[data.Idx] = metaSliceItem{
							fee:         data.Data.Fee,
							sizeInBytes: data.Data.SizeInBytes,
							coinbase:    data.Data.IsCoinbase,
							conflicting: data.Data.Conflicting,
							creating:    data.Data.Creating,
							isSet:       true,
							txInpoints:  data.Data.TxInpoints,
						}

						// if the tx is conflicting, we need to check if it is conflicting on the current chain
						if txMetaSlice[data.Idx].conflicting {
							if err = u.checkCounterConflictingOnCurrentChain(ctx, data.Hash, blockIds); err != nil {
								return errors.NewProcessingError("[processTxMetaUsingStore][%s] failed to check counter conflicting tx on current chain", data.Hash.String(), err)
							}
						}
					}

					return nil
				}
			})
		}

		if err := g.Wait(); err != nil {
			return int(missed.Load()), errors.NewContextCanceledError("[processTxMetaUsingStore]", err)
		}

		return int(missed.Load()), nil
	} else {
		for i := 0; i < len(txHashes); i += batchSize {
			i := i

			g.Go(func() error {
				// cycle through the batch size, making sure not to go over the length of the txHashes
				for j := 0; j < subtree.Min(batchSize, len(txHashes)-i); j++ {
					select {
					case <-gCtx.Done(): // Listen for cancellation signal
						// Return the error that caused the cancellation
						return errors.NewContextCanceledError(errProcessTxMetaContextDone, gCtx.Err())

					default:
						txHash := txHashes[i+j]

						missingTxThresholdInt32, err := safeconversion.IntToInt32(missingTxThreshold)
						if err != nil {
							return err
						}

						if txHash.Equal(*subtree.CoinbasePlaceholderHash) {
							// coinbase placeholder is not in the store
							continue
						}

						if !txMetaSlice[i+j].isSet {
							txMeta := &meta.Data{}

							// Retried for the same reason as the batched path: a
							// transient store failure here would otherwise fail
							// the whole block. This branch is reachable whenever
							// subtreevalidation_batch_missing_transactions is off.
							if err := u.withStoreRetry(gCtx, "get tx meta", func() error {
								return store.GetMeta(gCtx, &txHash, txMeta)
							}); err != nil {
								if gCtx.Err() != nil {
									return errors.NewContextCanceledError(errProcessTxMetaContextDone, gCtx.Err())
								}

								return errors.NewStorageError("error getting tx meta from utxo store", err)
							}

							// Auto-recovery: only use txMeta if it's not in the "creating" state
							// If Creating is true, treat as missing to trigger re-processing
							if !txMeta.Creating {
								txMetaSlice[i+j] = metaSliceItem{
									fee:         txMeta.Fee,
									sizeInBytes: txMeta.SizeInBytes,
									coinbase:    txMeta.IsCoinbase,
									conflicting: txMeta.Conflicting,
									creating:    txMeta.Creating,
									isSet:       true,
									txInpoints:  txMeta.TxInpoints,
								}

								continue
							}
						}

						newMissed := missed.Add(1)

						if failFast && missingTxThreshold > 0 && newMissed > missingTxThresholdInt32 {
							return errors.NewThresholdExceededError("threshold exceeded for missing txs: %d > %d", newMissed, missingTxThreshold)
						}
					}
				}

				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return int(missed.Load()), errors.NewContextCanceledError(errProcessTxMetaContextDone, err)
		}

		return int(missed.Load()), nil
	}
}
