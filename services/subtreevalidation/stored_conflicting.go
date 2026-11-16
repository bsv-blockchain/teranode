package subtreevalidation

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/txrules"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
	"golang.org/x/sync/errgroup"
)

// storedCheckFields are the fields of a tx's record that tell what a block does with the
// verdict the UTXO store holds for it (storedCheckFor).
var storedCheckFields = []fields.FieldName{fields.Conflicting, fields.BlockIDs, fields.BlockHeights, fields.UnminedSince, fields.LockTime}

// storedCheck is what a block does with a stored tx's verdict.
type storedCheck uint8

const (
	// storedTake takes the verdict: it holds at the block's height.
	storedTake storedCheck = iota
	// storedAtHeight runs the checks that depend on the block's height and branch
	// (validator.CheckStoredTxAtHeight): coinbase maturity and finality.
	storedAtHeight
	// storedFull checks the tx again in full, scripts included
	// (validator.RecheckStoredTxAtHeight).
	storedFull
	// storedMaturity checks only that no coinbase the tx spends is immature at the block's
	// height (checkCoinbaseMaturity): the verdict holds under the same rules, but only at a
	// height above the block, a competing block at the tip's height for one.
	storedMaturity
	// storedMinedOnChain rejects the tx: it is already mined on the candidate's chain, as
	// blessMissingTransaction rejects a tx it validates. Decided before any check, so a
	// pruned parent cannot turn the verdict into a retry.
	storedMinedOnChain
)

// onChainFunc reports whether a record whose block ids are blockIDs is mined in one of the
// candidate chain's blocks.
type onChainFunc func(blockIDs []uint32) (bool, error)

// inBlockIds is the onChainFunc of the candidate chain's blocks blockIds (the window
// GetBlockHeaderIDs returns for its parent).
func inBlockIds(blockIds map[uint32]bool) onChainFunc {
	return func(blockIDs []uint32) (bool, error) {
		for _, id := range blockIDs {
			if blockIds[id] {
				return true, nil
			}
		}

		return false, nil
	}
}

// errMinedOnChain is the verdict on a block including a stored tx already mined on its chain.
func errMinedOnChain(txHash chainhash.Hash) error {
	return errors.NewTxInvalidError("[storedChecks][%s] transaction is already mined on our chain", txHash.String())
}

// storedCheckFor returns what a block at blockHeight does with the verdict the UTXO store
// holds for a tx whose record is data. The record proves the heights the tx was checked at:
// the heights of the blocks it is in, each of which checked it at its own height, and the
// height it was validated at while unmined, unless a reorg or an invalidation backdated it
// (txrules.UnminedValidatedHeight). Where none of them was under the block's rules
// (txrules.Era.Holds), the tx is checked again in full. Otherwise the height checks run for
// a conflicting record, which another candidate block at another height may have created, a
// tx locked to a time, whose finality depends on the block's own branch, and a tx with a lock
// time checked only above the block. A tx without a lock time checked only above the block, a
// competing block at the tip's height for one, has its scripts verify the same way and is
// final at any height, so only the maturity of the coinbases it spends is checked
// (storedMaturity). tip returns the UTXO
// store's block height; it is read only when the block heights prove nothing, and a tip of 0
// proves nothing either.
func (u *Server) storedCheckFor(data *meta.Data, era txrules.Era, tip func() uint32) storedCheck {
	rules, height := era.Holds(data.BlockHeights)
	if !height {
		// a tip of 0 cannot tell a backdated unmined_since from a real one
		var validated uint32
		if t := tip(); t != 0 {
			validated = txrules.UnminedValidatedHeight(data.UnminedSince, t, u.settings.UtxoStore.UnminedTxRetention)
		}

		unminedRules, unminedHeight := era.Holds([]uint32{validated})
		rules, height = rules || unminedRules, unminedHeight
	}

	switch {
	case !rules:
		return storedFull
	case data.Conflicting, txrules.LockTimeIsTime(data.LockTime), !height && data.LockTime != 0:
		return storedAtHeight
	case !height:
		return storedMaturity
	default:
		return storedTake
	}
}

// checkStoredTxsAtHeight checks, for a block at blockHeight, the txs the lookups found in the
// UTXO store or the txmeta cache, which the block would otherwise take on their stored verdict.
// The store keeps one verdict per tx, without the rules or the height it was reached under, so
// a tx gets what storedCheckFor says: a tx validated below an activation height, or moved back
// by a reorg, is checked again in full and gets the verdict a node that never saw it gives the
// block.
//
// Both checks read the tx and its parents and write nothing; validating the tx again would
// spend inputs a record already holds, or, for a conflicting record, does not hold. opts are
// the options the caller validates a tx it has not seen with, so a tx checked again gets the
// verdict it would get as a new one. txs, when not nil, holds the block's txs by index;
// otherwise a tx is read from the store.
//
// A tx the lookups found, in the txmeta cache, whose record is no longer in the store has no
// verdict to check: its txMetaSlice entry is cleared, so the caller validates it as a new tx,
// as a node without the cache entry does. It returns how many entries it cleared.
func (u *Server) checkStoredTxsAtHeight(ctx context.Context, txHashes []chainhash.Hash, txMetaSlice []metaSliceItem,
	txs []*bt.Tx, blockHeight uint32, blockIds map[uint32]bool, opts *validator.Options) (int, error) {
	stored := make([]int, 0, len(txMetaSlice))

	for i := range txMetaSlice {
		if txMetaSlice[i].isSet && !txHashes[i].Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			stored = append(stored, i)
		}
	}

	if len(stored) == 0 {
		return 0, nil
	}

	checks, gone, err := u.storedChecks(ctx, txHashes, stored, blockHeight, inBlockIds(blockIds))
	if err != nil {
		return 0, err
	}

	for k, i := range stored {
		if !gone[k] && checks[k] == storedMinedOnChain {
			return 0, errMinedOnChain(txHashes[i])
		}
	}

	cleared := 0

	var maturity []maturityCandidate

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(u.logger, g, u.settings.SubtreeValidation.SpendBatcherSize*2)

	for k, i := range stored {
		if gone[k] {
			txMetaSlice[i] = metaSliceItem{}
			cleared++

			continue
		}

		if checks[k] == storedMaturity {
			candidate := maturityCandidate{hash: txHashes[i]}
			if txs != nil {
				candidate.tx = txs[i]
			}

			maturity = append(maturity, candidate)

			continue
		}

		// the record's current conflicting flag, read with the rest, decides, as on the
		// cached path
		check := checks[k]
		if check == storedTake {
			continue
		}

		var tx *bt.Tx
		if txs != nil {
			tx = txs[i]
		}

		txHash := txHashes[i]

		g.Go(func() error {
			return u.checkStoredTxAtHeight(gCtx, txHash, tx, blockHeight, check == storedFull, opts)
		})
	}

	if len(maturity) > 0 {
		g.Go(func() error {
			return u.checkCoinbaseMaturity(gCtx, maturity, blockHeight)
		})
	}

	if err = g.Wait(); err != nil {
		return 0, err
	}

	return cleared, nil
}

// maturityCandidate is a stored tx whose coinbase parents checkCoinbaseMaturity checks; tx is
// nil when the caller has no body for it.
type maturityCandidate struct {
	hash chainhash.Hash
	tx   *bt.Tx
}

// checkCoinbaseMaturity returns TX_INVALID when a candidate spends a coinbase not yet mature at
// blockHeight (validator.ImmatureCoinbase). A verdict reached under the same rules above the
// block, for a tx without a lock time, is final at any height, but a coinbase it spends may
// mature between the two heights; BIP68 relative locks below Genesis, and whether a parent is on
// the candidate's branch at all, are not checked here either. It reads the parents of every
// candidate in batches, and, for a candidate without a body, its parent hashes first, from the
// body when the record lists none. Every candidate whose records it could read is checked
// before a missing record is reported, so one pruned record does not hide another candidate's
// immature spend; a missing record is no verdict, and the error carries ErrTxNotFound.
func (u *Server) checkCoinbaseMaturity(ctx context.Context, candidates []maturityCandidate, blockHeight uint32) error {
	parentsOf := make([][]chainhash.Hash, len(candidates))
	unreadable := make([]bool, len(candidates))

	var withoutBody []*utxo.UnresolvedMetaData

	for i, c := range candidates {
		if c.tx == nil {
			withoutBody = append(withoutBody, &utxo.UnresolvedMetaData{Hash: c.hash, Idx: i})

			continue
		}

		parentsOf[i] = inputParentHashes(c.tx)
	}

	if err := u.readRecords(ctx, withoutBody, fields.TxInpoints); err != nil {
		return err
	}

	// a record may list no inpoints (a store path that writes none): read its body instead
	var noInpoints []*utxo.UnresolvedMetaData

	for _, item := range withoutBody {
		switch {
		case item.Err != nil:
			unreadable[item.Idx] = true
		case len(item.Data.TxInpoints.ParentTxHashes) == 0:
			noInpoints = append(noInpoints, &utxo.UnresolvedMetaData{Hash: item.Hash, Idx: item.Idx})
		default:
			parentsOf[item.Idx] = item.Data.TxInpoints.ParentTxHashes
		}
	}

	if err := u.readRecords(ctx, noInpoints, fields.Tx); err != nil {
		return err
	}

	for _, item := range noInpoints {
		if item.Err != nil || item.Data.Tx == nil || len(item.Data.Tx.Inputs) == 0 {
			// a tx with no input is a coinbase, never a stored block tx: nothing to check it by
			unreadable[item.Idx] = true

			continue
		}

		parentsOf[item.Idx] = inputParentHashes(item.Data.Tx)
	}

	unique := make(map[chainhash.Hash]*utxo.UnresolvedMetaData)

	var parents []*utxo.UnresolvedMetaData

	for _, hashes := range parentsOf {
		for _, hash := range hashes {
			if _, ok := unique[hash]; !ok {
				item := &utxo.UnresolvedMetaData{Hash: hash}
				unique[hash] = item
				parents = append(parents, item)
			}
		}
	}

	if err := u.readRecords(ctx, parents, fields.IsCoinbase, fields.BlockHeights); err != nil {
		return err
	}

	missing := 0

	for i, c := range candidates {
		if unreadable[i] {
			missing++

			continue
		}

		for _, hash := range parentsOf[i] {
			parent := unique[hash]
			if parent.Err != nil {
				unreadable[i] = true

				continue
			}

			if !parent.Data.IsCoinbase {
				continue
			}

			if len(parent.Data.BlockHeights) == 0 {
				return errors.NewProcessingError("[checkCoinbaseMaturity][%s] coinbase parent %s has no block height", c.hash.String(), hash.String())
			}

			inputParent := validator.InputParent{IsCoinbase: true, Height: slices.Min(parent.Data.BlockHeights)}
			if err := validator.ImmatureCoinbase(inputParent, blockHeight, u.settings); err != nil {
				return errors.NewTxInvalidError("[checkCoinbaseMaturity][%s] spends coinbase %s, not yet mature at height %d", c.hash.String(), hash.String(), blockHeight, err)
			}
		}

		if unreadable[i] {
			missing++
		}
	}

	if missing > 0 {
		return errors.NewProcessingError("[checkCoinbaseMaturity] %d of %d tx(s) or their parents are no longer in the store", missing, len(candidates),
			errors.NewTxNotFoundError("transaction not found"))
	}

	return nil
}

// inputParentHashes returns the parent tx hash of each input of tx.
func inputParentHashes(tx *bt.Tx) []chainhash.Hash {
	hashes := make([]chainhash.Hash, len(tx.Inputs))
	for i, input := range tx.Inputs {
		hashes[i] = *input.PreviousTxIDChainHash()
	}

	return hashes
}

// readRecords decorates items with readFields from the origin store, in batches, retrying a
// storage fault. A record missing from the store, or returned empty, is left on its item with
// ErrTxNotFound: the caller decides what it means.
func (u *Server) readRecords(ctx context.Context, items []*utxo.UnresolvedMetaData, readFields ...fields.FieldName) error {
	if len(items) == 0 {
		return nil
	}

	batchSize := u.settings.BlockValidation.ProcessTxMetaUsingStoreBatchSize
	if batchSize <= 0 {
		batchSize = len(items)
	}

	store, _ := u.decorateReadPath(true)

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(u.logger, g, u.settings.BlockValidation.ProcessTxMetaUsingStoreConcurrency)

	for start := 0; start < len(items); start += batchSize {
		batch := items[start:min(start+batchSize, len(items))]

		g.Go(func() error {
			if err := u.withStoreRetry(gCtx, fmt.Sprintf("record read of %d txs", len(batch)), func() error {
				for _, item := range batch {
					item.Data, item.Err = nil, nil
				}

				if err := store.BatchDecorate(gCtx, batch, readFields...); err != nil {
					return err
				}

				for _, item := range batch {
					if item.Err != nil && !errors.Is(item.Err, errors.ErrTxNotFound) {
						return errors.NewStorageError("[readRecords] failed to read record %s", item.Hash.String(), item.Err)
					}
				}

				return nil
			}); err != nil {
				return err
			}

			for _, item := range batch {
				if item.Err == nil && item.Data == nil {
					item.Err = errors.NewTxNotFoundError("transaction %s not found", item.Hash.String())
				}
			}

			return nil
		})
	}

	return g.Wait()
}

// storedChecks reads the records of the txs txHashes[idx] for idx in stored, and returns, in
// the order of stored, what the block does with each one's verdict: storedMinedOnChain when
// onChain says the record is mined on the candidate's chain, otherwise
// storedCheckFor; and whether its record is no longer in the store.
func (u *Server) storedChecks(ctx context.Context, txHashes []chainhash.Hash, stored []int, blockHeight uint32, onChain onChainFunc) ([]storedCheck, []bool, error) {
	batchSize := u.settings.BlockValidation.ProcessTxMetaUsingStoreBatchSize
	if batchSize <= 0 {
		batchSize = len(stored)
	}

	checks := make([]storedCheck, len(stored))
	gone := make([]bool, len(stored))
	tip := sync.OnceValue(u.utxoStore.GetBlockHeight)
	era := txrules.EraOf(u.settings.ChainCfgParams, blockHeight)

	// Straight to the origin store: a narrow read must not populate the txmeta cache (see
	// decorateReadPath).
	store, _ := u.decorateReadPath(true)

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(u.logger, g, u.settings.BlockValidation.ProcessTxMetaUsingStoreConcurrency)

	for start := 0; start < len(stored); start += batchSize {
		end := min(start+batchSize, len(stored))

		g.Go(func() error {
			items := make([]*utxo.UnresolvedMetaData, end-start)
			for k := range items {
				items[k] = &utxo.UnresolvedMetaData{Hash: txHashes[stored[start+k]], Idx: stored[start+k]}
			}

			if err := u.readStoredChecks(gCtx, store, items); err != nil {
				return err
			}

			for k, item := range items {
				if item.Err != nil || item.Data == nil {
					// absent from the store; no record and no error, which no backend
					// returns, is taken the same way
					gone[start+k] = true

					continue
				}

				mined, err := onChain(item.Data.BlockIDs)
				if err != nil {
					return err
				}

				if mined {
					checks[start+k] = storedMinedOnChain
				} else {
					checks[start+k] = u.storedCheckFor(item.Data, era, tip)
				}
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	return checks, gone, nil
}

// readStoredChecks decorates items with storedCheckFields, retrying a storage fault. A record
// missing from the store is left on its item as ErrTxNotFound.
func (u *Server) readStoredChecks(ctx context.Context, store utxo.Store, items []*utxo.UnresolvedMetaData) error {
	// withStoreRetry reports a cancelled context as cancellation, not as a storage fault
	// blockvalidation would redeliver the block for.
	return u.withStoreRetry(ctx, fmt.Sprintf("stored verdict read of %d txs", len(items)), func() error {
		for _, item := range items {
			item.Data, item.Err = nil, nil
		}

		if err := store.BatchDecorate(ctx, items, storedCheckFields...); err != nil {
			return err
		}

		// Aerospike reports a per-record timeout on the item, not from the call; any
		// per-record failure other than not-found fails the attempt as a storage error, so
		// the bounded retry covers it.
		for _, item := range items {
			if item.Err != nil && !errors.Is(item.Err, errors.ErrTxNotFound) {
				return errors.NewStorageError("[readStoredChecks] failed to read record %s", item.Hash.String(), item.Err)
			}
		}

		return nil
	})
}

// checkStoredTxAtHeight checks one stored tx at blockHeight, reading it from the store when tx
// is nil: in full (validator.RecheckStoredTxAtHeight) when full is set, otherwise only the
// checks that depend on the height (validator.CheckStoredTxAtHeight).
func (u *Server) checkStoredTxAtHeight(ctx context.Context, txHash chainhash.Hash, tx *bt.Tx, blockHeight uint32, full bool, opts *validator.Options) error {
	if tx == nil {
		stored, err := u.utxoStore.Get(ctx, &txHash, fields.Tx)
		if err != nil {
			return errors.NewProcessingError("[checkStoredTxAtHeight][%s] failed to read the stored tx", txHash.String(), err)
		}

		if stored.Tx == nil {
			return errors.NewProcessingError("[checkStoredTxAtHeight][%s] the stored tx has no body", txHash.String())
		}

		tx = stored.Tx
	}

	if full {
		prometheusSubtreeValidationStoredVerdictRechecks.Inc()

		return validator.RecheckStoredTxAtHeight(ctx, u.utxoStore, u.storedTxValidator(), tx, blockHeight, u.settings, opts)
	}

	parents, err := validator.InputParents(ctx, u.utxoStore, tx)
	if err != nil {
		return err
	}

	return validator.CheckStoredTxAtHeight(tx, parents, blockHeight, u.settings, opts)
}

// storedTxValidator returns the transaction validator checkStoredTxAtHeight checks a stored tx
// with, created on first use. It runs in this service whether or not the validator does: a
// check of a stored tx writes nothing, so it needs no validator service.
func (u *Server) storedTxValidator() *validator.TxValidator {
	u.storedTxValidatorOnce.Do(func() {
		u.storedTxValidatorInstance = validator.NewTxValidator(u.logger, u.settings)
	})

	return u.storedTxValidatorInstance
}
