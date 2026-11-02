package subtreeprocessor

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
)

// keepReorgDeletedTxs saves the txs that removeCoinbaseUtxos is about to delete, so that moving the
// block forward again can recreate them. They are saved under the hash of the block whose coinbase
// they spend, parents before children, each with the previous outputs it spends.
//
// A file left by an earlier attempt is merged in, not replaced: a retry after a partial delete finds
// some of the txs already gone and must not lose them.
func (stp *SubtreeProcessor) keepReorgDeletedTxs(ctx context.Context, block *model.Block, txHashes []chainhash.Hash) error {
	if stp.subtreeStore == nil {
		return errors.NewProcessingError("[keepReorgDeletedTxs][%s] no subtree store to save %d txs in", block.String(), len(txHashes))
	}

	blockHash := block.Header.Hash()

	byHash := make(map[chainhash.Hash]*bt.Tx, len(txHashes))
	order := make([]chainhash.Hash, 0, len(txHashes))

	previous, err := stp.readReorgDeletedTxs(ctx, blockHash)
	if err != nil {
		return err
	}

	for _, tx := range previous {
		hash := *tx.TxIDChainHash()

		if _, ok := byHash[hash]; !ok {
			byHash[hash] = tx
			order = append(order, hash)
		}
	}

	for i := range txHashes {
		if _, ok := byHash[txHashes[i]]; ok {
			continue
		}

		txMeta, getErr := stp.utxoStore.Get(ctx, &txHashes[i], fields.Tx)
		if getErr != nil {
			if errors.Is(getErr, errors.ErrTxNotFound) {
				continue
			}

			return errors.NewStorageError("[keepReorgDeletedTxs][%s] error getting tx %s", block.String(), txHashes[i].String(), getErr)
		}

		if txMeta.Tx == nil {
			return errors.NewProcessingError("[keepReorgDeletedTxs][%s] store returned no tx for %s", block.String(), txHashes[i].String())
		}

		if err = stp.utxoStore.PreviousOutputsDecorate(ctx, txMeta.Tx); err != nil {
			return errors.NewStorageError("[keepReorgDeletedTxs][%s] error decorating previous outputs of %s", block.String(), txHashes[i].String(), err)
		}

		byHash[txHashes[i]] = txMeta.Tx
		order = append(order, txHashes[i])
	}

	if len(order) == 0 {
		return nil
	}

	ordered := parentsFirst(order, byHash)
	dah := stp.utxoStore.GetBlockHeight() + stp.settings.GetSubtreeValidationBlockHeightRetention()

	if err = stp.subtreeStore.Set(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs, util.SerializeTxList(ordered), options.WithDeleteAt(dah), options.WithAllowOverwrite(true)); err != nil {
		return errors.NewStorageError("[keepReorgDeletedTxs][%s] error saving %d txs", block.String(), len(ordered), err)
	}

	return nil
}

// readReorgDeletedTxs returns the txs saved for the block, or nil when none were saved.
func (stp *SubtreeProcessor) readReorgDeletedTxs(ctx context.Context, blockHash *chainhash.Hash) ([]*bt.Tx, error) {
	if stp.subtreeStore == nil {
		return nil, nil
	}

	b, err := stp.subtreeStore.Get(ctx, blockHash[:], fileformat.FileTypeReorgDeletedTxs)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) {
			return nil, nil
		}

		return nil, errors.NewStorageError("[readReorgDeletedTxs][%s] error reading saved txs", blockHash.String(), err)
	}

	txs, err := util.ReadTxList(b)
	if err != nil {
		return nil, errors.NewProcessingError("[readReorgDeletedTxs][%s] saved txs are unreadable", blockHash.String(), err)
	}

	return txs, nil
}

// restoreReorgDeletedTxs recreates the txs that removeCoinbaseUtxos deleted when the block was moved
// back. It runs after the block's coinbase is back in the store, so the txs can spend it again. The
// txs were validated before they were deleted, and the block that holds them is valid, so they are
// written without validation, as a block's txs are.
func (stp *SubtreeProcessor) restoreReorgDeletedTxs(ctx context.Context, block *model.Block) error {
	txs, err := stp.readReorgDeletedTxs(ctx, block.Header.Hash())
	if err != nil {
		return err
	}

	if len(txs) == 0 {
		return nil
	}

	blockHeight := block.Height
	if blockHeight == 0 {
		blockHeight = stp.utxoStore.GetBlockHeight()
	}

	// The txs spend this block's coinbase, so they were mined at least a coinbase maturity later. The
	// coinbase is immature at the height of this block, so spend at the first height it is mature.
	spendHeight := blockHeight + uint32(stp.settings.ChainCfgParams.CoinbaseMaturity) //nolint:gosec // maturity is a small chain constant

	results, err := stp.utxoStore.SpendAndCreateMulti(ctx, txs, spendHeight)
	if err != nil {
		return errors.NewStorageError("[restoreReorgDeletedTxs][%s] error recreating %d txs", block.String(), len(txs), err)
	}

	for i, result := range results {
		switch result.Status {
		case utxostore.MultiTxCreated, utxostore.MultiTxExisted:
		default:
			return errors.NewProcessingError("[restoreReorgDeletedTxs][%s] tx %s was not recreated, status %d", block.String(), txs[i].TxIDChainHash().String(), result.Status, result.Err)
		}
	}

	stp.logger.Warnf("[restoreReorgDeletedTxs][%s] recreated %d txs deleted when the block was moved back", block.String(), len(txs))

	return nil
}

// parentsFirst returns the txs ordered so that no tx comes before a parent that is also in the set.
func parentsFirst(hashes []chainhash.Hash, byHash map[chainhash.Hash]*bt.Tx) []*bt.Tx {
	ordered := make([]*bt.Tx, 0, len(hashes))
	done := make(map[chainhash.Hash]struct{}, len(hashes))

	var visit func(hash chainhash.Hash)

	visit = func(hash chainhash.Hash) {
		if _, ok := done[hash]; ok {
			return
		}

		done[hash] = struct{}{}

		tx := byHash[hash]

		for _, input := range tx.Inputs {
			parent := *input.PreviousTxIDChainHash()

			if _, inSet := byHash[parent]; inSet {
				visit(parent)
			}
		}

		ordered = append(ordered, tx)
	}

	for _, hash := range hashes {
		visit(hash)
	}

	return ordered
}
