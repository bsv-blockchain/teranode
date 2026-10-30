package subtreevalidation

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

// checkStoredConflictingAtHeight runs validator.CheckStoredTxAtHeight on every tx the lookups
// took from a conflicting record. The block path creates a tx as conflicting at its block's
// height, and another candidate block at another height may have created the record: a tx valid
// there can be invalid here, for instance spending a coinbase that matures in between. The check
// reads the tx and its parents and writes nothing; validating the tx again would spend inputs the
// record does not hold. txs, when not nil, holds the block's txs by index; otherwise a tx is read
// from the store.
func (u *Server) checkStoredConflictingAtHeight(ctx context.Context, txHashes []chainhash.Hash, txMetaSlice []metaSliceItem,
	txs []*bt.Tx, blockHeight uint32, opts *validator.Options) error {
	for i := range txMetaSlice {
		if !txMetaSlice[i].isSet || !txMetaSlice[i].conflicting {
			continue
		}

		var tx *bt.Tx
		if txs != nil {
			tx = txs[i]
		}

		if err := u.checkStoredTxAtHeight(ctx, txHashes[i], tx, blockHeight, opts); err != nil {
			return err
		}
	}

	return nil
}

// checkStoredTxAtHeight runs validator.CheckStoredTxAtHeight on one stored tx, reading it from the
// store when tx is nil.
func (u *Server) checkStoredTxAtHeight(ctx context.Context, txHash chainhash.Hash, tx *bt.Tx, blockHeight uint32, opts *validator.Options) error {
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

	parents, err := validator.InputParents(ctx, u.utxoStore, tx)
	if err != nil {
		return err
	}

	return validator.CheckStoredTxAtHeight(tx, parents, blockHeight, u.settings, opts)
}
