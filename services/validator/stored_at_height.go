package validator

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
)

// InputParent is what CheckStoredTxAtHeight needs to know of an input's parent.
type InputParent struct {
	IsCoinbase bool
	// Height is the lowest height the parent was mined at.
	Height uint32
}

// CheckStoredTxAtHeight runs, on a tx already in the UTXO store, the checks whose verdict depends
// on the height of the block it is in: no input spends a coinbase output not yet mature, and the
// tx is final. Another candidate block, at another height, may have created the record. It reads
// and writes nothing, so it never takes a spend: parents gives each input's parent, in input order.
// Both are TX_INVALID: opts, with SkipPolicyChecks, carries the block's own candidate times, so either is a verdict on
// the block, as blessMissingTransaction gives it for a tx it validates (errors.IsBlockSpendRuleViolation).
func CheckStoredTxAtHeight(tx *bt.Tx, parents []InputParent, blockHeight uint32, tSettings *settings.Settings, opts *Options) error {
	txID := tx.TxIDChainHash().String()

	if len(parents) != len(tx.Inputs) {
		return errors.NewProcessingError("[CheckStoredTxAtHeight][%s] %d parents for %d inputs", txID, len(parents), len(tx.Inputs))
	}

	for i, parent := range parents {
		if err := immatureCoinbase(parent, blockHeight, tSettings); err != nil {
			return errors.NewTxInvalidError("[CheckStoredTxAtHeight][%s] input %d spends a coinbase output not yet mature at height %d", txID, i, blockHeight, err)
		}
	}

	// a block path checks finality against its candidate times, never the tip's
	comparisonTime, skipFinality, err := selectFinalityComparisonTime(opts, blockHeight, uint32(tSettings.ChainCfgParams.CSVHeight), utxo.BlockState{})
	if err != nil {
		return err
	}

	if !skipFinality {
		if err = util.IsTransactionFinal(tx, blockHeight, comparisonTime); err != nil {
			return errors.NewTxInvalidError("[CheckStoredTxAtHeight][%s] transaction is not final at height %d", txID, blockHeight,
				errors.NewUtxoNonFinalError("transaction is not final", err))
		}
	}

	return nil
}

// immatureCoinbase returns TX_COINBASE_IMMATURE when parent is a coinbase whose outputs are not yet
// spendable at blockHeight. The stores make a coinbase output spendable from its height plus the
// maturity.
func immatureCoinbase(parent InputParent, blockHeight uint32, tSettings *settings.Settings) error {
	maturity := uint32(tSettings.ChainCfgParams.CoinbaseMaturity)

	if parent.IsCoinbase && blockHeight < parent.Height+maturity {
		return errors.NewTxCoinbaseImmatureError("spends a coinbase output mined at %d, spendable from %d", parent.Height, parent.Height+maturity)
	}

	return nil
}

// InputParents reads, for each input of tx, whether its parent is a coinbase and the lowest height
// it was mined at.
func InputParents(ctx context.Context, utxoStore utxo.Store, tx *bt.Tx) ([]InputParent, error) {
	read := make(map[chainhash.Hash]InputParent, len(tx.Inputs))
	parents := make([]InputParent, len(tx.Inputs))

	for i, input := range tx.Inputs {
		hash := *input.PreviousTxIDChainHash()

		parent, ok := read[hash]
		if !ok {
			var err error
			if parent, err = readInputParent(ctx, utxoStore, &hash); err != nil {
				return nil, errors.NewProcessingError("[InputParents][%s] failed to read parent %s", tx.TxIDChainHash().String(), hash.String(), err)
			}

			read[hash] = parent
		}

		parents[i] = parent
	}

	return parents, nil
}

// readInputParent reads whether the tx hash is a coinbase and the lowest height it was mined at.
func readInputParent(ctx context.Context, utxoStore utxo.Store, hash *chainhash.Hash) (InputParent, error) {
	var parent InputParent

	data, err := utxoStore.Get(ctx, hash, fields.IsCoinbase, fields.BlockHeights)
	if err != nil {
		return parent, err
	}

	parent.IsCoinbase = data.IsCoinbase

	if len(data.BlockHeights) > 0 {
		parent.Height = data.BlockHeights[0]
		for _, h := range data.BlockHeights[1:] {
			parent.Height = min(parent.Height, h)
		}
	} else if parent.IsCoinbase {
		return parent, errors.NewProcessingError("coinbase %s has no block height", hash.String())
	}

	return parent, nil
}
