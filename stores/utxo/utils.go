package utxo

import (
	"context"
	"github.com/bitcoin-sv/ubsv/errors"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

func CalculateUtxoStatus(spendingTxId *chainhash.Hash, coinbaseSpendingHeight uint32, blockHeight uint32) Status {
	status := Status_OK

	if spendingTxId != nil {
		status = Status_SPENT
	} else if coinbaseSpendingHeight > 0 && coinbaseSpendingHeight > blockHeight {
		status = Status_LOCKED
	}

	return status
}

func CalculateUtxoStatus2(spendingTxId *chainhash.Hash) Status {
	status := Status_OK

	if spendingTxId != nil {
		status = Status_SPENT
	}

	return status
}

// GetFeesAndUtxoHashes returns the fees and utxo hashes for the outputs of a transaction.
// It will return an error if the context is cancelled.
func GetFeesAndUtxoHashes(ctx context.Context, tx *bt.Tx, blockHeight uint32) (uint64, []*chainhash.Hash, error) {
	if !util.IsExtended(tx, blockHeight) && !tx.IsCoinbase() {
		return 0, nil, errors.NewProcessingError("tx is not extended")
	}

	var fees uint64
	utxoHashes := make([]*chainhash.Hash, len(tx.Outputs))

	if !tx.IsCoinbase() {
		for _, input := range tx.Inputs {
			fees += input.PreviousTxSatoshis
		}
	}

	txid := tx.TxIDChainHash()

	for i, output := range tx.Outputs {
		select {
		case <-ctx.Done():
			return fees, utxoHashes, errors.NewProcessingError("[GetFeesAndUtxoHashes] timeout - managed to prepare %d of %d", i, len(tx.Outputs))
		default:
			fees -= output.Satoshis

			utxoHash, utxoErr := util.UTXOHashFromOutput(txid, output, uint32(i))
			if utxoErr != nil {
				return 0, nil, errors.NewProcessingError("error getting output utxo hash: %s", utxoErr)
			}

			utxoHashes[i] = utxoHash
		}
	}

	return fees, utxoHashes, nil
}

// GetUtxoHashes returns the utxo hashes for the outputs of a transaction.
func GetUtxoHashes(tx *bt.Tx) ([]chainhash.Hash, error) {
	txChainHash := tx.TxIDChainHash()

	utxoHashes := make([]chainhash.Hash, len(tx.Outputs))
	for i, output := range tx.Outputs {
		utxoHash, utxoErr := util.UTXOHashFromOutput(txChainHash, output, uint32(i))
		if utxoErr != nil {
			return nil, errors.NewProcessingError("error getting output utxo hash: %s", utxoErr)
		}

		utxoHashes[i] = *utxoHash
	}

	return utxoHashes, nil
}
