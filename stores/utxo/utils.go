package utxo

import (
	"context"
	"fmt"
	"time"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

func CalculateUtxoStatus(spendingTxId *chainhash.Hash, lockTime uint32, blockHeight uint32) utxostore_api.Status {
	status := utxostore_api.Status_OK
	if spendingTxId != nil {
		status = utxostore_api.Status_SPENT
	} else if lockTime > 0 {
		if lockTime < 500000000 && lockTime > blockHeight {
			status = utxostore_api.Status_LOCKED
		} else if lockTime >= 500000000 && lockTime > uint32(time.Now().Unix()) {
			// TODO this should be a check for the median time past for the last 11 blocks
			status = utxostore_api.Status_LOCKED
		}
	}

	return status
}

func GetFeesAndUtxoHashes(ctx context.Context, tx *bt.Tx) (uint64, []*chainhash.Hash, error) {
	var fees uint64
	utxoHashes := make([]*chainhash.Hash, 0, len(tx.Outputs))

	for _, input := range tx.Inputs {
		fees += input.PreviousTxSatoshis
	}

	for i, output := range tx.Outputs {
		select {
		case <-ctx.Done():
			return fees, utxoHashes, fmt.Errorf("[GetFeesAndUtxoHashes] timeout - managed to prepare %d of %d", i, len(tx.Outputs))
		default:
			if output.Satoshis > 0 {
				fees -= output.Satoshis

				utxoHash, utxoErr := util.UTXOHashFromOutput(tx.TxIDChainHash(), output, uint32(i))
				if utxoErr != nil {
					return 0, nil, fmt.Errorf("error getting output utxo hash: %s", utxoErr.Error())
				}

				utxoHashes = append(utxoHashes, utxoHash)
			}
		}
	}

	return fees, utxoHashes, nil
}
