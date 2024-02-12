package utxo

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"golang.org/x/sync/errgroup"
)

func CalculateUtxoStatus(spendingTxId *chainhash.Hash, lockTime uint32, blockHeight uint32) Status {
	status := Status_OK
	if spendingTxId != nil {
		status = Status_SPENT
	} else if lockTime > 0 {
		if lockTime < 500000000 && lockTime > blockHeight {
			status = Status_LOCKED
		} else if lockTime >= 500000000 && lockTime > uint32(time.Now().Unix()) {
			// TODO this should be a check for the median time past for the last 11 blocks
			status = Status_LOCKED
		}
	}

	return status
}

// GetFeesAndUtxoHashes returns the fees and utxo hashes for the outputs of a transaction.
// It will return an error if the context is cancelled.
func GetFeesAndUtxoHashes(ctx context.Context, tx *bt.Tx) (uint64, []*chainhash.Hash, error) {
	if !tx.IsExtended() && !tx.IsCoinbase() {
		return 0, nil, fmt.Errorf("tx is not extended")
	}

	var fees uint64
	utxoHashes := make([]*chainhash.Hash, 0, len(tx.Outputs))

	if !tx.IsCoinbase() {
		for _, input := range tx.Inputs {
			fees += input.PreviousTxSatoshis
		}
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

// GetUtxoHashes returns the utxo hashes for the outputs of a transaction.
func GetUtxoHashes(tx *bt.Tx) ([]chainhash.Hash, error) {
	utxoHashes := make([]chainhash.Hash, 0, len(tx.Outputs))
	utxoHashesMu := sync.Mutex{}

	g := errgroup.Group{}

	for i, output := range tx.Outputs {
		if output.Satoshis > 0 {
			i := i
			output := output
			g.Go(func() error {
				utxoHash, utxoErr := util.UTXOHashFromOutput(tx.TxIDChainHash(), output, uint32(i))
				if utxoErr != nil {
					return fmt.Errorf("error getting output utxo hash: %s", utxoErr.Error())
				}

				utxoHashesMu.Lock()
				utxoHashes = append(utxoHashes, *utxoHash)
				utxoHashesMu.Unlock()

				return nil
			})
		}
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return utxoHashes, nil
}
