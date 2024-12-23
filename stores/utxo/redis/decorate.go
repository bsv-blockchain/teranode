package redis

import (
	"context"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/util"
)

func (s *Store) BatchDecorate(ctx context.Context, items []*utxo.UnresolvedMetaData, _ ...string) error {
	for i, item := range items {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return errors.NewStorageError("timeout un-spending %d of %d utxos", i, len(items))
			}

			return errors.NewStorageError("context cancelled un-spending %d of %d utxos", i, len(items))

		default:
			data, err := s.Get(ctx, &item.Hash)
			if err != nil {
				items[i].Data = nil

				if !util.CoinbasePlaceholderHash.Equal(items[i].Hash) {
					items[i].Err = err
				}
			} else {
				items[i].Data = data
			}
		}
	}

	return nil
}

func (s *Store) PreviousOutputsDecorate(ctx context.Context, outpoints []*meta.PreviousOutput) error {
	for i, outpoint := range outpoints {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return errors.NewStorageError("timeout un-spending %d of %d utxos", i, len(outpoints))
			}

			return errors.NewStorageError("context cancelled un-spending %d of %d utxos", i, len(outpoints))

		default:
			txMeta, err := s.Get(ctx, &outpoint.PreviousTxID)
			if err != nil {
				return err
			}

			data := txMeta.Tx.Outputs[outpoint.Vout]

			outpoint.Satoshis = data.Satoshis
			outpoint.LockingScript = *data.LockingScript
		}
	}

	return nil
}
