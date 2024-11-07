package redis2

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
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
			data, err := s.client.HGet(ctx, outpoint.PreviousTxID.String(), fmt.Sprintf("output:%d", outpoint.Vout)).Result()
			if err != nil {
				return err
			}

			b, err := hex.DecodeString(data)
			if err != nil {
				return err
			}

			var output bt.Output

			if _, err := output.ReadFrom(bytes.NewReader(b)); err != nil {
				return err
			}

			outpoint.Satoshis = output.Satoshis
			outpoint.LockingScript = output.LockingScript.Bytes()
		}
	}

	return nil
}
