package redis

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/utxo"
)

func (s *Store) UnSpend(ctx context.Context, spends []*utxo.Spend) error {
	unSpendFn := fmt.Sprintf("unspend_%s", s.version)

	for i, spend := range spends {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return errors.NewStorageError("timeout un-spending %d of %d utxos", i, len(spends))
			}

			return errors.NewStorageError("context cancelled un-spending %d of %d utxos", i, len(spends))

		default:
			if spend != nil {
				var txID string

				if spend.SpendingTxID != nil {
					txID = spend.SpendingTxID.String()
				}

				s.logger.Warnf("un-spending utxo %s of tx %s:%d, spending tx: %s", spend.UTXOHash.String(), spend.TxID.String(), spend.Vout, txID)

				cmd := s.client.Do(ctx,
					"FCALL",
					unSpendFn,               // function name
					1,                       // number of key args
					spend.TxID.String(),     // key[1]
					spend.Vout,              // args[1] - offset
					spend.UTXOHash.String(), // args[2] - utxoHash
				)

				if err := cmd.Err(); err != nil {
					return err
				}

				text, err := cmd.Text()
				if err != nil {
					return err
				}

				res, err := parseLuaReturnValue(text)
				if err != nil {
					return errors.NewProcessingError("Could not parse redis LUA response", err)
				}

				if res.returnValue != LuaOk {
					return errors.NewProcessingError("Redis spend: %s", res)
				}
			}
		}
	}

	return nil
}
