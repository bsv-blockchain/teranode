package redis

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
)

func (s *Store) Spend(ctx context.Context, spends []*utxo.Spend, _ uint32) (err error) {
	if len(spends) == 0 {
		return errors.NewProcessingError("No spends provided", nil)
	}

	spendFn := fmt.Sprintf("spend_%s", s.version)

	// s.blockHeight is the last mined block, but for the LUA script we are telling it to
	// evaluate this spend in this block height (i.e. 1 greater)
	thisBlockHeight := s.blockHeight.Load() + 1

	for i, spend := range spends {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return errors.NewStorageError("timeout un-spending %d of %d utxos", i, len(spends))
			}

			return errors.NewStorageError("context cancelled un-spending %d of %d utxos", i, len(spends))

		default:
			if spend.TxID == nil {
				return errors.NewProcessingError("TxID is required", nil)
			}

			if spend.UTXOHash == nil {
				return errors.NewProcessingError("UTXOHash is required", nil)
			}

			if spend.SpendingTxID == nil {
				return errors.NewProcessingError("SpendingTxID is required", nil)
			}

			cmd := s.client.Do(ctx,
				"FCALL",
				spendFn,                     // function name
				1,                           // number of key args
				spend.TxID.String(),         // key[1]
				spend.Vout,                  // args[1] - offset
				spend.UTXOHash.String(),     // args[2] - utxoHash
				spend.SpendingTxID.String(), // args[3] - spendingTxID
				thisBlockHeight,             // args[4] - height
				s.expiration.Seconds(),      // args[5] - ttl
			)

			err = cmd.Err()
			if err != nil {
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

			switch res.returnValue {
			case LuaOk:
				continue
			case LuaSpent:
				return utxo.NewErrSpent(spend.TxID, spend.Vout, spend.UTXOHash, res.spendingTxID)
			case LuaFrozen:
				return errors.NewUtxoFrozenError("[SPEND_LUA][%s] transaction is frozen, blockHeight %d: %d - %s", spend.TxID.String(), thisBlockHeight, spend.Vout, text)
			default:
				return errors.NewProcessingError("Redis spend: %s", res)
			}
		}
	}

	return nil
}
