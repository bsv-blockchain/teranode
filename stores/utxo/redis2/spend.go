package redis2

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/libsv/go-bt/v2"
)

func (s *Store) Spend(ctx context.Context, tx *bt.Tx) ([]*utxo.Spend, error) {
	spendFn := fmt.Sprintf("spend_%s", s.version)

	// s.blockHeight is the last mined block, but for the LUA script we are telling it to
	// evaluate this spend in this block height (i.e. 1 greater)
	thisBlockHeight := s.blockHeight.Load() + 1

	spends, err := utxo.GetSpends(tx)
	if err != nil {
		return nil, err
	}

	if len(spends) == 0 {
		return nil, errors.NewProcessingError("No spends provided", nil)
	}

	var (
		spentSpends = make([]*utxo.Spend, 0, len(spends))

		errorFound bool
	)

	for i, spend := range spends {
		select {
		case <-ctx.Done():
			errorFound = true

			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				spend.Err = errors.NewStorageError("timeout un-spending %d of %d utxos", i, len(spends))
			} else {
				spend.Err = errors.NewStorageError("context cancelled un-spending %d of %d utxos", i, len(spends))
			}

			continue

		default:
			if spend.TxID == nil {
				errorFound = true
				spend.Err = errors.NewProcessingError("TxID is required", nil)

				continue
			}

			if spend.UTXOHash == nil {
				errorFound = true
				spend.Err = errors.NewProcessingError("UTXOHash is required", nil)

				continue
			}

			if spend.SpendingTxID == nil {
				errorFound = true
				spend.Err = errors.NewProcessingError("SpendingTxID is required", nil)

				continue
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
				errorFound = true
				spend.Err = errors.NewStorageError("Error spending %d of %d utxos: %s", i, len(spends), err)

				continue
			}

			text, err := cmd.Text()
			if err != nil {
				errorFound = true
				spend.Err = errors.NewProcessingError("Could not get text from redis cmd", err)

				continue
			}

			res, err := parseLuaReturnValue(text)
			if err != nil {
				spend.Err = errors.NewProcessingError("Could not parse redis LUA response", err)
			}

			switch res.returnValue {
			case LuaOk:
				spentSpends = append(spentSpends, spend)

				continue
			case LuaSpent:
				errorFound = true
				spend.ConflictingTxID = res.spendingTxID
				spend.Err = errors.NewUtxoSpentError(*spend.TxID, spend.Vout, *spend.UTXOHash, *res.spendingTxID)

				continue
			case LuaCoinbaseImmature:
				errorFound = true
				spend.Err = errors.NewTxCoinbaseImmatureError("[SPEND_LUA][%s] coinbase is not spendable until blockHeight %d: %d - %s", spend.TxID.String(), thisBlockHeight, spend.Vout, text)

				continue
			case LuaFrozen:
				errorFound = true
				spend.Err = errors.NewUtxoFrozenError("[SPEND_LUA][%s] transaction is frozen, blockHeight %d: %d - %s", spend.TxID.String(), thisBlockHeight, spend.Vout, text)

				continue
			case LuaConflicting:
				errorFound = true
				spend.Err = errors.NewTxConflictingError("[SPEND_LUA][%s] conflicting transaction: %s", spend.TxID.String(), text)

				continue
			default:
				if res.signal == "UTXO not found" {
					errorFound = true
					spend.Err = errors.NewNotFoundError("UTXO not found: %s:%d", spend.TxID.String(), spend.Vout)

					continue
				}

				if res.spendingTxID != nil {
					errorFound = true
					spend.Err = errors.NewUtxoSpentError(*spend.TxID, spend.Vout, *spend.UTXOHash, *res.spendingTxID)

					continue
				}

				errorFound = true
				spend.Err = errors.NewProcessingError("Redis spend: %s:%s", res.returnValue, res.signal)
			}
		}
	}

	if errorFound {
		unSpendErr := s.UnSpend(ctx, spentSpends)

		return spends, errors.NewTxInvalidError("Error(s) found spending utxos", unSpendErr)
	}

	return spends, nil
}
