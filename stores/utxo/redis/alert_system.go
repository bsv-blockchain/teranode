package redis

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/utxo"
)

func (s *Store) FreezeUTXOs(ctx context.Context, utxos []*utxo.Spend) error {
	return s.freezeUnfreeze(ctx, utxos, fmt.Sprintf("freeze_%s", s.version))
}

func (s *Store) UnFreezeUTXOs(ctx context.Context, utxos []*utxo.Spend) error {
	return s.freezeUnfreeze(ctx, utxos, fmt.Sprintf("unfreeze_%s", s.version))
}

func (s *Store) freezeUnfreeze(ctx context.Context, utxos []*utxo.Spend, fn string) error {
	if len(utxos) == 0 {
		return errors.NewProcessingError("No utxos provided", nil)
	}

	for i, u := range utxos {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return errors.NewStorageError("timeout un-spending %d of %d utxos", i, len(utxos))
			}

			return errors.NewStorageError("context cancelled un-spending %d of %d utxos", i, len(utxos))

		default:
			if u.TxID == nil {
				return errors.NewProcessingError("TxID is required", nil)
			}

			if u.UTXOHash == nil {
				return errors.NewProcessingError("UTXOHash is required", nil)
			}

			if u.SpendingTxID == nil {
				return errors.NewProcessingError("SpendingTxID is required", nil)
			}

			cmd := s.client.Do(ctx,
				"FCALL",
				fn,                  // function name
				1,                   // number of key args
				u.TxID.String(),     // key[1]
				u.Vout,              // args[1] - offset
				u.UTXOHash.String(), // args[2] - utxoHash
			)

			err := cmd.Err()
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
				return errors.NewUtxoSpentError(*u.TxID, u.Vout, *u.UTXOHash, *res.spendingTxID)
			case LuaFrozen:
				return errors.NewUtxoFrozenError("utxo already frozen: %s", text)
			default:
				return errors.NewProcessingError("Redis freeze: %s", res)
			}
		}
	}

	return nil
}

func (s *Store) ReAssignUTXO(ctx context.Context, oldUTXO *utxo.Spend, newUTXO *utxo.Spend) error {
	// -- KEYS[1]: transaction key
	// -- ARGV[1]: offset
	// -- ARGV[2]: utxoHash
	// -- ARGV[3]: newUtxoHash
	// -- ARGV[4]: blockHeight
	// -- ARGV[5]: spendableAfter
	if oldUTXO.TxID == nil {
		return errors.NewProcessingError("TxID is required", nil)
	}

	if oldUTXO.UTXOHash == nil {
		return errors.NewProcessingError("Old UTXOHash is required", nil)
	}

	if newUTXO.UTXOHash == nil {
		return errors.NewProcessingError("New UTXOHash is required", nil)
	}

	reassignFn := fmt.Sprintf("reassign_%s", s.version)

	cmd := s.client.Do(ctx,
		"FCALL",
		reassignFn,                              // function name
		1,                                       // number of key args
		oldUTXO.TxID.String(),                   // key[1]
		oldUTXO.Vout,                            // args[1] - offset
		oldUTXO.UTXOHash.String(),               // args[2] - utxoHash
		newUTXO.UTXOHash.String(),               // args[3] - newUtxoHash
		s.blockHeight.Load(),                    // args[4] - blockHeight
		utxo.ReAssignedUtxoSpendableAfterBlocks, // args[5] - spendableAfter
	)

	err := cmd.Err()
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
		return nil
	default:
		return errors.NewProcessingError("Redis reassign: %s", res)
	}
}
