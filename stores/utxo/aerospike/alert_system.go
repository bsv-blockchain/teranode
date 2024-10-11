package aerospike

import (
	"context"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
)

// FreezeUTXOs will freeze the UTXOs by setting the spendingTxID to FF...FF
// This will be checked by the LUA script when a spend is attempted
func (s *Store) FreezeUTXOs(_ context.Context, spends []*utxo.Spend) error {
	batchUDFPolicy := aerospike.NewBatchUDFPolicy()
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(spends))

	for _, spend := range spends {
		// nolint: gosec
		keySource := uaerospike.CalculateKeySource(spend.TxID, spend.Vout/uint32(s.utxoBatchSize))

		aeroKey, aErr := aerospike.NewKey(s.namespace, s.setName, keySource)
		if aErr != nil {
			return aErr
		}

		batchRecords = append(batchRecords, aerospike.NewBatchUDF(batchUDFPolicy, aeroKey, luaPackage, "freeze",
			aerospike.NewValue(s.calculateOffsetForOutput(spend.Vout)),
			aerospike.NewValue(spend.UTXOHash[:]),
		))
	}

	batchID := s.batchID.Add(1)

	batchPolicy := util.GetAerospikeBatchPolicy()
	if err := s.client.BatchOperate(batchPolicy, batchRecords); err != nil {
		return errors.NewStorageError("[FREEZE_BATCH_LUA][%d] failed to batch freeze %d aerospike utxos", batchID, len(spends), err)
	}

	// check the return value of the batch operation
	errorsThrown := make([]error, 0, len(spends))

	for _, record := range batchRecords {
		if record.BatchRec().Err != nil {
			errorsThrown = append(errorsThrown, errors.NewStorageError("[FREEZE_BATCH_LUA][%d] failed to batch freeze %d aerospike utxos", batchID, len(spends), record.BatchRec().Err))
		} else {
			// check the return value of the batch operation
			response := record.BatchRec().Record
			if response != nil && response.Bins != nil && response.Bins["SUCCESS"] != nil {
				responseMsg, ok := response.Bins["SUCCESS"].(string)
				if ok {
					responseMsgParts, err := s.parseLuaReturnValue(responseMsg)
					if err != nil {
						errorsThrown = append(errorsThrown, errors.NewStorageError("[FREEZE_BATCH_LUA][%d] failed to batch freeze %d aerospike utxos", batchID, len(spends), err))
					} else if responseMsgParts.returnValue == LuaError {
						errorsThrown = append(errorsThrown, errors.NewStorageError("[FREEZE_BATCH_LUA][%d] failed to freeze aerospike utxo: %s", batchID, responseMsgParts.signal))
					}
				}
			}
		}
	}

	if len(errorsThrown) > 0 {
		return errors.NewStorageError("[FREEZE_BATCH_LUA][%d] failed to batch freeze %d aerospike utxos: %v", batchID, len(spends), errorsThrown)
	}

	return nil
}

// UnFreezeUTXOs will unfreeze the UTXOs by unsetting the frozen spendingTxID
func (s *Store) UnFreezeUTXOs(_ context.Context, spends []*utxo.Spend) error {
	batchUDFPolicy := aerospike.NewBatchUDFPolicy()
	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(spends))

	for _, spend := range spends {
		// nolint: gosec
		keySource := uaerospike.CalculateKeySource(spend.TxID, spend.Vout/uint32(s.utxoBatchSize))

		aeroKey, aErr := aerospike.NewKey(s.namespace, s.setName, keySource)
		if aErr != nil {
			return aErr
		}

		batchRecords = append(batchRecords, aerospike.NewBatchUDF(batchUDFPolicy, aeroKey, luaPackage, "unfreeze",
			aerospike.NewValue(s.calculateOffsetForOutput(spend.Vout)),
			aerospike.NewValue(spend.UTXOHash[:]),
		))
	}

	batchID := s.batchID.Add(1)

	batchPolicy := util.GetAerospikeBatchPolicy()
	if err := s.client.BatchOperate(batchPolicy, batchRecords); err != nil {
		return errors.NewStorageError("[UNFREEZE_BATCH_LUA][%d] failed to batch freeze %d aerospike utxos", batchID, len(spends), err)
	}

	// check the return value of the batch operation
	errorsThrown := make([]error, 0, len(spends))

	for _, record := range batchRecords {
		if record.BatchRec().Err != nil {
			errorsThrown = append(errorsThrown, errors.NewStorageError("[UNFREEZE_BATCH_LUA][%d] failed to batch unfreeze %d aerospike utxos", batchID, len(spends), record.BatchRec().Err))
		} else {
			// check the return value of the batch operation
			response := record.BatchRec().Record
			if response != nil && response.Bins != nil && response.Bins["SUCCESS"] != nil {
				responseMsg, ok := response.Bins["SUCCESS"].(string)
				if ok {
					responseMsgParts, err := s.parseLuaReturnValue(responseMsg)
					if err != nil {
						errorsThrown = append(errorsThrown, errors.NewStorageError("[UNFREEZE_BATCH_LUA][%d] failed to batch unfreeze %d aerospike utxos", batchID, len(spends), err))
					} else if responseMsgParts.returnValue == LuaError {
						errorsThrown = append(errorsThrown, errors.NewStorageError("[UNFREEZE_BATCH_LUA][%d] failed to unfreeze aerospike utxo: %s", batchID, responseMsgParts.signal))
					}
				}
			}
		}
	}

	if len(errorsThrown) > 0 {
		return errors.NewStorageError("[UNFREEZE_BATCH_LUA][%d] failed to batch unfreeze %d aerospike utxos: %v", batchID, len(spends), errorsThrown)
	}

	return nil
}

// ReAssignUTXO will reassign the transaction output idx UTXO to a new UTXO
func (s *Store) ReAssignUTXO(_ context.Context, oldUtxo *utxo.Spend, newUtxo *utxo.Spend) error {
	// nolint: gosec
	keySource := uaerospike.CalculateKeySource(oldUtxo.TxID, oldUtxo.Vout/uint32(s.utxoBatchSize))

	aeroKey, aErr := aerospike.NewKey(s.namespace, s.setName, keySource)
	if aErr != nil {
		return aErr
	}

	batchUDFPolicy := aerospike.NewBatchUDFPolicy()

	batchRecords := []aerospike.BatchRecordIfc{
		aerospike.NewBatchUDF(batchUDFPolicy, aeroKey, luaPackage, "reassign",
			aerospike.NewValue(s.calculateOffsetForOutput(oldUtxo.Vout)),
			aerospike.NewValue(oldUtxo.UTXOHash[:]),
			aerospike.NewValue(newUtxo.UTXOHash[:]),
			aerospike.NewIntegerValue(int(s.blockHeight.Load())),
			aerospike.NewIntegerValue(utxo.ReAssignedUtxoSpendableAfterBlocks),
		),
	}

	batchPolicy := util.GetAerospikeBatchPolicy()
	if err := s.client.BatchOperate(batchPolicy, batchRecords); err != nil {
		return errors.NewStorageError("[REASSIGN_BATCH_LUA] failed to reassign aerospike utxo", err)
	}

	// check whether an error was thrown
	if batchRecords[0].BatchRec().Err != nil {
		return errors.NewStorageError("[REASSIGN_BATCH_LUA] failed to reassign aerospike utxo", batchRecords[0].BatchRec().Err)
	}

	// check the return value of the batch operation
	response := batchRecords[0].BatchRec().Record
	if response != nil && response.Bins != nil && response.Bins["SUCCESS"] != nil {
		responseMsg, ok := response.Bins["SUCCESS"].(string)
		if ok {
			responseMsgParts, err := s.parseLuaReturnValue(responseMsg)
			if err != nil {
				return errors.NewStorageError("[REASSIGN_BATCH_LUA] failed to reassign aerospike utxo: %s", err)
			} else if responseMsgParts.returnValue == LuaError {
				return errors.NewStorageError("[REASSIGN_BATCH_LUA] failed to reassign aerospike utxo: %s", responseMsgParts.signal)
			}
		}
	}

	return nil
}
