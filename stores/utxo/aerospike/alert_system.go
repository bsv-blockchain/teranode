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

	return nil
}

func (s *Store) ReAssignUTXO(ctx context.Context, utxo *utxo.Spend, newUtxo *utxo.Spend) error {
	return nil
}
