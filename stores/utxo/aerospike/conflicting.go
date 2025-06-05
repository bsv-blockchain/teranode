package aerospike

import (
	"context"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/stores/utxo/spend"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"golang.org/x/sync/errgroup"
)

func (s *Store) GetCounterConflicting(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	ctx, _, _ = tracing.StartTracing(ctx, "GetCounterConflicting",
		tracing.WithHistogram(prometheusTxMetaAerospikeMapGetCounterConflicting),
	)

	return utxo.GetCounterConflictingTxHashes(ctx, s, txHash)
}

// GetConflictingChildren returns the conflicting transactions for the given transaction hash
func (s *Store) GetConflictingChildren(ctx context.Context, hash chainhash.Hash) ([]chainhash.Hash, error) {
	ctx, _, _ = tracing.StartTracing(ctx, "GetConflicting",
		tracing.WithHistogram(prometheusTxMetaAerospikeMapGetConflicting),
	)

	return utxo.GetConflictingChildren(ctx, s, hash)
}

// SetConflicting sets the conflicting flag on the given transactions
// the flag should always be stored and read on the main record
func (s *Store) SetConflicting(ctx context.Context, txHashes []chainhash.Hash, setValue bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	var (
		batchRecords   = make([]aerospike.BatchRecordIfc, 0, len(txHashes))
		batchUDFPolicy = aerospike.NewBatchUDFPolicy()
		txs            = make([]*bt.Tx, len(txHashes))
		g, gCtx        = errgroup.WithContext(ctx)
	)

	// first get all the tx data in parallel
	for idx, txHash := range txHashes {
		idx := idx
		txHash := txHash

		if txHash.Equal(util.CoinbasePlaceholderHashValue) {
			// skip coinbase placeholder
			continue
		}

		g.Go(func() error {
			txMeta, err := s.get(gCtx, &txHash, []fields.FieldName{fields.Tx, fields.ConflictingChildren, fields.Utxos})
			if err != nil {
				return err
			}

			txs[idx] = txMeta.Tx

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	affectedParentSpends := make([]*utxo.Spend, 0, len(txHashes))
	spendingTxHashes := make([]chainhash.Hash, 0, len(txHashes))

	for _, tx := range txs {
		// first make sure the parents have been updated that this transaction is conflicting
		// if this fails, the transaction will not be marked as conflicting and the function will return an error
		if err := s.updateParentConflictingChildren(tx); err != nil {
			return nil, nil, err
		}

		key, err := aerospike.NewKey(s.namespace, s.setName, tx.TxIDChainHash().CloneBytes())
		if err != nil {
			return nil, nil, errors.NewProcessingError("could not create aerospike key", err)
		}

		// set the conflicting flag using a lua script in the batch
		batchRecords = append(batchRecords, aerospike.NewBatchUDF(
			batchUDFPolicy,
			key,
			LuaPackage,
			"setConflicting",
			aerospike.NewValue(setValue),
			aerospike.NewIntegerValue(int(s.blockHeight.Load())),
			aerospike.NewValue(s.settings.UtxoStore.BlockHeightRetention),
		))

		for i, input := range tx.Inputs {
			utxoHash, err := util.UTXOHashFromInput(input)
			if err != nil {
				return nil, nil, err
			}

			spend := &utxo.Spend{
				TxID:         input.PreviousTxIDChainHash(),
				Vout:         input.PreviousTxOutIndex,
				UTXOHash:     utxoHash,
				SpendingData: spend.NewSpendingData(tx.TxIDChainHash(), i),
			}

			affectedParentSpends = append(affectedParentSpends, spend)
		}

		for vOut, output := range tx.Outputs {
			vOutUint32, err := util.SafeIntToUint32(vOut)
			if err != nil {
				return nil, nil, err
			}

			utxoHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), output, vOutUint32)
			if err != nil {
				return nil, nil, err
			}

			spend := &utxo.Spend{
				TxID:     tx.TxIDChainHash(),
				Vout:     vOutUint32,
				UTXOHash: utxoHash,
			}

			// optimize to get all in 1 query
			spendResponse, err := s.GetSpend(ctx, spend)
			if err != nil {
				return nil, nil, err
			}

			if spendResponse.SpendingData != nil && spendResponse.SpendingData.TxID != nil {
				spendingTxHashes = append(spendingTxHashes, *spendResponse.SpendingData.TxID)
			}
		}
	}

	if err := s.client.BatchOperate(util.GetAerospikeBatchPolicy(s.settings), batchRecords); err != nil {
		return nil, nil, errors.NewProcessingError("could not batch write conflicting flag", err)
	}

	return affectedParentSpends, spendingTxHashes, nil
}

func (s *Store) updateParentConflictingChildren(tx *bt.Tx) error {
	updateParentTxHashes := make(map[chainhash.Hash]struct{})
	for _, input := range tx.Inputs {
		updateParentTxHashes[*input.PreviousTxIDChainHash()] = struct{}{}
	}

	var (
		batchWritePolicy = aerospike.NewBatchWritePolicy()
		batchRecords     = make([]aerospike.BatchRecordIfc, 0, len(updateParentTxHashes))
		listPolicy       = aerospike.NewListPolicy(aerospike.ListOrderUnordered, aerospike.ListWriteFlagsAddUnique|aerospike.ListWriteFlagsNoFail)
	)

	// only allow updates to existing records
	batchWritePolicy.RecordExistsAction = aerospike.UPDATE_ONLY

	for parentTxHash := range updateParentTxHashes {
		// create the aerospike key on the main records
		key, err := aerospike.NewKey(s.namespace, s.setName, parentTxHash.CloneBytes())
		if err != nil {
			return errors.NewProcessingError("could not create aerospike key", err)
		}

		// update the conflictingChildren bin with the tx ID of this parent making sure it is unique in the list
		op := aerospike.ListAppendWithPolicyOp(listPolicy, fields.ConflictingChildren.String(), tx.TxIDChainHash().CloneBytes())

		batchRecords = append(batchRecords, aerospike.NewBatchWrite(batchWritePolicy, key, op))
	}

	if err := s.client.BatchOperate(util.GetAerospikeBatchPolicy(s.settings), batchRecords); err != nil {
		return errors.NewStorageError("could not update parent conflicting children", err)
	}

	return nil
}
