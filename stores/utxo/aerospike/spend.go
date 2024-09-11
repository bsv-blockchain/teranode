// //go:build aerospike

package aerospike

import (
	"context"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
	"github.com/libsv/go-bt/v2/chainhash"
	"golang.org/x/sync/errgroup"
)

type batchSpend struct {
	spend *utxo.Spend
	done  chan error
}

func (s *Store) Spend(ctx context.Context, spends []*utxo.Spend, _ uint32) (err error) {
	return s.spend(ctx, spends)
}

func (s *Store) spend(ctx context.Context, spends []*utxo.Spend) (err error) {
	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			prometheusUtxoMapErrors.WithLabelValues("Spend", "Failed Spend Cleaning").Inc()
			s.logger.Errorf("ERROR panic in aerospike Spend: %v\n", recoverErr)
		}
	}()

	spentSpends := make([]*utxo.Spend, 0, len(spends))

	var mu sync.Mutex

	g := errgroup.Group{}

	for _, spend := range spends {
		if spend == nil {
			continue
		}

		spend := spend

		g.Go(func() error {
			done := make(chan error)
			s.spendBatcher.Put(&batchSpend{
				spend: spend,
				done:  done,
			})

			// this waits for the batch to be sent and the response to be received from the batch operation
			batchErr := <-done
			if batchErr != nil {
				// just return the raw error, should already be wrapped
				return batchErr
			}

			mu.Lock()
			spentSpends = append(spentSpends, spend)
			mu.Unlock()

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		s.logger.Errorf("error in aerospike spend (batched mode): %v", err)

		// revert the successfully spent utxos
		unspendErr := s.UnSpend(ctx, spentSpends)
		if unspendErr != nil {
			err = errors.Join(err, unspendErr)
		}

		return errors.NewError("error in aerospike spend (batched mode)", err)
	}

	prometheusUtxoMapSpend.Add(float64(len(spends)))

	return nil
}

func (s *Store) sendSpendBatchLua(batch []*batchSpend) {
	start := time.Now()
	_, stat, deferFn := tracing.StartTracing(context.Background(), "sendSpendBatchLua",
		tracing.WithParentStat(gocoreStat),
		tracing.WithHistogram(prometheusUtxoSpendBatch),
	)

	defer func() {
		prometheusUtxoSpendBatchSize.Observe(float64(len(batch)))
		deferFn()
	}()

	batchID := s.batchID.Add(1)
	s.logger.Debugf("[SPEND_BATCH_LUA] sending lua batch %d of %d spends", batchID, len(batch))

	defer func() {
		s.logger.Debugf("[SPEND_BATCH_LUA] sending lua batch %d of %d spends DONE", batchID, len(batch))
	}()

	batchPolicy := util.GetAerospikeBatchPolicy()

	batchRecords := make([]aerospike.BatchRecordIfc, 0, len(batch))

	// s.blockHeight is the last mined block, but for the LUA script we are telling it to
	// evaluate this spend in this block height (i.e. 1 greater)
	thisBlockHeight := s.blockHeight.Load() + 1

	var (
		key *aerospike.Key
		err error
		ok  bool
	)

	// create batches of spends, each batch is a record in aerospike
	// we batch the spends to the same record (tx) to the same call in LUA
	// this is to avoid the LUA script being called multiple times for the same transaction
	// we calculate the key source based on the txid and the vout divided by the utxoBatchSize
	aeroKeyMap := make(map[string]*aerospike.Key)
	batchesByKey := make(map[*aerospike.Key][]aerospike.MapValue, len(batch))
	for idx, bItem := range batch {
		keySource := uaerospike.CalculateKeySource(bItem.spend.TxID, bItem.spend.Vout/uint32(s.utxoBatchSize))
		keySourceStr := string(keySource)

		if key, ok = aeroKeyMap[keySourceStr]; !ok {
			key, err = aerospike.NewKey(s.namespace, s.setName, keySource)
			if err != nil {
				// we just return the error on the channel, we cannot process this utxo any further
				bItem.done <- errors.NewProcessingError("[SPEND_BATCH_LUA][%s] failed to init new aerospike key for spend: %w", bItem.spend.TxID.String(), err)
				continue
			}

			aeroKeyMap[keySourceStr] = key
		}

		newMapValue := aerospike.NewMapValue(map[interface{}]interface{}{
			"idx":          idx,
			"offset":       s.calculateOffsetForOutput(bItem.spend.Vout),
			"vOut":         bItem.spend.Vout,
			"utxoHash":     bItem.spend.UTXOHash[:],
			"spendingTxID": bItem.spend.SpendingTxID[:],
		})

		if _, ok = batchesByKey[key]; !ok {
			batchesByKey[key] = []aerospike.MapValue{newMapValue}
		} else {
			batchesByKey[key] = append(batchesByKey[key], newMapValue)
		}
	}

	// TODO #1035 group all spends to the same record (tx) to the same call in LUA and change the LUA script to handle multiple spends
	batchUDFPolicy := aerospike.NewBatchUDFPolicy()
	for aeroKey, batchItems := range batchesByKey {
		batchRecords = append(batchRecords, aerospike.NewBatchUDF(batchUDFPolicy, aeroKey, luaPackage, "spendMulti",
			aerospike.NewValue(batchItems),
			aerospike.NewValue(thisBlockHeight),
			aerospike.NewValue(s.expiration), // ttl
		))
	}

	err = s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		s.logger.Errorf("[SPEND_BATCH_LUA][%d] failed to batch spend aerospike map utxos in batchId %d: %v", batchID, len(batch), err)

		for idx, bItem := range batch {
			bItem.done <- errors.NewStorageError("[SPEND_BATCH_LUA][%s] failed to batch spend aerospike map utxo in batchId %d: %d - %w", bItem.spend.TxID.String(), batchID, idx, err)
		}
	}

	start = stat.NewStat("BatchOperate").AddTime(start)

	// batchOperate may have no errors, but some of the records may have failed
	for _, batchRecord := range batchRecords {
		err = batchRecord.BatchRec().Err
		aeroKey := batchRecord.BatchRec().Key // will this be the same memory address as the key in the loop above?

		batchByKey := batchesByKey[aeroKey]
		txID := batch[batchByKey[0]["idx"].(int)].spend.TxID // all the same ...

		if err != nil {
			// error occurred, we need to send the error to the done channel for each spend in this batch
			for _, batchItem := range batchByKey {
				idx := batchItem["idx"].(int)
				batch[idx].done <- errors.NewStorageError("[SPEND_BATCH_LUA][%s] error in aerospike spend batch record, blockHeight %d: %d - %w", batch[idx].spend.TxID.String(), thisBlockHeight, batchID, err)
			}
		} else {
			response := batchRecord.BatchRec().Record
			if response != nil && response.Bins != nil && response.Bins["SUCCESS"] != nil {
				responseMsg, ok := response.Bins["SUCCESS"].(string)
				if ok {
					responseMsgParts := strings.Split(responseMsg, ":")
					switch responseMsgParts[0] {
					case "OK":
						for _, batchItem := range batchByKey {
							idx := batchItem["idx"].(int)
							batch[idx].done <- nil
						}

						if len(responseMsgParts) > 1 && responseMsgParts[1] == "ALLSPENT" {
							// all utxos in this record are spent so we decrement the nrRecords in the master record
							// we do this in a separate go routine to avoid blocking the batcher
							go func() {
								res, err := s.incrementNrRecords(txID, -1)
								if err != nil {
									// TODO if this goes wrong, we never decrement the nrRecords and the record will never be deleted
									s.logger.Errorf("[SPEND_BATCH_LUA][%s] failed to decrement nrRecords: %v", txID.String(), err)
								}

								if r, ok := res.(string); ok {
									if r == "OK:TTLSET" {
										// TODO - we should TTL all the pagination records for this TX
									}
								}
							}()
						}

					case "FROZEN":
						for _, batchItem := range batchByKey {
							idx := batchItem["idx"].(int)
							batch[idx].done <- errors.NewStorageError("[SPEND_BATCH_LUA][%s] transaction is frozen, blockHeight %d: %d - %s", txID.String(), thisBlockHeight, batchID, responseMsg)
						}

					case "SPENT":
						// spent by another transaction
						spendingTxID, hashErr := chainhash.NewHashFromStr(responseMsgParts[1])
						if hashErr != nil {
							for _, batchItem := range batchByKey {
								idx := batchItem["idx"].(int)
								batch[idx].done <- errors.NewProcessingError("[SPEND_BATCH_LUA][%s] could not parse spending tx hash: %w", txID.String(), hashErr)
							}
						}
						// TODO we need to be able to send the spending TX ID in the error down the line
						//      this is very indiscriminate, we need to know which output was spent by which tx
						for _, batchItem := range batchByKey {
							idx := batchItem["idx"].(int)
							batch[idx].done <- utxo.NewErrSpent(batch[idx].spend.TxID, batch[idx].spend.Vout, batch[idx].spend.UTXOHash, spendingTxID)
						}
					case "ERROR":
						if len(responseMsgParts) > 1 && responseMsgParts[1] == "TX not found" {
							for _, batchItem := range batchByKey {
								idx := batchItem["idx"].(int)
								batch[idx].done <- errors.NewTxNotFoundError("[SPEND_BATCH_LUA][%s] transaction not found, blockHeight %d: %d - %s", txID.String(), thisBlockHeight, batchID, responseMsg)
							}
						} else {
							for _, batchItem := range batchByKey {
								idx := batchItem["idx"].(int)
								batch[idx].done <- errors.NewStorageError("[SPEND_BATCH_LUA][%s] error in LUA spend batch record, blockHeight %d: %d - %s", txID.String(), thisBlockHeight, batchID, responseMsgParts[1])
							}
						}
					default:
						for _, batchItem := range batchByKey {
							idx := batchItem["idx"].(int)
							batch[idx].done <- errors.NewStorageError("[SPEND_BATCH_LUA][%s] error in LUA spend batch record, blockHeight %d: %d - %s", txID.String(), thisBlockHeight, batchID, responseMsg)
						}
					}
				}
			} else {
				for _, batchItem := range batchByKey {
					idx := batchItem["idx"].(int)
					batch[idx].done <- errors.NewProcessingError("[SPEND_BATCH_LUA][%s] could not parse response", txID.String())
				}
			}
		}
	}

	stat.NewStat("postBatchOperate").AddTime(start)
}

func (s *Store) incrementNrRecords(txid *chainhash.Hash, increment int) (interface{}, error) {
	policy := util.GetAerospikeWritePolicy(0, math.MaxUint32)

	key, err := aerospike.NewKey(s.namespace, s.setName, txid[:])
	if err != nil {
		s.logger.Warnf("[incrementNrRecords][%s] failed to create key for %v: %v", txid, err)
	}

	res, err := s.client.Execute(
		policy,
		key,
		luaPackage,
		"incrementNrRecords",
		aerospike.NewIntegerValue(increment),
	)
	if err != nil {
		return nil, errors.NewProcessingError("[incrementNrRecords][%s] failed to increment nrRecords", key, err)
	}

	return res, nil
}
