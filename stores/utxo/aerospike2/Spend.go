// //go:build aerospike

package aerospike2

import (
	"context"
	_ "embed"
	"math"
	"strings"
	"sync"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

//go:embed spend.lua
var spendLUA []byte

var luaSpendFunction = "spend_v2.1"

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

	if s.spendBatcher != nil {
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
			// revert the successfully spent utxos
			unspendErr := s.UnSpend(ctx, spentSpends)
			if unspendErr != nil {
				err = errors.Join(err, unspendErr)
			}
			return errors.New(errors.ERR_ERROR, "error in aerospike spend record", err)
		}

		prometheusUtxoMapSpend.Add(float64(len(spends)))
	}

	policy := util.GetAerospikeWritePolicy(0, math.MaxUint32)
	policy.RecordExistsAction = aerospike.UPDATE_ONLY

	for i, spend := range spends {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return errors.New(errors.ERR_PROCESSING, "timeout spending %d of %d utxos", i, len(spends))
			}
			return errors.New(errors.ERR_PROCESSING, "context cancelled spending %d of %d utxos", i, len(spends))

		default:
			if spend == nil {
				continue
			}

			done := make(chan error)

			go func() {
				s.sendSpendBatchLua([]*batchSpend{
					{
						spend: spend,
						done:  done,
					},
				})
			}()

			err = <-done
			if err != nil {
				if errors.Is(err, utxo.NewErrSpent(spend.TxID, spend.Vout, spend.UTXOHash, spend.SpendingTxID)) {
					return err
				}

				// another error encountered, reverse all spends and return error
				if resetErr := s.UnSpend(context.Background(), spends); resetErr != nil {
					s.logger.Errorf("ERROR in aerospike reset: %v\n", resetErr)
				}

				return errors.New(errors.ERR_ERROR, "error in aerospike spend record", err)
			}
		}
	}

	return nil
}

func (s *Store) sendSpendBatchLua(batch []*batchSpend) {
	batchId := s.batchId.Add(1)
	s.logger.Debugf("[SPEND_BATCH_LUA] sending lua batch %d of %d spends", batchId, len(batch))
	defer func() {
		s.logger.Debugf("[SPEND_BATCH_LUA] sending lua batch %d of %d spends DONE", batchId, len(batch))
	}()

	batchPolicy := util.GetAerospikeBatchPolicy()

	batchRecords := make([]aerospike.BatchRecordIfc, len(batch))

	utxoBatchSize, _ := gocore.Config().GetInt("utxoBatchSize", 20_000)

	var key *aerospike.Key
	var err error
	for idx, bItem := range batch {
		keySource := calculateKeySource(bItem.spend.TxID, bItem.spend.Vout/uint32(utxoBatchSize))

		key, err = aerospike.NewKey(s.namespace, s.setName, keySource)
		if err != nil {
			// we just return the error on the channel, we cannot process this utxo any further
			bItem.done <- errors.New(errors.ERR_PROCESSING, "[SPEND_BATCH_LUA][%s] failed to init new aerospike key for spend: %w", bItem.spend.UTXOHash.String(), err)
			continue
		}

		offset := calculateOffsetForOutput(bItem.spend.Vout, uint32(utxoBatchSize))

		batchUDFPolicy := aerospike.NewBatchUDFPolicy()
		batchRecords[idx] = aerospike.NewBatchUDF(batchUDFPolicy, key, luaSpendFunction, "spend",
			aerospike.NewIntegerValue(int(offset)),          // vout adjusted for utxo batch size
			aerospike.NewValue(bItem.spend.UTXOHash[:]),     // utxo hash
			aerospike.NewValue(bItem.spend.SpendingTxID[:]), // spending tx id
			aerospike.NewValue(s.blockHeight.Load()),        // block height
			aerospike.NewValue(s.expiration),                // ttl
		)
	}

	err = s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		s.logger.Errorf("[SPEND_BATCH_LUA][%d] failed to batch spend aerospike map utxos in batchId %d: %v", batchId, len(batch), err)
		for idx, bItem := range batch {
			bItem.done <- errors.New(errors.ERR_STORAGE_ERROR, "[SPEND_BATCH_LUA][%s] failed to batch spend aerospike map utxo in batchId %d: %d - %w", bItem.spend.UTXOHash.String(), batchId, idx, err)
		}
	}

	// batchOperate may have no errors, but some of the records may have failed
	for idx, batchRecord := range batchRecords {
		spend := batch[idx].spend
		err = batchRecord.BatchRec().Err
		if err != nil {
			batch[idx].done <- errors.New(errors.ERR_ERROR, "[SPEND_BATCH_LUA][%s] error in aerospike spend batch record, blockHeight %d: %d - %w", spend.UTXOHash.String(), s.blockHeight.Load(), batchId, err)
		} else {
			response := batchRecord.BatchRec().Record
			if response != nil && response.Bins != nil && response.Bins["SUCCESS"] != nil {
				responseMsg, ok := response.Bins["SUCCESS"].(string)
				if ok {
					responseMsgParts := strings.Split(responseMsg, ":")
					switch responseMsgParts[0] {
					case "OK":
						batch[idx].done <- nil
						if len(responseMsgParts) > 1 && responseMsgParts[1] == "ALLSPENT" {
							// all utxos in this record are spent so we decrement the nrRecords in the master record
							// we do this in a separate go routine to avoid blocking the batcher
							go s.incrementNrRecords(spend.TxID, -1)
						}

					case "SPENT":
						// spent by another transaction
						// TODO - Check if this needs to be reversed
						spendingTxID, hashErr := chainhash.NewHashFromStr(responseMsgParts[1])
						if hashErr != nil {
							batch[idx].done <- errors.New(errors.ERR_PROCESSING, "[SPEND_BATCH_LUA][%s] could not parse spending tx hash: %w", spend.UTXOHash.String(), hashErr)
						}
						// TODO we need to be able to send the spending TX ID in the error down the line
						batch[idx].done <- utxo.NewErrSpent(spend.TxID, spend.Vout, spend.UTXOHash, spendingTxID)
					case "ERROR":
						batch[idx].done <- errors.New(errors.ERR_STORAGE_ERROR, "[SPEND_BATCH_LUA][%s] error in aerospike spend batch record, blockHeight %d: %d - %s", spend.UTXOHash.String(), s.blockHeight.Load(), batchId, responseMsgParts[1])
					default:
						batch[idx].done <- errors.New(errors.ERR_STORAGE_ERROR, "[SPEND_BATCH_LUA][%s] error in aerospike spend batch record, blockHeight %d: %d - %s", spend.UTXOHash.String(), s.blockHeight.Load(), batchId, responseMsg)
					}
				}
			} else {
				batch[idx].done <- errors.New(errors.ERR_PROCESSING, "[SPEND_BATCH_LUA][%s] could not parse response", spend.UTXOHash.String())
			}
		}
	}
}

func (s *Store) incrementNrRecords(txid *chainhash.Hash, increment int) {
	policy := util.GetAerospikeWritePolicy(0, math.MaxUint32)

	key, err := aerospike.NewKey(s.namespace, s.setName, txid[:])
	if err != nil {
		s.logger.Warnf("[incrementNrRecords][%s] failed to create key for %v: %v", txid, err)
	}

	if _, err := s.client.Execute(
		policy,
		key,
		luaSpendFunction,
		"incrementNrRecords",
		aerospike.NewIntegerValue(increment),
	); err != nil {
		s.logger.Errorf("[incrementNrRecords][%s] failed to increment nrRecords: %v", key, err)
	}
}
