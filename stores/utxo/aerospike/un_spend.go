// //go:build aerospike

package aerospike

import (
	"context"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
)

func (s *Store) UnSpend(ctx context.Context, spends []*utxo.Spend) (err error) {
	return s.unSpend(ctx, spends)
}

func (s *Store) unSpend(ctx context.Context, spends []*utxo.Spend) (err error) {
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

				if err = s.unSpendLua(spend); err != nil {
					// just return the raw error, should already be wrapped
					return err
				}
			}
		}
	}

	return nil
}

func (s *Store) unSpendLua(spend *utxo.Spend) error {
	policy := util.GetAerospikeWritePolicy(0, aerospike.TTLDontExpire)

	// nolint gosec
	keySource := uaerospike.CalculateKeySource(spend.TxID, spend.Vout/uint32(s.utxoBatchSize))

	key, aErr := aerospike.NewKey(s.namespace, s.setName, keySource)
	if aErr != nil {
		prometheusUtxoMapErrors.WithLabelValues("Reset", aErr.Error()).Inc()
		return errors.NewProcessingError("error in aerospike NewKey", aErr)
	}

	offset := s.calculateOffsetForOutput(spend.Vout)

	ret, aErr := s.client.Execute(policy, key, LuaPackage, "unSpend",
		aerospike.NewIntegerValue(int(offset)), // vout adjusted for utxoBatchSize
		aerospike.NewValue(spend.UTXOHash[:]),  // utxo hash
	)
	if aErr != nil {
		prometheusUtxoMapErrors.WithLabelValues("Reset", aErr.Error()).Inc()
		return errors.NewStorageError("error in aerospike unspend record", aErr)
	}

	responseMsg, ok := ret.(string)
	if !ok {
		prometheusUtxoMapErrors.WithLabelValues("Reset", "response not string").Inc()
		return errors.NewStorageError("error in aerospike unspend record", aErr)
	}

	resp, err := s.ParseLuaReturnValue(responseMsg)
	if err != nil {
		prometheusUtxoMapErrors.WithLabelValues("Reset", "error parsing response").Inc()
		return errors.NewProcessingError("error parsing response %s", responseMsg, err)
	}

	switch resp.ReturnValue {
	case LuaOk:
		if resp.Signal == LuaNotAllSpent {
			go func() {
				if _, err := s.IncrementNrRecords(spend.TxID, 1); err != nil {
					s.logger.Errorf("error incrementing nrRecords for tx %s: %v", spend.TxID.String(), err)
				}
			}()

			if resp.External {
				go s.setTTLExternalTransaction(s.ctx, spend.TxID, 0)
			}
		}

	case LuaError:
		prometheusUtxoMapErrors.WithLabelValues("Reset", "error response").Inc()
		return errors.NewStorageError("error in aerospike unspend record: %s", responseMsg)

	default:
		prometheusUtxoMapErrors.WithLabelValues("Reset", "default response").Inc()
		return errors.NewStorageError("error in aerospike unspend record: %s", responseMsg)
	}

	prometheusUtxoMapReset.Inc()

	return nil
}
