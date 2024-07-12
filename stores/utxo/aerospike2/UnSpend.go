// //go:build aerospike

package aerospike2

import (
	"context"
	"math"

	"github.com/bitcoin-sv/ubsv/stores/utxo"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/util"
)

func (s *Store) UnSpend(ctx context.Context, spends []*utxo.Spend) (err error) {
	return s.unSpend(ctx, spends)
}

func (s *Store) unSpend(ctx context.Context, spends []*utxo.Spend) (err error) {
	for i, spend := range spends {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return errors.New(errors.ERR_STORAGE_ERROR, "timeout un-spending %d of %d utxos", i, len(spends))
			}
			return errors.New(errors.ERR_STORAGE_ERROR, "context cancelled un-spending %d of %d utxos", i, len(spends))
		default:
			s.logger.Warnf("un-spending utxo %s of tx %s:%d, spending tx: %s", spend.UTXOHash.String(), spend.TxID.String(), spend.Vout, spend.SpendingTxID.String())
			if err = s.unSpendLua(spend); err != nil {
				// just return the raw error, should already be wrapped
				return err
			}
		}
	}

	return nil
}

func (s *Store) unSpendLua(spend *utxo.Spend) error {
	if s.utxoBatchSize == 0 {
		panic("aerospike utxo store initialised without specifying a non-zero utxoBatchSize")
	}

	policy := util.GetAerospikeWritePolicy(3, math.MaxUint32)

	keySource := calculateKeySource(spend.TxID, spend.Vout/uint32(s.utxoBatchSize))

	key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
	if err != nil {
		prometheusUtxoMapErrors.WithLabelValues("Reset", err.Error()).Inc()
		return errors.New(errors.ERR_PROCESSING, "error in aerospike NewKey", err)
	}

	offset := s.calculateOffsetForOutput(spend.Vout)

	ret, err := s.client.Execute(policy, key, luaPackage, "unSpend",
		aerospike.NewIntegerValue(int(offset)), // vout adjusted for utxoBatchSize
		aerospike.NewValue(spend.UTXOHash[:]),  // utxo hash
	)

	if err != nil || ret != "OK" {
		prometheusUtxoMapErrors.WithLabelValues("Reset", err.Error()).Inc()
		return errors.New(errors.ERR_STORAGE_ERROR, "error in aerospike unspend record", err)
	}

	prometheusUtxoMapReset.Inc()

	return nil
}
