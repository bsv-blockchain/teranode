// //go:build aerospike

package aerospike2

import (
	"context"
	_ "embed"
	"math"

	"github.com/bitcoin-sv/ubsv/stores/utxo"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/gocore"
)

//go:embed unspend.lua
var unSpendLUA []byte

var luaUnSpendFunction = "unspend_v2.1"

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
			s.logger.Warnf("unspending utxo %s of tx %s:%d, spending tx: %s", spend.UTXOHash.String(), spend.TxID.String(), spend.Vout, spend.SpendingTxID.String())
			if err = s.unSpendLua(spend); err != nil {
				// just return the raw error, should already be wrapped
				return err
			}
		}
	}

	return nil
}

func (s *Store) unSpendLua(spend *utxo.Spend) error {
	utxoBatchSize, _ := gocore.Config().GetInt("utxoBatchSize", 20_000)

	policy := util.GetAerospikeWritePolicy(3, math.MaxUint32)

	keySource := calculateKeySource(spend.TxID, spend.Vout/uint32(utxoBatchSize))

	key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
	if err != nil {
		prometheusUtxoMapErrors.WithLabelValues("Reset", err.Error()).Inc()
		return errors.New(errors.ERR_PROCESSING, "error in aerospike NewKey", err)
	}

	offset := calculateOffsetForOutput(spend.Vout, uint32(utxoBatchSize))

	ret, err := s.client.Execute(policy, key, luaUnSpendFunction, "unSpend",
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
