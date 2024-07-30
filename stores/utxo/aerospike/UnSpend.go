// //go:build aerospike

package aerospike

import (
	"context"
	"math"
	"strings"

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
				return errors.NewStorageError("timeout un-spending %d of %d utxos", i, len(spends))
			}
			return errors.NewStorageError("context cancelled un-spending %d of %d utxos", i, len(spends))
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
		s.utxoBatchSize = defaultUxtoBatchSize
	}

	policy := util.GetAerospikeWritePolicy(0, math.MaxUint32)

	keySource := calculateKeySource(spend.TxID, spend.Vout/uint32(s.utxoBatchSize))

	key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
	if err != nil {
		prometheusUtxoMapErrors.WithLabelValues("Reset", err.Error()).Inc()
		return errors.NewProcessingError("error in aerospike NewKey", err)
	}

	offset := s.calculateOffsetForOutput(spend.Vout)

	ret, err := s.client.Execute(policy, key, luaPackage, "unSpend",
		aerospike.NewIntegerValue(int(offset)), // vout adjusted for utxoBatchSize
		aerospike.NewValue(spend.UTXOHash[:]),  // utxo hash
	)

	if err != nil {
		prometheusUtxoMapErrors.WithLabelValues("Reset", err.Error()).Inc()
		return errors.NewStorageError("error in aerospike unspend record", err)
	}

	responseMsg, ok := ret.(string)
	if !ok {
		prometheusUtxoMapErrors.WithLabelValues("Reset", "response not string").Inc()
		return errors.NewStorageError("error in aerospike unspend record", err)
	}

	responseMsgParts := strings.Split(responseMsg, ":")
	switch responseMsgParts[0] {
	case "OK":
		if len(responseMsgParts) > 1 && responseMsgParts[1] == "NOTALLSPENT" {
			go s.incrementNrRecords(spend.TxID, 1)
		}

	case "ERROR":
		prometheusUtxoMapErrors.WithLabelValues("Reset", "error response").Inc()
		return errors.NewStorageError("error in aerospike unspend record: %s", responseMsg)

	default:
		prometheusUtxoMapErrors.WithLabelValues("Reset", "default response").Inc()
		return errors.NewStorageError("error in aerospike unspend record: %s", responseMsg)
	}

	prometheusUtxoMapReset.Inc()

	return nil
}
