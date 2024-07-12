// //go:build aerospike

package aerospike2

import (
	"context"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *Store) Delete(_ context.Context, hash *chainhash.Hash) error {
	policy := util.GetAerospikeWritePolicy(0, 0)

	key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		return errors.New(errors.ERR_PROCESSING, "error in aerospike NewKey", err)
	}

	_, err = s.client.Delete(policy, key)
	if err != nil {
		// if the key is not found, we don't need to delete, it's not there anyway
		if errors.Is(err, aerospike.ErrKeyNotFound) {
			return nil
		}

		prometheusUtxoMapErrors.WithLabelValues("Delete", err.Error()).Inc()
		return errors.New(errors.ERR_STORAGE_ERROR, "error in aerospike delete key", err)
	}

	prometheusUtxoMapDelete.Inc()

	return nil
}
