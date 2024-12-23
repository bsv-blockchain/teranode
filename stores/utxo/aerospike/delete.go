// //go:build aerospike

package aerospike

import (
	"context"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *Store) Delete(_ context.Context, hash *chainhash.Hash) error {
	policy := util.GetAerospikeWritePolicy(0, aerospike.TTLDontExpire)

	key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		return errors.NewProcessingError("error in aerospike NewKey", err)
	}

	_, err = s.client.Delete(policy, key)
	if err != nil {
		// if the key is not found, we don't need to delete, it's not there anyway
		if errors.Is(err, aerospike.ErrKeyNotFound) {
			return nil
		}

		prometheusUtxoMapErrors.WithLabelValues("Delete", err.Error()).Inc()

		return errors.NewStorageError("error in aerospike delete key", err)
	}

	prometheusUtxoMapDelete.Inc()

	return nil
}
