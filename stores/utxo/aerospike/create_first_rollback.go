package aerospike

import (
	"context"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
)

// BeginCreateFirstRollback sets the rollingBack bin on a master record that still carries
// the creating bin, atomically, so a concurrent finalize or roll-forward refuses the record
// from then on. The write is UPDATE_ONLY and filtered on the creating bin, so a finalized
// or absent record is never touched.
//
// It reports true when the record is rolling back after the call, whether this call set the
// marker or an earlier rollback of the same transaction did. A filtered-out write is read
// back: the write is idempotent, so the only way it is filtered out is that the creating bin
// is gone, which reports false, while a record that still shows rollingBack reports true. An
// absent record reports false. Any other error returns (false, err): the caller leaves the
// record creating and the sweep heals it.
func (s *Store) BeginCreateFirstRollback(_ context.Context, hash *chainhash.Hash) (bool, error) {
	key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		return false, errors.NewProcessingError("error in aerospike NewKey", err)
	}

	writePolicy := util.GetAerospikeWritePolicy(s.settings, 0)
	writePolicy.RecordExistsAction = aerospike.UPDATE_ONLY
	writePolicy.FilterExpression = aerospike.ExpBinExists(fields.Creating.String())

	_, putErr := s.client.Operate(writePolicy, key, aerospike.PutOp(aerospike.NewBin(fields.RollingBack.String(), true)))
	if putErr == nil {
		return true, nil
	}

	if errors.Is(putErr, aerospike.ErrKeyNotFound) {
		return false, nil
	}

	if !isFilteredOut(putErr) {
		prometheusUtxoMapErrors.WithLabelValues("BeginCreateFirstRollback", "error").Inc()

		return false, errors.NewStorageError("error marking record as rolling back", putErr)
	}

	state, getErr := s.readCreatingState(key)
	if getErr != nil {
		return false, errors.NewStorageError("error reading record after the rolling back marker was filtered out", getErr)
	}

	switch {
	case state.absent, !state.creating:
		return false, nil
	case state.rollingBack:
		return true, nil
	}

	return false, errors.NewStorageError("rolling back marker for %s was filtered out but the record is still creating", hash.String())
}

// creatingState is what a master record's creating and rollingBack bins say.
type creatingState struct {
	absent      bool
	creating    bool
	rollingBack bool
}

// readCreatingState reads the creating and rollingBack bins of the master record at key. An
// absent record is reported, not an error.
func (s *Store) readCreatingState(key *aerospike.Key) (creatingState, error) {
	rec, err := s.client.Get(util.GetAerospikeReadPolicy(s.settings), key, fields.Creating.String(), fields.RollingBack.String())
	if err != nil {
		if errors.Is(err, aerospike.ErrKeyNotFound) {
			return creatingState{absent: true}, nil
		}

		return creatingState{}, err
	}

	var state creatingState

	if rec != nil && rec.Bins != nil {
		_, state.creating = rec.Bins[fields.Creating.String()]
		rb, ok := rec.Bins[fields.RollingBack.String()].(bool)
		state.rollingBack = ok && rb
	}

	return state, nil
}
