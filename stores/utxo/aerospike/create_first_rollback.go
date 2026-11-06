package aerospike

import (
	"context"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
)

// BeginCreateFirstRollback claims the rollback of a master record that still carries the
// creating bin, by setting the rollingBack bin atomically, so a concurrent finalize or
// roll-forward refuses the record from then on. The write is UPDATE_ONLY and filtered on the
// creating bin being present and the rollingBack bin being absent, so a finalized or absent
// record is never touched and only one caller can set the marker.
//
// It reports true only to the caller whose write set the marker: that caller owns the
// rollback and is the one that unspends the inputs and deletes the record. It reports false,
// writing nothing, when the record is absent, already final, or already rolling back under
// another caller. A filtered-out write is read back only to tell those cases from a store
// fault. Any other error returns (false, err): the caller leaves the record creating and the
// sweep heals it.
func (s *Store) BeginCreateFirstRollback(_ context.Context, hash *chainhash.Hash) (bool, error) {
	key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		return false, errors.NewProcessingError("error in aerospike NewKey", err)
	}

	writePolicy := util.GetAerospikeWritePolicy(s.settings, 0)
	writePolicy.RecordExistsAction = aerospike.UPDATE_ONLY
	writePolicy.FilterExpression = finalizeFilter()

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

	if state.absent || !state.creating || state.rollingBack {
		return false, nil
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
