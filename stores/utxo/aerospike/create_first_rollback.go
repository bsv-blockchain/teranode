package aerospike

import (
	"bytes"
	"context"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
)

// defaultHoldDuration is the life of a hold when the store names no utxostore_spendRollbackTimeout.
const defaultHoldDuration = 120 * time.Second

// holdSeconds returns the life of a hold in seconds: utxostore_spendRollbackTimeout, the bound of
// every actor that reverses spends under the hold.
func (s *Store) holdSeconds() int64 {
	d := s.SpendRollbackTimeout()
	if d <= 0 {
		d = defaultHoldDuration
	}

	return int64(d / time.Second)
}

// BeginCreateFirstRollback claims the rollback of a master record that still carries the
// creating bin, by setting the rollingBack bin atomically, so a concurrent finalize or
// roll-forward refuses the record from then on. The same write marks the record busy and sets
// holdUntil to now plus utxostore_spendRollbackTimeout, so the caller that owns the rollback
// holds the hold for its whole reversal of spends: no other actor can take it and reverse the
// same spends, and a block-context create cannot adopt the record while it is busy. The owner
// calls ReleaseRollbackHold once the reversal is done. The write is UPDATE_ONLY and filtered on the
// creating bin being present, the rollingBack bin being absent and the attempt bin carrying the
// token, so a finalized or absent record is never touched, a record of another attempt is never
// claimed, and only one caller can set the marker.
//
// It reports true only to the caller whose write set the marker: that caller owns the
// rollback and is the one that unspends the inputs and ends the rollback. It reports false,
// writing nothing, when the record is absent, already final, owned by another attempt, or
// already rolling back under another caller. A filtered-out write is read back only to tell
// those cases from a store fault. Any other error returns (false, err): the caller leaves the
// record creating and the sweep heals it.
func (s *Store) BeginCreateFirstRollback(_ context.Context, hash *chainhash.Hash, attempt []byte) (bool, error) {
	key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		return false, errors.NewProcessingError("error in aerospike NewKey", err)
	}

	writePolicy := util.GetAerospikeWritePolicy(s.settings, 0)
	writePolicy.RecordExistsAction = aerospike.UPDATE_ONLY
	writePolicy.FilterExpression = finalizeFilter(attempt)

	_, putErr := s.client.Operate(writePolicy, key,
		aerospike.PutOp(aerospike.NewBin(fields.RollingBack.String(), true)),
		aerospike.PutOp(aerospike.NewBin(fields.Busy.String(), true)),
		aerospike.PutOp(aerospike.NewBin(fields.HoldUntil.String(), int(time.Now().Unix()+s.holdSeconds()))),
	)
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

	if state.absent || !state.creating || state.rollingBack || !bytes.Equal(state.attempt, attempt) {
		return false, nil
	}

	return false, errors.NewStorageError("rolling back marker for %s was filtered out but the record is still creating", hash.String())
}

// EndCreateFirstRollback ends the rollback of the attempt that carries the token. Its callers
// call it only once every spend of the attempt is reversed, and its first write records that
// on the master as the reversed bin, so that a recovery that finds the record after the external
// blob is gone finishes the rollback without reading the transaction. It then removes the
// pagination children, filtered on the token, then the external blobs, and last turns the master
// record into a hold: one REPLACE_ONLY write that keeps the txid, the creating and rollingBack
// markers, the token and the unminedSince selector, and adds the hold marker and holdUntil, now
// plus utxostore_spendRollbackTimeout. The write is filtered on the record still rolling back
// under the token, not already a hold and not busy with a live holder.
//
// The master record is in place for the whole cascade. A new attempt at the same txid finds it
// and is refused, so it cannot create children or blobs that this call would then remove, and
// the child and blob deletes cannot reach another attempt's records. The hold keeps refusing
// creates after the cascade, and the spend gates, which key on the creating bin, keep refusing
// spends of its outputs. The unminedSince bin stays so the pruner sweep, which selects by it,
// finds an expired hold.
//
// A master that is absent, already a hold, no longer rolling back or owned by another attempt
// means this attempt's rollback is already over: the call reports success and touches nothing. A
// live holder that is reversing spends under TakeRollbackHold makes the conversion fail, because the
// conversion would drop the busy marker that protects it. A holder that crashed, whose holdUntil has
// passed by a further utxostore_spendRollbackTimeout, does not: the conversion proceeds over its
// marker. After a refusal the record is left rolling back and the sweep ends the rollback on a
// later pass. A cascade that fails leaves the master
// rolling back for the same reason, so a retry finds the children and blobs by the master's
// child count. A conversion that fails after the blob is deleted leaves the master rolling back
// and reversed, and the sweep calls this method again for it without reading the transaction.
func (s *Store) EndCreateFirstRollback(ctx context.Context, hash *chainhash.Hash, attempt []byte) error {
	key, keyErr := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if keyErr != nil {
		return errors.NewProcessingError("error in aerospike NewKey", keyErr)
	}

	state, readErr := s.readCreatingState(key)
	if readErr != nil {
		return errors.NewStorageError("error reading master record to end the rollback", readErr)
	}

	if state.absent || state.hold || !state.rollingBack || !bytes.Equal(state.attempt, attempt) {
		return nil
	}

	proceed, markErr := s.markReversed(key, hash, attempt)
	if markErr != nil {
		return markErr
	}

	if !proceed {
		return nil
	}

	if state.childCount > 0 {
		if childErr := s.deleteChildRecordsWithRetry(ctx, hash, state.childCount, attemptFilter(attempt)); childErr != nil {
			return errors.NewStorageError("ending the rollback of %s: the pagination child pass did not complete; the master record is left rolling back so a retry can find the %d child record(s)", hash.String(), state.childCount, childErr)
		}
	}

	if blobErr := s.deleteExternalBlobs(ctx, hash); blobErr != nil {
		return blobErr
	}

	return s.convertToHold(key, hash, state, attempt)
}

// markReversed records on the master that every spend of the rolling back attempt has been
// reversed, so a recovery that finds the record after the blob is gone can finish the rollback
// without the transaction data. One UPDATE_ONLY write, filtered on the record rolling back under
// the token and not a hold. A record that is absent, a hold, no longer rolling back or owned by
// another attempt means the rollback is already over: it reports false and is not an error.
func (s *Store) markReversed(key *aerospike.Key, hash *chainhash.Hash, attempt []byte) (bool, error) {
	writePolicy := util.GetAerospikeWritePolicy(s.settings, 0)
	writePolicy.RecordExistsAction = aerospike.UPDATE_ONLY
	writePolicy.FilterExpression = aerospike.ExpAnd(
		aerospike.ExpBinExists(fields.RollingBack.String()),
		aerospike.ExpNot(aerospike.ExpBinExists(fields.Hold.String())),
		attemptFilter(attempt),
	)

	_, putErr := s.client.Operate(writePolicy, key, aerospike.PutOp(aerospike.NewBin(fields.Reversed.String(), true)))
	if putErr == nil {
		return true, nil
	}

	if errors.Is(putErr, aerospike.ErrKeyNotFound) {
		return false, nil
	}

	if !isFilteredOut(putErr) {
		prometheusUtxoMapErrors.WithLabelValues("EndCreateFirstRollback", "error").Inc()

		return false, errors.NewStorageError("error marking %s as reversed", hash.String(), putErr)
	}

	after, getErr := s.readCreatingState(key)
	if getErr != nil {
		return false, errors.NewStorageError("error reading record after the reversed marker was filtered out", getErr)
	}

	if after.absent || after.hold || !after.rollingBack || !bytes.Equal(after.attempt, attempt) {
		return false, nil
	}

	return false, errors.NewStorageError("reversed marker of %s was filtered out but the record is still rolling back", hash.String())
}

// convertToHold rewrites the master record of a rolled back attempt as its hold.
func (s *Store) convertToHold(key *aerospike.Key, hash *chainhash.Hash, state creatingState, attempt []byte) error {
	writePolicy := util.GetAerospikeWritePolicy(s.settings, 0)
	writePolicy.RecordExistsAction = aerospike.REPLACE_ONLY
	writePolicy.FilterExpression = aerospike.ExpAnd(
		aerospike.ExpBinExists(fields.RollingBack.String()),
		aerospike.ExpNot(aerospike.ExpBinExists(fields.Hold.String())),
		s.notBusyOrStale(time.Now().Unix()),
		attemptFilter(attempt),
	)

	bins := []*aerospike.Bin{
		aerospike.NewBin(fields.TxID.String(), aerospike.NewBytesValue(hash[:])),
		aerospike.NewBin(fields.Creating.String(), true),
		aerospike.NewBin(fields.RollingBack.String(), true),
		aerospike.NewBin(fields.Hold.String(), true),
		aerospike.NewBin(fields.HoldUntil.String(), int(time.Now().Unix()+s.holdSeconds())),
	}

	if len(attempt) > 0 {
		bins = append(bins, aerospike.NewBin(fields.Attempt.String(), attempt))
	}

	if state.hasUnminedSince {
		bins = append(bins, aerospike.NewBin(fields.UnminedSince.String(), state.unminedSince))
	}

	putErr := s.client.PutBins(writePolicy, key, bins...)
	if putErr == nil {
		return nil
	}

	if errors.Is(putErr, aerospike.ErrKeyNotFound) {
		return nil
	}

	if !isFilteredOut(putErr) {
		prometheusUtxoMapErrors.WithLabelValues("EndCreateFirstRollback", "error").Inc()

		return errors.NewStorageError("error turning the rolled back record %s into a hold", hash.String(), putErr)
	}

	after, getErr := s.readCreatingState(key)
	if getErr != nil {
		return errors.NewStorageError("error reading record after the hold conversion was filtered out", getErr)
	}

	switch {
	case after.absent, after.hold, !after.rollingBack, !bytes.Equal(after.attempt, attempt):
		return nil
	case after.busy && time.Now().Unix() < after.holdUntil+s.holdSeconds():
		return errors.NewProcessingError("ending the rollback of %s: a holder is reversing spends; the record is left rolling back for the sweep", hash.String())
	}

	return errors.NewStorageError("hold conversion of %s was filtered out but the record is still rolling back", hash.String())
}

// notBusyOrStale selects a record whose busy bin is absent, or whose holder has crashed: every
// take renews holdUntil, so a busy bin that holdUntil has passed by a further
// utxostore_spendRollbackTimeout is not a live holder's.
func (s *Store) notBusyOrStale(now int64) *aerospike.Expression {
	return aerospike.ExpOr(
		aerospike.ExpNot(aerospike.ExpBinExists(fields.Busy.String())),
		aerospike.ExpAnd(
			aerospike.ExpBinExists(fields.HoldUntil.String()),
			aerospike.ExpLessEq(aerospike.ExpIntBin(fields.HoldUntil.String()), aerospike.ExpIntVal(now-s.holdSeconds())),
		),
	)
}

// TakeRollbackHold reports whether the caller may reverse the spends of the inputs of the
// transaction whose attempt carries the token. One UPDATE_ONLY write, filtered on the record
// rolling back, carrying the token and not busy, or busy with a holder that crashed (holdUntil
// passed by a further utxostore_spendRollbackTimeout), marks the record busy and renews holdUntil
// to now plus utxostore_spendRollbackTimeout, so a live holder is never taken over. A filtered-out write or an absent record reports
// false and writes nothing: the record is gone, a later attempt owns the txid, or another actor
// is reversing under the hold.
func (s *Store) TakeRollbackHold(_ context.Context, hash *chainhash.Hash, attempt []byte) (bool, error) {
	key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		return false, errors.NewProcessingError("error in aerospike NewKey", err)
	}

	writePolicy := util.GetAerospikeWritePolicy(s.settings, 0)
	writePolicy.RecordExistsAction = aerospike.UPDATE_ONLY
	writePolicy.FilterExpression = aerospike.ExpAnd(
		aerospike.ExpBinExists(fields.RollingBack.String()),
		s.notBusyOrStale(time.Now().Unix()),
		attemptFilter(attempt),
	)

	_, putErr := s.client.Operate(writePolicy, key,
		aerospike.PutOp(aerospike.NewBin(fields.Busy.String(), true)),
		aerospike.PutOp(aerospike.NewBin(fields.HoldUntil.String(), int(time.Now().Unix()+s.holdSeconds()))),
	)
	if putErr == nil {
		return true, nil
	}

	if errors.Is(putErr, aerospike.ErrKeyNotFound) || isFilteredOut(putErr) {
		return false, nil
	}

	prometheusUtxoMapErrors.WithLabelValues("TakeRollbackHold", "error").Inc()

	return false, errors.NewStorageError("error taking the hold of %s", hash.String(), putErr)
}

// ReleaseRollbackHold clears the busy bin that TakeRollbackHold set. The write is filtered on the
// token and on the busy bin, so a record that is absent, belongs to another attempt or is not
// busy is left alone and the call reports success. A busy bin that is never cleared, because the
// holder crashed, ages out: the hold is cleared once holdUntil has passed by a further
// utxostore_spendRollbackTimeout.
func (s *Store) ReleaseRollbackHold(_ context.Context, hash *chainhash.Hash, attempt []byte) error {
	key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		return errors.NewProcessingError("error in aerospike NewKey", err)
	}

	writePolicy := util.GetAerospikeWritePolicy(s.settings, 0)
	writePolicy.RecordExistsAction = aerospike.UPDATE_ONLY
	writePolicy.FilterExpression = aerospike.ExpAnd(
		aerospike.ExpBinExists(fields.Busy.String()),
		attemptFilter(attempt),
	)

	_, putErr := s.client.Operate(writePolicy, key, aerospike.PutOp(aerospike.NewBin(fields.Busy.String(), nil)))
	if putErr == nil || errors.Is(putErr, aerospike.ErrKeyNotFound) || isFilteredOut(putErr) {
		return nil
	}

	prometheusUtxoMapErrors.WithLabelValues("ReleaseRollbackHold", "error").Inc()

	return errors.NewStorageError("error releasing the hold of %s", hash.String(), putErr)
}

// ClearCreateFirstHold removes the hold at the txid when it may go. It reports true when the
// record is absent or the hold was removed, and false when a hold stands or the record is not a
// hold.
//
// Without adopt the hold goes only once it has expired: holdUntil has passed and no holder is
// busy, or holdUntil has passed by a further utxostore_spendRollbackTimeout, which is a holder
// that crashed. The delete is filtered on the hold and on the holdUntil value that was read, and
// every take renews holdUntil, so a holder that takes the hold between the read and the delete
// makes the delete fail.
//
// With adopt, the call of a create in block context that must not wait out the hold, the hold
// goes at once unless a live holder is busy. A busy bin that holdUntil has passed by a further
// timeout is a crashed holder's and does not stop it. The delete is filtered on the hold and on the
// busy bin being absent or stale.
//
// A delete that the filter refused reports false: the record changed under the call, and the
// caller retries later.
func (s *Store) ClearCreateFirstHold(_ context.Context, hash *chainhash.Hash, adopt bool) (bool, error) {
	key, keyErr := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if keyErr != nil {
		return false, errors.NewProcessingError("error in aerospike NewKey", keyErr)
	}

	state, readErr := s.readCreatingState(key)
	if readErr != nil {
		return false, errors.NewStorageError("error reading record to clear its hold", readErr)
	}

	if state.absent {
		return true, nil
	}

	if !state.hold {
		return false, nil
	}

	var filter *aerospike.Expression

	now := time.Now().Unix()

	if adopt {
		if state.busy && now < state.holdUntil+s.holdSeconds() {
			return false, nil
		}

		filter = aerospike.ExpAnd(
			aerospike.ExpBinExists(fields.Hold.String()),
			s.notBusyOrStale(now),
		)
	} else {

		expired := now >= state.holdUntil && !state.busy
		crashed := now >= state.holdUntil+s.holdSeconds()

		if !expired && !crashed {
			return false, nil
		}

		filter = aerospike.ExpAnd(
			aerospike.ExpBinExists(fields.Hold.String()),
			aerospike.ExpEq(aerospike.ExpIntBin(fields.HoldUntil.String()), aerospike.ExpIntVal(state.holdUntil)),
		)
	}

	writePolicy := util.GetAerospikeWritePolicy(s.settings, 0)
	writePolicy.FilterExpression = filter

	if _, delErr := s.client.Delete(writePolicy, key); delErr != nil {
		if errors.Is(delErr, aerospike.ErrKeyNotFound) {
			return true, nil
		}

		if isFilteredOut(delErr) {
			return false, nil
		}

		prometheusUtxoMapErrors.WithLabelValues("ClearCreateFirstHold", "error").Inc()

		return false, errors.NewStorageError("error deleting the hold of %s", hash.String(), delErr)
	}

	prometheusUtxoMapDelete.Inc()

	return true, nil
}

// creatingState is what a master record's creating, rolling back and hold bins say.
type creatingState struct {
	attempt         []byte
	holdUntil       int64
	unminedSince    int
	childCount      int
	absent          bool
	creating        bool
	rollingBack     bool
	hold            bool
	busy            bool
	hasUnminedSince bool
}

// creatingStateBins are the bins readCreatingState reads.
var creatingStateBins = []string{
	fields.Creating.String(),
	fields.RollingBack.String(),
	fields.Attempt.String(),
	fields.Hold.String(),
	fields.HoldUntil.String(),
	fields.Busy.String(),
	fields.TotalExtraRecs.String(),
	fields.UnminedSince.String(),
}

// readCreatingState reads the bins that say where a master record stands in a create-first
// attempt. An absent record is reported, not an error.
func (s *Store) readCreatingState(key *aerospike.Key) (creatingState, error) {
	rec, err := s.client.Get(util.GetAerospikeReadPolicy(s.settings), key, creatingStateBins...)
	if err != nil {
		if errors.Is(err, aerospike.ErrKeyNotFound) {
			return creatingState{absent: true}, nil
		}

		return creatingState{}, err
	}

	var state creatingState

	if rec == nil || rec.Bins == nil {
		return state, nil
	}

	_, state.creating = rec.Bins[fields.Creating.String()]

	state.rollingBack, _ = rec.Bins[fields.RollingBack.String()].(bool)
	state.hold, _ = rec.Bins[fields.Hold.String()].(bool)
	state.busy, _ = rec.Bins[fields.Busy.String()].(bool)
	state.attempt, _ = rec.Bins[fields.Attempt.String()].([]byte)

	if v, ok := rec.Bins[fields.HoldUntil.String()].(int); ok {
		state.holdUntil = int64(v)
	}

	if v, ok := rec.Bins[fields.TotalExtraRecs.String()].(int); ok {
		state.childCount = v
	}

	if v, ok := rec.Bins[fields.UnminedSince.String()].(int); ok {
		state.unminedSince = v
		state.hasUnminedSince = true
	}

	return state, nil
}
