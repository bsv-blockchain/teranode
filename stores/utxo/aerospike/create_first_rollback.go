package aerospike

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
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

	return max(int64(d/time.Second), 1)
}

// spendLeaseUntil is the time by which a spend that starts now under a lease must end.
func (s *Store) spendLeaseUntil() int {
	return int(time.Now().Unix() + s.holdSeconds())
}

// spendLeaseOp returns the write that puts the lease of the attempt that creates a master record
// into its spenders bin. The creator's lease is its attempt token.
func (s *Store) spendLeaseOp(lease []byte) *aerospike.Operation {
	return aerospike.PutOp(aerospike.NewBin(fields.Spenders.String(), map[interface{}]interface{}{
		hex.EncodeToString(lease): s.spendLeaseUntil(),
	}))
}

// TakeSpendLease registers the caller as a spender of the inputs of the attempt that carries the
// token, before the caller spends. One UPDATE_ONLY write, filtered on the record still creating,
// not rolling back and carrying the token, adds a random lease to the spenders bin with the time
// by which the spend must end, now plus utxostore_spendRollbackTimeout. A rollback sets its marker
// with a write that is refused while a lease is live, and this write is refused once the marker is
// set. A spend therefore either starts before the marker, and the rollback waits to begin until
// the lease is released or stale, or is refused here and never starts.
//
// Lease times are the unix clocks of the hosts that write and read them. A lease is treated as
// live for a further utxostore_spendRollbackTimeout past its time, so the hosts of one store must
// agree on the time to within that setting. The same holds for the busy marker.
//
// It reports false, writing nothing, when the record is absent, final, rolling back or owned by
// another attempt.
func (s *Store) TakeSpendLease(_ context.Context, hash *chainhash.Hash, attempt []byte) ([]byte, bool, error) {
	key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		return nil, false, errors.NewProcessingError("error in aerospike NewKey", err)
	}

	lease := make([]byte, attemptTokenLength)
	if _, randErr := rand.Read(lease); randErr != nil {
		return nil, false, errors.NewProcessingError("failed to generate the spend lease", randErr)
	}

	writePolicy := util.GetAerospikeWritePolicy(s.settings, 0)
	writePolicy.RecordExistsAction = aerospike.UPDATE_ONLY
	writePolicy.FilterExpression = finalizeFilter(attempt)

	// The second write drops the leases that are stale, so that spenders that never released theirs
	// do not make the bin grow.
	_, putErr := s.client.Operate(writePolicy, key,
		aerospike.MapPutOp(aerospike.DefaultMapPolicy(), fields.Spenders.String(), hex.EncodeToString(lease), s.spendLeaseUntil()),
		aerospike.MapRemoveByValueRangeOp(fields.Spenders.String(), nil, int(time.Now().Unix()-s.holdSeconds()), aerospike.MapReturnType.NONE),
	)
	if putErr == nil {
		return lease, true, nil
	}

	if errors.Is(putErr, aerospike.ErrKeyNotFound) || isFilteredOut(putErr) {
		return nil, false, nil
	}

	prometheusUtxoMapErrors.WithLabelValues("TakeSpendLease", "error").Inc()

	return nil, false, errors.NewStorageError("error taking a spend lease on %s", hash.String(), putErr)
}

// ReleaseSpendLease removes a lease from the spenders bin of the attempt that carries the token.
// The caller calls it once its spend has returned with a known outcome. A record that is absent,
// final, a hold or owned by another attempt carries no such lease and the call reports success.
func (s *Store) ReleaseSpendLease(_ context.Context, hash *chainhash.Hash, attempt, lease []byte) error {
	key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		return errors.NewProcessingError("error in aerospike NewKey", err)
	}

	writePolicy := util.GetAerospikeWritePolicy(s.settings, 0)
	writePolicy.RecordExistsAction = aerospike.UPDATE_ONLY
	writePolicy.FilterExpression = aerospike.ExpAnd(
		aerospike.ExpBinExists(fields.Spenders.String()),
		attemptFilter(attempt),
	)

	_, putErr := s.client.Operate(writePolicy, key,
		aerospike.MapRemoveByKeyOp(fields.Spenders.String(), hex.EncodeToString(lease), aerospike.MapReturnType.NONE),
	)
	if putErr == nil || errors.Is(putErr, aerospike.ErrKeyNotFound) || isFilteredOut(putErr) {
		return nil
	}

	prometheusUtxoMapErrors.WithLabelValues("ReleaseSpendLease", "error").Inc()

	return errors.NewStorageError("error releasing a spend lease on %s", hash.String(), putErr)
}

// SpendLeasesLive reports whether a spender of the attempt that carries the token may still be
// spending. A lease is live until the time it names has passed by a further
// utxostore_spendRollbackTimeout: a spender that returned with an unknown outcome, or crashed,
// never releases its lease, and its write can still land for as long as the store may apply it.
// The owner of a rollback calls it after it has set the marker, so no lease can be added while it
// waits. A record that is absent or owned by another attempt has none.
func (s *Store) SpendLeasesLive(_ context.Context, hash *chainhash.Hash, attempt []byte) (bool, error) {
	key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		return false, errors.NewProcessingError("error in aerospike NewKey", err)
	}

	rec, getErr := s.client.Get(util.GetAerospikeReadPolicy(s.settings), key, fields.Attempt.String(), fields.Spenders.String())
	if getErr != nil {
		if errors.Is(getErr, aerospike.ErrKeyNotFound) {
			return false, nil
		}

		return false, errors.NewStorageError("error reading the spend leases of %s", hash.String(), getErr)
	}

	if rec == nil || rec.Bins == nil {
		return false, nil
	}

	if recAttempt, _ := rec.Bins[fields.Attempt.String()].([]byte); !bytes.Equal(recAttempt, attempt) {
		return false, nil
	}

	raw, present := rec.Bins[fields.Spenders.String()]
	if !present || raw == nil {
		return false, nil
	}

	leases, ok := raw.(map[interface{}]interface{})
	if !ok {
		// A bin this code cannot read is not proof that nobody is spending.
		return true, nil
	}

	staleBefore := time.Now().Unix() - s.holdSeconds()

	for _, v := range leases {
		if until, ok := v.(int); !ok || int64(until) >= staleBefore {
			return true, nil
		}
	}

	return false, nil
}

// BeginCreateFirstRollback claims the rollback of a master record that still carries the
// creating bin, by setting the rollingBack bin atomically, so a concurrent finalize or
// roll-forward refuses the record from then on. The same write marks the record busy and sets
// holdUntil to now plus utxostore_spendRollbackTimeout, so the caller that owns the rollback
// holds the hold for its whole reversal of spends: no other actor can take it and reverse the
// same spends, and a block-context create cannot adopt the record while it is busy. The owner
// calls ReleaseRollbackHold once the reversal is done. The write is also filtered on the record
// carrying no live spend lease, and from this write on TakeSpendLease refuses the record. The two
// are single-record writes, so the server orders them: while a spender holds a lease no rollback
// begins, and once a rollback has begun no spender starts. No spend of the attempt can therefore
// land after the owner's reversal, and no rollback can remove the records of an attempt while a
// lease holder is clearing their creating state. A refusal for a live lease returns an error for
// which utxo.IsSpendLeaseLive is true, and the record stays creating. The write is UPDATE_ONLY and filtered on the
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
	writePolicy.FilterExpression = aerospike.ExpAnd(finalizeFilter(attempt), s.noLiveSpendLease(time.Now().Unix()))

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

	return false, utxo.MarkSpendLeaseLive(errors.NewProcessingError("rolling back marker for %s was refused: a spender of the attempt holds a live lease", hash.String()))
}

// noLiveSpendLease selects a record with no live spend lease: no spenders bin, or no lease whose
// time is at or after now minus utxostore_spendRollbackTimeout. The server evaluates it inside the
// write it filters, so a lease cannot be added between the check and the write.
func (s *Store) noLiveSpendLease(now int64) *aerospike.Expression {
	return aerospike.ExpOr(
		aerospike.ExpNot(aerospike.ExpBinExists(fields.Spenders.String())),
		aerospike.ExpEq(
			aerospike.ExpMapGetByValueRange(aerospike.MapReturnType.COUNT, aerospike.ExpIntVal(now-s.holdSeconds()), nil, aerospike.ExpMapBin(fields.Spenders.String())),
			aerospike.ExpIntVal(0),
		),
	)
}

// EndCreateFirstRollback ends the rollback of the attempt that carries the token. Its callers
// call it only once every spend of the attempt is reversed and no spend lease is live. Its first
// write records that on the master as the reversed bin, so that a recovery that finds the record
// after the external blob is gone finishes the rollback without reading the transaction. The same
// write marks the record busy, so the call owns the cleanup: it is filtered on the record not being
// busy with a live holder, and a second caller, or an actor that is still reversing spends under
// TakeRollbackHold, makes it fail. The call then removes the pagination children, filtered on the
// token, then the external blobs, and last turns the master record into a hold: one REPLACE_ONLY
// write that keeps the txid, the creating and rollingBack markers, the token and the unminedSince
// selector, and adds the hold marker and holdUntil, now plus utxostore_spendRollbackTimeout. The
// hold is written with that time to live, so the server removes it on its own.
//
// The master record is in place for the whole cascade and busy under this call. A new attempt at
// the same txid finds it and is refused, so it cannot create children or blobs that this call would
// then remove, and no second cleanup can run beside this one and delete the blobs of an attempt
// that replaced the hold. The call runs under a context bounded by utxostore_spendRollbackTimeout,
// half the time after which another caller may treat its busy marker as a crashed holder's. The hold
// keeps refusing creates after the cascade, and the spend gates, which key on the creating bin, keep
// refusing spends of its outputs. The unminedSince bin stays so the pruner sweep, which selects by
// it, finds a hold on a namespace that does not expire records.
//
// A master that is absent, already a hold, no longer rolling back or owned by another attempt
// means this attempt's rollback is already over: the call reports success and touches nothing. A
// cascade or a conversion that fails releases the busy marker and leaves the master rolling back
// and reversed, so a retry finds the children and blobs by the master's child count, and the sweep
// calls this method again for it without reading the transaction.
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

	cleanupCtx, cancel := context.WithTimeout(ctx, time.Duration(s.holdSeconds())*time.Second)
	defer cancel()

	endErr := s.removeRolledBackData(cleanupCtx, hash, state.childCount, attempt)
	if endErr == nil {
		endErr = s.convertToHold(key, hash, state, attempt)
	}

	if endErr != nil {
		if relErr := s.ReleaseRollbackHold(ctx, hash, attempt); relErr != nil {
			s.logger.Errorf("[EndCreateFirstRollback] releasing the cleanup of %s failed, it expires on its own: %v", hash.String(), relErr)
		}
	}

	return endErr
}

// removeRolledBackData removes the pagination children that carry the token and the external
// blobs of a rolled back attempt. The caller holds the busy marker of the master.
func (s *Store) removeRolledBackData(ctx context.Context, hash *chainhash.Hash, childCount int, attempt []byte) error {
	if childCount > 0 {
		if childErr := s.deleteChildRecordsWithRetry(ctx, hash, childCount, attemptFilter(attempt)); childErr != nil {
			return errors.NewStorageError("ending the rollback of %s: the pagination child pass did not complete; the master record is left rolling back so a retry can find the %d child record(s)", hash.String(), childCount, childErr)
		}
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return errors.NewProcessingError("ending the rollback of %s: out of time before the external blobs were removed", hash.String(), ctxErr)
	}

	return s.deleteExternalBlobs(ctx, hash)
}

// markReversed records on the master that every spend of the rolling back attempt has been
// reversed, so a recovery that finds the record after the blob is gone can finish the rollback
// without the transaction data, and takes the busy marker for the cleanup that follows. One
// UPDATE_ONLY write, filtered on the record rolling back under the token, not a hold and not busy
// with a live holder. A record that is absent, a hold, no longer rolling back or owned by another
// attempt means the rollback is already over: it reports false and is not an error. A record that
// is busy with a live holder is an error: that holder is reversing spends or running the cleanup,
// and the record is left for it or for the sweep.
func (s *Store) markReversed(key *aerospike.Key, hash *chainhash.Hash, attempt []byte) (bool, error) {
	writePolicy := util.GetAerospikeWritePolicy(s.settings, 0)
	writePolicy.RecordExistsAction = aerospike.UPDATE_ONLY
	writePolicy.FilterExpression = aerospike.ExpAnd(
		aerospike.ExpBinExists(fields.RollingBack.String()),
		aerospike.ExpNot(aerospike.ExpBinExists(fields.Hold.String())),
		s.notBusyOrStale(time.Now().Unix()),
		attemptFilter(attempt),
	)

	_, putErr := s.client.Operate(writePolicy, key,
		aerospike.PutOp(aerospike.NewBin(fields.Reversed.String(), true)),
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
		prometheusUtxoMapErrors.WithLabelValues("EndCreateFirstRollback", "error").Inc()

		return false, errors.NewStorageError("error marking %s as reversed", hash.String(), putErr)
	}

	after, getErr := s.readCreatingState(key)
	if getErr != nil {
		return false, errors.NewStorageError("error reading record after the reversed marker was filtered out", getErr)
	}

	switch {
	case after.absent, after.hold, !after.rollingBack, !bytes.Equal(after.attempt, attempt):
		return false, nil
	case after.busy && time.Now().Unix() < after.holdUntil+s.holdSeconds():
		return false, errors.NewProcessingError("ending the rollback of %s: a holder is reversing spends or removing its records; the record is left rolling back for the sweep", hash.String())
	}

	return false, errors.NewStorageError("reversed marker of %s was filtered out but the record is still rolling back", hash.String())
}

// convertToHold rewrites the master record of a rolled back attempt as its hold. The caller holds
// the busy marker that markReversed set, and the rewrite drops it. The hold is written with a time
// to live of utxostore_spendRollbackTimeout. A namespace that forbids a time to live gets the hold
// without one, and the pruner sweep removes it.
func (s *Store) convertToHold(key *aerospike.Key, hash *chainhash.Hash, state creatingState, attempt []byte) error {
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

	putErr := s.client.PutBins(s.holdWritePolicy(attempt, true), key, bins...)
	if putErr != nil && putErr.Matches(types.FAIL_FORBIDDEN) {
		s.logger.Warnf("[EndCreateFirstRollback] the namespace forbids a time to live, the hold of %s is left for the pruner sweep: %v", hash.String(), putErr)

		putErr = s.client.PutBins(s.holdWritePolicy(attempt, false), key, bins...)
	}

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

	if after.absent || after.hold || !after.rollingBack || !bytes.Equal(after.attempt, attempt) {
		return nil
	}

	return errors.NewStorageError("hold conversion of %s was filtered out but the record is still rolling back", hash.String())
}

// holdWritePolicy is the policy of the write that turns a rolled back master into a hold: filtered
// on the record rolling back under the token, reversed and not already a hold, and with the life of
// the hold as its time to live when expire is set.
func (s *Store) holdWritePolicy(attempt []byte, expire bool) *aerospike.WritePolicy {
	writePolicy := util.GetAerospikeWritePolicy(s.settings, 0)
	writePolicy.RecordExistsAction = aerospike.REPLACE_ONLY

	// Set on the policy itself: the helper applies no option when the default policies are in use.
	if expire {
		writePolicy.Expiration = uint32(s.holdSeconds()) // #nosec G115 -- a timeout in seconds
	}

	writePolicy.FilterExpression = aerospike.ExpAnd(
		aerospike.ExpBinExists(fields.RollingBack.String()),
		aerospike.ExpBinExists(fields.Reversed.String()),
		aerospike.ExpNot(aerospike.ExpBinExists(fields.Hold.String())),
		attemptFilter(attempt),
	)

	return writePolicy
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
// to now plus utxostore_spendRollbackTimeout, so a live holder is never taken over. A filtered-out
// write or an absent record reports false and writes nothing: the record is gone, a later attempt
// owns the txid, another actor holds the hold, or the cleanup of the rollback is running.
//
// Two states can be taken. An open rollback that is not yet recorded as reversed is taken by the
// actor that reverses the spends of the attempt. A hold is taken by a spend-first caller that has
// to reverse spends it made itself beside the hold: while it is busy no create can adopt the hold,
// so the caller cannot clear the spends of a replacement. A record that is reversed and not yet a
// hold is refused: its cleanup is running, and its transaction data may be gone, so the reversal
// of the attempt must not be reopened.
func (s *Store) TakeRollbackHold(_ context.Context, hash *chainhash.Hash, attempt []byte) (bool, error) {
	key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		return false, errors.NewProcessingError("error in aerospike NewKey", err)
	}

	// An open rollback first. Its record never expires, and the write leaves that as it is.
	took, takeErr := s.takeHold(key, aerospike.TTLDontUpdate,
		aerospike.ExpNot(aerospike.ExpBinExists(fields.Reversed.String())),
		aerospike.ExpNot(aerospike.ExpBinExists(fields.Hold.String())),
		attemptFilter(attempt),
	)
	if takeErr != nil || took {
		return took, wrapTakeErr(hash, takeErr)
	}

	// Then a hold. The take renews its time to live for twice the bound of the reversal it covers,
	// so the hold cannot expire under the holder.
	took, takeErr = s.takeHold(key, uint32(2*s.holdSeconds()), // #nosec G115 -- a timeout in seconds
		aerospike.ExpBinExists(fields.Hold.String()),
		attemptFilter(attempt),
	)
	if takeErr != nil && takeErr.Matches(types.FAIL_FORBIDDEN) {
		took, takeErr = s.takeHold(key, aerospike.TTLDontUpdate,
			aerospike.ExpBinExists(fields.Hold.String()),
			attemptFilter(attempt),
		)
	}

	return took, wrapTakeErr(hash, takeErr)
}

// takeHold is one take: an UPDATE_ONLY write with the given time to live, filtered on the record
// rolling back, not busy with a live holder, and on the extra conditions. It marks the record busy
// and renews holdUntil. An absent record and a filtered-out write report false.
func (s *Store) takeHold(key *aerospike.Key, expiration uint32, conditions ...*aerospike.Expression) (bool, aerospike.Error) {
	writePolicy := util.GetAerospikeWritePolicy(s.settings, 0)
	writePolicy.RecordExistsAction = aerospike.UPDATE_ONLY
	writePolicy.Expiration = expiration
	writePolicy.FilterExpression = aerospike.ExpAnd(append([]*aerospike.Expression{
		aerospike.ExpBinExists(fields.RollingBack.String()),
		s.notBusyOrStale(time.Now().Unix()),
	}, conditions...)...)

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

	return false, putErr
}

// wrapTakeErr turns the store error of a take into the error TakeRollbackHold returns.
func wrapTakeErr(hash *chainhash.Hash, takeErr aerospike.Error) error {
	if takeErr == nil {
		return nil
	}

	prometheusUtxoMapErrors.WithLabelValues("TakeRollbackHold", "error").Inc()

	return errors.NewStorageError("error taking the hold of %s", hash.String(), takeErr)
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

	// A hold keeps its time to live.
	writePolicy.Expiration = aerospike.TTLDontUpdate

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
