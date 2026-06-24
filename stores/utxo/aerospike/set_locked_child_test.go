package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// These tests cover setLockedBatch's child/extra-record path — the inline
// "build children, issue one direct batchOperate, aggregate per item" logic
// (locked.go) that replaced the old same-pool lockedBatcher recursion (issue
// #1033, fixed by #928). The integration regression test
// (TestClose_DrainsQueuedSetLockedMultiRecord) needs a live Aerospike and is
// skipped in short mode; these drive the same branch hermetically through the
// batchOperateFn seam.
//
// newTestStoreForGet leaves lockedBatcher nil, so any re-entry into the batcher
// would panic — every require.NotPanics here is also a guard against the
// recursion being reintroduced.

// writeLuaOK populates rec with a successful setLocked Lua response. A non-zero
// childCount makes the master record report that many child/pagination records.
func writeLuaOK(rec aerospike.BatchRecordIfc, childCount int) {
	m := map[interface{}]interface{}{"status": string(LuaStatusOK)}
	if childCount > 0 {
		m["childCount"] = childCount
	}

	rec.BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{LuaSuccess.String(): m}}
}

// writeLuaErr populates rec with a failed setLocked Lua response.
func writeLuaErr(rec aerospike.BatchRecordIfc, msg string) {
	m := map[interface{}]interface{}{"status": "ERROR", "message": msg}
	rec.BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{LuaSuccess.String(): m}}
}

func newLockedItem(b byte) *batchLocked {
	// Production sizes errCh at cap 1; here we use cap 2 so that a regression
	// which double-signals an item (production sends via the non-blocking
	// trySignal) is not silently dropped on the second send — it lands in the
	// buffer where recvOnce's "exactly once" check can observe it.
	return &batchLocked{ctx: context.Background(), txHash: chainhash.Hash{b}, setValue: true, errCh: make(chan error, 2)}
}

// recvOnce returns the value delivered on a completion channel and asserts the
// item was signalled exactly once. The channel is sized cap 2 (see
// newLockedItem) so a spurious second trySignal is buffered rather than dropped,
// making double-signalling observable here.
func recvOnce(t *testing.T, ch chan error) error {
	t.Helper()

	var e error
	select {
	case e = <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("errCh was not signalled")
	}

	select {
	case <-ch:
		t.Fatal("errCh signalled more than once")
	default:
	}

	return e
}

// TestSetLockedBatch_MultiRecordSuccess: a master reporting childCount=N drives a
// second batchOperate carrying exactly N child records keyed for indexes 1..N,
// and the item completes with nil.
func TestSetLockedBatch_MultiRecordSuccess(t *testing.T) {
	s := newTestStoreForGet(t)

	const childCount = 3

	var (
		calls     int
		childPass []aerospike.BatchRecordIfc
	)

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, recs []aerospike.BatchRecordIfc) aerospike.Error {
		calls++

		if calls == 1 { // master pass
			for _, r := range recs {
				writeLuaOK(r, childCount)
			}

			return nil
		}

		// child pass
		childPass = recs
		for _, r := range recs {
			writeLuaOK(r, 0)
		}

		return nil
	}

	item := newLockedItem(1)
	require.NotPanics(t, func() { s.setLockedBatch([]*batchLocked{item}) })

	require.Equal(t, 2, calls, "expected one master and one child batchOperate")
	require.Len(t, childPass, childCount, "child pass must carry exactly childCount records")

	// Child records must be keyed for indexes 1..childCount of this tx, in order.
	for i := 1; i <= childCount; i++ {
		ks := uaerospike.CalculateKeySourceInternal(&item.txHash, uint32(i))
		want, err := aerospike.NewKey(s.namespace, s.setName, ks)
		require.NoError(t, err)
		require.True(t, want.Equals(childPass[i-1].BatchRec().Key), "child record %d has wrong key", i)
	}

	require.NoError(t, recvOnce(t, item.errCh))
}

// TestSetLockedBatch_MixedChildCounts: single-record items complete from the
// master pass without contributing children; only paginated items add child
// records. Every item is signalled exactly once.
func TestSetLockedBatch_MixedChildCounts(t *testing.T) {
	s := newTestStoreForGet(t)

	items := []*batchLocked{newLockedItem(1), newLockedItem(2), newLockedItem(3)}
	counts := []int{0, 2, 0} // only items[1] paginates

	var (
		calls     int
		childPass []aerospike.BatchRecordIfc
	)

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, recs []aerospike.BatchRecordIfc) aerospike.Error {
		calls++

		if calls == 1 {
			require.Len(t, recs, len(items), "master pass covers every item")
			for i, r := range recs {
				writeLuaOK(r, counts[i])
			}

			return nil
		}

		childPass = recs
		for _, r := range recs {
			writeLuaOK(r, 0)
		}

		return nil
	}

	require.NotPanics(t, func() { s.setLockedBatch(items) })

	require.Equal(t, 2, calls, "child pass runs because one item paginates")
	require.Len(t, childPass, 2, "only the paginated item contributes children")

	for _, it := range items {
		require.NoError(t, recvOnce(t, it.errCh))
	}
}

// TestSetLockedBatch_ChildBatchError: when the child batchOperate fails, only the
// child-bearing items report the error; single-record items already completed nil.
func TestSetLockedBatch_ChildBatchError(t *testing.T) {
	s := newTestStoreForGet(t)

	single := newLockedItem(1) // childCount 0 — completes in master pass
	paged := newLockedItem(2)  // childCount 2 — deferred to child pass

	var calls int

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, recs []aerospike.BatchRecordIfc) aerospike.Error {
		calls++

		if calls == 1 {
			writeLuaOK(recs[0], 0)
			writeLuaOK(recs[1], 2)

			return nil
		}

		return &aerospike.AerospikeError{ResultCode: types.TIMEOUT} // child batch fails
	}

	require.NotPanics(t, func() { s.setLockedBatch([]*batchLocked{single, paged}) })

	require.Equal(t, 2, calls)
	require.NoError(t, recvOnce(t, single.errCh), "single-record item is unaffected by child-batch failure")

	err := recvOnce(t, paged.errCh)
	require.Error(t, err, "paginated item must surface the child-batch write error")
	require.Contains(t, err.Error(), "could not batch write locked child records")
}

// TestSetLockedBatch_ChildRecordFailure: a per-record failure in the child pass
// fails only its owning item; a sibling whose children all succeed completes nil.
func TestSetLockedBatch_ChildRecordFailure(t *testing.T) {
	s := newTestStoreForGet(t)

	bad := newLockedItem(1)  // first in batch; one of its children returns ERROR
	good := newLockedItem(2) // children all OK

	var calls int

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, recs []aerospike.BatchRecordIfc) aerospike.Error {
		calls++

		if calls == 1 {
			writeLuaOK(recs[0], 2) // bad: 2 children
			writeLuaOK(recs[1], 1) // good: 1 child

			return nil
		}

		// child pass order: bad's 2 children, then good's 1 child.
		writeLuaErr(recs[0], "child boom")
		writeLuaOK(recs[1], 0)
		writeLuaOK(recs[2], 0)

		return nil
	}

	require.NotPanics(t, func() { s.setLockedBatch([]*batchLocked{bad, good}) })

	require.Equal(t, 2, calls)

	err := recvOnce(t, bad.errCh)
	require.Error(t, err, "item with a failing child record must surface the error")
	require.Contains(t, err.Error(), "error from setLocked child")

	require.NoError(t, recvOnce(t, good.errCh), "sibling with all children OK is unaffected")
}
