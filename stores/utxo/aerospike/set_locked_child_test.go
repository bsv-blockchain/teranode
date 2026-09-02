package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-batcher/v2/completion"
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

func newLockedItem(b byte, group *completion.Group) *batchLocked {
	return &batchLocked{ctx: context.Background(), txHash: chainhash.Hash{b}, setValue: true, group: group}
}

// requireBatchCompleted waits once for every item in the batch (via the
// shared group, sized to len(items) by the caller) and asserts it did so
// without timing out. Unlike the old per-item errCh/recvOnce check, this
// cannot directly observe a double-complete() call — complete is CAS-guarded
// and silently absorbs a second call by design (the production-safe
// equivalent of the previous trySignal-on-a-full-buffer no-op). What it does
// still catch: an item never being completed (Wait times out), or an
// over-decrement of the shared counter (Group.Done panics on more calls than
// its count — see completion.Group), both of which would fail this call.
func requireBatchCompleted(t *testing.T, group *completion.Group) {
	t.Helper()
	require.NoError(t, group.Wait(context.Background(), 2*time.Second), "not every item in the batch completed")
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

	group := completion.NewGroup(1)
	item := newLockedItem(1, group)
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

	requireBatchCompleted(t, group)
	require.NoError(t, item.result)
}

// TestSetLockedBatch_MixedChildCounts: single-record items complete from the
// master pass without contributing children; only paginated items add child
// records. Every item is completed exactly once.
func TestSetLockedBatch_MixedChildCounts(t *testing.T) {
	s := newTestStoreForGet(t)

	group := completion.NewGroup(3)
	items := []*batchLocked{newLockedItem(1, group), newLockedItem(2, group), newLockedItem(3, group)}
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

	requireBatchCompleted(t, group)
	for _, it := range items {
		require.NoError(t, it.result)
	}
}

// TestSetLockedBatch_ChildBatchError: when the child batchOperate fails, only the
// child-bearing items report the error; single-record items already completed nil.
func TestSetLockedBatch_ChildBatchError(t *testing.T) {
	s := newTestStoreForGet(t)

	group := completion.NewGroup(2)
	single := newLockedItem(1, group) // childCount 0 — completes in master pass
	paged := newLockedItem(2, group)  // childCount 2 — deferred to child pass

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
	requireBatchCompleted(t, group)

	require.NoError(t, single.result, "single-record item is unaffected by child-batch failure")

	require.Error(t, paged.result, "paginated item must surface the child-batch write error")
	require.Contains(t, paged.result.Error(), "could not batch write locked child records")
}

// TestSetLockedBatch_ChildRecordFailure: a per-record failure in the child pass
// fails only its owning item; a sibling whose children all succeed completes nil.
func TestSetLockedBatch_ChildRecordFailure(t *testing.T) {
	s := newTestStoreForGet(t)

	group := completion.NewGroup(2)
	bad := newLockedItem(1, group)  // first in batch; one of its children returns ERROR
	good := newLockedItem(2, group) // children all OK

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
	requireBatchCompleted(t, group)

	require.Error(t, bad.result, "item with a failing child record must surface the error")
	require.Contains(t, bad.result.Error(), "error from setLocked child")

	require.NoError(t, good.result, "sibling with all children OK is unaffected")
}
