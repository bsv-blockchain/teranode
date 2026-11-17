package aerospike

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// These are the group-completion-protocol counterparts of the old channel-model
// TestSendIncrementBatch_NeverOrphans / TestSendSetDAHBatch_NeverOrphans in
// batch_dispatch_leak_test.go (which are removed during the integration pass,
// since batchIncrement/batchDAH no longer carry result channels). They drive
// each dispatch fn through its panic / BatchOperate-error / result paths via the
// batchOperateFn seam and assert every batch item is completed exactly once — a
// single group.Wait standing in for "no item was orphaned". The panicOperate /
// errorOperate / okOperate / requireGroupCompleted helpers are shared from
// batch_dispatch_leak_test.go.

func TestSendIncrementBatch_Group_NeverOrphans(t *testing.T) {
	const n = 4

	mk := func(group *completion.Group) []*batchIncrement {
		b := make([]*batchIncrement, n)
		for i := range b {
			h := chainhash.Hash{byte(i + 1)}
			b[i] = &batchIncrement{txID: &h, increment: 1, group: group}
		}

		return b
	}

	// "ok (nil records)" exercises the missing-record branch (the mocked records
	// carry no Record), which must complete with an error rather than fall
	// through and orphan the submitter.
	for name, fn := range map[string]func() func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error{
		"panic": panicOperate, "batchOperate error": errorOperate, "ok (nil records)": okOperate,
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestStoreForGet(t)
			s.batchOperateFn = fn()

			group := completion.NewGroup(n)
			b := mk(group)

			require.NotPanics(t, func() { s.sendIncrementBatch(b) })
			requireGroupCompleted(t, group, 2*time.Second)

			for i, it := range b {
				require.True(t, it.completed.Load(), "increment item %d was not completed", i)
			}
		})
	}
}

func TestSendSetDAHBatch_Group_NeverOrphans(t *testing.T) {
	const n = 4

	mk := func(group *completion.Group) []*batchDAH {
		b := make([]*batchDAH, n)
		for i := range b {
			h := chainhash.Hash{byte(i + 1)}
			b[i] = &batchDAH{txID: &h, childIdx: uint32(i + 1), deleteAtHeight: 100, group: group}
		}

		return b
	}

	for name, fn := range map[string]func() func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error{
		"panic": panicOperate, "batchOperate error": errorOperate, "ok (nil records)": okOperate,
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestStoreForGet(t)
			s.batchOperateFn = fn()

			group := completion.NewGroup(n)
			b := mk(group)

			require.NotPanics(t, func() { s.sendSetDAHBatch(b) })
			requireGroupCompleted(t, group, 2*time.Second)

			for i, it := range b {
				require.True(t, it.completed.Load(), "setDAH item %d was not completed", i)
			}
		})
	}
}

// TestSendSetDAHBatch_FireAndForget_NilGroup verifies a fire-and-forget item
// (group == nil — the counter-drift master-DAH clear in handleExtraRecords)
// flows through the dispatcher without panicking and is marked completed, while
// never decrementing a group (there is none to Done). It is mixed into a batch
// with waited items to prove the two coexist: the waited items alone must drive
// the shared group to completion.
func TestSendSetDAHBatch_FireAndForget_NilGroup(t *testing.T) {
	const waited = 3

	for name, fn := range map[string]func() func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error{
		"panic": panicOperate, "batchOperate error": errorOperate, "ok (nil records)": okOperate,
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestStoreForGet(t)
			s.batchOperateFn = fn()

			group := completion.NewGroup(waited)

			b := make([]*batchDAH, 0, waited+1)

			// One fire-and-forget item with a nil group (master record clear).
			h0 := chainhash.Hash{0xFF}
			fireAndForget := &batchDAH{txID: &h0, childIdx: 0, deleteAtHeight: 0, group: nil}
			b = append(b, fireAndForget)

			for i := 0; i < waited; i++ {
				h := chainhash.Hash{byte(i + 1)}
				b = append(b, &batchDAH{txID: &h, childIdx: uint32(i + 1), deleteAtHeight: 100, group: group})
			}

			require.NotPanics(t, func() { s.sendSetDAHBatch(b) })

			// The waited items alone must complete the group; the fire-and-forget
			// item must NOT have decremented it (nil group → Done skipped).
			requireGroupCompleted(t, group, 2*time.Second)

			require.True(t, fireAndForget.completed.Load(), "fire-and-forget item was not completed")
		})
	}
}
