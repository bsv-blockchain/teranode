package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// mkSpendItem builds a batchSpend with a minimal, valid *utxo.Spend so that
// error-path logging (which reads spend.TxID/Vout) never nil-derefs.
func mkSpendItem(tag byte, group *completion.Group) *batchSpend {
	txID := chainhash.HashH([]byte{tag, 't', 'x'})
	uh := chainhash.HashH([]byte{tag, 'u'})

	return &batchSpend{
		spend: &utxo.Spend{TxID: &txID, Vout: uint32(tag), UTXOHash: &uh},
		group: group,
	}
}

// TestBatchSpendComplete_PublishesAndNoClobber pins the two invariants the P0
// abort-race fix relies on:
//  1. complete() writes spend.Err, then sets published — so an abort-path
//     reader gating on published==true has a happens-before edge to the slot.
//  2. A second complete() (as the whole-batch panic sweep issues for every
//     item, including ones already completed) is a no-op: it must NOT clobber
//     the already-written slot and must NOT double-Done the group.
func TestBatchSpendComplete_PublishesAndNoClobber(t *testing.T) {
	group := completion.NewGroup(1)
	item := mkSpendItem(1, group)

	first := errors.NewProcessingError("first")
	item.complete(first)

	require.True(t, item.completed.Load(), "completed must be set")
	require.True(t, item.published.Load(), "published must be set after the slot write")
	require.Equal(t, first, item.spend.Err)

	// Second call models the panic sweep re-completing an already-done item.
	item.complete(errors.NewProcessingError("second (panic sweep)"))
	require.Equal(t, first, item.spend.Err, "panic-sweep re-complete must not clobber the slot")

	// Done was called exactly once, so the group is satisfied and Wait is nil.
	require.NoError(t, group.Wait(context.Background(), time.Second))
}

// TestResolveSpendCompletions_OnlyReadsPublished verifies the abort path
// (onlyCompleted=true) reads a slot only once its item is published, and skips
// still-in-flight (unpublished) items entirely — the read gate that keeps
// resolveSpendCompletions from racing the dispatcher's slot write.
func TestResolveSpendCompletions_OnlyReadsPublished(t *testing.T) {
	s := newTestStoreForGet(t)

	// published + success (Err nil) -> should be counted as a completed spend.
	a := mkSpendItem(1, nil)
	a.completed.Store(true)
	a.published.Store(true)

	// published + failure -> not a successful spend.
	b := mkSpendItem(2, nil)
	b.spend.Err = errors.NewProcessingError("spend failed")
	b.completed.Store(true)
	b.published.Store(true)

	// NOT published (dispatcher still in-flight). Its Err is the nil zero-value;
	// if the gate wrongly read it, it would be miscounted as a successful spend.
	c := mkSpendItem(3, nil)

	res := s.resolveSpendCompletions(context.Background(), bt.NewTx(), []*batchSpend{a, b, c}, true)

	require.Len(t, res.spentSpends, 1, "only the published successful spend must be counted; the unpublished item must be skipped")
	require.Same(t, a.spend, res.spentSpends[0])
}
