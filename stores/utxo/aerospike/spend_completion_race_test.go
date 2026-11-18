package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
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

// TestAbortPathNeverReversesAnIdempotentMatchWhileAnInputIsUnanswered: tx C
// spends A:0 (a marker, still in flight when the wait aborts), B:0 (already
// records C, so an idempotent match: the confirmed spend) and D:0 (fresh), and
// E:0 answers ErrSpent so a rollback is warranted. The abort path used to decide
// "historical" by reading every slot, including A's in-flight one, which reads
// Err==nil. It then reversed B:0 and handed a confirmed output to anyone. An
// unanswered input must count as a possible marker hit. Reproduced by review.
func TestAbortPathNeverReversesAnIdempotentMatchWhileAnInputIsUnanswered(t *testing.T) {
	s := newTestStoreForGet(t)

	a := mkSpendItem(1, nil) // in flight: not published

	b := mkSpendItem(2, nil)
	b.idempotent = true
	b.completed.Store(true)
	b.published.Store(true)

	d := mkSpendItem(3, nil)
	d.completed.Store(true)
	d.published.Store(true)

	e := mkSpendItem(4, nil)
	e.spend.Err = errors.NewUtxoSpentError(*e.spend.TxID, e.spend.Vout, *e.spend.UTXOHash, spendpkg.NewSpendingData(e.spend.TxID, 0))
	e.completed.Store(true)
	e.published.Store(true)

	res := s.resolveSpendCompletions(context.Background(), bt.NewTx(), []*batchSpend{a, b, d, e}, true)

	require.True(t, res.rollbackNeeded, "fixture: E's ErrSpent warrants a rollback")
	require.Equal(t, 1, res.unresolved, "A is still in flight")
	require.False(t, res.prunedRejection, "no resolved input hit a marker")

	rollback := res.rollbackSet()
	require.Equal(t, []*utxo.Spend{d.spend}, rollback, "only the fresh write is reversed; B:0's confirmed spend stays")

	// Once every input is answered and none hit a marker, the idempotent match
	// is reversible again: it may be an orphan of an earlier failed attempt, and
	// a call whose only successful input was that match is still healed.
	a.completed.Store(true)
	a.published.Store(true)
	a.spend.Err = errors.NewUtxoSpentError(*a.spend.TxID, a.spend.Vout, *a.spend.UTXOHash, spendpkg.NewSpendingData(a.spend.TxID, 0))

	res = s.resolveSpendCompletions(context.Background(), bt.NewTx(), []*batchSpend{a, b, e}, true)
	require.Zero(t, res.unresolved)
	require.Equal(t, []*utxo.Spend{b.spend}, res.rollbackSet())

	// And a resolved marker hit makes it historical.
	a.spend.Err = errors.NewUtxoSpendingTxPrunedError("pruned")
	res = s.resolveSpendCompletions(context.Background(), bt.NewTx(), []*batchSpend{a, b, e}, true)
	require.True(t, res.prunedRejection)
	require.Empty(t, res.rollbackSet())
}

// TestRollbackSet_RequireParentOutputsKeepsIdempotentMatches: on the second
// wave of DefaultSpendAndCreateMulti (RequireParentOutputs) an idempotent match
// is the first wave's own write or a confirmed spend, never an orphan to heal,
// so a failed call reverses only what it wrote.
func TestRollbackSet_RequireParentOutputsKeepsIdempotentMatches(t *testing.T) {
	written := mkSpendItem(1, nil).spend
	match := mkSpendItem(2, nil).spend

	res := &spendCompletionResult{spentSpends: []*utxo.Spend{written}, idempotentSpends: []*utxo.Spend{match}, requireParentOutput: true}
	require.Equal(t, []*utxo.Spend{written}, res.rollbackSet())

	res.requireParentOutput = false
	require.Equal(t, []*utxo.Spend{written, match}, res.rollbackSet(), "control: without it the match is reversible")
}

// TestResolveSpendCompletions_CarriesRequireParentOutputs: the call-level flag
// comes from the items, so the rollback set of a RequireParentOutputs call
// keeps its idempotent matches.
func TestResolveSpendCompletions_CarriesRequireParentOutputs(t *testing.T) {
	s := newTestStoreForGet(t)

	match := mkSpendItem(1, nil)
	match.idempotent = true

	fresh := mkSpendItem(2, nil)

	spent := mkSpendItem(3, nil)
	spent.spend.Err = errors.NewUtxoSpentError(*spent.spend.TxID, spent.spend.Vout, *spent.spend.UTXOHash, spendpkg.NewSpendingData(spent.spend.TxID, 0))

	for _, item := range []*batchSpend{match, fresh, spent} {
		item.requireParentOutput = true
		item.completed.Store(true)
		item.published.Store(true)
	}

	res := s.resolveSpendCompletions(context.Background(), bt.NewTx(), []*batchSpend{match, fresh, spent}, false)
	require.True(t, res.requireParentOutput)
	require.Equal(t, []*utxo.Spend{fresh.spend}, res.rollbackSet(), "the first wave's match stays")
}

// TestResolveSpendCompletions_PrunedReplayIsUnwinnable: a pruned-replay answer
// means this txid can never win, so it lifts the suppression a transient lock
// on a sibling input would otherwise put on the rollback.
func TestResolveSpendCompletions_PrunedReplayIsUnwinnable(t *testing.T) {
	s := newTestStoreForGet(t)

	locked := mkSpendItem(1, nil)
	locked.spend.Err = errors.NewTxLockedError("locked")

	pruned := mkSpendItem(2, nil)
	pruned.spend.Err = errors.NewUtxoSpendingTxPrunedError("pruned")

	for _, item := range []*batchSpend{locked, pruned} {
		item.completed.Store(true)
		item.published.Store(true)
	}

	res := s.resolveSpendCompletions(context.Background(), bt.NewTx(), []*batchSpend{locked, pruned}, false)
	require.True(t, res.sawTransientLock, "fixture: the sibling is locked")
	require.True(t, res.sawUnwinnable, "a pruned replay can never win")
}

// TestProcessSingleBatchResult_FlagsIdempotentBeforePublishing: the rollback
// decision reads an item's idempotent flag once the item is published, so the
// flag must be set before the item is completed. Run under -race, as make test
// does: a flag written after publishing races the reader below.
func TestProcessSingleBatchResult_FlagsIdempotentBeforePublishing(t *testing.T) {
	s := newTestStoreForGet(t)

	group := completion.NewGroup(1)
	item := mkSpendItem(1, group)

	key, err := aerospike.NewKey("test-ns", "test-set", []byte("parent"))
	require.NoError(t, err)

	record := aerospike.NewBatchRead(nil, key, nil)
	record.Record = &aerospike.Record{Key: key, Bins: aerospike.BinMap{
		LuaSuccess.String(): map[interface{}]interface{}{"status": "OK", "idempotent": []interface{}{0}},
	}}

	go s.processSingleBatchResult(context.Background(), record, []aerospike.MapValue{{"idx": 0}}, []*batchSpend{item}, item.spend.TxID, 0, 1)

	require.NoError(t, group.Wait(context.Background(), 5*time.Second))
	require.True(t, item.published.Load())
	require.True(t, item.idempotent, "the flag is set before the item is published")
}
