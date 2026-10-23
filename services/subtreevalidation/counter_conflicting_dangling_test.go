package subtreevalidation

// End-to-end coverage, on a real sqlitememory UTXO store, for the parent-depth
// guard that lets checkCounterConflictingOnCurrentChain tolerate a dangling
// spender reference — a parent output spent by a counter whose own record is
// absent (#1214) — instead of hard-erroring and wedging the block, while still
// failing closed when the parent is buried beyond the pruning horizon, where the
// absent counter could instead be a mined-then-pruned double-spend.

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

const testRetention = 288

// buildDanglingRef sets up, in a fresh sqlitememory store: parentTx1 mined at
// parentHeight, its output 0 spent by an ABSENT counter (a double-spend clone of
// tx1 that is spent but never created), and tx1 created as the conflicting winner.
// The store's tip is set to tip. It returns a Server ready for the check.
func buildDanglingRef(ctx context.Context, t *testing.T, name string, parentHeight, tip uint32) *Server {
	t.Helper()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.GlobalBlockHeightRetention = testRetention

	utxoStoreURL, err := url.Parse("sqlitememory:///" + name)
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	s := &Server{utxoStore: utxoStore, settings: tSettings, logger: logger}

	_, _, err = utxoStore.SpendAndCreate(ctx, parentTx1, parentHeight, utxo.WithCreateOnly())
	require.NoError(t, err)

	// Pin the parent's confirmed height on the longest chain so the guard can
	// measure its depth (clears unmined_since and records the block height).
	_, err = utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{parentTx1.TxIDChainHash()}, utxo.MinedBlockInfo{
		BlockID:        1,
		BlockHeight:    parentHeight,
		SubtreeIdx:     0,
		OnLongestChain: true,
	})
	require.NoError(t, err)

	// The counter double-spends parentTx1's output (taking the first-seen slot)
	// but its own record is never created: WithSpendOnly with no matching
	// WithCreateOnly is precisely the spend-first window in SequentialSpendAndCreate
	// that leaves a dangling spender reference behind.
	counter := tx1.Clone()
	counter.Version = 2

	_, _, err = utxoStore.SpendAndCreate(ctx, counter, parentHeight, utxo.WithSpendOnly())
	require.NoError(t, err)

	// Self-check: the dangling reference must actually exist — the counter's record
	// is absent, but the parent still records it as a spender.
	_, gErr := utxoStore.Get(ctx, counter.TxIDChainHash())
	require.Error(t, gErr, "test setup: counter record must be absent (dangling ref)")

	parentMeta, err := utxoStore.Get(ctx, parentTx1.TxIDChainHash(), fields.Utxos)
	require.NoError(t, err)

	var refsCounter bool

	for _, sd := range parentMeta.SpendingDatas {
		if sd != nil && sd.TxID.IsEqual(counter.TxIDChainHash()) {
			refsCounter = true
			break
		}
	}

	require.True(t, refsCounter, "test setup: parent must still reference the absent counter as a spender")

	// tx1 is the winner that arrives in a block and double-spends the same output;
	// it is created Conflicting=true, which is what arms the counter-conflicting check.
	_, _, err = utxoStore.SpendAndCreate(ctx, tx1, tip, utxo.WithConflicting(true), utxo.WithCreateOnly())
	require.NoError(t, err)

	require.NoError(t, utxoStore.SetBlockHeight(tip))

	return s
}

// The field-bug repro end to end: the parent is confirmed 5 blocks below tip (well
// within retention) and the counter record is absent. The check must pass — the
// block is valid, and SVNode-following peers accept it.
func TestCheckCounterConflictingOnCurrentChain_ToleratesDanglingRefUnderRecentParent(t *testing.T) {
	InitPrometheusMetrics()

	ctx := context.Background()
	s := buildDanglingRef(ctx, t, "dangling_tolerate", 1000, 1005)

	err := s.checkCounterConflictingOnCurrentChain(ctx, *tx1.TxIDChainHash(), map[uint32]bool{})

	require.NoError(t, err, "a dangling spender ref under a recent parent must be tolerated, not wedge the block")
}

// The consensus guard end to end: the parent is confirmed far below tip (beyond
// retention), so the absent counter could be a mined-then-pruned spend on the
// active chain. The check must still reject — SVNode would reject such a block.
func TestCheckCounterConflictingOnCurrentChain_FailsClosedOnDanglingRefUnderBuriedParent(t *testing.T) {
	InitPrometheusMetrics()

	ctx := context.Background()
	s := buildDanglingRef(ctx, t, "dangling_failclosed", 10, 1000)

	err := s.checkCounterConflictingOnCurrentChain(ctx, *tx1.TxIDChainHash(), map[uint32]bool{})

	require.Error(t, err, "a dangling spender ref under a parent buried beyond retention must fail closed")
}

// buildMinedCounter is buildDanglingRef with a counter whose record EXISTS and is
// mined on the current chain in block counterBlockID. When absentChild is true the
// counter's output is also spent by a descendant whose own record is absent, so a
// walk of the counter's cone reports a not-found that comes from a descendant, not
// from the counter.
func buildMinedCounter(ctx context.Context, t *testing.T, name string, counterBlockID uint32, absentChild bool) *Server {
	t.Helper()

	const parentHeight, tip = 1000, 1005

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.GlobalBlockHeightRetention = testRetention

	utxoStoreURL, err := url.Parse("sqlitememory:///" + name)
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	s := &Server{utxoStore: utxoStore, settings: tSettings, logger: logger}

	_, _, err = utxoStore.SpendAndCreate(ctx, parentTx1, parentHeight, utxo.WithCreateOnly())
	require.NoError(t, err)

	_, err = utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{parentTx1.TxIDChainHash()}, utxo.MinedBlockInfo{
		BlockID: 1, BlockHeight: parentHeight, SubtreeIdx: 0, OnLongestChain: true,
	})
	require.NoError(t, err)

	counter := tx1.Clone()
	counter.Version = 2

	_, _, err = utxoStore.SpendAndCreate(ctx, counter, parentHeight)
	require.NoError(t, err)

	_, err = utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{counter.TxIDChainHash()}, utxo.MinedBlockInfo{
		BlockID: counterBlockID, BlockHeight: parentHeight + 1, SubtreeIdx: 0, OnLongestChain: true,
	})
	require.NoError(t, err)

	if absentChild {
		child := bt.NewTx()
		require.NoError(t, child.From(counter.TxID(), 0, counter.Outputs[0].LockingScript.String(), counter.Outputs[0].Satoshis))
		require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

		_, _, err = utxoStore.SpendAndCreate(ctx, child, tip, utxo.WithSpendOnly())
		require.NoError(t, err)

		_, gErr := utxoStore.Get(ctx, child.TxIDChainHash())
		require.Error(t, gErr, "test setup: the child record must be absent")
	}

	_, _, err = utxoStore.SpendAndCreate(ctx, tx1, tip, utxo.WithConflicting(true), utxo.WithCreateOnly())
	require.NoError(t, err)

	require.NoError(t, utxoStore.SetBlockHeight(tip))

	return s
}

// The retention these tests are written against. util/test defaults to 10, which
// the guard's tip-lag margin would consume whole, so the builders set the
// production default instead.
func TestDanglingRefTests_RunWithTheRetentionTheyAssume(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.GlobalBlockHeightRetention = testRetention

	require.EqualValues(t, testRetention, tSettings.GetUtxoStoreBlockHeightRetention())
}

// The branch that decides accept or reject: a counter whose record is present and
// mined in a block on our chain makes the winner invalid.
func TestCheckCounterConflictingOnCurrentChain_RejectsCounterMinedOnOurChain(t *testing.T) {
	InitPrometheusMetrics()

	ctx := context.Background()
	s := buildMinedCounter(ctx, t, "mined_counter_rejects", 7, false)

	err := s.checkCounterConflictingOnCurrentChain(ctx, *tx1.TxIDChainHash(), map[uint32]bool{7: true})

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxInvalid), "a counter mined on our chain must invalidate the winner")
}

// The same state with the counter mined in a block that is not on our chain must
// pass, so the rejection above is driven by blockIds and not by the mere presence
// of a mined counter.
func TestCheckCounterConflictingOnCurrentChain_AcceptsCounterMinedOffOurChain(t *testing.T) {
	InitPrometheusMetrics()

	ctx := context.Background()
	s := buildMinedCounter(ctx, t, "mined_counter_off_chain", 7, false)

	err := s.checkCounterConflictingOnCurrentChain(ctx, *tx1.TxIDChainHash(), map[uint32]bool{8: true})

	require.NoError(t, err)
}

// A not-found that comes from a descendant of a present, mined counter is not an
// absent counter. Tolerating it would drop the counter from the set, skip its
// BlockIDs check, and accept a double-spend of an output a confirmed tx spent.
func TestCheckCounterConflictingOnCurrentChain_FailsClosedOnAbsentDescendantOfMinedCounter(t *testing.T) {
	InitPrometheusMetrics()

	ctx := context.Background()
	s := buildMinedCounter(ctx, t, "mined_counter_absent_child", 7, true)

	err := s.checkCounterConflictingOnCurrentChain(ctx, *tx1.TxIDChainHash(), map[uint32]bool{7: true})

	require.Error(t, err, "an absent descendant of a present counter must not be tolerated as an absent counter")
	require.False(t, errors.Is(err, errors.ErrTxInvalid), "the walk must fail before the mined-block check")
}
