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

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// buildDanglingRef sets up, in a fresh sqlitememory store: parentTx1 mined at
// parentHeight, its output 0 spent by an ABSENT counter (a double-spend clone of
// tx1 that is spent but never created), and tx1 created as the conflicting winner.
// The store's tip is set to tip. It returns a Server ready for the check.
func buildDanglingRef(ctx context.Context, t *testing.T, name string, parentHeight, tip uint32) *Server {
	t.Helper()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

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
