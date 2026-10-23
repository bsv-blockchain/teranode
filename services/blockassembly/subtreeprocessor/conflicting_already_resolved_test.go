package subtreeprocessor

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// These tests drive processConflictingTransactions against a real SQLite store,
// so the resolution and its re-application are the store's own behaviour rather
// than a mock's.

// resolvedParentTx builds a funded parent with one spendable output. The
// previous-output script is set so the transaction is extended, which the store's
// create path needs for fee and utxo hash computation.
func resolvedParentTx(seed byte) *bt.Tx {
	parent := bt.NewTx()
	in := &bt.Input{
		PreviousTxOutIndex: 0,
		PreviousTxSatoshis: 200000,
		PreviousTxScript:   bscript.NewFromBytes([]byte{0x51}),
		SequenceNumber:     0xFFFFFFFF,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x51}),
	}
	_ = in.PreviousTxIDAdd(&chainhash.Hash{seed, seed, seed, 0xAB})
	parent.Inputs = []*bt.Input{in}
	parent.Outputs = []*bt.Output{{Satoshis: 100000, LockingScript: bscript.NewFromBytes([]byte{0x52})}}

	return parent
}

// resolvedSpendTx builds a transaction spending parent[0]; outSats makes its id unique.
func resolvedSpendTx(t *testing.T, parent *bt.Tx, outSats uint64) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.From(parent.TxIDChainHash().String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
	tx.Outputs = []*bt.Output{{Satoshis: outSats, LockingScript: bscript.NewFromBytes([]byte{0x52})}}

	return tx
}

// newResolvedTestStp returns a subtree processor over a fresh in-memory SQLite
// store and a blockchain mock that reports every block mined and no block an
// ancestor of another, so no demotion is refused.
func newResolvedTestStp(t *testing.T) (*SubtreeProcessor, utxo.Store) {
	t.Helper()

	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

	storeURL, err := url.Parse(fmt.Sprintf("sqlitememory:///resolved_%d", time.Now().UnixNano()))
	require.NoError(t, err)

	store, err := sql.New(t.Context(), ulogger.TestLogger{}, settings, storeURL)
	require.NoError(t, err)

	bc := &blockchain.Mock{}
	bc.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)
	bc.On("CheckBlockIsAncestorOfBlock", mock.Anything, mock.Anything, mock.Anything).Return(false, nil)

	newSubtreeChan := make(chan NewSubtreeRequest, 4)
	t.Cleanup(func() { close(newSubtreeChan) })

	stp, err := NewSubtreeProcessor(context.Background(), ulogger.TestLogger{}, settings, blob_memory.New(), bc, store, newSubtreeChan)
	require.NoError(t, err)

	return stp, store
}

func resolvedIsConflicting(t *testing.T, store utxo.Store, h chainhash.Hash) bool {
	t.Helper()

	m, err := store.Get(context.Background(), &h, fields.Conflicting)
	require.NoError(t, err)
	require.NotNil(t, m)

	return m.Conflicting
}

// resolvedSpender returns the transaction the store records as spending tx[0],
// or nil when the output is unspent.
func resolvedSpender(t *testing.T, store utxo.Store, tx *bt.Tx) *chainhash.Hash {
	t.Helper()

	m, err := store.Get(context.Background(), tx.TxIDChainHash(), fields.Utxos)
	require.NoError(t, err)
	require.NotNil(t, m)
	require.NotEmpty(t, m.SpendingDatas)

	if m.SpendingDatas[0] == nil {
		return nil
	}

	return m.SpendingDatas[0].TxID
}

// TestProcessConflictingTransactions_AlreadyResolvedWinner reproduces the Hetzner
// mainnet wedge of 2026-10-19. Block assembly applied block 971704, which resolved
// its 38 double spends in the store, and was then killed before its own checkpoint
// recorded the block. Every later catch-up re-applied 971704 from the immutable
// subtree meta, found the winners already resolved, and failed on
// ProcessConflicting's "tx is not conflicting" guard, so the node never advanced.
//
// The second application below is that re-run: same block, same conflicting node,
// no memory of the first pass. It must succeed, hand back the loser so the
// in-memory subtrees still evict it, and leave the store exactly as the first pass
// left it. In particular a child of the winner mined afterwards must stay
// unflagged and keep its spend: re-running the resolution would walk the winner's
// descendants as losers, because the parents' recorded spender is now the winner.
func TestProcessConflictingTransactions_AlreadyResolvedWinner(t *testing.T) {
	ctx := context.Background()
	stp, store := newResolvedTestStp(t)

	parent := resolvedParentTx(0x11)
	_, _, err := store.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, true))

	// The loser arrived first and holds parent[0].
	loser := resolvedSpendTx(t, parent, 80000)
	_, _, err = store.SpendAndCreate(ctx, loser, 2)
	require.NoError(t, err)

	// The winner is the mined double spend, stored conflicting and unspent.
	winner := resolvedSpendTx(t, parent, 90000)
	_, _, err = store.SpendAndCreate(ctx, winner, 2, utxo.WithConflicting(true), utxo.WithCreateOnly())
	require.NoError(t, err)

	winnerHash := *winner.TxIDChainHash()
	loserHash := *loser.TxIDChainHash()
	block := &model.Block{Header: dummyHeader(), Height: 2}

	// First application: the normal path resolves the double spend.
	losers, _, err := stp.processConflictingTransactions(ctx, block, []chainhash.Hash{winnerHash}, map[chainhash.Hash]struct{}{})
	require.NoError(t, err)
	require.NotNil(t, losers)
	require.True(t, losers.Exists(loserHash), "first pass must report the displaced loser")
	require.False(t, resolvedIsConflicting(t, store, winnerHash))
	require.True(t, resolvedIsConflicting(t, store, loserHash))
	require.Equal(t, &winnerHash, resolvedSpender(t, store, parent))

	// A child of the winner, mined in a later block, as on mainnet.
	child := resolvedSpendTx(t, winner, 50000)
	_, _, err = store.SpendAndCreate(ctx, child, 3)
	require.NoError(t, err)

	childHash := *child.TxIDChainHash()
	require.Equal(t, &childHash, resolvedSpender(t, store, winner))

	// Second application of the same block with no memory of the first.
	losers, conflictingSet, err := stp.processConflictingTransactions(ctx, block, []chainhash.Hash{winnerHash}, map[chainhash.Hash]struct{}{})
	require.NoError(t, err, "a block whose resolution is already in the store must apply again")
	require.NotNil(t, losers)
	require.True(t, losers.Exists(loserHash), "the re-run must still surface the loser for in-memory eviction")
	require.Contains(t, conflictingSet, loserHash)
	require.False(t, losers.Exists(winnerHash), "the winner is not a loser")
	require.False(t, losers.Exists(childHash), "the winner's descendant is not a loser")

	// The store is exactly as the first pass left it.
	require.False(t, resolvedIsConflicting(t, store, winnerHash))
	require.True(t, resolvedIsConflicting(t, store, loserHash))
	require.False(t, resolvedIsConflicting(t, store, childHash), "re-running the resolution would demote the winner's own child")
	require.Equal(t, &winnerHash, resolvedSpender(t, store, parent))
	require.Equal(t, &childHash, resolvedSpender(t, store, winner), "the child's spend of the winner must survive")
}

// TestProcessConflictingTransactions_UnflaggedWinnerNotSpenderIsRefused pins the
// boundary of the skip: a conflicting node that is not flagged but is not the
// recorded spender of its input is not a state this code can explain, so it is
// still handed to ProcessConflicting, whose guard refuses it.
func TestProcessConflictingTransactions_UnflaggedWinnerNotSpenderIsRefused(t *testing.T) {
	ctx := context.Background()
	stp, store := newResolvedTestStp(t)

	parent := resolvedParentTx(0x22)
	_, _, err := store.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	require.NoError(t, store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, true))

	loser := resolvedSpendTx(t, parent, 80000)
	_, _, err = store.SpendAndCreate(ctx, loser, 2)
	require.NoError(t, err)

	// Stored without the conflicting flag while the loser still holds parent[0].
	winner := resolvedSpendTx(t, parent, 90000)
	_, _, err = store.SpendAndCreate(ctx, winner, 2, utxo.WithCreateOnly())
	require.NoError(t, err)

	block := &model.Block{Header: dummyHeader(), Height: 2}

	_, _, err = stp.processConflictingTransactions(ctx, block, []chainhash.Hash{*winner.TxIDChainHash()}, map[chainhash.Hash]struct{}{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "tx is not conflicting")

	require.Equal(t, loser.TxIDChainHash(), resolvedSpender(t, store, parent), "nothing may be mutated on refusal")
}
