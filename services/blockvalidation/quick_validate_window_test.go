package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// These tests pin the seam between quick validation and the quick window: a batch registers
// its transaction ids, a spend of a coin an in-flight PREDECESSOR is still creating waits on
// that predecessor's gate, and a spend that finds no coin although the parent was registered
// is a bug on our side rather than a bad block.

// windowHarness gives the one-wave harness a two-deep window whose committer is a no-op, so
// createAndSpendUTXOsForBatch can be driven directly for two consecutive blocks.
func windowHarness(t *testing.T, dbName string) (*BlockValidation, *applyRecorder, *quickWindow, func()) {
	t.Helper()

	return windowHarnessWithCommit(t, dbName, func(context.Context, *windowEntry) error { return nil })
}

// windowHarnessWithCommit is windowHarness with the committer under the test's control, so a
// test can hold a block in the window after its store work is done and watch what a successor
// is and is not allowed to do meanwhile.
func windowHarnessWithCommit(t *testing.T, dbName string, commit func(context.Context, *windowEntry) error) (*BlockValidation, *applyRecorder, *quickWindow, func()) {
	t.Helper()

	initPrometheusMetrics()

	bv, rec, cleanup := newOneWaveHarness(t, dbName)
	bv.settings.BlockValidation.QuickWindowBlocks = 2
	bv.settings.BlockValidation.QuickValidateSkipUtxoLock = true

	ctx, cancel := context.WithCancel(context.Background())
	w := newQuickWindow(bv.logger, 2, 64, commit)
	w.Start(ctx)
	bv.quickWindow = w

	return bv, rec, w, func() { cancel(); cleanup() }
}

// twoBlocks returns a parent and its child, at heights the window can admit in order.
func twoBlocks(t *testing.T) (*model.Block, *model.Block) {
	t.Helper()

	blocks := chainOf(t, 2)
	blocks[0].Height = 100
	blocks[0].ID = 100
	blocks[1].Height = 101
	blocks[1].ID = 101

	return blocks[0], blocks[1]
}

// Block 2 spends an output block 1 creates. With block 1's gate open, block 2's spend must wait;
// when block 1's batch closes its gate, block 2's spend proceeds and the coin is spent by block 2.
func TestWindow_SpendOfInFlightPredecessorWaitsForItsGate(t *testing.T) {
	bv, _, w, cleanup := windowHarness(t, "window-gate")
	defer cleanup()

	ctx := context.Background()
	b1, b2 := twoBlocks(t)

	root, key := seedRoot(t, bv.utxoStore, 2, "gate")
	p := spendOf(t, key, root, 0, 40_000) // in block 1
	c := spendOf(t, key, p, 0, 30_000)    // in block 2, spends p

	e1, _, err := w.Admit(ctx, b1)
	require.NoError(t, err)
	e2, _, err := w.Admit(ctx, b2)
	require.NoError(t, err)

	batch1 := oneWaveBatchFor(t, bv, b1, []*bt.Tx{p})
	batch1.window = e1
	gate1, err := e1.RegisterBatch([]chainhash.Hash{*p.TxIDChainHash()})
	require.NoError(t, err)
	batch1.gate = gate1
	e1.RegistrationComplete()

	batch2 := oneWaveBatchFor(t, bv, b2, []*bt.Tx{c})
	batch2.window = e2
	gate2, err := e2.RegisterBatch([]chainhash.Hash{*c.TxIDChainHash()})
	require.NoError(t, err)
	batch2.gate = gate2
	e2.RegistrationComplete()

	// Run block 2's batch first. It must not complete until block 1's gate closes.
	done2 := make(chan error, 1)
	go func() { done2 <- bv.createAndSpendUTXOsForBatch(ctx, b2, batch2) }()

	select {
	case err := <-done2:
		t.Fatalf("block 2 completed before block 1 created its coin: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	require.NoError(t, bv.createAndSpendUTXOsForBatch(ctx, b1, batch1))
	require.NoError(t, <-done2)
	requireSpentBy(t, bv.utxoStore, p, 0, c)
}

// A failed predecessor releases the waiter with a service error and no spend is issued.
func TestWindow_FailedPredecessorFailsTheDependentBlockAsALocalFault(t *testing.T) {
	bv, rec, w, cleanup := windowHarness(t, "window-fail")
	defer cleanup()

	ctx := context.Background()
	b1, b2 := twoBlocks(t)

	root, key := seedRoot(t, bv.utxoStore, 2, "fail")
	p := spendOf(t, key, root, 0, 40_000)
	c := spendOf(t, key, p, 0, 30_000)

	e1, _, err := w.Admit(ctx, b1)
	require.NoError(t, err)
	e2, _, err := w.Admit(ctx, b2)
	require.NoError(t, err)

	_, err = e1.RegisterBatch([]chainhash.Hash{*p.TxIDChainHash()})
	require.NoError(t, err)
	e1.RegistrationComplete()

	batch2 := oneWaveBatchFor(t, bv, b2, []*bt.Tx{c})
	batch2.window = e2
	gate2, err := e2.RegisterBatch([]chainhash.Hash{*c.TxIDChainHash()})
	require.NoError(t, err)
	batch2.gate = gate2
	e2.RegistrationComplete()

	done2 := make(chan error, 1)
	go func() { done2 <- bv.createAndSpendUTXOsForBatch(ctx, b2, batch2) }()

	// Park the waiter on block 1's gate BEFORE failing block 1. Without this probe the failure
	// can win the race against block 2's partition loop: block 1's gate would already be closed
	// and out of the open map, block 2 would classify c as independent, its combined call would
	// reach the store and fail with not-found, and the miss backstop would produce an error
	// that passes every assertion below without batchGate.Wait ever running.
	select {
	case err := <-done2:
		t.Fatalf("block 2 completed before it parked on block 1's gate: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	e1.Fail(errors.NewProcessingError("block 1 broke"))

	err = <-done2
	require.Error(t, err)
	require.True(t, errors.IsTransientLocalError(err), "got %v", err)
	require.Contains(t, err.Error(), "its outputs cannot be spent",
		"the failure must come from the gate, not from the miss backstop: %v", err)
	require.Zero(t, rec.spendCallsFor(c), "no spend may be issued for a block whose parent failed")
}

// Block 2 spends only old coins: it never waits, even with block 1 in flight.
func TestWindow_IndependentSuccessorDoesNotWait(t *testing.T) {
	bv, _, w, cleanup := windowHarness(t, "window-indep")
	defer cleanup()

	ctx := context.Background()
	b1, b2 := twoBlocks(t)

	root, key := seedRoot(t, bv.utxoStore, 2, "indep")
	p := spendOf(t, key, root, 0, 40_000)
	q := spendOf(t, key, root, 1, 40_000)

	e1, _, err := w.Admit(ctx, b1)
	require.NoError(t, err)
	e2, _, err := w.Admit(ctx, b2)
	require.NoError(t, err)

	_, err = e1.RegisterBatch([]chainhash.Hash{*p.TxIDChainHash()})
	require.NoError(t, err)
	e1.RegistrationComplete()

	batch2 := oneWaveBatchFor(t, bv, b2, []*bt.Tx{q})
	batch2.window = e2
	gate2, err := e2.RegisterBatch([]chainhash.Hash{*q.TxIDChainHash()})
	require.NoError(t, err)
	batch2.gate = gate2
	e2.RegistrationComplete()

	done2 := make(chan error, 1)
	go func() { done2 <- bv.createAndSpendUTXOsForBatch(ctx, b2, batch2) }()

	select {
	case err := <-done2:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("independent block 2 waited on block 1")
	}

	requireSpentBy(t, bv.utxoStore, root, 1, q)
}

// The store answering not-found for a REGISTERED parent is a gate bug: it must be reported as
// a local fault and counted, never as a peer fault.
func TestWindow_MissOnRegisteredParentIsReclassifiedAndCounted(t *testing.T) {
	bv, _, w, cleanup := windowHarness(t, "window-miss")
	defer cleanup()

	ctx := context.Background()
	b1, b2 := twoBlocks(t)

	root, key := seedRoot(t, bv.utxoStore, 2, "miss")
	p := spendOf(t, key, root, 0, 40_000)
	c := spendOf(t, key, p, 0, 30_000)

	e1, _, err := w.Admit(ctx, b1)
	require.NoError(t, err)
	e2, _, err := w.Admit(ctx, b2)
	require.NoError(t, err)

	// Block 1 registers p and CLOSES its gate without ever creating p (simulating a gate bug).
	g1, err := e1.RegisterBatch([]chainhash.Hash{*p.TxIDChainHash()})
	require.NoError(t, err)
	e1.RegistrationComplete()
	g1.Close()

	batch2 := oneWaveBatchFor(t, bv, b2, []*bt.Tx{c})
	batch2.window = e2
	gate2, err := e2.RegisterBatch([]chainhash.Hash{*c.TxIDChainHash()})
	require.NoError(t, err)
	batch2.gate = gate2
	e2.RegistrationComplete()

	before := testutil.ToFloat64(prometheusBlockValidationQuickWindowMissTotal)

	err = bv.createAndSpendUTXOsForBatch(ctx, b2, batch2)
	require.Error(t, err)
	require.True(t, errors.IsTransientLocalError(err), "got %v", err)
	require.Equal(t, before+1, testutil.ToFloat64(prometheusBlockValidationQuickWindowMissTotal))
}

// A coinbase-only block registers no batches, so the entry point is the only place that can
// tell the window its registration is finished. If it stays silent, a successor's dependency
// check falls through to the committed channel instead, which only closes once the ordered
// committer has run — collapsing the window to depth 1 for exactly the blocks early mainnet is
// made of. The committer here is held open so "registered" and "committed" cannot be confused.
func TestWindow_CoinbaseOnlyBlockCompletesItsRegistration(t *testing.T) {
	release := make(chan struct{})
	committed := make(chan uint32, 2)

	bv, _, w, cleanup := windowHarnessWithCommit(t, "window-coinbase-only", func(_ context.Context, e *windowEntry) error {
		<-release
		committed <- e.Height()

		return nil
	})
	defer cleanup()
	defer close(release)

	ctx := context.Background()
	b1, b2 := twoBlocks(t)

	// The zero-subtree branch needs a coinbase to get past the entry point's checks and a
	// blockchain client to hand out the block id; the block itself carries no subtrees.
	_, publicKey := bec.PrivateKeyFromBytes([]byte("window-coinbase-only"))
	b1.CoinbaseTx = transactions.Create(t,
		transactions.WithCoinbaseData(1, "/coinbase-only/"),
		transactions.WithP2PKHOutputs(1, 100_000, publicKey),
	)
	b1.ID = 0

	blockchainMock := &blockchain.Mock{}
	blockchainMock.On("AssignBlockID", mock.Anything, mock.Anything).Return(uint64(7), nil)
	bv.blockchainClient = blockchainMock

	e1, _, err := w.Admit(ctx, b1)
	require.NoError(t, err)
	e2, _, err := w.Admit(ctx, b2)
	require.NoError(t, err)

	done1 := make(chan error, 1)
	go func() { done1 <- bv.quickValidateBlockInner(e1.Context(), b1, "test-peer", "", e1) }()

	// Block 1 is now in the window with its store work done and its commit held. Block 2 must
	// be free to start its own UTXO work: the dependency check is complete.
	waited := make(chan error, 1)
	go func() {
		waited <- e2.WaitPredecessorsRegistered(ctx)
	}()

	select {
	case err := <-waited:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("block 2 waited on block 1's commit for a registration block 1 had already finished")
	}

	require.Empty(t, committed, "block 1 must not have committed for this to mean anything")

	release <- struct{}{}
	require.NoError(t, <-done1)
	require.Equal(t, uint32(100), <-committed)
	require.Equal(t, uint32(7), b1.ID)
}

// windowMissCase is one row of the backstop's classification table. ownRegistered runs the row
// with the calling block itself as the entry that registered the parent, which is the case the
// backstop must NOT reclassify.
type windowMissCase struct {
	name          string
	err           func(parent *chainhash.Hash) error
	reclassify    bool
	ownRegistered bool
	description   string
}

// The backstop turns exactly two error shapes into a local fault, and only when the parent the
// error points at is registered by an in-flight block OTHER than the one being applied.
// Everything else is handed back untouched so the block fails the way it always did.
func TestWindow_MissBackstopClassification(t *testing.T) {
	initPrometheusMetrics()

	logger := ulogger.TestLogger{}

	_, publicKey := bec.PrivateKeyFromBytes([]byte("miss-table-parent"))
	privateKey, _ := bec.PrivateKeyFromBytes([]byte("miss-table-parent"))

	parentTx := transactions.Create(t,
		transactions.WithCoinbaseData(1, "/miss-table/"),
		transactions.WithP2PKHOutputs(1, 100_000, publicKey),
	)
	child := spendOf(t, privateKey, parentTx, 0, 90_000)
	parent := parentTx.TxIDChainHash()

	spender := &spendpkg.SpendingData{TxID: child.TxIDChainHash(), Vin: 0}

	cases := []windowMissCase{
		{
			name:        "plain processing error",
			err:         func(*chainhash.Hash) error { return errors.NewProcessingError("something else broke") },
			reclassify:  false,
			description: "an unrelated failure is not a gate miss",
		},
		{
			name: "not found naming the registered parent",
			err: func(p *chainhash.Hash) error {
				return errors.NewTxNotFoundError("output %s:%d not found", p, 0)
			},
			reclassify:  true,
			description: "no coin for a parent an in-flight block claims is our bug",
		},
		{
			name: "spent with a conflicting spender",
			err: func(p *chainhash.Hash) error {
				return errors.NewUtxoSpentError(*p, 0, chainhash.Hash{}, spender)
			},
			reclassify:  false,
			description: "a named double spend is a real conflict, not a missed gate",
		},
		{
			name: "spent naming no spender",
			err: func(p *chainhash.Hash) error {
				return errors.NewUtxoSpentError(*p, 0, chainhash.Hash{}, nil)
			},
			reclassify:  true,
			description: "an anonymous spent answer for a claimed parent is the miss shape",
		},
		{
			name: "not found naming an id this block registered itself",
			err: func(p *chainhash.Hash) error {
				return errors.NewTxNotFoundError("output %s:%d not found", p, 0)
			},
			reclassify:    false,
			ownRegistered: true,
			description:   "an in-block parent was never gated, so a miss on it is this block's own hard failure",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newQuickWindow(logger, 2, 8, func(context.Context, *windowEntry) error { return nil })

			e, _, err := w.Admit(context.Background(), &model.Block{
				Header: &model.BlockHeader{Version: 1, HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}},
				Height: 100,
			})
			require.NoError(t, err)

			_, err = e.RegisterBatch([]chainhash.Hash{*parent})
			require.NoError(t, err)

			// nil owner is "some other block is applying this transaction"; e is "the block
			// that registered the parent is the one applying the spend".
			var owner *windowEntry
			if tc.ownRegistered {
				owner = e
			}

			before := testutil.ToFloat64(prometheusBlockValidationQuickWindowMissTotal)
			in := tc.err(parent)
			out := windowMissError(&model.Block{Header: &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}}, w, owner, child, in)
			after := testutil.ToFloat64(prometheusBlockValidationQuickWindowMissTotal)

			if tc.reclassify {
				require.True(t, errors.IsTransientLocalError(out), "%s: got %v", tc.description, out)
				require.Contains(t, out.Error(), "gate miss", tc.description)
				require.Equal(t, before+1, after, "a reclassified miss must be counted")

				return
			}

			require.Equal(t, in, out, "%s", tc.description)
			require.Equal(t, before, after, "only a reclassified miss may be counted")
		})
	}
}

// An unregistered parent is never a gate miss, whatever the error shape: the coin was simply
// not there and the block fails as it always did.
func TestWindow_MissBackstopIgnoresUnregisteredParents(t *testing.T) {
	initPrometheusMetrics()

	logger := ulogger.TestLogger{}

	_, publicKey := bec.PrivateKeyFromBytes([]byte("miss-unregistered"))
	privateKey, _ := bec.PrivateKeyFromBytes([]byte("miss-unregistered"))

	parentTx := transactions.Create(t,
		transactions.WithCoinbaseData(1, "/miss-unregistered/"),
		transactions.WithP2PKHOutputs(1, 100_000, publicKey),
	)
	child := spendOf(t, privateKey, parentTx, 0, 90_000)

	w := newQuickWindow(logger, 2, 8, func(context.Context, *windowEntry) error { return nil })

	before := testutil.ToFloat64(prometheusBlockValidationQuickWindowMissTotal)
	in := errors.NewTxNotFoundError("output %s:%d not found", parentTx.TxIDChainHash(), 0)
	out := windowMissError(&model.Block{Header: &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}}, w, nil, child, in)

	require.Equal(t, in, out)
	require.Equal(t, before, testutil.ToFloat64(prometheusBlockValidationQuickWindowMissTotal))
}
