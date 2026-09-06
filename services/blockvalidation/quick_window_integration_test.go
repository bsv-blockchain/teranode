package blockvalidation

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// These tests drive three real, genesis-linked blocks WITH transactions through
// Server.processBlockFound, over the sql UTXO store on sqlitememory and the real blockchain
// local client on the sqlitememory blockchain store. Nothing below is mocked: the only test
// affordances are the Task 5 blockchain-client decorator (which can hold one AddBlock open)
// and a UTXO store decorator that can hold or fail one chosen SpendAndCreate.
//
// What the three blocks are, and why. Block 1 creates coins and spends a sibling of its own,
// block 2 spends one of block 1's coins and one already-mined coin, block 3 spends already-
// mined coins only. That shape puts one of every dependency the window has to get right into
// a single chain: a cross-block spend (block 2 on block 1), an in-block spend (block 1 on
// itself) and an independent block that can legitimately run ahead (block 3).

// ---- the UTXO store spy ----

// applyKind is the shape of one SpendAndCreate: the combined one-wave call, the create half of
// the two-wave path, or its spend half.
type applyKind int

const (
	applyCombinedCall applyKind = iota
	applyCreateOnlyCall
	applySpendOnlyCall
)

func (k applyKind) String() string {
	switch k {
	case applyCreateOnlyCall:
		return "create-only"
	case applySpendOnlyCall:
		return "spend-only"
	default:
		return "combined"
	}
}

// spyKey names one call: which transaction, in which of the three shapes.
type spyKey struct {
	tx   chainhash.Hash
	kind applyKind
}

func (k spyKey) String() string { return fmt.Sprintf("%s %s", k.kind, k.tx.String()) }

// spyStore is a real utxo.Store with three test affordances: it counts every SpendAndCreate by
// shape, it can park a chosen call until the test releases it, and it can fail a chosen call
// once. Parking is what pins the moment under test — a block still creating while its successor
// partitions, or a block still spending while two more blocks are delivered behind it.
type spyStore struct {
	utxo.Store

	mu       sync.Mutex
	seen     map[spyKey]int
	holds    map[spyKey]chan struct{}
	failOnce map[spyKey]error
}

func newSpyStore(inner utxo.Store) *spyStore {
	return &spyStore{
		Store:    inner,
		seen:     make(map[spyKey]int),
		holds:    make(map[spyKey]chan struct{}),
		failOnce: make(map[spyKey]error),
	}
}

// hold parks every call matching key until the returned release runs. Calls arriving after the
// release pass straight through.
func (s *spyStore) hold(key spyKey) (release func()) {
	ch := make(chan struct{})

	s.mu.Lock()
	s.holds[key] = ch
	s.mu.Unlock()

	var once sync.Once

	return func() { once.Do(func() { close(ch) }) }
}

// failNext makes the next call matching key fail with err, without reaching the store. Later
// calls for the same key run normally, which is what makes the replay converge.
func (s *spyStore) failNext(key spyKey, err error) {
	s.mu.Lock()
	s.failOnce[key] = err
	s.mu.Unlock()
}

// count returns how many calls matching key the store has seen.
func (s *spyStore) count(key spyKey) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.seen[key]
}

func (s *spyStore) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32,
	opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	options := parseCreateOptions(opts)

	kind := applyCombinedCall

	switch {
	case options.CreateOnly:
		kind = applyCreateOnlyCall
	case options.SpendOnly:
		kind = applySpendOnlyCall
	}

	key := spyKey{tx: *tx.TxIDChainHash(), kind: kind}

	// Recorded BEFORE parking, so a test waiting on "this call has been reached" is released by
	// the very call it is about to hold.
	s.mu.Lock()
	s.seen[key]++
	hold := s.holds[key]

	failErr := s.failOnce[key]
	if failErr != nil {
		delete(s.failOnce, key)
	}
	s.mu.Unlock()

	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
			return nil, nil, errors.NewProcessingError("[spyStore] cancelled while holding %s", key.String(), ctx.Err())
		}
	}

	if failErr != nil {
		return nil, nil, failErr
	}

	return s.Store.SpendAndCreate(ctx, tx, blockHeight, opts...)
}

// ---- the harness ----

// integrationChain is the three-block chain and everything needed to assert on it.
type integrationChain struct {
	root *bt.Tx

	t1a *bt.Tx // block 1: spends an already-mined coin, creates two coins
	t1b *bt.Tx // block 1: spends t1a's first coin, so block 1 depends on itself
	t2a *bt.Tx // block 2: spends t1a's second coin, so block 2 depends on block 1
	t2b *bt.Tx // block 2: spends an already-mined coin
	t3a *bt.Tx // block 3: already-mined coins only
	t3b *bt.Tx

	block1 *model.Block
	block2 *model.Block
	block3 *model.Block
}

// txs returns every transaction whose outputs the end-state comparison covers.
func (c *integrationChain) txs() []*bt.Tx {
	return []*bt.Tx{c.root, c.t1a, c.t1b, c.t2a, c.t2b, c.t3a, c.t3b}
}

// blocks returns the chain in height order.
func (c *integrationChain) blocks() []*model.Block {
	return []*model.Block{c.block1, c.block2, c.block3}
}

// newIntegrationServer builds the Task 5 server harness at the requested window depth and
// wraps the UTXO store the validation pipeline sees in a spy. The raw store is returned too:
// seeding and end-state assertions go through it, so nothing a test asserts is filtered by the
// decorator it also uses to inject failures.
func newIntegrationServer(t *testing.T, name string, depth int) (*windowServer, *spyStore, utxo.Store) {
	t.Helper()

	ws := newWindowServer(t, name, func(s *settings.Settings) {
		s.BlockValidation.QuickWindowBlocks = depth
	})

	require.Equal(t, depth, ws.s.blockValidation.quickWindow.Depth(), "the window must run at the requested depth")

	real := ws.s.blockValidation.utxoStore

	spy := newSpyStore(real)
	// Swapped before any goroutine of this test exists, so this is not a race: it is the only
	// write to the field for the life of the harness.
	ws.s.blockValidation.utxoStore = spy

	return ws, spy, real
}

// spendOutputs builds a transaction spending one output of parent into nOutputs new coins.
// spendOf covers the single-output case; only the coin-creating transaction needs more.
func spendOutputs(t *testing.T, key *bec.PrivateKey, parent *bt.Tx, vout uint32, nOutputs int, sats uint64) *bt.Tx {
	t.Helper()

	_, publicKey := bec.PrivateKeyFromBytes([]byte("one-wave-out"))

	return transactions.Create(t,
		transactions.WithPrivateKey(key),
		transactions.WithInput(parent, vout),
		transactions.WithP2PKHOutputs(nOutputs, sats, publicKey),
	)
}

// windowBlockWithTxs builds a genesis-linked block carrying txs in one subtree, and writes the
// subtree and subtree-data files quick validation reads them back from. The merkle root is the
// subtree's own root with the coinbase substituted for the placeholder node, which is what
// validateSubtrees re-derives and checks.
func windowBlockWithTxs(t *testing.T, ws *windowServer, prev chainhash.Hash, height uint32, txs []*bt.Tx) *model.Block {
	t.Helper()

	_, publicKey := bec.PrivateKeyFromBytes([]byte("quick-window-integration"))

	coinbase := transactions.Create(t,
		transactions.WithCoinbaseData(height, "/quick-window-integration/"),
		transactions.WithP2PKHOutputs(1, 5_000_000_000, publicKey),
	)

	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(len(txs) + 1)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	for _, tx := range txs {
		require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 0, uint64(tx.Size())))
	}

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)

	ctx := context.Background()
	// Netsync writes the to-check file; findLocalSubtreeFile consults both, and the full
	// .subtree file is what quick validation writes for itself on the way through.
	require.NoError(t, ws.s.subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes))

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(coinbase, 0))

	for i, tx := range txs {
		require.NoError(t, subtreeData.AddTx(tx, i+1))
	}

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)
	require.NoError(t, ws.s.subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

	merkleRoot, err := subtree.RootHashWithReplaceRootNode(coinbase.TxIDChainHash(), 0, 0)
	require.NoError(t, err)

	prevHash := prev

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &prevHash,
			HashMerkleRoot: merkleRoot,
			Timestamp:      1_700_000_000 + height,
			Bits:           ws.bits,
			Nonce:          height,
		},
		CoinbaseTx:       coinbase,
		TransactionCount: uint64(len(txs) + 1),
		Subtrees:         []*chainhash.Hash{subtree.RootHash()},
		Height:           height,
	}
}

// newIntegrationChain seeds an already-mined root transaction and builds the three blocks on
// top of the store's genesis. seed keys the root, so two harnesses given the same seed produce
// byte-identical transactions and their UTXO end states can be compared directly.
func newIntegrationChain(t *testing.T, ws *windowServer, store utxo.Store, seed string) *integrationChain {
	t.Helper()

	root, key := seedRoot(t, store, 6, seed)

	c := &integrationChain{root: root}

	c.t1a = spendOutputs(t, key, root, 0, 2, 40_000)
	c.t1b = spendOf(t, key, c.t1a, 0, 30_000)
	c.t2a = spendOf(t, key, c.t1a, 1, 30_000)
	c.t2b = spendOf(t, key, root, 1, 90_000)
	c.t3a = spendOf(t, key, root, 2, 90_000)
	c.t3b = spendOf(t, key, root, 3, 90_000)

	c.block1 = windowBlockWithTxs(t, ws, ws.genesis, 1, []*bt.Tx{c.t1a, c.t1b})
	c.block2 = windowBlockWithTxs(t, ws, *c.block1.Hash(), 2, []*bt.Tx{c.t2a, c.t2b})
	c.block3 = windowBlockWithTxs(t, ws, *c.block2.Hash(), 3, []*bt.Tx{c.t3a, c.t3b})

	return c
}

// ---- delivery and assertion helpers ----

// deliver runs one processBlockFound on its own goroutine, the way legacy sync calls it.
func deliver(ws *windowServer, block *model.Block) <-chan error {
	done := make(chan error, 1)

	go func() {
		done <- ws.s.processBlockFound(context.Background(), block.Hash(), "peer-1", "legacy", block)
	}()

	return done
}

// requireAdmitted waits for block to reach the quick window. Legacy sync delivers block N+1
// only after N's own call has started, and admission is that start: waiting on it here is what
// makes the delivery order deterministic without a sleep.
func requireAdmitted(t *testing.T, ws *windowServer, block *model.Block) *windowEntry {
	t.Helper()

	var entry *windowEntry

	require.Eventually(t, func() bool {
		entry = ws.s.blockValidation.quickWindow.Lookup(block.Hash())

		return entry != nil
	}, 30*time.Second, 5*time.Millisecond, "block %s must be admitted to the window", block.Hash().String())

	return entry
}

// freshBlock re-parses a block from its wire bytes, the way every real delivery arrives. Two
// deliveries must never share one model.Block: processBlockFound writes the settled height into
// it, which would be a test-only data race.
func freshBlock(t *testing.T, block *model.Block) *model.Block {
	t.Helper()

	raw, err := block.Bytes()
	require.NoError(t, err)

	fresh, err := model.NewBlockFromBytes(raw)
	require.NoError(t, err)
	require.True(t, block.Hash().IsEqual(fresh.Hash()), "the re-parsed block must be the same block")

	return fresh
}

// requireChainCommitted asserts the chain store saw exactly these blocks, in height order, at
// consecutive heights from 1, with strictly increasing block ids.
func requireChainCommitted(t *testing.T, ws *windowServer, blocks ...*model.Block) {
	t.Helper()

	ctx := context.Background()

	want := make([]chainhash.Hash, 0, len(blocks))
	for _, b := range blocks {
		want = append(want, *b.Hash())
	}

	require.Equal(t, want, ws.client.addedBlocks(), "the chain store must see the blocks in height order")

	for i, b := range blocks {
		requireStored(t, ws, b.Hash())

		_, blockMeta, err := ws.s.blockchainClient.GetBlockHeader(ctx, b.Hash())
		require.NoError(t, err)
		require.NotNil(t, blockMeta)
		require.Equal(t, uint32(i+1), blockMeta.Height, "block %d must be stored at height %d", i+1, i+1)
	}

	for i := 1; i < len(blocks); i++ {
		require.Less(t, blocks[i-1].ID, blocks[i].ID,
			"block ids must be handed out in height order: %d then %d", blocks[i-1].ID, blocks[i].ID)
	}
}

// spendSnapshot records, for every output of every transaction, who spent it (empty when
// unspent). It is the whole observable UTXO effect of the three blocks.
func spendSnapshot(t *testing.T, store utxo.Store, txs []*bt.Tx) map[string]string {
	t.Helper()

	ctx := context.Background()
	out := make(map[string]string, len(txs)*2)

	for _, tx := range txs {
		hash := tx.TxIDChainHash()

		for vout := range tx.Outputs {
			resp, err := store.GetSpend(ctx, &utxo.Spend{TxID: hash, Vout: uint32(vout)})
			require.NoError(t, err)

			key := fmt.Sprintf("%s:%d", hash.String(), vout)
			if resp.SpendingData == nil {
				out[key] = ""
				continue
			}

			out[key] = resp.SpendingData.TxID.String()
		}
	}

	return out
}

// requireEndState pins the UTXO state the three blocks must leave behind, however they were
// delivered. Every test that runs the chain to completion asserts exactly this, which is what
// makes "the same end state" a literal claim rather than a description.
func requireEndState(t *testing.T, store utxo.Store, c *integrationChain) {
	t.Helper()

	rootID := c.root.TxIDChainHash().String()
	t1aID := c.t1a.TxIDChainHash().String()

	want := map[string]string{
		rootID + ":0": c.t1a.TxIDChainHash().String(),
		rootID + ":1": c.t2b.TxIDChainHash().String(),
		rootID + ":2": c.t3a.TxIDChainHash().String(),
		rootID + ":3": c.t3b.TxIDChainHash().String(),
		rootID + ":4": "",
		rootID + ":5": "",
		t1aID + ":0":  c.t1b.TxIDChainHash().String(),
		t1aID + ":1":  c.t2a.TxIDChainHash().String(),
	}

	for _, tx := range []*bt.Tx{c.t1b, c.t2a, c.t2b, c.t3a, c.t3b} {
		want[tx.TxIDChainHash().String()+":0"] = ""
	}

	require.Equal(t, want, spendSnapshot(t, store, c.txs()))
}

// ---- the tests ----

// TestWindowIntegration_ThreeBlocksSecondSpendsFirst is the window's reason to exist, end to
// end: three blocks in flight at once, with block 2 spending a coin block 1 has not finished
// creating. Block 2 must wait on block 1's gate rather than fail on a missing coin, block 3
// must not wait on anything, and all three must still reach the chain store in height order.
//
// Block 1's one-wave apply is held so its gate is provably still open when block 2 partitions;
// without that the test would pass whenever block 1 happened to win the race, and prove
// nothing about the gate.
func TestWindowIntegration_ThreeBlocksSecondSpendsFirst(t *testing.T) {
	ws, spy, store := newIntegrationServer(t, "window_integration_dependency", 3)
	c := newIntegrationChain(t, ws, store, "WINDOW_INTEGRATION_DEPENDENCY")

	gateWaitsBefore := testutil.ToFloat64(prometheusBlockValidationQuickWindowGateWaits)

	release := spy.hold(spyKey{tx: *c.t1a.TxIDChainHash(), kind: applyCombinedCall})

	err1 := deliver(ws, c.block1)
	requireAdmitted(t, ws, c.block1)

	err2 := deliver(ws, c.block2)
	requireAdmitted(t, ws, c.block2)

	err3 := deliver(ws, c.block3)
	requireAdmitted(t, ws, c.block3)

	// t2a's create-only call is issued only after block 2 has partitioned it as depending on an
	// in-flight predecessor, so seeing that call is proof block 2 found block 1's gate open.
	require.Eventually(t, func() bool {
		return spy.count(spyKey{tx: *c.t2a.TxIDChainHash(), kind: applyCreateOnlyCall}) > 0
	}, 30*time.Second, 5*time.Millisecond,
		"block 2 must reach its create wave while block 1 is still creating the coin it spends")

	release()

	require.NoError(t, <-err1)
	require.NoError(t, <-err2)
	require.NoError(t, <-err3)

	requireChainCommitted(t, ws, c.block1, c.block2, c.block3)
	requireSpentBy(t, store, c.t1a, 1, c.t2a)
	requireEndState(t, store, c)

	require.Greater(t, testutil.ToFloat64(prometheusBlockValidationQuickWindowGateWaits), gateWaitsBefore,
		"block 2's spend must have waited on block 1's gate")
}

// TestWindowIntegration_AbortAndReplayMatchesSerial is the fail-closed half. Block 1's spend
// wave is failed once while blocks 2 and 3 are in flight behind it. The failure is ours to
// own: block 1 comes back with the store's own non-transient error, blocks 2 and 3 come back
// as local faults so legacy sync re-delivers instead of rotating the peer, and nothing reaches
// the chain store. Re-delivering the three serially must then land on exactly the state a run
// that never failed would have produced, which is what the second harness is for.
func TestWindowIntegration_AbortAndReplayMatchesSerial(t *testing.T) {
	ws, spy, store := newIntegrationServer(t, "window_integration_abort", 3)
	c := newIntegrationChain(t, ws, store, "WINDOW_INTEGRATION_ABORT")

	spendKey := spyKey{tx: *c.t1b.TxIDChainHash(), kind: applySpendOnlyCall}

	// Park block 1's spend until blocks 2 and 3 are admitted, then fail it once. Both together
	// are what put the abort cascade under test: a failure with two successors resident.
	release := spy.hold(spendKey)
	spy.failNext(spendKey, errors.NewProcessingError("forced spend failure on the first attempt"))

	err1 := deliver(ws, c.block1)
	requireAdmitted(t, ws, c.block1)

	err2 := deliver(ws, c.block2)
	requireAdmitted(t, ws, c.block2)

	err3 := deliver(ws, c.block3)
	requireAdmitted(t, ws, c.block3)

	release()

	failure := <-err1
	require.Error(t, failure, "block 1 must fail on its forced spend failure")
	require.False(t, errors.IsTransientLocalError(failure),
		"a store failure is not a local fault; legacy sync must see it as such: %v", failure)
	require.Contains(t, failure.Error(), "forced spend failure")

	for i, ch := range []<-chan error{err2, err3} {
		err := <-ch
		require.Error(t, err, "block %d must not come back nil behind a failed predecessor", i+2)
		require.True(t, errors.IsTransientLocalError(err),
			"an aborted successor is our fault, not the peer's: %v", err)
	}

	require.Empty(t, ws.client.addedBlocks(), "nothing may be committed once block 1 failed")

	for _, b := range c.blocks() {
		exists, err := ws.s.blockchainClient.GetBlockExists(context.Background(), b.Hash())
		require.NoError(t, err)
		require.False(t, exists, "block %s must not be in the blockchain store", b.Hash().String())
	}

	require.Equal(t, 1, spy.count(spendKey), "the forced failure must have fired exactly once")

	// Re-deliver serially, each call after the previous returned, which is what legacy sync
	// does on its next pass.
	replayed := make([]*model.Block, 0, 3)

	for _, b := range c.blocks() {
		fresh := freshBlock(t, b)
		require.NoError(t, ws.s.processBlockFound(context.Background(), fresh.Hash(), "peer-1", "legacy", fresh),
			"the replay of block %s must succeed", b.Hash().String())

		replayed = append(replayed, fresh)
	}

	requireChainCommitted(t, ws, replayed...)
	requireEndState(t, store, c)

	// And the same chain, never failed, on a second harness: the two UTXO end states must be
	// identical output for output. Same seed, so the transactions are the same transactions.
	ref, _, refStore := newIntegrationServer(t, "window_integration_abort_serial", 3)
	refChain := newIntegrationChain(t, ref, refStore, "WINDOW_INTEGRATION_ABORT")

	for _, b := range refChain.blocks() {
		require.NoError(t, ref.s.processBlockFound(context.Background(), b.Hash(), "peer-1", "legacy", b))
	}

	require.Equal(t, spendSnapshot(t, refStore, refChain.txs()), spendSnapshot(t, store, c.txs()),
		"the aborted-then-replayed run must land on the serial run's UTXO state")
}

// TestWindowIntegration_DuplicateDelivery covers legacy sync sending the same block twice while
// its parent is still in flight. Both deliveries must return the one live attempt's outcome,
// and the chain store must see each block exactly once: a second attempt would race the first
// through the store.
func TestWindowIntegration_DuplicateDelivery(t *testing.T) {
	ws, _, store := newIntegrationServer(t, "window_integration_duplicate", 3)
	c := newIntegrationChain(t, ws, store, "WINDOW_INTEGRATION_DUPLICATE")

	// Hold block 1 inside its commit so it is resident in the window for the whole test.
	arrived, releaseCommit := ws.client.hold(c.block1.Hash())

	err1 := deliver(ws, c.block1)

	select {
	case <-arrived:
	case err := <-err1:
		t.Fatalf("block 1 returned before reaching its commit: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("block 1 never reached its commit")
	}

	require.NotNil(t, ws.s.blockValidation.quickWindow.Lookup(c.block1.Hash()), "block 1 must be in flight")

	// Two deliveries of block 2 released together, each with its own parsed block.
	first := c.block2
	second := freshBlock(t, c.block2)

	start := make(chan struct{})
	results := make(chan error, 2)

	var wg sync.WaitGroup

	for _, b := range []*model.Block{first, second} {
		wg.Add(1)

		go func() {
			defer wg.Done()

			<-start

			results <- ws.s.processBlockFound(context.Background(), b.Hash(), "peer-1", "legacy", b)
		}()
	}

	close(start)

	requireAdmitted(t, ws, c.block2)

	releaseCommit()

	require.NoError(t, <-err1)
	require.NoError(t, <-results, "the first delivery of block 2 must succeed")
	require.NoError(t, <-results, "the duplicate delivery must return the live attempt's outcome")

	wg.Wait()

	require.Equal(t, 1, ws.client.countOf(c.block1.Hash()), "block 1 must reach the chain store once")
	require.Equal(t, 1, ws.client.countOf(c.block2.Hash()), "block 2 must reach the chain store once, not twice")
	requireSpentBy(t, store, c.t1a, 1, c.t2a)
}

// TestWindowIntegration_IdsMonotone pins the one ordering the window must impose on work that
// is otherwise free to overlap. Block 3's store work is allowed to finish first, by holding
// block 1's create; the block ids must still come out in height order, because a successor
// asks for its id only after its predecessor has asked for one.
func TestWindowIntegration_IdsMonotone(t *testing.T) {
	ws, spy, store := newIntegrationServer(t, "window_integration_ids", 3)
	c := newIntegrationChain(t, ws, store, "WINDOW_INTEGRATION_IDS")

	release := spy.hold(spyKey{tx: *c.t1a.TxIDChainHash(), kind: applyCombinedCall})

	err1 := deliver(ws, c.block1)
	entry1 := requireAdmitted(t, ws, c.block1)

	err2 := deliver(ws, c.block2)
	requireAdmitted(t, ws, c.block2)

	err3 := deliver(ws, c.block3)
	entry3 := requireAdmitted(t, ws, c.block3)

	// Block 3 spends already-mined coins only, so nothing holds it back: its whole store phase
	// must complete while block 1 is still parked in its very first create.
	require.Eventually(t, func() bool {
		return closed(entry3.storeDone)
	}, 30*time.Second, 5*time.Millisecond, "block 3's store work must finish while block 1 is still creating")

	require.False(t, closed(entry1.storeDone), "block 1 must still be held in its create wave")

	release()

	require.NoError(t, <-err1)
	require.NoError(t, <-err2)
	require.NoError(t, <-err3)

	requireChainCommitted(t, ws, c.block1, c.block2, c.block3)
	requireEndState(t, store, c)

	require.Less(t, c.block1.ID, c.block2.ID, "block 2's id must be above block 1's")
	require.Less(t, c.block2.ID, c.block3.ID, "block 3's id must be above block 2's, although its store work finished first")
}

// TestWindowIntegration_DepthOneMatchesToday is the parity check that makes the window safe to
// ship: at depth 1 the same three blocks, delivered one at a time, must produce exactly the end
// state the overlapping runs produce, and no block may ever wait on another's gate.
func TestWindowIntegration_DepthOneMatchesToday(t *testing.T) {
	ws, _, store := newIntegrationServer(t, "window_integration_depth_one", 1)
	c := newIntegrationChain(t, ws, store, "WINDOW_INTEGRATION_DEPTH_ONE")

	gateWaitsBefore := testutil.ToFloat64(prometheusBlockValidationQuickWindowGateWaits)

	for _, b := range c.blocks() {
		require.NoError(t, ws.s.processBlockFound(context.Background(), b.Hash(), "peer-1", "legacy", b),
			"block %s must validate at depth 1", b.Hash().String())
	}

	requireChainCommitted(t, ws, c.block1, c.block2, c.block3)
	requireSpentBy(t, store, c.t1a, 1, c.t2a)
	requireEndState(t, store, c)

	require.Equal(t, gateWaitsBefore, testutil.ToFloat64(prometheusBlockValidationQuickWindowGateWaits),
		"at depth 1 no block can be in flight alongside another, so no gate can ever be waited on")
}
