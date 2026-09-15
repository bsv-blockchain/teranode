package blockvalidation

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// These tests pin processBlockFound's half of the quick window: a block whose parent is still
// in flight is resolved from the window rather than diverted to catchup, admission happens
// before the block-assembly gate, and a window-route block whose parent is nowhere to be found
// comes back as a local fault instead of a silent nil.
//
// The blocks are coinbase-only (len(Subtrees) == 0). That is the branch early mainnet is made
// of, it needs no subtree files on disk, and it exercises everything this task changed:
// parent resolution, height settlement from an in-flight entry, admission order and the
// never-nil rule. The subtree-carrying branch is already covered by the one-wave and window
// tests either side of this seam.

// holdingBlockchainClient is the real blockchain client (LocalClient over a sqlitememory
// blockchain store) with two test affordances bolted on: it records every block that reaches
// AddBlock, and it can hold one block's AddBlock open. Holding is what keeps a block resident
// in the window while the next block is delivered, which is the whole situation under test.
type holdingBlockchainClient struct {
	blockchain.ClientI

	mu    sync.Mutex
	added []chainhash.Hash

	holdHash   *chainhash.Hash
	arrived    chan struct{}
	arrivedOne sync.Once
	release    chan struct{}
}

// hold arms the client to block inside AddBlock for hash. The returned channel is closed once
// AddBlock has been reached; release lets it through.
func (c *holdingBlockchainClient) hold(hash *chainhash.Hash) (arrived <-chan struct{}, release func()) {
	c.mu.Lock()
	c.holdHash = hash
	c.arrived = make(chan struct{})
	c.release = make(chan struct{})
	c.mu.Unlock()

	var once sync.Once

	return c.arrived, func() { once.Do(func() { close(c.release) }) }
}

func (c *holdingBlockchainClient) AddBlock(ctx context.Context, block *model.Block, peerID string, opts ...options.StoreBlockOption) error {
	c.mu.Lock()
	held := c.holdHash != nil && c.holdHash.IsEqual(block.Hash())
	arrived, release := c.arrived, c.release
	c.mu.Unlock()

	if held {
		c.arrivedOne.Do(func() { close(arrived) })
		<-release
	}

	if err := c.ClientI.AddBlock(ctx, block, peerID, opts...); err != nil {
		return err
	}

	c.mu.Lock()
	c.added = append(c.added, *block.Hash())
	c.mu.Unlock()

	return nil
}

// addedBlocks returns the hashes AddBlock stored, in the order it stored them.
func (c *holdingBlockchainClient) addedBlocks() []chainhash.Hash {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]chainhash.Hash(nil), c.added...)
}

// countOf returns how many times hash was stored.
func (c *holdingBlockchainClient) countOf(hash *chainhash.Hash) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	n := 0

	for _, h := range c.added {
		if h.IsEqual(hash) {
			n++
		}
	}

	return n
}

// windowServer is a Server wired for the unified below-checkpoint route with a two-deep quick
// window: real sqlitememory blockchain and UTXO stores, no block-assembly client so the gate
// is skipped, and no p2p client so no peer is malicious.
type windowServer struct {
	s      *Server
	client *holdingBlockchainClient
	// genesis is the store's own genesis hash, the parent every test chain starts from.
	genesis chainhash.Hash
	bits    model.NBit
}

// newWindowServer builds the harness. tweak, if given, adjusts the settings after the window
// defaults are in place and before anything is constructed from them.
func newWindowServer(t *testing.T, name string, tweak ...func(*settings.Settings)) *windowServer {
	t.Helper()

	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// CreateBaseTestSettings hands back a private copy of the regtest params, so pinning a
	// checkpoint above the test heights here affects nothing else. model.BelowCheckpoint keys
	// off this list, and without it the outpoint-only gate (and so the unified route, and so
	// the window) never opens.
	genesisHash := tSettings.ChainCfgParams.GenesisHash
	tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1000, Hash: genesisHash}}

	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	tSettings.BlockValidation.OutpointOnlyBelowCheckpoint = true
	tSettings.BlockValidation.QuickWindowBlocks = 2
	tSettings.BlockValidation.QuickValidateSkipUtxoLock = true

	for _, fn := range tweak {
		fn(tSettings)
	}

	utxoStoreURL, err := url.Parse("sqlitememory:///" + name)
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	blockchainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	localClient, err := blockchain.NewLocalClient(logger, tSettings, blockchainStore, nil, utxoStore)
	require.NoError(t, err)

	client := &holdingBlockchainClient{ClientI: localClient}

	subtreeStore := blobmemory.New()
	txStore := blobmemory.New()

	s := New(logger, tSettings, subtreeStore, txStore, utxoStore, nil, client, nil, nil, nil)
	s.blockValidation = NewBlockValidation(ctx, logger, tSettings, client, subtreeStore, txStore, utxoStore, nil, nil)

	if tSettings.BlockValidation.QuickWindowBlocks == 0 {
		require.False(t, s.blockValidation.quickWindow.Enabled(), "at 0 the service builds no window at all")
	} else {
		// Written out rather than read back from quickWindowDepth, which is the code under
		// test: with the setting at 2, coins created unlocked (so nothing forces it to 1) and
		// the default block-assembly allowance of 20 (half of which, 10, does not bite), the
		// window runs two deep. A tweak may ask for a different depth, and the test that asked
		// for it asserts what it expects.
		if len(tweak) == 0 {
			require.Equal(t, 2, s.blockValidation.quickWindow.Depth(), "the settings above must produce a two-deep window")
		}

		require.True(t, s.blockValidation.quickWindow.Enabled())
	}

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	return &windowServer{s: s, client: client, genesis: *genesisHash, bits: *bits}
}

// coinbaseOnlyBlock builds a block with a coinbase and no subtrees, anchored on prev. height
// is written into block.Height the way the legacy client writes the request height.
func coinbaseOnlyBlock(t *testing.T, ws *windowServer, prev chainhash.Hash, height uint32) *model.Block {
	t.Helper()

	_, publicKey := bec.PrivateKeyFromBytes([]byte("quick-window-server"))

	coinbase := transactions.Create(t,
		transactions.WithCoinbaseData(height, "/quick-window-server/"),
		transactions.WithP2PKHOutputs(1, 5_000_000_000, publicKey),
	)

	prevHash := prev

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &prevHash,
			HashMerkleRoot: coinbase.TxIDChainHash(),
			Timestamp:      1_700_000_000 + height,
			Bits:           ws.bits,
			Nonce:          height,
		},
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
		Height:           height,
	}
}

// requireStored waits for hash to appear in the blockchain store.
func requireStored(t *testing.T, ws *windowServer, hash *chainhash.Hash) {
	t.Helper()

	require.Eventually(t, func() bool {
		exists, err := ws.s.blockchainClient.GetBlockExists(context.Background(), hash)

		return err == nil && exists
	}, 10*time.Second, 10*time.Millisecond, "block %s must be stored", hash.String())
}

// TestProcessBlockFound_ParentInFlightIsResolvedFromTheWindow is the case the window exists
// for: block 2 arrives while block 1 is still in flight, so its parent is in no store yet.
// processBlockFound must find block 1 in the window, take its height from that entry, admit
// block 2 behind it and run it through quick validation. It must NOT take the catch-up divert,
// which returns nil and would have legacy sync record the block as accepted while nothing
// ever stored it.
func TestProcessBlockFound_ParentInFlightIsResolvedFromTheWindow(t *testing.T) {
	ws := newWindowServer(t, "window_server_parent_in_flight")

	block1 := coinbaseOnlyBlock(t, ws, ws.genesis, 1)
	block2 := coinbaseOnlyBlock(t, ws, *block1.Hash(), 2)

	// Hold block 1 inside its commit, which is exactly the state legacy sync delivers block 2
	// in: block 1 admitted, its store work done, its chain-store commit not yet run.
	arrived, release := ws.client.hold(block1.Hash())

	err1 := make(chan error, 1)

	go func() {
		err1 <- ws.s.processBlockFound(context.Background(), block1.Hash(), "peer-1", "legacy", block1)
	}()

	select {
	case <-arrived:
	case err := <-err1:
		t.Fatalf("block 1 returned before reaching its commit: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("block 1 never reached its commit")
	}

	require.NotNil(t, ws.s.blockValidation.quickWindow.Lookup(block1.Hash()), "block 1 must be in flight")

	exists, err := ws.s.blockchainClient.GetBlockExists(context.Background(), block1.Hash())
	require.NoError(t, err)
	require.False(t, exists, "block 1 must not be stored yet, otherwise block 2's parent is resolvable without the window")

	err2 := make(chan error, 1)

	go func() {
		err2 <- ws.s.processBlockFound(context.Background(), block2.Hash(), "peer-1", "legacy", block2)
	}()

	// Block 2 must reach the window, not the catchup channel.
	require.Eventually(t, func() bool {
		return ws.s.blockValidation.quickWindow.Lookup(block2.Hash()) != nil
	}, 10*time.Second, 10*time.Millisecond, "block 2 must be admitted to the window behind its in-flight parent")

	release()

	require.NoError(t, <-err1)
	require.NoError(t, <-err2)

	requireStored(t, ws, block1.Hash())
	requireStored(t, ws, block2.Hash())

	require.Equal(t, uint32(1), block1.Height, "block 1's height comes from the stored genesis parent")
	require.Equal(t, uint32(2), block2.Height, "block 2's height comes from its in-flight parent's entry")

	require.Equal(t, []chainhash.Hash{*block1.Hash(), *block2.Hash()}, ws.client.addedBlocks(),
		"the chain store must see the two blocks in height order")
}

// TestProcessBlockFound_UnknownParentOnWindowRouteIsAServiceErrorNotNil covers the rule that
// makes the window safe to sit in front of legacy sync: on the window route a parent that is
// neither stored nor in flight is a local fault, returned as a transient local error. The
// catch-up divert must not be taken, because it returns nil and legacy sync would mark the
// block accepted without anything having applied it.
func TestProcessBlockFound_UnknownParentOnWindowRouteIsAServiceErrorNotNil(t *testing.T) {
	ws := newWindowServer(t, "window_server_unknown_parent")

	// Shrink the parent wait: the production ten seconds is for a stalled predecessor, and
	// there is nothing to wait for here.
	restore := quickWindowParentWait
	quickWindowParentWait = 250 * time.Millisecond

	t.Cleanup(func() { quickWindowParentWait = restore })

	orphan := coinbaseOnlyBlock(t, ws, chainhash.Hash{0x01, 0x02, 0x03}, 2)

	err := ws.s.processBlockFound(context.Background(), orphan.Hash(), "peer-1", "legacy", orphan)
	require.Error(t, err, "a window-route block with an unknown parent must never come back nil")
	require.True(t, errors.IsTransientLocalError(err), "the failure is ours, not the peer's: %v", err)
	require.Contains(t, err.Error(), "neither stored nor in flight")

	require.Empty(t, ws.client.addedBlocks(), "nothing may be stored for a block whose parent is unknown")
}

// TestProcessBlockFound_DuplicateInFlightBlockReturnsTheLiveOutcome pins the dedup: legacy
// sync can deliver the same block twice, and the second delivery must wait for the live
// attempt and return its outcome rather than start a second one.
func TestProcessBlockFound_DuplicateInFlightBlockReturnsTheLiveOutcome(t *testing.T) {
	ws := newWindowServer(t, "window_server_duplicate")

	block1 := coinbaseOnlyBlock(t, ws, ws.genesis, 1)

	// The second delivery gets its own block struct, the way it really arrives: every
	// ProcessBlock call parses the wire bytes into a fresh model.Block. Sharing one struct
	// between the two calls would be a test-only data race on block.Height.
	raw, err := block1.Bytes()
	require.NoError(t, err)

	redelivered, err := model.NewBlockFromBytes(raw)
	require.NoError(t, err)
	require.True(t, block1.Hash().IsEqual(redelivered.Hash()), "both deliveries must be the same block")

	arrived, release := ws.client.hold(block1.Hash())

	first := make(chan error, 1)

	go func() {
		first <- ws.s.processBlockFound(context.Background(), block1.Hash(), "peer-1", "legacy", block1)
	}()

	select {
	case <-arrived:
	case err := <-first:
		t.Fatalf("the first delivery returned before reaching its commit: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("the first delivery never reached its commit")
	}

	// The live entry must be there before the second delivery starts, otherwise the second
	// call could take the block-already-exists early return and the test would pass without
	// ever reaching the duplicate branch.
	require.NotNil(t, ws.s.blockValidation.quickWindow.Lookup(block1.Hash()), "the first delivery must be in flight")

	var (
		second     = make(chan error, 1)
		secondDone atomic.Bool
	)

	go func() {
		err := ws.s.processBlockFound(context.Background(), redelivered.Hash(), "peer-2", "legacy", redelivered)
		secondDone.Store(true)
		second <- err
	}()

	// The duplicate must park on the live attempt. If it had started its own, or taken the
	// early return, it would come back while the first is still held in its commit.
	require.Never(t, secondDone.Load, 300*time.Millisecond, 20*time.Millisecond,
		"the duplicate returned while the live attempt was still held in its commit")

	release()

	require.NoError(t, <-first)
	require.NoError(t, <-second, "the duplicate must return the live attempt's outcome")

	requireStored(t, ws, block1.Hash())
	require.Equal(t, 1, ws.client.countOf(block1.Hash()), "the block must be added to the chain store exactly once")
}

// windowChainLen is the length of the window's ordered commit chain, which is not the same as
// the number of entries resident in it: a failed or committed entry is dropped from the chain
// immediately but stays resident, holding its dedup slot, until its owner calls Leave.
func windowChainLen(w *quickWindow) int {
	w.mu.Lock()
	defer w.mu.Unlock()

	return len(w.entries)
}

// gateStub is a block-assembly client reporting a height the gate will never accept, so
// blockassemblyutil.WaitForBlockAssemblyReady keeps retrying until the caller's context runs
// out. observe is called on every gate evaluation, which is how the test proves the block was
// already admitted by the time the gate ran. Every other method of the interface is nil: the
// gate is the only thing processBlockFound asks a block-assembly client for.
type gateStub struct {
	blockassembly.ClientI

	observe func() bool

	mu       sync.Mutex
	observed []bool
}

func (g *gateStub) GetBlockAssemblyState(_ context.Context) (*blockassembly_api.StateMessage, error) {
	g.mu.Lock()
	g.observed = append(g.observed, g.observe())
	g.mu.Unlock()

	return &blockassembly_api.StateMessage{CurrentHeight: 0}, nil
}

// observations returns what observe saw, in call order.
func (g *gateStub) observations() []bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	return append([]bool(nil), g.observed...)
}

// TestProcessBlockFound_GateFailureAfterAdmissionUnwindsTheEntry pins the ordering this task
// exists for and the unwind that pays for it. The block-assembly gate can park a block for as
// long as block assembly is behind; a block parked outside the window holds no slot in it, so
// the successor legacy sync is already sending would find no parent to chain to. Admission
// therefore comes first, and the gate's own failure has to hand the slot back: wrapped as a
// local fault so legacy sync neither rejects the block nor rotates the peer, then failed and
// left so the window drains behind it.
func TestProcessBlockFound_GateFailureAfterAdmissionUnwindsTheEntry(t *testing.T) {
	// maxBlocksBehind 0 means block assembly must be at or above the block's own height, and
	// the stub reports height 0, so the gate can never pass. It also collapses the window to
	// one block: an allowance of 0 or 1 is the tightest possible gate, so the shared depth rule
	// floors the cap at 1. This test admits a single block, so that costs it nothing.
	ws := newWindowServer(t, "window_server_gate", func(s *settings.Settings) {
		s.BlockValidation.MaxBlocksBehindBlockAssembly = 0
	})

	w := ws.s.blockValidation.quickWindow
	require.Equal(t, 1, w.Depth(), "an allowance of 0 floors the window at one block")

	block1 := coinbaseOnlyBlock(t, ws, ws.genesis, 1)

	stub := &gateStub{observe: func() bool { return w.Lookup(block1.Hash()) != nil }}
	ws.s.blockAssemblyClient = stub

	// The gate's retry ladder is fixed at 100 attempts, so the caller's context is what ends
	// it, exactly as it would on a shutdown with block assembly still behind.
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	err := ws.s.processBlockFound(ctx, block1.Hash(), "peer-1", "legacy", block1)
	require.Error(t, err, "a block the gate never let through must not come back nil")
	require.True(t, errors.IsTransientLocalError(err), "a parked gate is our condition, not the peer's: %v", err)
	require.Contains(t, err.Error(), "block assembly not ready")

	observed := stub.observations()
	require.NotEmpty(t, observed, "the gate must have been evaluated")
	require.True(t, observed[0], "the block must already be admitted the first time the gate is evaluated")

	require.Nil(t, w.Lookup(block1.Hash()), "the gate failure must leave the window")
	require.Zero(t, windowChainLen(w), "and must not leave the commit chain blocked behind it")
	require.Empty(t, ws.client.addedBlocks(), "nothing may be stored for a block the gate refused")
}

// TestProcessBlockFound_ParentThatLeftTheWindowWithoutCommittingIsRefused covers the gap
// between finding a parent in flight and admitting behind it. A parent that fails in that gap
// is dropped from the commit chain but stays resident, so the child still resolves it and then
// gets admitted into an empty window, which Admit reads as "the caller confirmed the parent is
// stored". It is not stored, and committing the child would put its height in the chain store
// with the parent's missing, so the child is refused as a local fault.
func TestProcessBlockFound_ParentThatLeftTheWindowWithoutCommittingIsRefused(t *testing.T) {
	ws := newWindowServer(t, "window_server_parent_left")

	w := ws.s.blockValidation.quickWindow

	block1 := coinbaseOnlyBlock(t, ws, ws.genesis, 1)
	block2 := coinbaseOnlyBlock(t, ws, *block1.Hash(), 2)

	// Put the parent in the window and fail it without ever committing or leaving it, which is
	// what an aborted predecessor looks like to a child arriving right behind it.
	parent, duplicate, err := w.Admit(context.Background(), block1)
	require.NoError(t, err)
	require.False(t, duplicate)

	parent.Fail(errors.NewServiceError("test: the parent failed"))

	require.Eventually(t, func() bool { return windowChainLen(w) == 0 }, 10*time.Second, 10*time.Millisecond,
		"the failed parent must be dropped from the commit chain")
	require.NotNil(t, w.Lookup(block1.Hash()), "and must still be resident, because nothing has Left it")

	err = ws.s.processBlockFound(context.Background(), block2.Hash(), "peer-1", "legacy", block2)
	require.Error(t, err, "a child whose parent failed must not come back nil")
	require.True(t, errors.IsTransientLocalError(err), "the parent's failure is ours, not the peer's: %v", err)
	require.Contains(t, err.Error(), "left the window without committing")

	require.Nil(t, w.Lookup(block2.Hash()), "the child must have left the window it was refused from")
	require.Empty(t, ws.client.addedBlocks(), "neither block may reach the chain store")
}

// TestProcessBlockFound_LegacyBlockWithTheWindowOffFailsClosed covers the settings mismatch
// between the two services that read blockvalidation_quick_window_blocks. With the window on in
// legacy sync and off here, legacy hands over block N+1 while N is still in flight — and N is
// in flight nowhere this service can see, so its parent is simply missing. The catch-up divert
// would return nil, which legacy records as an accepted block with nothing stored, so the block
// is refused with a local fault naming the setting instead. A legacy block never belongs in the
// divert anyway: legacy resolves its own orphans with a getblocks.
func TestProcessBlockFound_LegacyBlockWithTheWindowOffFailsClosed(t *testing.T) {
	ws := newWindowServer(t, "window_server_off_legacy", func(s *settings.Settings) {
		s.BlockValidation.QuickWindowBlocks = 0
	})

	require.False(t, ws.s.blockValidation.quickWindow.Enabled(), "precondition: the window is off on this service")

	orphan := coinbaseOnlyBlock(t, ws, chainhash.Hash{0x09, 0x08, 0x07}, 2)

	err := ws.s.processBlockFound(context.Background(), orphan.Hash(), "peer-1", "legacy", orphan)
	require.Error(t, err, "a legacy block with no stored parent must never come back nil")
	require.True(t, errors.IsTransientLocalError(err), "a settings mismatch is ours, not the peer's: %v", err)
	require.Contains(t, err.Error(), "blockvalidation_quick_window_blocks")

	require.Empty(t, ws.client.addedBlocks(), "nothing may be stored for a block whose parent is unknown")
}

// TestProcessBlockFound_NonLegacyBlockKeepsTheCatchupDivert is the other half of the rule
// above: only a legacy block is refused. Everything else with a missing parent still goes to
// catch-up, which is how the native path has always resolved an orphan, and still returns nil.
func TestProcessBlockFound_NonLegacyBlockKeepsTheCatchupDivert(t *testing.T) {
	ws := newWindowServer(t, "window_server_off_native", func(s *settings.Settings) {
		s.BlockValidation.QuickWindowBlocks = 0
	})

	orphan := coinbaseOnlyBlock(t, ws, chainhash.Hash{0x06, 0x05, 0x04}, 2)

	err := ws.s.processBlockFound(context.Background(), orphan.Hash(), "peer-1", "http://peer:8000", orphan)
	require.NoError(t, err, "a non-legacy block with a missing parent still takes the catch-up divert")

	require.Eventually(t, func() bool { return len(ws.s.catchupCh) == 1 }, 5*time.Second, 10*time.Millisecond,
		"the block must have been handed to catch-up")
	require.Empty(t, ws.client.addedBlocks())
}

// capturingLogger records the lines the window's startup log writes, so the depth resolution
// can be asserted on rather than merely run.
type capturingLogger struct {
	ulogger.TestLogger

	mu    sync.Mutex
	lines []string
}

func (l *capturingLogger) Infof(format string, args ...interface{}) { l.record(format, args...) }
func (l *capturingLogger) Warnf(format string, args ...interface{}) { l.record(format, args...) }

func (l *capturingLogger) record(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *capturingLogger) recorded() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.lines...)
}

// TestQuickWindowDepth_ResolvesFromSettingsAndLogsTheClamp pins block validation's half of the
// one-depth-rule: the number comes from the settings helper legacy sync also calls, and the
// startup line says what was resolved and why. The expected numbers are written out, so this
// test is not the code's own oracle.
func TestQuickWindowDepth_ResolvesFromSettingsAndLogsTheClamp(t *testing.T) {
	cases := []struct {
		name        string
		blocks      int
		skipLock    bool
		maxBehind   int
		expected    int
		expectClamp bool
	}{
		{name: "off", blocks: 0, skipLock: true, maxBehind: 20, expected: 0},
		{name: "under the cap", blocks: 4, skipLock: true, maxBehind: 20, expected: 4},
		{name: "capped at half the gate allowance", blocks: 20, skipLock: true, maxBehind: 20, expected: 10, expectClamp: true},
		{name: "forced to one without the skip-lock setting", blocks: 4, skipLock: false, maxBehind: 20, expected: 1, expectClamp: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockValidation.QuickWindowBlocks = tc.blocks
			tSettings.BlockValidation.QuickValidateSkipUtxoLock = tc.skipLock
			tSettings.BlockValidation.MaxBlocksBehindBlockAssembly = tc.maxBehind

			logger := &capturingLogger{}
			require.Equal(t, tc.expected, quickWindowDepth(tSettings, logger))

			fromSettings, _ := tSettings.BlockValidation.QuickWindowConfiguredDepth()
			require.Equal(t, fromSettings, tc.expected, "block validation must run the depth legacy sync resolves")

			lines := logger.recorded()
			require.Len(t, lines, 1, "the resolved depth is logged exactly once at startup")
			require.Contains(t, lines[0], fmt.Sprintf("blockvalidation_quick_window_blocks=%d", tc.blocks))
			require.Contains(t, lines[0], fmt.Sprintf("resolved to depth %d", tc.expected))

			if tc.expectClamp {
				require.Contains(t, lines[0], "blockvalidation_", "a clamp must name the setting that caused it")
				require.Greater(t, len(lines[0]), len(fmt.Sprintf("[quickWindow] blockvalidation_quick_window_blocks=%d resolved to depth %d", tc.blocks, tc.expected)),
					"a clamped depth must say why")
			}
		})
	}
}
