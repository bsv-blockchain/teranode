package blockvalidation

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// A block's transactions may only spend outputs of transactions mined in that block's OWN history,
// meaning the block's parent or a block below it. Most tests here build a main chain
// genesis - P0 - P1 - P2 and a fork block F on P0. F holds a transaction C that spends an output of
// a transaction T. When T is mined only in P2, F spends a coin that does not exist on F's chain, so
// F must be rejected even though P2 is on the main chain. When T is mined in P0, F's own parent, F
// is valid.
//
// Both checkOldBlockIDs routes run, the default prefetch route and blockchain_use_in_memory_chain_check.
// The blockchain store is a real sqlitememory store, so the chain answers are the store's own.

// forkParentFundedIn names the main-chain block the funding transaction T is mined in.
type forkParentFundedIn int

const (
	fundedInP0 forkParentFundedIn = iota // F's own parent, so in F's history
	fundedInP2                           // on the main chain but not in F's history
)

// chainCheckCall records one chain-membership call and what the store answered. anchor is nil for
// CheckBlockIsInCurrentChain.
type chainCheckCall struct {
	method   string
	blockIDs []uint32
	anchor   *chainhash.Hash
	answer   bool
}

// chainCheckSpy delegates every call to the real blockchain client and records the
// CheckBlockIsInCurrentChain and CheckBlockIsAncestorOfBlock calls, so a test can show which
// answer decided a block.
type chainCheckSpy struct {
	blockchain.ClientI

	mu    sync.Mutex
	calls []chainCheckCall
}

func (s *chainCheckSpy) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	answer, err := s.ClientI.CheckBlockIsInCurrentChain(ctx, blockIDs)
	s.record(chainCheckCall{method: "CheckBlockIsInCurrentChain", blockIDs: slices.Clone(blockIDs), answer: answer})

	return answer, err
}

func (s *chainCheckSpy) CheckBlockIsAncestorOfBlock(ctx context.Context, blockIDs []uint32, blockHash *chainhash.Hash) (bool, error) {
	answer, err := s.ClientI.CheckBlockIsAncestorOfBlock(ctx, blockIDs, blockHash)
	s.record(chainCheckCall{method: "CheckBlockIsAncestorOfBlock", blockIDs: slices.Clone(blockIDs), anchor: blockHash, answer: answer})

	return answer, err
}

func (s *chainCheckSpy) record(call chainCheckCall) {
	s.mu.Lock()
	s.calls = append(s.calls, call)
	s.mu.Unlock()
}

func (s *chainCheckSpy) recorded() []chainCheckCall {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.calls)
}

// ancestorCalls returns the recorded CheckBlockIsAncestorOfBlock calls.
func (s *chainCheckSpy) ancestorCalls() []chainCheckCall {
	var out []chainCheckCall

	for _, c := range s.recorded() {
		if c.method == "CheckBlockIsAncestorOfBlock" {
			out = append(out, c)
		}
	}

	return out
}

func describeChainCheckCalls(calls []chainCheckCall) string {
	if len(calls) == 0 {
		return "none"
	}

	parts := make([]string, 0, len(calls))
	for _, c := range calls {
		parts = append(parts, fmt.Sprintf("%s%v -> %t", c.method, c.blockIDs, c.answer))
	}

	return strings.Join(parts, ", ")
}

// parentCheckStores holds the real stores one parent-check fixture builds on.
type parentCheckStores struct {
	settings     *settings.Settings
	client       blockchain.ClientI
	spy          *chainCheckSpy
	utxoStore    utxostore.Store
	subtreeStore blob.Store
	txStore      blob.Store
}

func newParentCheckStores(ctx context.Context, t *testing.T, inMemoryChainCheck bool) *parentCheckStores {
	t.Helper()

	initPrometheusMetrics()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Set the route explicitly: the default can come from the machine's settings_local.conf.
	tSettings.BlockChain.UseInMemoryChainCheck = inMemoryChainCheck

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	realClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	utxoStoreURL, err := url.Parse("sqlitememory:///forkparentonotherbranch")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	return &parentCheckStores{
		settings:     tSettings,
		client:       realClient,
		spy:          &chainCheckSpy{ClientI: realClient},
		utxoStore:    utxoStore,
		subtreeStore: blobmemory.New(),
		txStore:      blobmemory.New(),
	}
}

// newBlockValidation builds the BlockValidation under test on an already cancelled context, so its
// background workers exit at once and do not run setTxMined behind the test's back. ValidateBlock
// does not use this context. Subtree validation is mocked to succeed.
func (s *parentCheckStores) newBlockValidation(ctx context.Context, t *testing.T) *BlockValidation {
	t.Helper()

	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	bvCtx, bvCancel := context.WithCancel(ctx)
	bvCancel()

	bv := NewBlockValidation(bvCtx, ulogger.TestLogger{}, s.settings, s.spy, s.subtreeStore, s.txStore, s.utxoStore, nil, subtreeValidationClient)
	t.Cleanup(bv.StopCaches)

	return bv
}

// buildBlockSpendingFrom stores a funding transaction T as mined only in the block fundingMeta
// names, and returns a block on parent at height holding a transaction C that spends T's output.
// The block is not stored.
func (s *parentCheckStores) buildBlockSpendingFrom(ctx context.Context, t *testing.T, fundingMeta *model.BlockHeaderMeta,
	parent *chainhash.Hash, height uint32, timestamp uint32,
) (*model.Block, chainhash.Hash) {
	t.Helper()

	return s.buildBlockSpendingFromEach(ctx, t, []*model.BlockHeaderMeta{fundingMeta}, parent, height, timestamp)
}

// buildBlockSpendingFromEach is buildBlockSpendingFrom with one funding transaction per entry of
// fundingMetas, each mined only in the block its entry names. C has one input per funding
// transaction, in the order given.
func (s *parentCheckStores) buildBlockSpendingFromEach(ctx context.Context, t *testing.T, fundingMetas []*model.BlockHeaderMeta,
	parent *chainhash.Hash, height uint32, timestamp uint32,
) (*model.Block, chainhash.Hash) {
	t.Helper()

	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	childTx := bt.NewTx()

	for i, fundingMeta := range fundingMetas {
		// The index byte keeps each funding transaction's hash distinct.
		fundingTx := bt.NewTx()
		require.NoError(t, fundingTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
		fundingTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x65, 0x00, 0x00, 0x00, '/', 'F', 'u', 'n', 'd', byte(i)})
		require.NoError(t, fundingTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000))

		require.NoError(t, childTx.FromUTXOs(&bt.UTXO{
			TxIDHash:      fundingTx.TxIDChainHash(),
			Vout:          0,
			LockingScript: fundingTx.Outputs[0].LockingScript,
			Satoshis:      fundingTx.Outputs[0].Satoshis,
		}))

		_, _, err = s.utxoStore.SpendAndCreate(ctx, fundingTx, fundingMeta.Height,
			utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: fundingMeta.ID, BlockHeight: fundingMeta.Height, OnLongestChain: true}),
			utxostore.WithCreateOnly())
		require.NoError(t, err)
	}

	require.NoError(t, childTx.AddP2PKHOutputFromAddress(address.AddressString, 49*100000000))
	require.NoError(t, childTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey}))

	_, _, err = s.utxoStore.SpendAndCreate(ctx, childTx, height, utxostore.WithCreateOnly())
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*childTx.TxIDChainHash(), uint64(childTx.Size()), 0)) //nolint:gosec

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(childTx))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, s.subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeMetaBytes, err := subtreeMeta.Serialize()
	require.NoError(t, err)
	require.NoError(t, s.subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

	// The coinbase claims nothing, so the block passes the reward check at any height: the regtest
	// subsidy halves every 150 blocks and is spent long before the deep fixture's height.
	coinbaseTx := coinbaseAtHeight(t, height)
	coinbaseTx.Outputs[0].Satoshis = 0

	replicated := subtree.Duplicate()
	replicated.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec

	hdr := minedBIP34Header(t, 4, parent, replicated.RootHash())
	remineAt(t, hdr, timestamp)

	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{subtree.RootHash()},
		uint64(subtree.Length()), uint64(coinbaseTx.Size()+childTx.Size()), height, 0) //nolint:gosec
	require.NoError(t, err)

	return block, *childTx.TxIDChainHash()
}

type forkParentFixture struct {
	bv              *BlockValidation
	spy             *chainCheckSpy
	blockF          *model.Block
	childTxHash     chainhash.Hash
	fundingBlockID  uint32
	p2ID            uint32
	blockchainStore blockchain.ClientI
}

// newForkParentFixture builds genesis - P0 - P1 - P2 on the main chain and the fork block F on P0,
// with T mined only in the block fundedIn names and C, which spends T, in F. F is NOT stored, so
// ValidateBlock runs the full validation on it.
func newForkParentFixture(ctx context.Context, t *testing.T, inMemoryChainCheck bool, fundedIn forkParentFundedIn) *forkParentFixture {
	t.Helper()

	stores := newParentCheckStores(ctx, t, inMemoryChainCheck)
	realClient := stores.client

	// Rising timestamps ending in the past, so every block passes median-time-past.
	baseTime := uint32(time.Now().Add(-time.Hour).Unix()) //nolint:gosec

	// Main chain: genesis - P0 - P1 - P2.
	blockP0 := ancestorVerdictCoinbaseOnlyBlockAt(t, stores.settings.ChainCfgParams.GenesisHash, 1, baseTime+1)
	require.NoError(t, realClient.AddBlock(ctx, blockP0, "test"))

	blockP1 := ancestorVerdictCoinbaseOnlyBlockAt(t, blockP0.Hash(), 2, baseTime+2)
	require.NoError(t, realClient.AddBlock(ctx, blockP1, "test"))

	blockP2 := ancestorVerdictCoinbaseOnlyBlockAt(t, blockP1.Hash(), 3, baseTime+3)
	require.NoError(t, realClient.AddBlock(ctx, blockP2, "test"))

	_, p0Meta, err := realClient.GetBlockHeader(ctx, blockP0.Hash())
	require.NoError(t, err)

	_, p2Meta, err := realClient.GetBlockHeader(ctx, blockP2.Hash())
	require.NoError(t, err)
	require.NotZero(t, p2Meta.ID, "precondition: P2 has a real block id, not the genesis id")

	fundingMeta := p0Meta
	if fundedIn == fundedInP2 {
		fundingMeta = p2Meta
	}

	// F: the fork block on P0 at height 2, holding C.
	const forkHeight = uint32(2)

	blockF, childTxHash := stores.buildBlockSpendingFrom(ctx, t, fundingMeta, blockP0.Hash(), forkHeight, baseTime+forkHeight)

	// F's parent has had setTxMined run, as it would on a live node.
	require.NoError(t, realClient.SetBlockMinedSet(ctx, blockP0.Hash()))

	// Preconditions, all read from the real store.
	bestHeader, _, err := realClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, blockP2.Hash().String(), bestHeader.Hash().String(), "precondition: P2 is the main-chain tip")

	historyOfF, err := realClient.GetBlockHeaderIDs(ctx, blockF.Header.HashPrevBlock, 10_000)
	require.NoError(t, err)
	require.NotContains(t, historyOfF, p2Meta.ID, "precondition: P2 is not in F's history")
	require.Contains(t, historyOfF, p0Meta.ID, "precondition: P0 is in F's history")

	p2OnMain, err := realClient.CheckBlockIsInCurrentChain(ctx, []uint32{p2Meta.ID})
	require.NoError(t, err)
	require.True(t, p2OnMain, "precondition: the store reports P2 on the main chain")

	return &forkParentFixture{
		bv:              stores.newBlockValidation(ctx, t),
		spy:             stores.spy,
		blockF:          blockF,
		childTxHash:     childTxHash,
		fundingBlockID:  fundingMeta.ID,
		p2ID:            p2Meta.ID,
		blockchainStore: realClient,
	}
}

var forkParentRoutes = []struct {
	name               string
	inMemoryChainCheck bool
}{
	{name: "prefetch route", inMemoryChainCheck: false},
	{name: "in-memory chain check route", inMemoryChainCheck: true},
}

// TestCheckOldBlockIDs_RejectsForkBlockSpendingParentMinedOnlyOnOtherBranch calls checkOldBlockIDs
// with what block.Valid hands it for F: C mapped to the block ids T is mined in, here [P2].
func TestCheckOldBlockIDs_RejectsForkBlockSpendingParentMinedOnlyOnOtherBranch(t *testing.T) {
	for _, route := range forkParentRoutes {
		t.Run(route.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			fx := newForkParentFixture(ctx, t, route.inMemoryChainCheck, fundedInP2)

			oldBlockIDsMap := txmap.NewSyncedMap[chainhash.Hash, []uint32]()
			oldBlockIDsMap.Set(fx.childTxHash, []uint32{fx.p2ID})

			err := fx.bv.checkOldBlockIDs(ctx, oldBlockIDsMap, fx.blockF)

			require.Error(t, err, "fork block F was accepted although its transaction spends an output mined only in P2 (block id %d), which is not in F's history; chain calls: %s",
				fx.p2ID, describeChainCheckCalls(fx.spy.recorded()))
			require.ErrorIs(t, err, errors.ErrBlockInvalid)
			require.Contains(t, err.Error(), "are not from current chain")
		})
	}
}

// TestValidateBlock_RejectsForkBlockSpendingParentMinedOnlyOnOtherBranch runs F through
// ValidateBlock with optimistic mining off, so the verdict returns synchronously. Subtree validation
// is mocked to succeed, so the validator's spend of T's output never runs here.
func TestValidateBlock_RejectsForkBlockSpendingParentMinedOnlyOnOtherBranch(t *testing.T) {
	for _, route := range forkParentRoutes {
		t.Run(route.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			fx := newForkParentFixture(ctx, t, route.inMemoryChainCheck, fundedInP2)

			err := fx.bv.ValidateBlock(ctx, fx.blockF, "test", true)

			require.Error(t, err, "fork block F was accepted although its transaction spends an output mined only in P2 (block id %d), which is not in F's history; chain calls: %s",
				fx.p2ID, describeChainCheckCalls(fx.spy.recorded()))
			require.ErrorIs(t, err, errors.ErrBlockInvalid)
			require.Contains(t, err.Error(), "are not from current chain")
		})
	}
}

// TestValidateBlock_AcceptsForkBlockSpendingParentMinedInOwnHistory is the positive control for the
// two tests above: the same F, with T mined in P0, F's own parent, must be accepted. It shows the
// fixture can produce an accepted block, so a rejection above comes from the parent check.
func TestValidateBlock_AcceptsForkBlockSpendingParentMinedInOwnHistory(t *testing.T) {
	for _, route := range forkParentRoutes {
		t.Run(route.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			fx := newForkParentFixture(ctx, t, route.inMemoryChainCheck, fundedInP0)

			oldBlockIDsMap := txmap.NewSyncedMap[chainhash.Hash, []uint32]()
			oldBlockIDsMap.Set(fx.childTxHash, []uint32{fx.fundingBlockID})
			require.NoError(t, fx.bv.checkOldBlockIDs(ctx, oldBlockIDsMap, fx.blockF))

			require.NoError(t, fx.bv.ValidateBlock(ctx, fx.blockF, "test", true))

			_, fMeta, err := fx.blockchainStore.GetBlockHeader(ctx, fx.blockF.Hash())
			require.NoError(t, err)
			require.False(t, fMeta.Invalid)
		})
	}
}

// TestValidateBlock_AcceptsBlockSpendingParentMinedDeepOnItsOwnFork is the reverse case. The block C
// is honest: it spends a transaction mined in F1, its own ancestor 12 blocks down, on a fork that a
// longer main chain has moved off the main chain. F1 is past the 11-header window, so block.Valid
// hands it to checkOldBlockIDs. The newAncestorVerdictFixture deepFork variant builds this chain.
func TestValidateBlock_AcceptsBlockSpendingParentMinedDeepOnItsOwnFork(t *testing.T) {
	for _, route := range forkParentRoutes {
		t.Run(route.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			bv, blockchainClient, blockC, _ := newAncestorVerdictFixture(ctx, t, ancestorVerdictFixtureOpts{deepFork: true, parentMinedSet: true})

			// deepFork turns the in-memory route on; the prefetch subtest turns it back off. The
			// fixture's store and bv share the settings, so this switches the route checkOldBlockIDs takes.
			bv.settings.BlockChain.UseInMemoryChainCheck = route.inMemoryChainCheck

			validateErr := bv.ValidateBlock(ctx, blockC, "test", true)

			// Read the stored verdict before asserting, so a rejection also reports whether it was persisted.
			_, cMeta, err := blockchainClient.GetBlockHeader(ctx, blockC.Hash())
			require.NoError(t, err)
			require.NotNil(t, cMeta)

			require.NoError(t, validateErr, "an honest block spending a parent mined deep on its own fork was rejected (stored as invalid: %t)", cMeta.Invalid)
			require.False(t, cMeta.Invalid, "the honest block was stored as invalid")
		})
	}
}

// TestValidateBlock_AcceptsMainChainBlockSpendingParentMinedPastThePrefetchWindow covers a parent
// mined deeper than the prefetch route's 10,000-ID window. The main chain is genesis - B1 .. B10002,
// T is mined in B1, and block C on B10002 spends it. B1 is C's own ancestor, so C is valid. B1 is
// outside both the header window block.Valid checks and the prefetch window, so both routes must
// resolve it with the ancestor lookup and accept C.
func TestValidateBlock_AcceptsMainChainBlockSpendingParentMinedPastThePrefetchWindow(t *testing.T) {
	const chainLength = uint32(10_002)

	for _, route := range forkParentRoutes {
		t.Run(route.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			stores := newParentCheckStores(ctx, t, route.inMemoryChainCheck)

			// One minute apart, ending in the past, so every block passes median-time-past.
			baseTime := uint32(time.Now().Add(-time.Duration(chainLength+10) * time.Minute).Unix()) //nolint:gosec

			parent := stores.settings.ChainCfgParams.GenesisHash

			var blockB1 *model.Block

			for height := uint32(1); height <= chainLength; height++ {
				b := ancestorVerdictCoinbaseOnlyBlockAt(t, parent, height, baseTime+height*60)
				require.NoError(t, stores.client.AddBlock(ctx, b, "test"))

				if height == 1 {
					blockB1 = b
				}

				parent = b.Hash()
			}

			_, b1Meta, err := stores.client.GetBlockHeader(ctx, blockB1.Hash())
			require.NoError(t, err)

			childHeight := chainLength + 1
			blockC, _ := stores.buildBlockSpendingFrom(ctx, t, b1Meta, parent, childHeight, baseTime+childHeight*60)
			require.NoError(t, stores.client.SetBlockMinedSet(ctx, parent))

			window, err := stores.client.GetBlockHeaderIDs(ctx, parent, 10_000)
			require.NoError(t, err)
			require.NotContains(t, window, b1Meta.ID, "precondition: B1 is outside the prefetch window")

			bv := stores.newBlockValidation(ctx, t)

			require.NoError(t, bv.ValidateBlock(ctx, blockC, "test", true), "a main-chain block spending a parent mined past the prefetch window was rejected; chain calls: %s",
				describeChainCheckCalls(stores.spy.recorded()))

			// Both routes reached the ancestor lookup for B1, anchored at C's parent.
			ancestorCalls := stores.spy.ancestorCalls()
			require.Len(t, ancestorCalls, 1, "chain calls: %s", describeChainCheckCalls(stores.spy.recorded()))
			require.Equal(t, []uint32{b1Meta.ID}, ancestorCalls[0].blockIDs)
			require.Equal(t, parent.String(), ancestorCalls[0].anchor.String())
			require.True(t, ancestorCalls[0].answer)
		})
	}
}

// TestValidateBlock_RejectsForkBlockWhoseTxHasOneParentOffItsHistory gives C two inputs, both past
// the 11-header window (the fewest median-time-past accepts). T1 is mined in B1, deep in F's own
// history. T2 is mined only in the main-chain tip B14, which is not in F's history because F is
// built on B13. F must be rejected for T2 however
// the parent checks are scheduled: every parent is checked on its own.
func TestValidateBlock_RejectsForkBlockWhoseTxHasOneParentOffItsHistory(t *testing.T) {
	const (
		chainLength  = uint32(14)
		headerWindow = uint64(11)
	)

	for _, route := range forkParentRoutes {
		for _, offHistoryFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/off-history input first=%t", route.name, offHistoryFirst), func(t *testing.T) {
				rejectsForkBlockWithOneParentOffItsHistory(t, route.inMemoryChainCheck, offHistoryFirst, chainLength, headerWindow)
			})
		}
	}
}

// rejectsForkBlockWithOneParentOffItsHistory runs one case of the test above. offHistoryFirst puts
// T2's input before T1's. The parent checks run concurrently, so on the old child-keyed map the
// order decided which parent survived; running both orders keeps the test from passing on luck.
func rejectsForkBlockWithOneParentOffItsHistory(t *testing.T, inMemoryChainCheck, offHistoryFirst bool, chainLength uint32, headerWindow uint64) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stores := newParentCheckStores(ctx, t, inMemoryChainCheck)
	stores.settings.BlockValidation.PreviousBlockHeaderCount = headerWindow

	baseTime := uint32(time.Now().Add(-time.Hour).Unix()) //nolint:gosec

	blocks := make([]*model.Block, 0, chainLength)
	parent := stores.settings.ChainCfgParams.GenesisHash

	for height := uint32(1); height <= chainLength; height++ {
		b := ancestorVerdictCoinbaseOnlyBlockAt(t, parent, height, baseTime+height)
		require.NoError(t, stores.client.AddBlock(ctx, b, "test"))
		blocks = append(blocks, b)
		parent = b.Hash()
	}

	blockB1, blockParentOfF, blockTip := blocks[0], blocks[chainLength-2], blocks[chainLength-1]

	_, b1Meta, err := stores.client.GetBlockHeader(ctx, blockB1.Hash())
	require.NoError(t, err)

	_, tipMeta, err := stores.client.GetBlockHeader(ctx, blockTip.Hash())
	require.NoError(t, err)

	fundingMetas := []*model.BlockHeaderMeta{b1Meta, tipMeta}
	if offHistoryFirst {
		fundingMetas = []*model.BlockHeaderMeta{tipMeta, b1Meta}
	}

	// F: the fork block on B13, at the tip's height.
	blockF, _ := stores.buildBlockSpendingFromEach(ctx, t, fundingMetas, blockParentOfF.Hash(), chainLength, baseTime+chainLength)
	require.NoError(t, stores.client.SetBlockMinedSet(ctx, blockParentOfF.Hash()))

	historyOfF, err := stores.client.GetBlockHeaderIDs(ctx, blockParentOfF.Hash(), 10_000)
	require.NoError(t, err)
	require.Contains(t, historyOfF, b1Meta.ID, "precondition: B1 is in F's history")
	require.NotContains(t, historyOfF, tipMeta.ID, "precondition: the tip is not in F's history")

	headerWindowOfF, err := stores.client.GetBlockHeaderIDs(ctx, blockParentOfF.Hash(), headerWindow)
	require.NoError(t, err)
	require.NotContains(t, headerWindowOfF, b1Meta.ID, "precondition: B1 is past the header window, so checkOldBlockIDs sees it")

	bv := stores.newBlockValidation(ctx, t)

	err = bv.ValidateBlock(ctx, blockF, "test", true)

	require.Error(t, err, "fork block F was accepted although one input spends an output mined only in the tip (block id %d), which is not in F's history; chain calls: %s",
		tipMeta.ID, describeChainCheckCalls(stores.spy.recorded()))
	require.ErrorIs(t, err, errors.ErrBlockInvalid)
	require.Contains(t, err.Error(), "are not from current chain")
}
