package blockvalidation

import (
	"context"
	"fmt"
	"net/url"
	"slices"
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
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// A block's transactions may only spend outputs of transactions mined in that block's OWN history.
// These tests build a main chain genesis - P0 - P1 - P2 and a fork block F on P0. F holds a
// transaction C that spends an output of a transaction T. When T is mined only in P2, F spends a
// coin that does not exist on F's chain, so F must be rejected even though P2 is on the main chain.
// When T is mined in P0, F's own parent, F is valid.
//
// Both checkOldBlockIDs routes run, the default prefetch route and blockchain_use_in_memory_chain_check.
// The blockchain store is a real sqlitememory store, so the main-chain answers are the store's own.

// forkParentFundedIn names the main-chain block the funding transaction T is mined in.
type forkParentFundedIn int

const (
	fundedInP0 forkParentFundedIn = iota // F's own parent, so in F's history
	fundedInP2                           // on the main chain but not in F's history
)

// chainCheckCall records one CheckBlockIsInCurrentChain call and what the store answered.
type chainCheckCall struct {
	blockIDs []uint32
	onChain  bool
}

// chainCheckSpy delegates every call to the real blockchain client and records the
// CheckBlockIsInCurrentChain calls, so a test can show which answer let a block through.
type chainCheckSpy struct {
	blockchain.ClientI

	mu    sync.Mutex
	calls []chainCheckCall
}

func (s *chainCheckSpy) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	onChain, err := s.ClientI.CheckBlockIsInCurrentChain(ctx, blockIDs)

	s.mu.Lock()
	s.calls = append(s.calls, chainCheckCall{blockIDs: slices.Clone(blockIDs), onChain: onChain})
	s.mu.Unlock()

	return onChain, err
}

func (s *chainCheckSpy) recorded() []chainCheckCall {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.calls)
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

	initPrometheusMetrics()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Set the route explicitly: the default can come from the machine's settings_local.conf.
	tSettings.BlockChain.UseInMemoryChainCheck = inMemoryChainCheck

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	realClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	spy := &chainCheckSpy{ClientI: realClient}

	utxoStoreURL, err := url.Parse("sqlitememory:///forkparentonotherbranch")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	subtreeStore := blobmemory.New()
	txStore := blobmemory.New()

	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Rising timestamps ending in the past, so every block passes median-time-past.
	baseTime := uint32(time.Now().Add(-time.Hour).Unix()) //nolint:gosec

	// Main chain: genesis - P0 - P1 - P2.
	blockP0 := ancestorVerdictCoinbaseOnlyBlockAt(t, tSettings.ChainCfgParams.GenesisHash, 1, baseTime+1)
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

	// T, the funding transaction, mined only in fundingMeta's block.
	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	fundingTx := bt.NewTx()
	require.NoError(t, fundingTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	fundingTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x65, 0x00, 0x00, 0x00, '/', 'F', 'u', 'n', 'd'})
	require.NoError(t, fundingTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000))

	// C spends T's output.
	childTx := bt.NewTx()
	require.NoError(t, childTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      fundingTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: fundingTx.Outputs[0].LockingScript,
		Satoshis:      fundingTx.Outputs[0].Satoshis,
	}))
	require.NoError(t, childTx.AddP2PKHOutputFromAddress(address.AddressString, 49*100000000))
	require.NoError(t, childTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey}))

	_, _, err = utxoStore.SpendAndCreate(ctx, fundingTx, fundingMeta.Height,
		utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: fundingMeta.ID, BlockHeight: fundingMeta.Height, OnLongestChain: true}),
		utxostore.WithCreateOnly())
	require.NoError(t, err)

	_, _, err = utxoStore.SpendAndCreate(ctx, childTx, 2, utxostore.WithCreateOnly())
	require.NoError(t, err)

	// F: the fork block on P0 at height 2, holding C.
	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*childTx.TxIDChainHash(), uint64(childTx.Size()), 0)) //nolint:gosec

	subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)
	require.NoError(t, subtreeMeta.SetTxInpointsFromTx(childTx))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeMetaBytes, err := subtreeMeta.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

	const forkHeight = uint32(2)

	coinbaseTx := coinbaseAtHeight(t, forkHeight)

	replicated := subtree.Duplicate()
	replicated.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec

	hdr := minedBIP34Header(t, 4, blockP0.Hash(), replicated.RootHash())
	remineAt(t, hdr, baseTime+forkHeight)

	blockF, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{subtree.RootHash()},
		uint64(subtree.Length()), uint64(coinbaseTx.Size()+childTx.Size()), forkHeight, 0) //nolint:gosec
	require.NoError(t, err)

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

	// Build bv on an already cancelled context, so its background workers exit at once and do not
	// run setTxMined behind the test's back. ValidateBlock does not use this context.
	bvCtx, bvCancel := context.WithCancel(ctx)
	bvCancel()

	bv := NewBlockValidation(bvCtx, logger, tSettings, spy, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)
	t.Cleanup(bv.StopCaches)

	return &forkParentFixture{
		bv:              bv,
		spy:             spy,
		blockF:          blockF,
		childTxHash:     *childTx.TxIDChainHash(),
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

			require.Error(t, err, "fork block F was accepted although its transaction spends an output mined only in P2 (block id %d), which is not in F's history; CheckBlockIsInCurrentChain calls: %s",
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

			require.Error(t, err, "fork block F was accepted although its transaction spends an output mined only in P2 (block id %d), which is not in F's history; CheckBlockIsInCurrentChain calls: %s",
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

func describeChainCheckCalls(calls []chainCheckCall) string {
	if len(calls) == 0 {
		return "none"
	}

	out := ""
	for i, c := range calls {
		if i > 0 {
			out += ", "
		}

		out += fmt.Sprintf("%v -> onMainChain=%t", c.blockIDs, c.onChain)
	}

	return out
}
