package blockvalidation

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/settings"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestValidateBlock_OptimisticPeerBlocks_SubtreeLessUnboundBodyRejectedBeforeAdd is a regression for
// bitcoin-sv/teranode#4844. When an operator opts into optimistic mining for peer-served blocks, the
// received body is added to the chain BEFORE block.Valid runs. A body carrying no subtrees used to
// be added unbound — transiently visible as a valid chain tip — and then persisted as invalid with
// the peer's chosen coinbase. The coinbase-only binding is pure (header plus coinbase), so it now
// runs before the add: the body is rejected as corrupt, the serving peer is struck, and nothing is
// written.
//
// A body CARRYING subtrees is bound by the integrity floor instead; the tests below cover it.
func TestValidateBlock_OptimisticPeerBlocks_SubtreeLessUnboundBodyRejectedBeforeAdd(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = true
	tSettings.BlockValidation.OptimisticMiningPeerBlocks = true
	tSettings.ChainCfgParams.Checkpoints = nil

	bv, client := newNoPersistHarness(ctx, t, tSettings)

	fake := &corruptStrikeP2PClient{}
	bv.p2pClient = fake

	const blockHeight = uint32(1)

	timestamp := uint32(time.Now().Unix()) //nolint:gosec

	expected, err := client.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
	require.NoError(t, err)
	require.NotNil(t, expected)

	// An unbound body: a single-transaction block whose header merkle root is NOT its coinbase
	// txid, so nothing reconciles the body to the header.
	coinbaseTx := canaryCoinbaseAtHeight(t, blockHeight)
	unboundRoot := chainhash.Hash{0xCD}
	hdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, &unboundRoot, *expected, timestamp)

	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), blockHeight, 0)
	require.NoError(t, err)

	err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{
		PeerID:                  "peer-serving",
		DisableOptimisticMining: optimisticMiningDisabledForPeerPath(tSettings, "http://localhost"),
	})
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "an unbound subtree-less body must be rejected as corrupt, got: %v", err)

	exists, err := client.GetBlockExists(ctx, block.Hash())
	require.NoError(t, err)
	require.False(t, exists, "the unbound body must not be added to the chain")

	calls := fake.recorded()
	require.Len(t, calls, 1, "the serving peer must be struck exactly once")
	require.Equal(t, "peer-serving", calls[0].peerID)
}

// TestValidateBlockWithOptions_GlobalFlagAloneIsNotOptimistic is a regression for
// bitcoin-sv/teranode#4844: the optimistic seed in ValidateBlockWithOptions used to read the global
// flag alone, so a caller passing no override (the ValidateBlock wrapper, the revalidation worker)
// went optimistic on the default-true global flag even with the peer-blocks opt-in off. The seed now
// requires both flags.
//
// The block is bound (coinbase-only, merkle root is its coinbase txid) and its coinbase pays twice
// the subsidy, so it fails consensus after the binding. A synchronous ErrBlockInvalid is only
// possible on the non-optimistic branch; the optimistic branch adds it first and returns nil.
func TestValidateBlockWithOptions_GlobalFlagAloneIsNotOptimistic(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = true
	tSettings.BlockValidation.OptimisticMiningPeerBlocks = false
	tSettings.ChainCfgParams.Checkpoints = nil

	bv, client := newNoPersistHarness(ctx, t, tSettings)

	const blockHeight = uint32(1)

	timestamp := uint32(time.Now().Unix()) //nolint:gosec

	expected, err := client.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
	require.NoError(t, err)
	require.NotNil(t, expected)

	coinbaseTx := coinbaseAtHeight(t, blockHeight)
	// 50 BTC is the regtest subsidy at this height; pay twice that so the no-inflation check fails.
	coinbaseTx.Outputs[0].Satoshis = 2 * 50 * 100000000

	hdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash(), *expected, timestamp)

	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), blockHeight, 0)
	require.NoError(t, err)
	require.NoError(t, block.CheckCoinbaseOnlyBodyBound(), "fixture precondition: the body is bound to its header")

	err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{
		PeerID:                  "peer-serving",
		DisableOptimisticMining: false,
	})
	require.Error(t, err, "the non-optimistic branch validates before adding, so the verdict is synchronous")
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "got: %v", err)
}

// validAddRecordingClient records every AddBlock that would put a block on the chain as valid, and
// separately every AddBlock carrying WithInvalid(true), which is the record of a verdict, not an add.
type validAddRecordingClient struct {
	blockchain.ClientI

	mu          sync.Mutex
	validAdds   []chainhash.Hash
	invalidAdds []chainhash.Hash

	// subtreesSet is closed on the first SetBlockSubtreesSet, the validation's last store write on
	// success, so a test can wait for that write before it returns.
	subtreesSet     chan struct{}
	subtreesSetOnce sync.Once

	// holdSubtreesSet, when not nil, keeps every SetBlockSubtreesSet waiting until it is closed. The
	// non-optimistic path makes that write before ValidateBlockWithOptions returns and the optimistic
	// path makes it in the background, so a test can tell the two apart by whether the call returns
	// while the write is held.
	holdSubtreesSet chan struct{}

	// beforeInvalidAdd, when not nil, runs at the start of every AddBlock carrying WithInvalid(true),
	// so a test can check what has already happened when the verdict is recorded.
	beforeInvalidAdd func()
}

func (c *validAddRecordingClient) SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error {
	if c.holdSubtreesSet != nil {
		<-c.holdSubtreesSet
	}

	err := c.ClientI.SetBlockSubtreesSet(ctx, blockHash)

	c.subtreesSetOnce.Do(func() { close(c.subtreesSet) })

	return err
}

func (c *validAddRecordingClient) AddBlock(ctx context.Context, block *model.Block, peerID string, opts ...blockchainoptions.StoreBlockOption) error {
	if c.beforeInvalidAdd != nil && blockchainoptions.ProcessStoreBlockOptions(opts...).Invalid {
		c.beforeInvalidAdd()
	}

	c.mu.Lock()
	if blockchainoptions.ProcessStoreBlockOptions(opts...).Invalid {
		c.invalidAdds = append(c.invalidAdds, *block.Hash())
	} else {
		c.validAdds = append(c.validAdds, *block.Hash())
	}
	c.mu.Unlock()

	return c.ClientI.AddBlock(ctx, block, peerID, opts...)
}

func (c *validAddRecordingClient) addedValid(hash *chainhash.Hash) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, added := range c.validAdds {
		if added.IsEqual(hash) {
			return true
		}
	}

	return false
}

// invalidRecords counts the AddBlock calls that stored hash as invalid.
func (c *validAddRecordingClient) invalidRecords(hash *chainhash.Hash) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	count := 0

	for _, added := range c.invalidAdds {
		if added.IsEqual(hash) {
			count++
		}
	}

	return count
}

// newOptimisticPeerBlockHarness is newNoPersistHarness with both optimistic flags on and the
// blockchain client wrapped so a test can see whether a block was added before it was validated. The
// wrapper is handed to NewBlockValidation, not swapped in afterwards, because the goroutines it
// starts read the client. opts go to NewBlockValidation too, so a test can wire a block assembly
// client the same way. The p2p client records peer strikes, so a test can count them.
func newOptimisticPeerBlockHarness(ctx context.Context, t *testing.T, opts ...interface{}) (*BlockValidation, *validAddRecordingClient, *corruptStrikeP2PClient, *settings.Settings) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = true
	tSettings.BlockValidation.OptimisticMiningPeerBlocks = true
	tSettings.ChainCfgParams.Checkpoints = nil

	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
	t.Cleanup(deferFunc)

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	localClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	recording := &validAddRecordingClient{ClientI: localClient, subtreesSet: make(chan struct{})}

	subtreeVal := &subtreevalidation.MockSubtreeValidation{}
	subtreeVal.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, recording, subtreeStore, txStore, utxoStore, nil, subtreeVal, opts...)

	strikes := &corruptStrikeP2PClient{}
	bv.p2pClient = strikes

	return bv, recording, strikes, tSettings
}

// blockWithStoredSubtree builds a height-1 block on genesis whose single subtree holds only the
// coinbase placeholder, stores that subtree, and commits the header's merkle root to the subtree
// with committedCoinbase in the placeholder's slot. The block carries deliveredCoinbase, so passing
// a different coinbase from the committed one builds a body that is not bound to its header.
func blockWithStoredSubtree(ctx context.Context, t *testing.T, bv *BlockValidation, tSettings *settings.Settings, committedCoinbase, deliveredCoinbase *bt.Tx) *model.Block {
	t.Helper()

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, bv.subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	// The meta file too, so a background block.Valid reads it locally instead of asking the peer.
	subtreeMetaBytes, err := subtreepkg.NewSubtreeMeta(subtree).Serialize()
	require.NoError(t, err)
	require.NoError(t, bv.subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

	merkleRoot, err := subtree.RootHashWithReplaceRootNode(committedCoinbase.TxIDChainHash(), 0, uint64(committedCoinbase.Size())) //nolint:gosec
	require.NoError(t, err)

	hdr := minedBIP34Header(t, 4, tSettings.ChainCfgParams.GenesisHash, merkleRoot)

	block, err := model.NewBlock(hdr, deliveredCoinbase, []*chainhash.Hash{subtree.RootHash()},
		uint64(subtree.Length()), uint64(deliveredCoinbase.Size()), 1, 0) //nolint:gosec
	require.NoError(t, err)

	return block
}

// requireNotTip asserts the chain tip is still genesis, so the block never became the tip.
func requireNotTip(ctx context.Context, t *testing.T, client blockchain.ClientI, tSettings *settings.Settings, hash *chainhash.Hash) {
	t.Helper()

	best, _, err := client.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.False(t, best.Hash().IsEqual(hash), "the block must never be the chain tip")
	require.True(t, best.Hash().IsEqual(tSettings.ChainCfgParams.GenesisHash), "the tip must still be genesis")
}

// TestValidateBlock_OptimisticPeerBlocks_NonFinalCoinbaseWithSubtreesNotAddedEarly: under the
// optimistic peer-block opt-in, a block carrying subtrees whose coinbase is not final must not be
// added before it is validated. The body is bound, so the integrity floor proves the coinbase is the
// block's own and then finds it non-final. The block is not added early, and the full validation
// that follows condemns it synchronously. Without the floor it was added, became the chain tip, and
// was only invalidated afterwards by the background block.Valid.
//
// The floor's failure must fall through to that full validation, not return on its own: only the
// full path records the block as invalid in the store, and a block that is not added early is
// handed to block assembly. The floor itself strikes no peer and records nothing, so the store holds
// exactly the one invalid record the full validation writes, and no corrupt strike goes to the
// serving peer (the invalid verdict reaches the peer through the Kafka invalid-block notification,
// which this harness does not wire). The block assembly hint must be launched before the full
// validation records its verdict. The hint goes out from a goroutine that production does not wait
// for, so this harness holds the invalid record until the hint arrives (5 s deadline): a hint
// launched only after the verdict fails the test.
func TestValidateBlock_OptimisticPeerBlocks_NonFinalCoinbaseWithSubtreesNotAddedEarly(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ba := blockassembly.NewMock()

	prepared := make(chan chainhash.Hash, 1)
	ba.On("PrepareBlock", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		prepared <- *args.Get(1).(*model.Block).Hash()
	}).Return(nil).Once()

	bv, client, strikes, tSettings := newOptimisticPeerBlockHarness(ctx, t, blockassembly.ClientI(ba))

	// Height-based nLockTime 500 at height 1, with a sequence number that does not opt out: not final.
	coinbaseTx := coinbaseAtHeight(t, 1)
	coinbaseTx.LockTime = 500
	coinbaseTx.Inputs[0].SequenceNumber = 0
	require.NotEmpty(t, model.CoinbaseFinalityViolation(coinbaseTx, 0, 1, tSettings.ChainCfgParams, 0), "fixture precondition: the coinbase is not final")
	require.True(t, model.IsConsensusCoinbase(coinbaseTx), "fixture precondition: the coinbase has consensus shape, so the shape gate does not catch it")

	block := blockWithStoredSubtree(ctx, t, bv, tSettings, coinbaseTx, coinbaseTx)

	var preparedBeforeVerdict atomic.Bool

	client.beforeInvalidAdd = func() {
		select {
		case hash := <-prepared:
			preparedBeforeVerdict.Store(hash.IsEqual(block.Hash()))
		case <-time.After(5 * time.Second):
		}
	}

	err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{
		PeerID:                  "peer-serving",
		DisableOptimisticMining: optimisticMiningDisabledForPeerPath(tSettings, "http://localhost"),
	})
	require.Error(t, err, "the block is validated before it is added, so the verdict is synchronous")
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "got: %v", err)
	require.Contains(t, err.Error(), "coinbase is not final")
	require.True(t, preparedBeforeVerdict.Load(), "prepareBlockAssembly must be called before the full validation records its verdict")

	require.False(t, client.addedValid(block.Hash()), "a block with a non-final coinbase must not be added before it is validated")
	requireNotTip(ctx, t, client, tSettings, block.Hash())

	_, meta, err := client.GetBlockHeader(ctx, block.Hash())
	require.NoError(t, err, "the full validation stores the bound, condemned block")
	require.True(t, meta.Invalid, "the floor's failure falls through to the full validation, which stores the block as invalid")
	require.Equal(t, 1, client.invalidRecords(block.Hash()), "only the full validation records the block as invalid; the floor records nothing")
	require.Empty(t, strikes.recorded(), "no corrupt strike: neither the floor nor the invalid verdict calls penalizeCorruptBlockPeer")
}

// TestValidateBlock_OptimisticPeerBlocks_SwappedCoinbaseWithSubtreesNotAddedEarly: honest subtrees
// with a coinbase other than the one the header commits to. The integrity floor cannot bind the body,
// so the block is not added early, and the full validation rejects it as corrupt.
//
// The floor's corrupt failure must fall through like any other: the full validation strikes the
// serving peer once and block assembly is told about the block. A floor that returned the corrupt
// error itself would strike nobody and skip block assembly; a floor that struck the peer as well
// would strike twice.
func TestValidateBlock_OptimisticPeerBlocks_SwappedCoinbaseWithSubtreesNotAddedEarly(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ba := blockassembly.NewMock()

	prepared := make(chan chainhash.Hash, 1)
	ba.On("PrepareBlock", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		prepared <- *args.Get(1).(*model.Block).Hash()
	}).Return(nil).Once()

	bv, client, strikes, tSettings := newOptimisticPeerBlockHarness(ctx, t, blockassembly.ClientI(ba))

	committed := coinbaseAtHeight(t, 1)
	delivered := coinbaseAtHeight(t, 1)
	require.False(t, committed.TxIDChainHash().IsEqual(delivered.TxIDChainHash()), "fixture precondition: the two coinbases differ")

	block := blockWithStoredSubtree(ctx, t, bv, tSettings, committed, delivered)

	err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{
		PeerID:                  "peer-serving",
		DisableOptimisticMining: optimisticMiningDisabledForPeerPath(tSettings, "http://localhost"),
	})
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "an unbound body must be rejected as corrupt, got: %v", err)

	require.False(t, client.addedValid(block.Hash()), "a body not bound to its header must not be added before it is validated")
	requireNotTip(ctx, t, client, tSettings, block.Hash())
	require.Zero(t, client.invalidRecords(block.Hash()), "an unbound body is never recorded as invalid")

	calls := strikes.recorded()
	require.Len(t, calls, 1, "the full validation strikes the serving peer once, and the floor strikes nobody")
	require.Equal(t, "peer-serving", calls[0].peerID)

	select {
	case hash := <-prepared:
		require.True(t, hash.IsEqual(block.Hash()))
	case <-time.After(5 * time.Second):
		t.Fatal("a block the floor turned away must still be handed to block assembly")
	}
}

// TestValidateBlock_OptimisticPeerBlocks_HonestBodyWithSubtreesAddedEarly is the control for the two
// tests above: an honest block carrying subtrees passes the integrity floor and is still added
// before the background validation, so a floor that refused everything would fail here.
//
// Both paths end with the block added and its subtrees set, so the test holds the subtrees-set write.
// Only the optimistic path returns while it is held, having already added the block; the
// non-optimistic path makes that write before it returns.
func TestValidateBlock_OptimisticPeerBlocks_HonestBodyWithSubtreesAddedEarly(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bv, client, _, tSettings := newOptimisticPeerBlockHarness(ctx, t)

	hold := make(chan struct{})
	client.holdSubtreesSet = hold

	released := false
	release := func() {
		if !released {
			released = true
			close(hold)
		}
	}
	defer release()

	coinbaseTx := coinbaseAtHeight(t, 1)
	block := blockWithStoredSubtree(ctx, t, bv, tSettings, coinbaseTx, coinbaseTx)

	done := make(chan error, 1)
	go func() {
		done <- bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{
			PeerID:                  "peer-serving",
			DisableOptimisticMining: optimisticMiningDisabledForPeerPath(tSettings, "http://localhost"),
		})
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		release()
		t.Logf("validation returned once the write was released: %v", <-done)
		t.Fatal("the validation waited for the subtrees-set write, so the block took the non-optimistic path")
	}

	require.True(t, client.addedValid(block.Hash()), "an honest block must take the optimistic add, before its validation finishes")

	release()

	// Wait for the background validation to finish, so it does not outlive the test.
	select {
	case <-client.subtreesSet:
	case <-time.After(10 * time.Second):
		t.Fatal("the background validation of the honest block did not complete")
	}
}
