package blockvalidation

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// headerIDsFailClient wraps a real blockchain client and makes GetBlockHeaderIDs fail, delegating
// every other method to the real client. It is the fault the GetBlockHeaderIDs-failure requeue needs:
// the optimistic background goroutine loads the parent header IDs before block.Valid runs, so failing
// that call drives the goroutine straight to its pre-block.Valid requeue without touching the rest of
// the real path.
type headerIDsFailClient struct {
	blockchain.ClientI
}

func (c *headerIDsFailClient) GetBlockHeaderIDs(context.Context, *chainhash.Hash, uint64) ([]uint32, error) {
	return nil, errors.NewServiceError("block header ids unavailable")
}

// The four post-AddBlock re-queues whose body has not successfully completed block.Valid must carry
// optimisticallyAdded=true, so a later corrupt verdict invalidates the on-chain body instead of
// leaving a silently-accepted corrupt tip (bitcoin-sv/teranode#4692). These tests reach each of the
// four routes on a real sqlitememory blockchain store (only the single injected fault per test is a
// wrapper around the real client) and assert the flag on the enqueued revalidateBlockData.
//
// The revalidate worker is disabled (a pre-cancelled lifecycle context) so the enqueue can be read off
// revalidateBlockChan directly rather than raced against the worker; ValidateBlock runs on its own
// request context, so the decoupled optimistic goroutine still runs to completion.

// TestOptimisticAdded_HeaderIDsFailure_RequeuesWithOnChainFlag pins the GetBlockHeaderIDs-failure
// route: the parent header-ID lookup fails before block.Valid ever runs, so the body is on-chain but
// unvalidated and the requeue must carry optimisticallyAdded=true.
func TestOptimisticAdded_HeaderIDsFailure_RequeuesWithOnChainFlag(t *testing.T) {
	initPrometheusMetrics()

	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = true

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	localClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	client := &headerIDsFailClient{ClientI: localClient}

	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	block, _ := buildFloaterBlock(t, utxoStore, subtreeStore)

	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	lifecycleCancel()

	bv := NewBlockValidation(lifecycleCtx, ulogger.TestLogger{}, tSettings, client, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	require.NoError(t, bv.ValidateBlock(context.Background(), block, "http://localhost"),
		"optimistic mining returns before background validation")

	select {
	case data := <-bv.revalidateBlockChan:
		require.Equal(t, block.Hash(), data.block.Hash())
		require.True(t, data.optimisticallyAdded,
			"the GetBlockHeaderIDs-failure requeue happens before block.Valid, so the on-chain body is unvalidated and must be flagged")
	case <-time.After(10 * time.Second):
		t.Fatal("the optimistically-added block was not re-queued after GetBlockHeaderIDs failed")
	}
}

// TestOptimisticAdded_CaughtUpFloaterInvalidateFailure_RequeuesWithOnChainFlag pins the caught-up
// floater route: block.Valid returns ErrBlockIncomplete for a floater, the RUNNING handler rolls it
// back via markBlockAsInvalid, and when that InvalidateBlock fails the requeue must carry the flag.
func TestOptimisticAdded_CaughtUpFloaterInvalidateFailure_RequeuesWithOnChainFlag(t *testing.T) {
	initPrometheusMetrics()

	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = true

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	localClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	// LocalClient hardwires FSM=RUNNING (isCaughtUp true); failing InvalidateBlock forces the caught-up
	// floater rollback to fall through to the requeue.
	client := &invalidateFailsFirstNClient{ClientI: localClient, failFirst: 999}

	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	block, _ := buildFloaterBlock(t, utxoStore, subtreeStore)

	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	lifecycleCancel()

	bv := NewBlockValidation(lifecycleCtx, ulogger.TestLogger{}, tSettings, client, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	require.NoError(t, bv.ValidateBlock(context.Background(), block, "http://localhost"),
		"optimistic mining returns before background validation")

	select {
	case data := <-bv.revalidateBlockChan:
		require.Equal(t, block.Hash(), data.block.Hash())
		require.True(t, data.optimisticallyAdded,
			"a caught-up floater whose markBlockAsInvalid failed is on-chain and unvalidated, so the requeue must be flagged")
	case <-time.After(10 * time.Second):
		t.Fatal("the optimistically-added floater was not re-queued after markBlockAsInvalid failed")
	}

	require.Equal(t, 1, client.attempts(), "the caught-up floater route must have attempted the invalidate once")
}

// TestOptimisticAdded_CatchupFloater_RequeuesWithOnChainFlag pins the transient else route: with the
// FSM in CATCHINGBLOCKS a floater is a not-yet-absorbed parent (#1031), so block.Valid's
// ErrBlockIncomplete is retried rather than invalidated — and because the body is already on-chain and
// unvalidated, that requeue must carry the flag.
func TestOptimisticAdded_CatchupFloater_RequeuesWithOnChainFlag(t *testing.T) {
	initPrometheusMetrics()

	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = true

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	localClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	// CATCHINGBLOCKS so isCaughtUp is false: the floater is transient and takes the retry (else) route.
	tracker := newTrackingBlockchainClient(localClient).withFSMState(blockchain.FSMStateCATCHINGBLOCKS)

	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	block, _ := buildFloaterBlock(t, utxoStore, subtreeStore)

	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	lifecycleCancel()

	bv := NewBlockValidation(lifecycleCtx, ulogger.TestLogger{}, tSettings, tracker, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	require.NoError(t, bv.ValidateBlock(context.Background(), block, "http://localhost"),
		"optimistic mining returns before background validation")

	select {
	case data := <-bv.revalidateBlockChan:
		require.Equal(t, block.Hash(), data.block.Hash())
		require.True(t, data.optimisticallyAdded,
			"a catchup floater retried on the else route is on-chain and unvalidated, so the requeue must be flagged")
	case <-time.After(10 * time.Second):
		t.Fatal("the optimistically-added catchup floater was not re-queued on the transient route")
	}

	require.False(t, tracker.invalidateWasCalled(),
		"#1031: a not-yet-absorbed parent in CATCHINGBLOCKS must be retried, not invalidated")
}

// TestOptimisticAdded_InvalidBodyInvalidateFailure_RequeuesWithOnChainFlag pins the ErrBlockInvalid
// route: block.Valid condemns a merkle-bound bad-coinbase-length body as invalid, the handler calls
// markBlockAsInvalid, and when that InvalidateBlock fails the on-chain unvalidated body must be
// re-queued with the flag rather than left silently accepted.
func TestOptimisticAdded_InvalidBodyInvalidateFailure_RequeuesWithOnChainFlag(t *testing.T) {
	initPrometheusMetrics()

	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = true

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	localClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	client := &invalidateFailsFirstNClient{ClientI: localClient, failFirst: 999}

	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// A merkle-bound body whose coinbase scriptSig (1 byte) is below the consensus floor: block.Valid
	// step 4b condemns it ErrBlockInvalid once the body is bound. CheckBlockSubtrees is mocked to pass,
	// so the invalid verdict comes only from block.Valid.
	coinbaseTx := shortScriptSigCoinbaseForService(t)

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(chainhash.Hash{0xAB}, 100, 0))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	replicated := subtree.Duplicate()
	replicated.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec
	merkleRoot := replicated.RootHash()

	hdr := minedBIP34Header(t, 4, tSettings.ChainCfgParams.GenesisHash, merkleRoot)
	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{subtree.RootHash()},
		uint64(subtree.Length()), uint64(coinbaseTx.Size()), 100, 0) //nolint:gosec
	require.NoError(t, err)

	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	lifecycleCancel()

	bv := NewBlockValidation(lifecycleCtx, ulogger.TestLogger{}, tSettings, client, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	require.NoError(t, bv.ValidateBlock(context.Background(), block, "http://localhost"),
		"optimistic mining returns before background validation")

	select {
	case data := <-bv.revalidateBlockChan:
		require.Equal(t, block.Hash(), data.block.Hash())
		require.True(t, data.optimisticallyAdded,
			"an invalid body whose markBlockAsInvalid failed is on-chain and unvalidated, so the requeue must be flagged")
	case <-time.After(10 * time.Second):
		t.Fatal("the optimistically-added invalid body was not re-queued after markBlockAsInvalid failed")
	}

	require.Equal(t, 1, client.attempts(), "the ErrBlockInvalid route must have attempted the invalidate once")
}
