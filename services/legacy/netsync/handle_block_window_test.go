package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/legacy/testdata"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// getBlockHeaderRefusingClient wraps blockchain.Mock and fails the test if
// GetBlockHeader is ever called. A hand-written override rather than a fully
// scripted testify expectation is fine here: the point under test is that
// HandleBlockDirect never consults the blockchain store for a parent's height
// once the dispatcher has already resolved it, not what GetBlockHeader would
// have returned had it been called.
type getBlockHeaderRefusingClient struct {
	*blockchain.Mock
	t *testing.T
}

func (c *getBlockHeaderRefusingClient) GetBlockHeader(_ context.Context, _ *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	c.t.Fatal("GetBlockHeader must not be called when the parent height is already resolved")
	return nil, nil, nil
}

// TestHandleBlockDirect_UsesResolvedParentHeight proves two things at once: the
// blockchain store is never consulted for the parent (GetBlockHeader fails the
// test if called), and the height HandleBlockDirect derives from the resolved
// parent (99 + 1 = 100) is the one that actually reaches prepareSubtrees. The
// second half rides on subtreeValidation's CheckSubtreeFromBlock mock: it only
// matches calls carrying blockHeight 100, so a wrong height fails as an
// unexpected-call panic rather than silently passing.
func TestHandleBlockDirect_UsesResolvedParentHeight(t *testing.T) {
	initPrometheusMetrics()

	// A real mined block: its header genuinely satisfies HasMetTargetDifficulty,
	// so the test doesn't need to fake proof-of-work. Height arrives unset
	// (BlockHeightUnknown), exactly like a block just off the wire.
	block, err := testdata.ReadBlockFromFile("../testdata/00000000000000000ad4cd15bbeaf6cb4583c93e13e311f9774194aadea87386.bin")
	require.NoError(t, err)
	require.LessOrEqual(t, block.Height(), int32(0), "fixture must arrive with height unset, like a real wire block")

	blockchainClient := &getBlockHeaderRefusingClient{Mock: &blockchain.Mock{}, t: t}
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	blockchainClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)

	blockAssembly := blockassembly.NewMock()
	blockAssembly.On("GetBlockAssemblyState", mock.Anything).Return(&blockassembly_api.StateMessage{CurrentHeight: 100}, nil)

	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.On("CheckSubtreeFromBlock", mock.Anything, mock.Anything, "legacy", uint32(100), mock.Anything, mock.Anything).Return(nil)

	sm := &SyncManager{
		settings:          test.CreateBaseTestSettings(t),
		logger:            ulogger.TestLogger{},
		orphanTxs:         expiringmap.New[chainhash.Hash, *orphanTxAndParents](10),
		blockchainClient:  blockchainClient,
		blockAssembly:     blockAssembly,
		utxoStore:         &nullstore.NullStore{},
		subtreeStore:      memory.New(),
		subtreeValidation: subtreeValidationClient,
		blockValidation:   &blockvalidation.MockBlockValidation{},
	}
	defer sm.orphanTxs.Stop()

	err = sm.HandleBlockDirect(context.Background(), &peer.Peer{}, *block.Hash(), block.MsgBlock(), &inflightParent{height: 99})
	require.NoError(t, err)

	subtreeValidationClient.AssertExpectations(t)
}

// TestHandleBlockDirect_GateFailureOnWindowRouteIsALocalFault proves that when the
// block-assembly gate fails for a block on the quick-window route, the error
// HandleBlockDirect returns is a local fault (errors.IsTransientLocalError), not
// a bare passthrough of WaitForBlockAssemblyReady's error — so a parked gate
// throttles re-delivery through the local-fault path instead of churning the
// sync peer as if the block itself were bad.
func TestHandleBlockDirect_GateFailureOnWindowRouteIsALocalFault(t *testing.T) {
	initPrometheusMetrics()

	const checkpointHeight = int32(1000)
	const parentHeight = uint32(499)
	const blockHeight = parentHeight + 1 // 500, below checkpointHeight

	tSettings, params := newOutpointOnlySettings(t, true, true, checkpointHeight)
	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	tSettings.BlockValidation.QuickWindowBlocks = 2

	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: parentHeight}, nil)

	// Block assembly reports a height far behind the block under test, so the
	// gate never opens. The bound on how long the test waits for that to give
	// up comes from the short context below, not from these mocked calls —
	// WaitForBlockAssemblyReady retries up to 100 times on a linear backoff.
	blockAssembly := blockassembly.NewMock()
	blockAssembly.On("GetBlockAssemblyState", mock.Anything).Return(&blockassembly_api.StateMessage{CurrentHeight: 0}, nil)

	sm := &SyncManager{
		settings:         tSettings,
		logger:           ulogger.TestLogger{},
		chainParams:      params,
		blockchainClient: blockchainClient,
		blockAssembly:    blockAssembly,
		utxoStore:        &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}},
	}

	require.True(t, sm.windowRoute(blockHeight), "precondition: this block must take the quick-window route")

	msgBlock := &wire.MsgBlock{Header: wire.BlockHeader{}}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	err := sm.HandleBlockDirect(ctx, &peer.Peer{}, chainhash.Hash{0x01}, msgBlock, nil)
	require.Error(t, err)
	require.True(t, errors.IsTransientLocalError(err), "a parked gate on the window route must be a local fault, got: %v", err)
}

// No blockchain client returns a nil meta alongside a nil error, but the height this lookup
// produces is what decides whether the block takes the window route, so resolveParent must
// fail closed on it rather than dereference the nil. Block validation already guards the same
// lookup in processBlockFound; this pins the legacy side to the same answer.
func TestResolveParent_NilParentMetaFailsClosed(t *testing.T) {
	initPrometheusMetrics()

	const checkpointHeight = int32(1000)

	tSettings, params := newOutpointOnlySettings(t, true, true, checkpointHeight)
	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	tSettings.BlockValidation.QuickWindowBlocks = 2

	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(&model.BlockHeader{}, (*model.BlockHeaderMeta)(nil), nil)

	sm := &SyncManager{
		settings:         tSettings,
		logger:           ulogger.TestLogger{},
		chainParams:      params,
		ctx:              context.Background(),
		blockchainClient: blockchainClient,
		utxoStore:        &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}},
	}
	sm.dispatcher = newBlockDispatcher(sm)

	require.True(t, sm.windowRouteEnabled(), "precondition: the window route must be on for the lookup to run at all")

	// A head-detected failure runs the tail's failure arm; catchingBlocks keeps it
	// from pushing a reject at the bare peer.
	d := &blockDispatch{
		msg:            &blockQueueMsg{blockHash: chainhash.Hash{0x01}, peer: &peer.Peer{}},
		peer:           &peer.Peer{},
		prevHash:       chainhash.Hash{0x02},
		catchingBlocks: true,
	}

	require.NotPanics(t, func() {
		finished, err := sm.resolveParent(d, &wire.MsgBlock{})

		require.True(t, finished, "a parent height that cannot be resolved finishes the head")
		require.Error(t, err, "and it finishes with an error rather than a silent nil route")
		require.Zero(t, d.height, "no height is derived from a nil meta")
		require.False(t, d.windowed, "and the block is never marked windowed on the back of one")
	})
}
