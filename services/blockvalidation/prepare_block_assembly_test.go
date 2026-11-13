package blockvalidation

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newPrepareHarness is newHarnessWithSubtreeResult with a block assembly client passed to the
// constructor. Subtree validation returns subtreeErr. The blocks' subtrees are not in the subtree
// store, so a validation that gets past subtree validation fails loading them.
func newPrepareHarness(ctx context.Context, t *testing.T, ba blockassembly.ClientI, subtreeErr error) (*BlockValidation, blockchain.ClientI) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false
	tSettings.ChainCfgParams.Checkpoints = nil

	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
	t.Cleanup(deferFunc)

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	subtreeVal := &subtreevalidation.MockSubtreeValidation{}
	subtreeVal.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(subtreeErr)

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeVal, ba)

	return bv, blockchainClient
}

// prepareChild returns a block of one subtree on a stored parent, with the difficulty bits the
// chain expects unless bits is set.
func prepareChild(ctx context.Context, t *testing.T, client blockchain.ClientI, bits *model.NBit) *model.Block {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	parent := storeCachedHeadersParent(ctx, t, client, tSettings, false)

	timestamp := uint32(time.Now().Unix()) //nolint:gosec

	if bits == nil {
		expected, err := client.GetNextWorkRequired(ctx, parent.Hash(), int64(timestamp))
		require.NoError(t, err)

		bits = expected
	}

	coinbaseTx := coinbaseAtHeight(t, 2)
	hdr := minedHeaderWithBits(t, parent.Hash(), coinbaseTx.TxIDChainHash(), *bits, timestamp)

	subtree := chainhash.HashH([]byte("prepare-block-assembly"))

	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{&subtree}, 2, 1000, 2, 0)
	require.NoError(t, err)

	return block
}

// requireNotPrepared asserts that block assembly was not told about a block. The hint is sent
// without waiting, so give a stray one time to arrive.
func requireNotPrepared(t *testing.T, ba *blockassembly.Mock) {
	t.Helper()

	time.Sleep(200 * time.Millisecond)
	ba.AssertNotCalled(t, "PrepareBlock", mock.Anything, mock.Anything)
}

// A block from a peer is handed to block assembly once its header checks and subtree validation
// passed, while the block itself is still being validated.
func TestValidateBlock_PreparesBlockAssemblyAfterSubtreeValidation(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ba := blockassembly.NewMock()
	bv, client := newPrepareHarness(ctx, t, ba, nil)
	block := prepareChild(ctx, t, client, nil)

	prepared := make(chan *model.Block, 1)
	ba.On("PrepareBlock", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		prepared <- args.Get(1).(*model.Block)
	}).Return(nil).Once()

	err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{DisableOptimisticMining: true})
	require.Error(t, err, "the block's subtree is not in the store")

	select {
	case b := <-prepared:
		require.True(t, b.Hash().IsEqual(block.Hash()))
		require.Equal(t, block.Subtrees, b.Subtrees)
		require.NotSame(t, block, b, "block assembly gets a copy, not the block being validated")
	case <-time.After(5 * time.Second):
		t.Fatal("block assembly was not told about the block")
	}
}

// Block assembly is not told about a block that failed subtree validation.
func TestValidateBlock_DoesNotPrepareBlockAssemblyWhenSubtreesFail(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ba := blockassembly.NewMock()
	ba.On("PrepareBlock", mock.Anything, mock.Anything).Return(nil)

	bv, client := newPrepareHarness(ctx, t, ba, errors.NewServiceError("subtree validation stopped by the test"))
	block := prepareChild(ctx, t, client, nil)

	err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{DisableOptimisticMining: true})
	require.ErrorContains(t, err, "subtree validation stopped by the test")

	requireNotPrepared(t, ba)
}

// Block assembly is not told about a block whose difficulty bits are wrong.
func TestValidateBlock_DoesNotPrepareBlockAssemblyForWrongBits(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ba := blockassembly.NewMock()
	ba.On("PrepareBlock", mock.Anything, mock.Anything).Return(nil)

	bv, client := newPrepareHarness(ctx, t, ba, nil)

	wrong := nBitsFrom(t, "207ffffe")
	block := prepareChild(ctx, t, client, &wrong)

	err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{DisableOptimisticMining: true})
	require.Error(t, err)

	requireNotPrepared(t, ba)
}

// Catch-up does not prepare block assembly: it is behind.
func TestValidateBlock_DoesNotPrepareBlockAssemblyInCatchup(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ba := blockassembly.NewMock()
	ba.On("PrepareBlock", mock.Anything, mock.Anything).Return(nil)

	bv, client := newPrepareHarness(ctx, t, ba, nil)
	block := prepareChild(ctx, t, client, nil)

	err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{
		DisableOptimisticMining: true,
		IsCatchupMode:           true,
	})
	require.Error(t, err)

	requireNotPrepared(t, ba)
}

// Revalidating a block stored as invalid does not prepare block assembly.
func TestValidateBlock_DoesNotPrepareBlockAssemblyOnRevalidation(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ba := blockassembly.NewMock()
	ba.On("PrepareBlock", mock.Anything, mock.Anything).Return(nil)

	bv, client := newPrepareHarness(ctx, t, ba, nil)
	block := prepareChild(ctx, t, client, nil)

	require.NoError(t, client.AddBlock(ctx, block, "test", blockchainoptions.WithInvalid(true)))

	err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{
		DisableOptimisticMining: true,
		IsRevalidation:          true,
	})
	require.Error(t, err, "the block's subtree is not in the store")
	require.NotContains(t, err.Error(), "not marked as invalid", "the revalidation must get past its entry check")

	requireNotPrepared(t, ba)
}

// A failing block assembly does not fail the validation of the block.
func TestValidateBlock_PrepareBlockAssemblyErrorIsIgnored(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ba := blockassembly.NewMock()

	called := make(chan struct{}, 1)
	ba.On("PrepareBlock", mock.Anything, mock.Anything).Run(func(mock.Arguments) {
		called <- struct{}{}
	}).Return(errors.NewServiceError("block assembly down"))

	bv, client := newPrepareHarness(ctx, t, ba, nil)
	block := prepareChild(ctx, t, client, nil)

	err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{DisableOptimisticMining: true})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "block assembly down")

	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("block assembly was not told about the block")
	}
}
