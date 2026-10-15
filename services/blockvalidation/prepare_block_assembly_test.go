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
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newPrepareHarness is newHarnessWithSubtreeResult with a block assembly client passed to the
// constructor. Subtree validation fails, so a validation stops right after the header checks.
func newPrepareHarness(ctx context.Context, t *testing.T, ba blockassembly.ClientI) (*BlockValidation, blockchain.ClientI) {
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
	subtreeVal.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.NewServiceError("subtree validation stopped by the test"))

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeVal, ba)

	return bv, blockchainClient
}

// prepareChild returns a block of one subtree on a stored parent, with the difficulty bits the
// chain expects.
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

// A block from a peer is handed to block assembly once its header checks passed, before the
// block itself is validated.
func TestValidateBlock_PreparesBlockAssemblyAfterHeaderChecks(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ba := blockassembly.NewMock()
	bv, client := newPrepareHarness(ctx, t, ba)
	block := prepareChild(ctx, t, client, nil)

	ba.On("PrepareBlock", mock.Anything, mock.MatchedBy(func(b *model.Block) bool {
		return b.Hash().IsEqual(block.Hash())
	})).Return(nil).Once()

	err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{DisableOptimisticMining: true})
	require.Error(t, err, "subtree validation is stopped by the test")

	ba.AssertExpectations(t)
}

// Block assembly is not told about a block whose difficulty bits are wrong.
func TestValidateBlock_DoesNotPrepareBlockAssemblyForWrongBits(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ba := blockassembly.NewMock()
	ba.On("PrepareBlock", mock.Anything, mock.Anything).Return(nil)

	bv, client := newPrepareHarness(ctx, t, ba)

	wrong := nBitsFrom(t, "207ffffe")
	block := prepareChild(ctx, t, client, &wrong)

	err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{DisableOptimisticMining: true})
	require.Error(t, err)

	ba.AssertNotCalled(t, "PrepareBlock", mock.Anything, mock.Anything)
}

// Catch-up and revalidation do not prepare block assembly: it is behind or the block is old.
func TestValidateBlock_DoesNotPrepareBlockAssemblyInCatchup(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ba := blockassembly.NewMock()
	ba.On("PrepareBlock", mock.Anything, mock.Anything).Return(nil)

	bv, client := newPrepareHarness(ctx, t, ba)
	block := prepareChild(ctx, t, client, nil)

	err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{
		DisableOptimisticMining: true,
		IsCatchupMode:           true,
	})
	require.Error(t, err)

	ba.AssertNotCalled(t, "PrepareBlock", mock.Anything, mock.Anything)
}

// A failing block assembly does not fail the validation of the block.
func TestValidateBlock_PrepareBlockAssemblyErrorIsIgnored(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ba := blockassembly.NewMock()
	ba.On("PrepareBlock", mock.Anything, mock.Anything).Return(errors.NewServiceError("block assembly down"))

	bv, client := newPrepareHarness(ctx, t, ba)
	block := prepareChild(ctx, t, client, nil)

	err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{DisableOptimisticMining: true})
	require.ErrorContains(t, err, "subtree validation stopped by the test", "the validation goes on to the subtrees")

	ba.AssertExpectations(t)
}
