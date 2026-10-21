package blockvalidation

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// A block that spends inside an alert-system freeze window is rejected but not persisted
// invalid: the verdict rests on the freeze records this node holds, which depend on when the
// alert reached it, so a node that had not seen the alert accepts the same block. Persisting it
// would make the disagreement permanent until an operator RevalidateBlock (issue #1422).
func TestValidateBlock_ConsensusFreezeVerdictIsNotPersisted(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false
	tSettings.ChainCfgParams.Checkpoints = nil

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	localClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	const vout = 3

	block, _ := buildBlockSpendingParent(t, utxoStore, subtreeStore, vout, func(ctx context.Context) {
		// Mined at genesis, so the chain check passes and only the freeze can reject.
		_, _, err := utxoStore.SpendAndCreate(ctx, parentTx, 0,
			utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: model.GenesisBlockID}), utxostore.WithCreateOnly())
		require.NoError(t, err)

		utxoHash, err := util.UTXOHashFromOutput(parentTx.TxIDChainHash(), parentTx.Outputs[vout], vout)
		require.NoError(t, err)

		require.NoError(t, utxoStore.FreezeUTXOs(ctx, []*utxostore.Spend{{
			TxID: parentTx.TxIDChainHash(), Vout: vout, UTXOHash: utxoHash, FreezeFrom: 50, FreezeUntil: 150,
		}}, tSettings))
	})

	blockValidation := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, localClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	err = blockValidation.ValidateBlock(ctx, block, "http://localhost")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "the block is rejected, got: %v", err)
	require.True(t, errors.Is(err, errors.ErrUtxoConsensusFrozen), "for the freeze, got: %v", err)
	require.True(t, isLocalCatchupFault(err), "a freeze verdict is not charged to the serving peer")

	exists, err := localClient.GetBlockExists(ctx, block.Header.Hash())
	require.NoError(t, err)
	require.False(t, exists, "a freeze verdict must not be persisted")
}

// A freeze verdict from subtree validation keeps the corrupt classification an unbound
// subtree list gets (bitcoin-sv/teranode#4844), so another peer's body is tried, but strikes
// no peer and is not reported malicious on catch-up.
func TestValidateBlock_SubtreeConsensusFreezeDoesNotStrikePeer(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false
	tSettings.ChainCfgParams.Checkpoints = nil

	bv, client := newHarnessWithSubtreeResult(ctx, t, tSettings,
		errors.NewTxInvalidError("transaction spends a consensus-frozen utxo",
			errors.NewUtxoConsensusFrozenError("utxo is frozen")))

	fake := &corruptStrikeP2PClient{}
	bv.p2pClient = fake

	const blockHeight = uint32(1)

	timestamp := uint32(time.Now().Unix()) //nolint:gosec

	expected, err := client.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
	require.NoError(t, err)
	require.NotNil(t, expected)

	coinbaseTx := coinbaseAtHeight(t, blockHeight)
	hdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash(), *expected, timestamp)

	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{{0xAB}}, 2, uint64(coinbaseTx.Size()), blockHeight, 0)
	require.NoError(t, err)

	err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "got: %v", err)
	require.True(t, errors.Is(err, errors.ErrUtxoConsensusFrozen), "got: %v", err)
	require.False(t, isUnboundTxInvalidVerdict(err), "catch-up must not report the primary malicious for a freeze")

	require.Empty(t, fake.recorded(), "a freeze verdict must not strike the serving peer")

	requireNothingPersisted(ctx, t, client, block.Hash())
}
