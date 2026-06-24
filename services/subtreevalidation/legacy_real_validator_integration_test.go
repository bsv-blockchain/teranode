package subtreevalidation

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-chaincfg"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestLegacyUnconfirmedParent_RealValidatorIntegration is the end-to-end
// regression for the legacy-sync wedge with nothing mocked between the gRPC
// handler and GoBDK: subtreevalidation Server → real validator.Validator →
// real TxValidator + GoBDK → real sqlitememory UTXO store.
//
// Scenario (mainnet fixtures, height 257727 — below mainnet CSVHeight so
// neither the handler's candidate-parent MTP fetch nor the validator's MTP
// pre-load needs real headers):
//
//   - the parent tx is created in the UTXO store WITHOUT mined info, so its
//     BlockHeights is empty. This is byte-identical to the state a first
//     subtree-validation call leaves behind: blessing a tx ends in
//     utxoStore.Create without mined info (mined info only arrives via
//     SetMinedMulti after block acceptance). The parent cannot be run
//     through the handler itself here because its own input's originating
//     tx is not constructible from fixtures — the equivalence makes that a
//     non-loss.
//   - the child's subtree goes through the real legacy handler: the
//     validator's UTXO-store fallback finds empty BlockHeights and stamps
//     the unconfirmedParentHeight sentinel, the legacy branch's
//     WithUnconfirmedParentsAtCandidateHeight(true) resolves it to the
//     candidate height, and real GoBDK accepts. Without the option this
//     exact flow is the testnet-1730003 wedge
//     (bad-txns-unconfirmed-input-in-block).
//
// It also pins the store side-effect contract after blessing: child meta
// exists unmined, parent output is spent by the child — the same state a
// policy-mode mempool admission of these txs would produce. Block-level
// membership is decided later by checkParentsExistOnChain
// (BlockIncompleteError until the parent is mined, pinned in
// model/Block_test.go "parent has no block ID").
func TestLegacyUnconfirmedParent_RealValidatorIntegration(t *testing.T) {
	InitPrometheusMetrics()

	const blockHeight = uint32(257727)

	// Same real-mainnet parent/child pair as the validator-level consensus
	// tests (TestValidate_ConsensusRejectsUnconfirmedParent and its accept
	// counterpart): the child spends parent output 1 with a valid signature,
	// so GoBDK script validation genuinely runs.
	childTx, err := bt.NewTxFromString("010000000000000000ef01febe0cbd7d87d44cbd4b5adac0a5bfcdbd2b672c9113f5d74a6459a2b85569db010000008b48304502207ec38d0a4ef79c3a4286ba3e5a5b6ede1fa678af9242465140d78a901af9e4e0022100c26c377d44b761469cf0bdcdbf4931418f2c5a02ce6b72bbb7af52facd7228c1014104bc9eb4fe4cb53e35df7e7734c4c3cd91c6af7840be80f4a1fff283e2cd6ae8f7713cb263a4590263240e3c01ec36bc603c32281ac08773484dc69b8152e48cecffffffff60b74700000000001976a9148ac9bdc626352d16e18c26f431e834f9aae30e2888ac0230424700000000001976a9148ac9bdc626352d16e18c26f431e834f9aae30e2888ac1027000000000000166a148ac9bdc626352d16e18c26f431e834f9aae30e2800000000")
	require.NoError(t, err)

	parentTx, err := bt.NewTxFromString("010000000000000000ef01154d5d31268f7ea94c80a7bf6de54e47812712feec25c17b8feceb570dfd9daf000000008b4830450220612b3ec065ec2b2a1757d97b7f57fba3c363645355cf6e1a5a1834411e6ab425022100bd071b90d391eb75dc9e2eea8b6774f36bf9c55439a971f0d1f4470b6448aef601410426e4e0654f72721b97a03c8170417c9ddabadcef97fe8ea626176ea62665b55ca2ff485f84df12ddec171e01ee8f9c7472c6c8467b0cf74ae8b3b614ed16cbdbffffffff008a6600000000001976a91429be45311cc66a5a6cc4a42516dbb7c9b126a3c188ac0280841e00000000001976a914996ed5e55d68aef653c85339f83873fac1321f0788ac60b74700000000001976a9148ac9bdc626352d16e18c26f431e834f9aae30e2888ac00000000")
	require.NoError(t, err)

	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tSettings := test.CreateBaseTestSettings(t)
	// Mainnet params: the fixtures are mainnet txs and blockHeight is below
	// mainnet CSVHeight (419328), so the CSV-gated MTP paths stay inactive.
	mainnetParams := chaincfg.MainNetParams
	tSettings.ChainCfgParams = &mainnetParams
	// No block assembly: matches the legacy-sync deployment state and the
	// AddTXToBlockAssembly(false) the legacy branch always sets.
	tSettings.BlockAssembly.Disabled = true

	utxoStoreURL, err := url.Parse("sqlitememory:///legacy_real_validator_integration")
	require.NoError(t, err)
	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	require.NoError(t, utxoStore.SetBlockHeight(blockHeight))

	txStore := blobmemory.New()
	subtreeStore := blobmemory.New()

	localClient, err := blockchain.NewLocalClient(logger, tSettings, &blockchainstore.MockStore{}, subtreeStore, utxoStore)
	require.NoError(t, err)

	// The legacy branch's option pairing is FSM-gated; LocalClient always
	// reports RUNNING, so pin the state this scenario lives in.
	blockchainClient := &fsmStateOverrideClient{ClientI: localClient, state: blockchain.FSMStateCATCHINGBLOCKS}

	// Real validator: TxValidator + GoBDK inside, no mocks.
	realValidator, err := validator.New(ctx, logger, tSettings, utxoStore,
		kafka.NewKafkaAsyncProducerMock(), kafka.NewKafkaAsyncProducerMock(), nil, nil, blockchainClient)
	require.NoError(t, err)

	// Parent in the store, unmined — BlockHeights empty: the wedge
	// precondition (see the function comment for why this Create is
	// equivalent to a first handler call having blessed it).
	_, err = utxoStore.Create(ctx, parentTx, blockHeight-1)
	require.NoError(t, err)

	parentMeta, err := utxoStore.Get(ctx, parentTx.TxIDChainHash(), fields.BlockHeights)
	require.NoError(t, err)
	require.Empty(t, parentMeta.BlockHeights, "parent must be unmined before the child validates — this is the wedge precondition")

	// Child's subtree as legacy files (full serialization + subtreeData).
	childSubtree, err := subtreepkg.NewTreeByLeafCount(1)
	require.NoError(t, err)
	require.NoError(t, childSubtree.AddNode(*childTx.TxIDChainHash(), 121, 0))

	childSubtreeBytes, err := childSubtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(t.Context(), childSubtree.RootHash()[:], fileformat.FileTypeSubtreeToCheck, childSubtreeBytes))
	require.NoError(t, subtreeStore.Set(t.Context(), childSubtree.RootHash()[:], fileformat.FileTypeSubtreeData, childTx.ExtendedBytes()))

	nilConsumer := &kafka.KafkaConsumerGroup{}

	server, err := New(ctx, logger, tSettings, subtreeStore, txStore, utxoStore, realValidator, blockchainClient, nilConsumer, nilConsumer, nil, nil)
	require.NoError(t, err)

	response, err := server.CheckSubtreeFromBlock(ctx, &subtreevalidation_api.CheckSubtreeFromBlockRequest{
		Hash:              childSubtree.RootHash()[:],
		BaseUrl:           "legacy",
		BlockHeight:       blockHeight,
		BlockHash:         make([]byte, 32),
		PreviousBlockHash: make([]byte, 32),
	})
	require.NoError(t, err, "child of an unmined same-block parent must validate on the legacy path — failure here is the bad-txns-unconfirmed-input-in-block wedge")
	require.True(t, response.Blessed)

	// Side-effect contract after blessing.
	childMeta, err := utxoStore.Get(ctx, childTx.TxIDChainHash(), fields.BlockIDs, fields.BlockHeights)
	require.NoError(t, err)
	require.Empty(t, childMeta.BlockIDs)
	require.Empty(t, childMeta.BlockHeights)

	spentVout := childTx.Inputs[0].PreviousTxOutIndex
	parentUtxoHash, err := util.UTXOHashFromOutput(parentTx.TxIDChainHash(), parentTx.Outputs[spentVout], spentVout)
	require.NoError(t, err)

	spendStatus, err := utxoStore.GetSpend(ctx, &utxostore.Spend{
		TxID:     parentTx.TxIDChainHash(),
		Vout:     spentVout,
		UTXOHash: parentUtxoHash,
	})
	require.NoError(t, err)
	require.NotNil(t, spendStatus.SpendingData, "parent output must be spent by the blessed child")
	require.Equal(t, *childTx.TxIDChainHash(), *spendStatus.SpendingData.TxID)
}
