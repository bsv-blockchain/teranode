//go:build test_full

package doublespendtest

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/chaincfg"
	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockvalidation"
	"github.com/bitcoin-sv/teranode/services/propagation"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	testkafka "github.com/bitcoin-sv/teranode/test/util/kafka"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type DoubleSpendTester struct {
	ctx                   context.Context
	logger                *ulogger.ErrorTestLogger
	d                     *daemon.Daemon
	blockchainClient      blockchain.ClientI
	blockAssemblyClient   *blockassembly.Client
	propagationClient     *propagation.Client
	blockValidationClient *blockvalidation.Client
	privKey               *bec.PrivateKey
	subtreeStore          blob.Store
	utxoStore             utxo.Store
}

func NewDoubleSpendTester(t *testing.T) *DoubleSpendTester {
	ctx, cancel := context.WithCancel(context.Background())

	logger := ulogger.NewErrorTestLogger(t, cancel)

	// Delete the sqlite db at the beginning of the test
	_ = os.RemoveAll("data")

	persistentStore, err := url.Parse("sqlite:///test")
	require.NoError(t, err)

	memoryStore, err := url.Parse("memory:///")
	require.NoError(t, err)

	if !isKafkaRunning() {
		kafkaContainer, err := testkafka.RunTestContainer(ctx)
		require.NoError(t, err)

		t.Cleanup(func() {
			_ = kafkaContainer.CleanUp()
		})

		gocore.Config().Set("KAFKA_PORT", strconv.Itoa(kafkaContainer.KafkaPort))
	}

	tSettings := settings.NewSettings() // This reads gocore.Config and applies sensible defaults

	// Override with test settings...
	tSettings.SubtreeValidation.SubtreeStore = memoryStore
	tSettings.BlockChain.StoreURL = persistentStore
	tSettings.UtxoStore.UtxoStore = persistentStore
	tSettings.ChainCfgParams = &chaincfg.RegressionNetParams
	tSettings.Asset.CentrifugeDisable = true

	readyCh := make(chan struct{})

	d := daemon.New()

	go d.Start(logger, []string{
		"-all=0",
		"-blockchain=1",
		"-subtreevalidation=1",
		"-blockvalidation=1",
		"-blockassembly=1",
		"-asset=1",
		"-propagation=1",
	}, tSettings, readyCh)

	<-readyCh

	bcClient, err := blockchain.NewClient(ctx, logger, tSettings, "test")
	require.NoError(t, err)

	baClient, err := blockassembly.NewClient(ctx, logger, tSettings)
	require.NoError(t, err)

	propagationClient, err := propagation.NewClient(ctx, logger, tSettings)
	require.NoError(t, err)

	blockValidationClient, err := blockvalidation.NewClient(ctx, logger, tSettings, "test")
	require.NoError(t, err)

	w, err := wif.DecodeWIF(tSettings.BlockAssembly.MinerWalletPrivateKeys[0])
	require.NoError(t, err)

	privKey := w.PrivKey

	subtreeStore, err := daemon.GetSubtreeStore(logger, tSettings)
	require.NoError(t, err)

	utxoStore, err := daemon.GetUtxoStore(ctx, logger, tSettings)
	require.NoError(t, err)

	return &DoubleSpendTester{
		ctx:                   ctx,
		logger:                logger,
		d:                     d,
		blockchainClient:      bcClient,
		blockAssemblyClient:   baClient,
		propagationClient:     propagationClient,
		blockValidationClient: blockValidationClient,
		privKey:               privKey,
		subtreeStore:          subtreeStore,
		utxoStore:             utxoStore,
	}
}

func (dst *DoubleSpendTester) verifyBlockByHeight(t *testing.T, expectedBlock *model.Block, height uint32) {
	tmpBlock, err := dst.blockchainClient.GetBlockByHeight(dst.ctx, height)
	require.NoError(t, err, "Failed to get block at height %d", height)
	assert.Equal(t, expectedBlock.Header.Hash().String(), tmpBlock.Header.Hash().String(),
		"Block hash mismatch at height %d", height)
}

func (dst *DoubleSpendTester) verifyBlockByHash(t *testing.T, expectedBlock *model.Block, hash *chainhash.Hash) {
	tmpBlock, err := dst.blockchainClient.GetBlock(dst.ctx, hash)
	require.NoError(t, err, "Failed to get block at hash %s", hash)
	assert.Equal(t, expectedBlock.Header.Hash().String(), tmpBlock.Header.Hash().String(),
		"Block hash mismatch at hash %s", hash)
}

func (dst *DoubleSpendTester) verifyConflictingInSubtrees(t *testing.T, subtreeHash *chainhash.Hash, expectedConflicts []chainhash.Hash) {
	latestSubtreeBytes, err := dst.subtreeStore.Get(dst.ctx, subtreeHash[:], options.WithFileExtension("subtree"))
	require.NoError(t, err, "Failed to get subtree")

	latestSubtree, err := util.NewSubtreeFromBytes(latestSubtreeBytes)
	require.NoError(t, err, "Failed to parse subtree bytes")

	require.Len(t, latestSubtree.ConflictingNodes, len(expectedConflicts),
		"Unexpected number of conflicting nodes in subtree")

	for i, conflict := range expectedConflicts {
		assert.Equal(t, conflict.String(), latestSubtree.ConflictingNodes[i].String(),
			"Conflicting node mismatch at index %d", i)
	}
}

func (dst *DoubleSpendTester) verifyConflictingInUtxoStore(t *testing.T, expectedConflicts []chainhash.Hash, conflictValue bool) {
	for _, conflict := range expectedConflicts {
		readTx, err := dst.utxoStore.Get(dst.ctx, &conflict)
		require.NoError(t, err, "Failed to get transaction %s", conflict.String())
		assert.Equal(t, conflictValue, readTx.Conflicting, "Expected transaction %s to be marked as conflicting", conflict.String())
	}
}

func (dst *DoubleSpendTester) createTestBlock(t *testing.T, txs []*bt.Tx, previousBlock *model.Block) (*util.Subtree, *model.Block) {
	// Create and save the subtree with the double spend tx
	subtree, err := createAndSaveSubtrees(dst.ctx, dst.subtreeStore, txs)
	require.NoError(t, err)

	address, err := bscript.NewAddressFromPublicKey(dst.privKey.PubKey(), true)
	require.NoError(t, err)

	coinbaseTx, err := model.CreateCoinbase(previousBlock.Height+1, 50e8, "test", []string{address.AddressString})
	require.NoError(t, err)

	merkleRoot, err := subtree.RootHashWithReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) // nolint:gosec
	require.NoError(t, err)

	block := &model.Block{
		Subtrees: []*chainhash.Hash{
			subtree.RootHash(),
		},
		CoinbaseTx: coinbaseTx,
		Header: &model.BlockHeader{
			HashPrevBlock:  previousBlock.Header.Hash(),
			HashMerkleRoot: merkleRoot,
			Timestamp:      uint32(time.Now().Unix()), // nolint:gosec
			Bits:           previousBlock.Header.Bits,
			Nonce:          0,
		},
		Height: previousBlock.Height + 1,
	}

	// Mine...
	for {
		ok, _, _ := block.Header.HasMetTargetDifficulty()
		if ok {
			break
		}

		block.Header.Nonce++
	}

	return subtree, block
}

func (dst *DoubleSpendTester) waitForBlockHeight(t *testing.T, height uint32, timeout time.Duration) (state *blockassembly_api.StateMessage) {
	deadline := time.Now().Add(timeout)

	var (
		err error
	)

	for state == nil || state.CurrentHeight < height {
		state, err = dst.blockAssemblyClient.GetBlockAssemblyState(dst.ctx)
		require.NoError(t, err)

		if time.Now().After(deadline) {
			t.Logf("Timeout waiting for block height %d", height)
			t.FailNow()
		}

		time.Sleep(10 * time.Millisecond)
	}

	return state
}

func createAndSaveSubtrees(ctx context.Context, subtreeStore blob.Store, txs []*bt.Tx) (*util.Subtree, error) {
	subtree, err := util.NewIncompleteTreeByLeafCount(len(txs) + 1)
	if err != nil {
		return nil, err
	}

	subtreeData := util.NewSubtreeData(subtree)

	err = subtree.AddCoinbaseNode()
	if err != nil {
		return nil, err
	}

	for i, tx := range txs {
		err = subtree.AddNode(*tx.TxIDChainHash(), uint64(i), uint64(i))
		if err != nil {
			return nil, err
		}

		err = subtreeData.AddTx(tx, 1)
		if err != nil {
			return nil, err
		}
	}

	subtreeBytes, err := subtree.Serialize()
	if err != nil {
		return nil, err
	}

	err = subtreeStore.Set(
		ctx,
		subtree.RootHash()[:],
		subtreeBytes,
		options.WithFileExtension("subtreeToCheck"),
		options.WithTTL(120*time.Minute),
	)
	if err != nil {
		return nil, err
	}

	subtreeDataBytes, err := subtreeData.Serialize()
	if err != nil {
		return nil, err
	}

	err = subtreeStore.Set(
		ctx,
		subtreeData.RootHash()[:],
		subtreeDataBytes,
		options.WithFileExtension("subtreeData"),
		options.WithTTL(120*time.Minute),
	)
	if err != nil {
		return nil, err
	}

	return subtree, nil
}

func isKafkaRunning() bool {
	port, _ := gocore.Config().GetInt("KAFKA_PORT", 9092)

	_, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return false
	}

	return true
}
