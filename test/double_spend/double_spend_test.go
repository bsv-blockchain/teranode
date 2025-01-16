////go:build test_full

package doublespendtest

import (
	"context"
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
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-bt/v2/unlocker"
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

	persitantStore, err := url.Parse("sqlite:///test")
	require.NoError(t, err)

	memoryStore, err := url.Parse("memory:///")
	require.NoError(t, err)

	// Start Kafka container with default port 9092
	kafkaContainer, err := testkafka.RunTestContainer(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = kafkaContainer.CleanUp()
	})

	gocore.Config().Set("KAFKA_PORT", strconv.Itoa(kafkaContainer.KafkaPort))

	tSettings := settings.NewSettings() // This reads gocore.Config and applies sensible defaults

	// Override with test settings...
	tSettings.SubtreeValidation.SubtreeStore = memoryStore
	tSettings.BlockChain.StoreURL = persitantStore
	tSettings.UtxoStore.UtxoStore = persitantStore
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

func TestDaemon(t *testing.T) {
	dst := NewDoubleSpendTester(t)

	// Set the FSM state to RUNNING...
	err := dst.blockchainClient.Run(dst.ctx, "test")
	require.NoError(t, err)

	err = dst.blockAssemblyClient.GenerateBlocks(dst.ctx, &blockassembly_api.GenerateBlocksRequest{Count: 101})
	require.NoError(t, err)

	state, err := dst.blockAssemblyClient.GetBlockAssemblyState(dst.ctx)
	require.NoError(t, err)
	t.Logf("State: %s", state.String())

	block1, err := dst.blockchainClient.GetBlockByHeight(dst.ctx, 1)
	require.NoError(t, err)

	coinbaseTx := block1.CoinbaseTx
	// t.Logf("Coinbase has %d outputs", len(coinbaseTx.Outputs))

	tx1 := createTransaction(t, coinbaseTx, dst.privKey, 49e8)
	tx2 := createTransaction(t, coinbaseTx, dst.privKey, 48e8)
	tx3 := createTransaction(t, tx2, dst.privKey, 47e8)

	err1 := dst.propagationClient.ProcessTransaction(dst.ctx, tx1)
	require.NoError(t, err1)

	dst.logger.SkipCancelOnFail(true)

	err2 := dst.propagationClient.ProcessTransaction(dst.ctx, tx2)
	require.Error(t, err2) // This should fail as it is a double spend

	dst.logger.SkipCancelOnFail(false)

	err = dst.blockAssemblyClient.GenerateBlocks(dst.ctx, &blockassembly_api.GenerateBlocksRequest{Count: 1})
	require.NoError(t, err)

	block102, err := dst.blockchainClient.GetBlockByHeight(dst.ctx, 102)
	require.NoError(t, err)

	assert.Equal(t, uint64(2), block102.TransactionCount)

	subtree, block103 := dst.generateBlock(t, []*bt.Tx{tx2}, coinbaseTx, block102)

	err = dst.blockValidationClient.ProcessBlock(dst.ctx, block103, block103.Height)
	require.NoError(t, err)

	readTx, err := dst.utxoStore.Get(dst.ctx, tx2.TxIDChainHash())
	require.NoError(t, err)
	assert.True(t, readTx.Conflicting)

	latestSubtreeBytes, err := dst.subtreeStore.Get(dst.ctx, subtree.RootHash()[:], options.WithFileExtension("subtree"))
	require.NoError(t, err)

	latestSubtree, err := util.NewSubtreeFromBytes(latestSubtreeBytes)
	require.NoError(t, err)

	assert.Len(t, latestSubtree.ConflictingNodes, 1)
	assert.Equal(t, tx2.TxIDChainHash().String(), latestSubtree.ConflictingNodes[0].String())

	subtree104, block104 := dst.generateBlock(t, []*bt.Tx{tx3}, coinbaseTx, block103)

	err = dst.blockValidationClient.ProcessBlock(dst.ctx, block104, block104.Height)
	require.NoError(t, err)

	readTx, err = dst.utxoStore.Get(dst.ctx, tx2.TxIDChainHash())
	require.NoError(t, err)
	assert.True(t, readTx.Conflicting)

	latestSubtreeBytes, err = dst.subtreeStore.Get(dst.ctx, subtree104.RootHash()[:], options.WithFileExtension("subtree"))
	require.NoError(t, err)

	latestSubtree, err = util.NewSubtreeFromBytes(latestSubtreeBytes)
	require.NoError(t, err)

	assert.Len(t, latestSubtree.ConflictingNodes, 1)
	assert.Equal(t, tx3.TxIDChainHash().String(), latestSubtree.ConflictingNodes[0].String())

	dst.d.Stop()
}

func (dst *DoubleSpendTester) generateBlock(t *testing.T, txs []*bt.Tx, coinbaseTx *bt.Tx, previousBlock *model.Block) (*util.Subtree, *model.Block) {
	// Create and save the subtree with the double spend tx
	subtree, err := createAndSaveSubtrees(dst.ctx, dst.subtreeStore, txs)
	require.NoError(t, err)

	merkleRoot, err := subtree.RootHashWithReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) // nolint:gosec
	require.NoError(t, err)

	block := &model.Block{
		Subtrees: []*chainhash.Hash{
			subtree.RootHash(),
		},
		CoinbaseTx: coinbaseTx,
		Header: &model.BlockHeader{
			HashPrevBlock:  previousBlock.Header.HashPrevBlock,
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

func createTransaction(t *testing.T, coinbaseTx *bt.Tx, privkey *bec.PrivateKey, amount uint64) *bt.Tx {
	tx := bt.NewTx()

	err := tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: coinbaseTx.Outputs[0].LockingScript,
		Satoshis:      coinbaseTx.Outputs[0].Satoshis,
	})
	require.NoError(t, err)

	err = tx.AddP2PKHOutputFromPubKeyBytes(privkey.PubKey().SerialiseCompressed(), amount)
	require.NoError(t, err)

	err = tx.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privkey})
	require.NoError(t, err)

	return tx
}
