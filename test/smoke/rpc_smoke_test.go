//go:build test_all || test_smoke_rpc || test_rpc

package smoke

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockvalidation"
	"github.com/bitcoin-sv/teranode/services/propagation"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	testkafka "github.com/bitcoin-sv/teranode/test/util/kafka"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/distributor"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type RPCDaemonTester struct {
	ctx                   context.Context
	logger                *ulogger.ErrorTestLogger
	d                     *daemon.Daemon
	kafkaContainer        *testkafka.GenericTestContainerWrapper
	blockchainClient      blockchain.ClientI
	blockAssemblyClient   *blockassembly.Client
	propagationClient     *propagation.Client
	blockValidationClient *blockvalidation.Client
	distributorClient     *distributor.Distributor
	subtreeStore          blob.Store
	utxoStore             utxo.Store
	rpcURL                string
	settings              *settings.Settings
}

func NewRPCDaemonTester(t *testing.T) *RPCDaemonTester {
	ctx, cancel := context.WithCancel(context.Background())
	logger := ulogger.NewErrorTestLogger(t, cancel)

	// Delete the sqlite db at the beginning of the test
	_ = os.RemoveAll("data")
	cmd := exec.Command("sh", "-c", "kill -9 $(pgrep -f teranode)")
	err := cmd.Run()

	// Start Kafka container with default port 9092
	kafkaContainer, err := testkafka.RunTestContainer(ctx)
	require.NoError(t, err)
	// t.Cleanup(func() {
	// 	_ = kafkaContainer.CleanUp()
	// })

	gocore.Config().Set("KAFKA_PORT", strconv.Itoa(kafkaContainer.KafkaPort))

	tSettings := settings.NewSettings("dev.system.test") // This reads gocore.Config and applies sensible defaults

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
		"-rpc=1",
		"-blockpersister=1",
	}, tSettings, readyCh)

	select {
	case <-readyCh:
		t.Log("Daemon started successfully")
	case <-time.After(10 * time.Second):
		t.Fatal("Daemon failed to start within timeout")
	}

	bcClient, err := blockchain.NewClient(ctx, logger, tSettings, "test")
	require.NoError(t, err)

	baClient, err := blockassembly.NewClient(ctx, logger, tSettings)
	require.NoError(t, err)

	propagationClient, err := propagation.NewClient(ctx, logger, tSettings)
	require.NoError(t, err)

	blockValidationClient, err := blockvalidation.NewClient(ctx, logger, tSettings, "test")
	require.NoError(t, err)

	distributorClient, err := distributor.NewDistributor(ctx, logger, tSettings,
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)
	require.NoError(t, err)

	rpcURL := fmt.Sprintf("http://localhost%s", tSettings.RPC.RPCListenerURL)

	subtreeStore, err := daemon.GetSubtreeStore(logger, tSettings)
	require.NoError(t, err)

	utxoStore, err := daemon.GetUtxoStore(ctx, logger, tSettings)
	require.NoError(t, err)

	// set run state
	err = bcClient.Run(ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks
	_, err = helper.CallRPC(rpcURL, "generate", []interface{}{101})
	require.NoError(t, err)

	return &RPCDaemonTester{
		ctx:                   ctx,
		logger:                logger,
		d:                     d,
		kafkaContainer:        kafkaContainer,
		blockchainClient:      bcClient,
		blockAssemblyClient:   baClient,
		propagationClient:     propagationClient,
		blockValidationClient: blockValidationClient,
		subtreeStore:          subtreeStore,
		distributorClient:     distributorClient,
		rpcURL:                rpcURL,
		settings:              tSettings,
		utxoStore:             utxoStore,
	}
}

func (tester *RPCDaemonTester) waitForBlockHeight(t *testing.T, height uint32, timeout time.Duration) (state *blockassembly_api.StateMessage) {
	deadline := time.Now().Add(timeout)

	var (
		err error
	)

	for state == nil || state.CurrentHeight < height {
		state, err = tester.blockAssemblyClient.GetBlockAssemblyState(tester.ctx)
		require.NoError(t, err)

		if time.Now().After(deadline) {
			t.Logf("Timeout waiting for block height %d", height)
			t.FailNow()
		}

		time.Sleep(10 * time.Millisecond)
	}

	return state
}

func (tester *RPCDaemonTester) createCoinbaseTxCandidate(m helper.MiningCandidate) (*bt.Tx, error) {
	tSettings := tester.settings

	arbitraryText := tSettings.Coinbase.ArbitraryText

	coinbasePrivKeys := tSettings.BlockAssembly.MinerWalletPrivateKeys
	if len(coinbasePrivKeys) == 0 {
		return nil, errors.NewConfigurationError("miner_wallet_private_keys not found in config")
	}

	walletAddresses := make([]string, len(coinbasePrivKeys))

	for i, coinbasePrivKey := range coinbasePrivKeys {
		privateKey, err := wif.DecodeWIF(coinbasePrivKey)
		if err != nil {
			return nil, errors.NewProcessingError("can't decode coinbase priv key", err)
		}

		walletAddress, err := bscript.NewAddressFromPublicKey(privateKey.PrivKey.PubKey(), true)
		if err != nil {
			return nil, errors.NewProcessingError("can't create coinbase address", err)
		}

		walletAddresses[i] = walletAddress.AddressString
	}

	a, b, err := model.GetCoinbaseParts(m.Result.Height, m.Result.CoinbaseValue, arbitraryText, walletAddresses)
	if err != nil {
		return nil, errors.NewProcessingError("error creating coinbase transaction", err)
	}

	extranonce := make([]byte, 12)
	_, _ = rand.Read(extranonce)
	a = append(a, extranonce...)
	a = append(a, b...)

	coinbaseTx, err := bt.NewTxFromBytes(a)
	if err != nil {
		return nil, errors.NewProcessingError("error decoding coinbase transaction", err)
	}

	return coinbaseTx, nil
}

func TestShouldAllowFairTxUseRpc(t *testing.T) {
	tester := NewRPCDaemonTester(t)
	defer func() {
		tester.d.Stop()
		tester.kafkaContainer.CleanUp()
	}()

	assert.NotNil(t, tester.blockchainClient)
	assert.NotNil(t, tester.blockAssemblyClient)
	assert.NotNil(t, tester.propagationClient)
	assert.NotNil(t, tester.blockValidationClient)
	assert.NotNil(t, tester.distributorClient)
	assert.NotNil(t, tester.subtreeStore)
	assert.NotNil(t, tester.rpcURL)

	ctx := context.Background()
	logger := tester.logger
	tSettings := tester.settings

	state := tester.waitForBlockHeight(t, 101, 5*time.Second)
	assert.Equal(t, uint32(101), state.CurrentHeight, "Expected block assembly to reach height 101")

	block1, err := tester.blockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)

	coinbaseTx := block1.CoinbaseTx

	coinbasePrivKey := tSettings.BlockAssembly.MinerWalletPrivateKeys[0]
	coinbasePrivateKey, err := wif.DecodeWIF(coinbasePrivKey)
	require.NoError(t, err)

	_, err = bscript.NewAddressFromPublicKey(coinbasePrivateKey.PrivKey.PubKey(), true)
	require.NoError(t, err)

	privateKey, err := bec.NewPrivateKey(bec.S256())
	require.NoError(t, err)

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	output := coinbaseTx.Outputs[0]
	utxo := &bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	newTx := bt.NewTx()
	err = newTx.FromUTXOs(utxo)
	require.NoError(t, err)

	err = newTx.AddP2PKHOutputFromAddress(address.AddressString, 10000)
	require.NoError(t, err)

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: coinbasePrivateKey.PrivKey})
	require.NoError(t, err)

	t.Logf("Sending New Transaction with RPC: %s\n", newTx.TxIDChainHash())
	txBytes := hex.EncodeToString(newTx.ExtendedBytes())

	resp, err := helper.CallRPC(tester.rpcURL, "sendrawtransaction", []interface{}{txBytes})
	require.NoError(t, err, "Failed to send new tx with rpc")
	t.Logf("Transaction sent with RPC: %s\n", resp)

	delay := tSettings.BlockAssembly.DoubleSpendWindow
	if delay != 0 {
		t.Logf("Waiting %dms [block assembly has delay processing txs to catch double spends]\n", delay)
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}

	_, err = helper.CallRPC(tester.rpcURL, "generate", []interface{}{1})
	require.NoError(t, err, "Failed to generate blocks")

	t.Logf("Resp: %s", resp)

	_, err = helper.CallRPC(tester.rpcURL, "generate", []interface{}{100})
	require.NoError(t, err, "Failed to generate blocks")

	t.Logf("Resp: %s", resp)

	state = tester.waitForBlockHeight(t, 202, 10*time.Second)
	assert.Equal(t, uint32(202), state.CurrentHeight, "Expected block assembly to reach height 202")

	block102, err := tester.blockchainClient.GetBlockByHeight(ctx, 102)
	require.NoError(t, err)

	err = block102.GetAndValidateSubtrees(ctx, tester.logger, tester.subtreeStore, nil)
	require.NoError(t, err)

	err = block102.CheckMerkleRoot(ctx)
	require.NoError(t, err)

	fallbackGetFunc := func(subtreeHash chainhash.Hash) error {
		return block102.SubTreesFromBytes(subtreeHash[:])
	}

	subtree, err := block102.GetSubtrees(ctx, logger, tester.subtreeStore, fallbackGetFunc)
	require.NoError(t, err)

	blFound := false
	for i := 0; i < len(subtree); i++ {
		st := subtree[i]
		for _, node := range st.Nodes {
			t.Logf("node.Hash: %s", node.Hash.String())
			t.Logf("tx.TxIDChainHash().String(): %s", newTx.TxIDChainHash().String())

			if node.Hash.String() == newTx.TxIDChainHash().String() {
				blFound = true
				break
			}
		}
	}

	assert.True(t, blFound, "TX not found in the blockstore")

	resp, err = helper.CallRPC(tester.rpcURL, "getblockchaininfo", []interface{}{})
	require.NoError(t, err)

	var blockchainInfo helper.BlockchainInfo
	errJSON := json.Unmarshal([]byte(resp), &blockchainInfo)

	if errJSON != nil {
		t.Errorf("JSON decoding error: %v", errJSON)
		return
	}

	block202, err := tester.blockchainClient.GetBlockByHeight(ctx, 202)
	require.NoError(t, err)
	t.Logf("blockchainInfo: %+v", blockchainInfo)
	assert.Equal(t, int(202), blockchainInfo.Result.Blocks)
	assert.Equal(t, block202.Hash().String(), blockchainInfo.Result.BestBlockHash)
	assert.Equal(t, "regtest", blockchainInfo.Result.Chain)
	assert.Equal(t, "9601000000000000000000000000000000000000000000000000000000000000", blockchainInfo.Result.Chainwork)
	assert.Equal(t, string("4.6565423739069247246592908691514574469873245939403288293276821765520783128832e-10"), blockchainInfo.Result.Difficulty)
	assert.Equal(t, int(863341), blockchainInfo.Result.Headers)
	assert.Equal(t, int(0), blockchainInfo.Result.Mediantime)
	assert.False(t, blockchainInfo.Result.Pruned)
	assert.Empty(t, blockchainInfo.Result.Softforks)
	assert.Equal(t, float64(0), blockchainInfo.Result.VerificationProgress)
	assert.Nil(t, blockchainInfo.Error)
	assert.Nil(t, blockchainInfo.ID)

	resp, err = helper.CallRPC(tester.rpcURL, "getinfo", []interface{}{})

	require.NoError(t, err)

	var getInfo helper.GetInfo
	errJSON = json.Unmarshal([]byte(resp), &getInfo)
	require.NoError(t, errJSON)
	require.NotNil(t, getInfo.Result)

	t.Logf("getInfo: %+v", getInfo)

	assert.Equal(t, int(202), getInfo.Result.Blocks)
	assert.Equal(t, int(1), getInfo.Result.Connections)
	assert.Equal(t, float64(1), getInfo.Result.Difficulty)
	assert.Equal(t, int(70016), getInfo.Result.ProtocolVersion)
	assert.Equal(t, "host:port", getInfo.Result.Proxy)
	assert.Equal(t, float64(100), getInfo.Result.RelayFee)
	assert.False(t, getInfo.Result.Stn)
	assert.False(t, getInfo.Result.TestNet)
	assert.Equal(t, int(1), getInfo.Result.TimeOffset)
	assert.Equal(t, int(1), getInfo.Result.Version)
	assert.Nil(t, getInfo.Error)
	assert.Equal(t, int(0), getInfo.ID)

	var getDifficulty helper.GetDifficultyResponse

	resp, err = helper.CallRPC(tester.rpcURL, "getdifficulty", []interface{}{})

	require.NoError(t, err)

	errJSON = json.Unmarshal([]byte(resp), &getDifficulty)
	require.NoError(t, errJSON)

	t.Logf("getDifficulty: %+v", getDifficulty)
	assert.Equal(t, float64(4.6565423739069247e-10), getDifficulty.Result)

	resp, err = helper.CallRPC(tester.rpcURL, "getblockhash", []interface{}{202})
	require.NoError(t, err, "Failed to generate blocks")

	var getBlockHash helper.GetBlockHashResponse
	errJSON = json.Unmarshal([]byte(resp), &getBlockHash)
	require.NoError(t, errJSON)

	t.Logf("getBlockHash: %+v", getBlockHash)
	assert.Equal(t, block202.Hash().String(), getBlockHash.Result)

	t.Logf("%s", resp)

	resp, err = helper.CallRPC(tester.rpcURL, "getblockbyheight", []interface{}{102})
	require.NoError(t, err, "Failed to get block by height")

	var getBlockByHeightResp helper.GetBlockByHeightResponse
	errJSON = json.Unmarshal([]byte(resp), &getBlockByHeightResp)
	require.NoError(t, errJSON)

	t.Logf("getBlockByHeightResp: %+v", getBlockByHeightResp)

	t.Logf("Resp: %s", resp)

	block103, err := tester.blockchainClient.GetBlockByHeight(ctx, 103)
	require.NoError(t, err)
	block101, err := tester.blockchainClient.GetBlockByHeight(ctx, 101)
	require.NoError(t, err)
	// Assert block properties
	require.Equal(t, block102.Hash().String(), getBlockByHeightResp.Result.Hash)
	require.Equal(t, 101, getBlockByHeightResp.Result.Confirmations)
	// require.Equal(t, 229, getBlockByHeightResp.Result.Size)
	require.Equal(t, 102, getBlockByHeightResp.Result.Height)
	require.Equal(t, 536870912, getBlockByHeightResp.Result.Version)
	require.Equal(t, "20000000", getBlockByHeightResp.Result.VersionHex)
	require.Equal(t, block102.Header.HashMerkleRoot.String(), getBlockByHeightResp.Result.Merkleroot)
	require.Equal(t, "207fffff", getBlockByHeightResp.Result.Bits)
	require.InDelta(t, 4.6565423739069247e-10, getBlockByHeightResp.Result.Difficulty, 1e-20)
	require.Equal(t, block101.Hash().String(), getBlockByHeightResp.Result.Previousblockhash)
	require.Equal(t, block103.Hash().String(), getBlockByHeightResp.Result.Nextblockhash)
}
