package testdaemon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/chaincfg"
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
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	testkafka "github.com/bitcoin-sv/teranode/test/util/kafka"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
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

type TestDaemon struct {
	Ctx                   context.Context
	ctxCancel             context.CancelFunc
	Logger                *ulogger.ErrorTestLogger
	d                     *daemon.Daemon
	BlockchainClient      blockchain.ClientI
	BlockAssemblyClient   *blockassembly.Client
	PropagationClient     *propagation.Client
	BlockValidationClient *blockvalidation.Client
	privKey               *bec.PrivateKey
	SubtreeStore          blob.Store
	UtxoStore             utxo.Store
	DistributorClient     *distributor.Distributor
	rpcURL                *url.URL
	Settings              *settings.Settings
}

type TestOptions struct {
	SkipRemoveDataDir bool
	KillTeranode      bool
	UtxoStoreOverride string
	UseTracing        bool
	EnableRPC         bool
	SettingsOverride  string
}

type JSONError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (je *JSONError) Error() string {
	return fmt.Sprintf("code: %d, message: %s", je.Code, je.Message)
}

func New(t *testing.T, opts TestOptions) *TestDaemon {
	ctx, cancel := context.WithCancel(context.Background())

	logger := ulogger.NewErrorTestLogger(t, cancel)

	if !opts.SkipRemoveDataDir {
		err := os.RemoveAll("./data")
		require.NoError(t, err)
	}

	if opts.KillTeranode {
		cmd := exec.Command("sh", "-c", "kill -9 $(pgrep -f teranode)")
		_ = cmd.Run()
	}

	persistentStore, err := url.Parse("sqlite:///test")
	require.NoError(t, err)

	useUxoStore := persistentStore
	if len(opts.UtxoStoreOverride) > 0 {
		useUxoStore, err = url.Parse(opts.UtxoStoreOverride)
		require.NoError(t, err)
	}

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

	var tSettings *settings.Settings
	if opts.SettingsOverride != "" {
		tSettings = settings.NewSettings("dev.system.test") // This reads gocore.Config and applies sensible defaults
	} else {
		tSettings = settings.NewSettings() // This reads gocore.Config and applies sensible defaults
		tSettings.SubtreeValidation.SubtreeStore = memoryStore
		tSettings.BlockChain.StoreURL = persistentStore
		tSettings.UtxoStore.UtxoStore = useUxoStore
		tSettings.ChainCfgParams = &chaincfg.RegressionNetParams
	}

	// Override with test settings...
	tSettings.Asset.CentrifugeDisable = true
	tSettings.UtxoStore.DBTimeout = 500 * time.Second
	tSettings.LocalTestStartFromState = "RUNNING"
	tSettings.SubtreeValidation.TxMetaCacheEnabled = false

	if opts.UseTracing {
		// tracing
		tSettings.UseOpenTracing = true
		tSettings.TracingSampleRate = "1" // 100% sampling during the test
	}

	readyCh := make(chan struct{})

	d := daemon.New()

	services := []string{
		"-all=0",
		"-blockchain=1",
		"-subtreevalidation=1",
		"-blockvalidation=1",
		"-blockassembly=1",
		"-asset=1",
		"-propagation=1",
	}

	if opts.EnableRPC {
		services = append(services, "-rpc=1")
	}

	go d.Start(logger, services, tSettings, readyCh)

	select {
	case <-readyCh:
		t.Log("Daemon started successfully")
	case <-time.After(20 * time.Second):
		t.Fatal("Daemon failed to start within timeout")
	}

	if opts.UseTracing {
		// start tracing after the global tracer has been set
		_, _, deferFn := tracing.StartTracing(ctx, "NewDoubleSpendTester",
			tracing.WithLogMessage(logger, "NewDoubleSpendTester"),
		)

		t.Cleanup(func() {
			deferFn()
		})
	}

	blockchainClient, err := blockchain.NewClient(ctx, logger, tSettings, "test")
	require.NoError(t, err)

	blockAssemlyClient, err := blockassembly.NewClient(ctx, logger, tSettings)
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

	w, err := wif.DecodeWIF(tSettings.BlockAssembly.MinerWalletPrivateKeys[0])
	require.NoError(t, err)

	privKey := w.PrivKey

	subtreeStore, err := daemon.GetSubtreeStore(logger, tSettings)
	require.NoError(t, err)

	utxoStore, err := daemon.GetUtxoStore(ctx, logger, tSettings)
	require.NoError(t, err)

	assert.NotNil(t, blockchainClient)
	assert.NotNil(t, blockAssemlyClient)
	assert.NotNil(t, propagationClient)
	assert.NotNil(t, blockValidationClient)
	assert.NotNil(t, subtreeStore)
	assert.NotNil(t, utxoStore)
	assert.NotNil(t, distributorClient)

	return &TestDaemon{
		Ctx:                   ctx,
		ctxCancel:             cancel,
		Logger:                logger,
		d:                     d,
		BlockchainClient:      blockchainClient,
		BlockAssemblyClient:   blockAssemlyClient,
		PropagationClient:     propagationClient,
		BlockValidationClient: blockValidationClient,
		privKey:               privKey,
		SubtreeStore:          subtreeStore,
		UtxoStore:             utxoStore,
		DistributorClient:     distributorClient,
		rpcURL:                tSettings.RPC.RPCListenerURL,
		Settings:              tSettings,
	}
}

func (td *TestDaemon) Stop() {
	_ = td.d.Stop()
	td.ctxCancel()
}

// Function to call the RPC endpoint with any method and parameters, returning the response and error
func (td *TestDaemon) CallRPC(method string, params []interface{}) (string, error) {
	// Create the request payload
	requestBody, err := json.Marshal(map[string]interface{}{
		"method": method,
		"params": params,
	})
	td.Logger.Infof("Request: %s", string(requestBody))

	if err != nil {
		return "", errors.NewProcessingError("failed to marshal request body", err)
	}

	// Create the HTTP request
	req, err := http.NewRequest("POST", td.rpcURL.String(), bytes.NewBuffer(requestBody))
	if err != nil {
		return "", errors.NewProcessingError("failed to create request", err)
	}

	// Set the appropriate headers
	req.SetBasicAuth("bitcoin", "bitcoin")
	req.Header.Set("Content-Type", "application/json")

	// Perform the request
	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		return "", errors.NewProcessingError("failed to perform request", err)
	}

	defer resp.Body.Close()

	// Check the status code
	if resp.StatusCode != http.StatusOK {
		return "", errors.NewProcessingError("expected status code 200, got %v", resp.StatusCode)
	}

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", errors.NewProcessingError("failed to read response body", err)
	}

	/*
		Example of a response:
		{
			"result": null,
			"error": {
				"code": -32601,
				"message": "Method not found"
		}
	*/

	// Check if the response body contains an error
	var jsonResponse struct {
		Error *JSONError `json:"error"`
	}

	if err := json.Unmarshal(body, &jsonResponse); err != nil {
		return string(body), errors.NewProcessingError("failed to parse response JSON", err)
	}

	if jsonResponse.Error != nil {
		return string(body), errors.NewProcessingError("RPC returned error", jsonResponse.Error)
	}

	// Return the response as a string
	return string(body), nil
}

func (td *TestDaemon) VerifyBlockByHeight(t *testing.T, expectedBlock *model.Block, height uint32) {
	tmpBlock, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, height)
	require.NoError(t, err, "Failed to get block at height %d", height)
	assert.Equal(t, expectedBlock.Header.Hash().String(), tmpBlock.Header.Hash().String(),
		"Block hash mismatch at height %d", height)
}

func (td *TestDaemon) VerifyBlockByHash(t *testing.T, expectedBlock *model.Block, hash *chainhash.Hash) {
	tmpBlock, err := td.BlockchainClient.GetBlock(td.Ctx, hash)
	require.NoError(t, err, "Failed to get block at hash %s", hash)
	assert.Equal(t, expectedBlock.Header.Hash().String(), tmpBlock.Header.Hash().String(),
		"Block hash mismatch at hash %s", hash)
}

func (td *TestDaemon) VerifyConflictingInSubtrees(t *testing.T, subtreeHash *chainhash.Hash, expectedConflicts []chainhash.Hash) {
	latestSubtreeBytes, err := td.SubtreeStore.Get(td.Ctx, subtreeHash[:], options.WithFileExtension("subtree"))
	require.NoError(t, err, "Failed to get subtree")

	latestSubtree, err := util.NewSubtreeFromBytes(latestSubtreeBytes)
	require.NoError(t, err, "Failed to parse subtree bytes")

	require.Len(t, latestSubtree.ConflictingNodes, len(expectedConflicts),
		"Unexpected number of conflicting nodes in subtree")

	for _, conflict := range expectedConflicts {
		// conflicting txs are not in order
		assert.True(t, slices.Contains(latestSubtree.ConflictingNodes, conflict), "Expected conflicting node %s not found in subtree", conflict.String())
	}
}

func (td *TestDaemon) VerifyConflictingInUtxoStore(t *testing.T, expectedConflicts []chainhash.Hash, conflictValue bool) {
	for _, conflict := range expectedConflicts {
		readTx, err := td.UtxoStore.Get(td.Ctx, &conflict)
		require.NoError(t, err, "Failed to get transaction %s", conflict.String())
		assert.Equal(t, conflictValue, readTx.Conflicting, "Expected transaction %s to be marked as conflicting", conflict.String())
	}
}

func (td *TestDaemon) VerifyNotInBlockAssembly(t *testing.T, txHash []chainhash.Hash) {
	// get a mining candidate and check the subtree does not contain the given transactions
	candidate, err := td.BlockAssemblyClient.GetMiningCandidate(td.Ctx, true)
	require.NoError(t, err)

	for _, subtreeHash := range candidate.SubtreeHashes {
		subtreeBytes, err := td.SubtreeStore.Get(td.Ctx, subtreeHash, options.WithFileExtension("subtree"))
		require.NoError(t, err, "Failed to get subtree")

		subtree, err := util.NewSubtreeFromBytes(subtreeBytes)
		require.NoError(t, err, "Failed to parse subtree bytes")

		for _, hash := range txHash {
			found := subtree.HasNode(hash)
			assert.False(t, found, "Expected subtree to not contain transaction %s", hash.String())
		}
	}
}

func (td *TestDaemon) VerifyInBlockAssembly(t *testing.T, txHash []chainhash.Hash) {
	// get a mining candidate and check the subtree does not contain the given transactions
	candidate, err := td.BlockAssemblyClient.GetMiningCandidate(td.Ctx, true)
	require.NoError(t, err)

	// Check the candidate has at least one subtree hash, otherwise there is nothing to check
	require.GreaterOrEqual(t, candidate.SubtreeHashes, 1, "Expected at least one subtree hash in the candidate")

	txFoundMap := make(map[chainhash.Hash]int)
	for _, hash := range txHash {
		txFoundMap[hash] = 0
	}

	for _, subtreeHash := range candidate.SubtreeHashes {
		subtreeBytes, err := td.SubtreeStore.Get(td.Ctx, subtreeHash, options.WithFileExtension("subtree"))
		require.NoError(t, err, "Failed to get subtree")

		subtree, err := util.NewSubtreeFromBytes(subtreeBytes)
		require.NoError(t, err, "Failed to parse subtree bytes")

		for _, hash := range txHash {
			found := subtree.HasNode(hash)
			if found {
				txFoundMap[hash]++
			}
		}
	}

	// check all transactions have been found exactly once
	for hash, count := range txFoundMap {
		assert.Equal(t, 1, count, "Expected transaction %s to be found exactly once", hash.String())
	}
}

func (td *TestDaemon) CreateTransaction(t *testing.T, parentTx *bt.Tx, amount uint64) *bt.Tx {
	tx := bt.NewTx()

	err := tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      parentTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: parentTx.Outputs[0].LockingScript,
		Satoshis:      parentTx.Outputs[0].Satoshis,
	})
	require.NoError(t, err)

	err = tx.AddP2PKHOutputFromPubKeyBytes(td.privKey.PubKey().SerialiseCompressed(), amount)
	require.NoError(t, err)

	err = tx.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: td.privKey})
	require.NoError(t, err)

	return tx
}

func (td *TestDaemon) CreateTransactionFromMultipleInputs(t *testing.T, parentTxs []*bt.Tx, amount uint64) *bt.Tx {
	tx := bt.NewTx()

	err := tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      parentTxs[0].TxIDChainHash(),
		Vout:          0,
		LockingScript: parentTxs[0].Outputs[0].LockingScript,
		Satoshis:      parentTxs[0].Outputs[0].Satoshis,
	}, &bt.UTXO{
		TxIDHash:      parentTxs[1].TxIDChainHash(),
		Vout:          0,
		LockingScript: parentTxs[1].Outputs[0].LockingScript,
		Satoshis:      parentTxs[1].Outputs[0].Satoshis,
	})
	require.NoError(t, err)

	err = tx.AddP2PKHOutputFromPubKeyBytes(td.privKey.PubKey().SerialiseCompressed(), amount)
	require.NoError(t, err)

	err = tx.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: td.privKey})
	require.NoError(t, err)

	return tx
}

func (td *TestDaemon) CreateTestBlock(t *testing.T, txs []*bt.Tx, previousBlock *model.Block, nonce uint32) (*util.Subtree, *model.Block) {
	// Create and save the subtree with the double spend tx
	subtree, err := createAndSaveSubtrees(td.Ctx, td.SubtreeStore, txs)
	require.NoError(t, err)

	address, err := bscript.NewAddressFromPublicKey(td.privKey.PubKey(), true)
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
			Nonce:          nonce,
			Version:        536870912,
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

func (td *TestDaemon) WaitForBlockHeight(t *testing.T, height uint32, timeout time.Duration) (state *blockassembly_api.StateMessage) {
	deadline := time.Now().Add(timeout)

	var (
		err error
	)

	for state == nil || state.CurrentHeight < height {
		state, err = td.BlockAssemblyClient.GetBlockAssemblyState(td.Ctx)
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
		err = subtree.AddNode(*tx.TxIDChainHash(), uint64(i), uint64(i)) // nolint:gosec
		if err != nil {
			return nil, err
		}

		err = subtreeData.AddTx(tx, i+1)
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

	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return false
	}

	_ = conn.Close()

	return true
}
