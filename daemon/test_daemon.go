package daemon

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/chaincfg"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockvalidation"
	"github.com/bitcoin-sv/teranode/services/propagation"
	distributor "github.com/bitcoin-sv/teranode/services/rpc"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/testutil"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
)

type TestDaemon struct {
	Ctx                   context.Context
	ctxCancel             context.CancelFunc
	Logger                ulogger.Logger
	composeDependencies   tc.ComposeStack
	d                     *Daemon
	BlockchainClient      blockchain.ClientI
	BlockAssemblyClient   *blockassembly.Client
	PropagationClient     *propagation.Client
	BlockValidationClient *blockvalidation.Client
	privKey               *bec.PrivateKey
	SubtreeStore          blob.Store
	UtxoStore             utxo.Store
	DistributorClient     *distributor.Distributor
	rpcURL                *url.URL
	AssetURL              string
	Settings              *settings.Settings
}

type TestOptions struct {
	SkipRemoveDataDir       bool
	UseTracing              bool
	EnableRPC               bool
	EnableP2P               bool
	EnableValidator         bool
	EnableLegacy            bool
	StartDaemonDependencies bool
	SettingsContext         string
	SettingsOverrideFunc    func(*settings.Settings)
	EnableFullLogging       bool
}

type JSONError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (je *JSONError) Error() string {
	return fmt.Sprintf("code: %d, message: %s", je.Code, je.Message)
}

func NewTestDaemon(t *testing.T, opts TestOptions) *TestDaemon {
	ctx, cancel := context.WithCancel(context.Background())

	var (
		composeDependencies tc.ComposeStack
		tSettings           *settings.Settings
	)

	if opts.SettingsContext != "" {
		tSettings = settings.NewSettings(opts.SettingsContext)
	} else {
		tSettings = settings.NewSettings() // This reads gocore.Config and applies sensible defaults
	}

	path := filepath.Join("data", tSettings.ClientName)
	if strings.HasPrefix(opts.SettingsContext, "dev.system.test") {
		// a bit hacky. Ideally all stores sit under data/${ClientName}
		path = "data"
	}

	if !opts.SkipRemoveDataDir {
		t.Logf("Removing data directory: %s", path)
		err := os.RemoveAll(path)
		require.NoError(t, err)
	}

	// if opts.StartDaemonDependencies {
	// composeDependencies = StartDaemonDependencies(ctx, t, !opts.SkipRemoveDataDir, calculateDependencies(t, opts.Settings))
	// }

	tSettings.ChainCfgParams = &chaincfg.RegressionNetParams
	tSettings.ChainCfgParams.CoinbaseMaturity = 1

	t.Logf("Creating data directory: %s", path)
	err := os.MkdirAll(path, 0755)
	require.NoError(t, err)

	quorumPath := tSettings.SubtreeValidation.QuorumPath
	require.NotNil(t, quorumPath, "No subtree_quorum_path specified")

	err = os.MkdirAll(quorumPath, 0755)
	require.NoError(t, err)

	// Override with test settings...
	tSettings.Asset.CentrifugeDisable = true
	tSettings.UtxoStore.DBTimeout = 500 * time.Second
	tSettings.LocalTestStartFromState = "RUNNING"
	tSettings.SubtreeValidation.TxMetaCacheEnabled = false
	tSettings.ProfilerAddr = ""

	if opts.UseTracing {
		// tracing
		tSettings.UseOpenTracing = true
		tSettings.TracingSampleRate = "1" // 100% sampling during the test
	} else {
		tSettings.UseOpenTracing = false
	}

	// Override with test settings...
	if opts.SettingsOverrideFunc != nil {
		opts.SettingsOverrideFunc(tSettings)
	}

	readyCh := make(chan struct{})

	var (
		logger        ulogger.Logger
		loggerFactory Option
	)

	if opts.EnableFullLogging {
		logger = ulogger.New(tSettings.ClientName)
		loggerFactory = WithLoggerFactory(func(serviceName string) ulogger.Logger {
			return ulogger.New(tSettings.ClientName+"-"+serviceName, ulogger.WithLevel("DEBUG"))
		})
	} else {
		logger = ulogger.NewErrorTestLogger(t, cancel)
		loggerFactory = WithLoggerFactory(func(serviceName string) ulogger.Logger {
			return logger
		})
	}

	d := New(loggerFactory, WithContext(ctx))

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

	if opts.EnableP2P {
		services = append(services, "-p2p=1")
	}

	if opts.EnableValidator {
		services = append(services, "-validator=1")
	}

	if opts.EnableLegacy {
		services = append(services, "-legacy=1")
	}

	WaitForPortsFree(t, tSettings)

	go d.Start(logger, services, tSettings, readyCh)

	select {
	case <-readyCh:
		t.Logf("Daemon %s started successfully", tSettings.ClientName)
	case <-time.After(20 * time.Second):
		t.Fatalf("Daemon %s failed to start within timeout", tSettings.ClientName)
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

	ports := []int{tSettings.HealthCheckPort}
	require.NoError(t, WaitForHealthLiveness(ports, 10*time.Second))

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

	subtreeStore, err := GetSubtreeStore(logger, tSettings)
	require.NoError(t, err)

	utxoStore, err := GetUtxoStore(ctx, logger, tSettings)
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
		composeDependencies:   composeDependencies,
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
		AssetURL:              fmt.Sprintf("http://localhost:%d", tSettings.Asset.HTTPPort),
		Settings:              tSettings,
	}
}

func (td *TestDaemon) Stop(t *testing.T) {
	if err := td.d.Stop(); err != nil {
		t.Errorf("Failed to stop daemon %s: %v", td.Settings.ClientName, err)
	}

	WaitForPortsFree(t, td.Settings)

	td.ctxCancel()

	t.Logf("Daemon %s stopped successfully", td.Settings.ClientName)
}

func (td *TestDaemon) StopDaemonDependencies() {
	StopDaemonDependencies(td.Ctx, td.composeDependencies)
}

func WaitForPortsFree(t *testing.T, settings *settings.Settings) {
	require.NoError(t, testutil.WaitForPortsFree(t.Context(), "localhost", GetPorts(settings), 10*time.Second, 100*time.Millisecond))
}

func GetPorts(tSettings *settings.Settings) []int {
	ports := []int{
		getPortFromString(tSettings.Asset.CentrifugeListenAddress),
		getPortFromString(tSettings.Asset.HTTPListenAddress),
		getPortFromString(tSettings.Block.PersisterHTTPListenAddress),
		getPortFromString(tSettings.BlockAssembly.GRPCListenAddress),
		getPortFromString(tSettings.BlockChain.GRPCListenAddress),
		getPortFromString(tSettings.BlockChain.HTTPListenAddress),
		getPortFromString(tSettings.BlockValidation.GRPCListenAddress),
		getPortFromString(tSettings.Validator.GRPCListenAddress),
		getPortFromString(tSettings.Validator.HTTPListenAddress),
		getPortFromString(tSettings.P2P.GRPCListenAddress),
		getPortFromString(tSettings.P2P.HTTPListenAddress),
		getPortFromString(tSettings.Coinbase.GRPCListenAddress),
		getPortFromString(tSettings.SubtreeValidation.GRPCListenAddress),
		getPortFromString(tSettings.Legacy.GRPCListenAddress),
		getPortFromString(tSettings.Propagation.HTTPListenAddress),
		getPortFromString(tSettings.Propagation.GRPCListenAddress),
		getPortFromString(tSettings.Faucet.HTTPListenAddress),
		getPortFromURL(tSettings.RPC.RPCListenerURL),
	}

	// remove all where port == 0
	ports = removeZeros(ports)

	return ports
}

func removeZeros(ports []int) []int {
	var result []int

	for _, port := range ports {
		if port != 0 {
			result = append(result, port)
		}
	}

	return result
}

func getPortFromString(address string) int {
	if address == "" {
		return 0
	}

	if !strings.Contains(address, ":") {
		return 0
	}

	portString := strings.Split(address, ":")[1]

	port, err := strconv.Atoi(portString)
	if err != nil {
		return 0
	}

	return port
}

func getPortFromURL(url *url.URL) int {
	port, err := strconv.Atoi(url.Port())
	if err != nil {
		return 0
	}

	return port
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

func (td *TestDaemon) VerifyConflictingInSubtrees(t *testing.T, subtreeHash *chainhash.Hash, expectedConflicts ...*bt.Tx) {
	latestSubtreeBytes, err := td.SubtreeStore.Get(td.Ctx, subtreeHash[:], options.WithFileExtension("subtree"))
	require.NoError(t, err, "Failed to get subtree")

	latestSubtree, err := util.NewSubtreeFromBytes(latestSubtreeBytes)
	require.NoError(t, err, "Failed to parse subtree bytes")

	require.Len(t, latestSubtree.ConflictingNodes, len(expectedConflicts),
		"Unexpected number of conflicting nodes in subtree")

	for _, conflict := range expectedConflicts {
		// conflicting txs are not in order
		assert.True(t, slices.Contains(latestSubtree.ConflictingNodes, *conflict.TxIDChainHash()), "Expected conflicting node %s not found in subtree", conflict.String())
	}
}

func (td *TestDaemon) VerifyConflictingInUtxoStore(t *testing.T, conflictValue bool, expectedConflicts ...*bt.Tx) {
	for _, conflict := range expectedConflicts {
		readTx, err := td.UtxoStore.Get(td.Ctx, conflict.TxIDChainHash())
		require.NoError(t, err, "Failed to get transaction %s", conflict.String())
		assert.Equal(t, conflictValue, readTx.Conflicting, "Expected transaction %s to be marked as conflicting", conflict.String())
	}
}

func (td *TestDaemon) VerifyNotInBlockAssembly(t *testing.T, txs ...*bt.Tx) {
	// get a mining candidate and check the subtree does not contain the given transactions
	candidate, err := td.BlockAssemblyClient.GetMiningCandidate(td.Ctx, true)
	require.NoError(t, err)

	for _, subtreeHash := range candidate.SubtreeHashes {
		subtreeBytes, err := td.SubtreeStore.Get(td.Ctx, subtreeHash, options.WithFileExtension("subtree"))
		require.NoError(t, err, "Failed to get subtree")

		subtree, err := util.NewSubtreeFromBytes(subtreeBytes)
		require.NoError(t, err, "Failed to parse subtree bytes")

		for _, tx := range txs {
			hash := *tx.TxIDChainHash()
			found := subtree.HasNode(hash)
			assert.False(t, found, "Expected subtree to not contain transaction %s", hash.String())
		}
	}
}

func (td *TestDaemon) VerifyInBlockAssembly(t *testing.T, txs ...*bt.Tx) {
	// get a mining candidate and check the subtree does not contain the given transactions
	candidate, err := td.BlockAssemblyClient.GetMiningCandidate(td.Ctx, true)
	require.NoError(t, err)

	// Check the candidate has at least one subtree hash, otherwise there is nothing to check
	require.GreaterOrEqual(t, len(candidate.SubtreeHashes), 1, "Expected at least one subtree hash in the candidate")

	txFoundMap := make(map[chainhash.Hash]int)

	for _, tx := range txs {
		hash := *tx.TxIDChainHash()
		txFoundMap[hash] = 0
	}

	for _, subtreeHash := range candidate.SubtreeHashes {
		subtreeBytes, err := td.SubtreeStore.Get(td.Ctx, subtreeHash, options.WithFileExtension("subtree"))
		require.NoError(t, err, "Failed to get subtree")

		subtree, err := util.NewSubtreeFromBytes(subtreeBytes)
		require.NoError(t, err, "Failed to parse subtree bytes")

		for _, tx := range txs {
			hash := *tx.TxIDChainHash()

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

func (td *TestDaemon) CreateTransaction(t *testing.T, parentTx *bt.Tx) *bt.Tx {
	tx := bt.NewTx()

	useParentOutput, _ := rand.Int(rand.Reader, big.NewInt(int64(len(parentTx.Outputs))))

	if useParentOutput.Int64() == int64(len(parentTx.Outputs)-1) {
		// if the last input was selected (the OP_RETURN output), use the first output instead
		useParentOutput = big.NewInt(0)
	}

	// convert to uint32
	useParentOutputUint32, err := util.SafeUint64ToUint32(useParentOutput.Uint64())
	require.NoError(t, err)

	err = tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      parentTx.TxIDChainHash(),
		Vout:          useParentOutputUint32,
		LockingScript: parentTx.Outputs[useParentOutputUint32].LockingScript,
		Satoshis:      parentTx.Outputs[useParentOutputUint32].Satoshis,
	})
	require.NoError(t, err)

	amount := parentTx.Outputs[useParentOutputUint32].Satoshis

	// create a random number of outputs
	numOutputs, _ := rand.Int(rand.Reader, big.NewInt(10))
	if numOutputs.Uint64() == 0 {
		numOutputs = big.NewInt(1)
	}

	fee := uint64(1)

	amountPerOutput := (amount - fee) / numOutputs.Uint64()

	for i := uint64(0); i < numOutputs.Uint64(); i++ {
		err = tx.AddP2PKHOutputFromPubKeyBytes(td.privKey.PubKey().SerialiseCompressed(), amountPerOutput)
		require.NoError(t, err)
	}

	// add some random data as OP_RETURN to make sure the transaction is unique
	randomBytes := make([]byte, 64)
	_, err = rand.Read(randomBytes)
	require.NoError(t, err)

	err = tx.AddOpReturnOutput(randomBytes)
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

func (td *TestDaemon) CreateTestBlock(t *testing.T, previousBlock *model.Block, nonce uint32, txs ...*bt.Tx) (*util.Subtree, *model.Block) {
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

func (td *TestDaemon) WaitForBlockHeight(t *testing.T, expectedBlock *model.Block, timeout time.Duration, skipVerifyChain ...bool) {
	deadline := time.Now().Add(timeout)

	var (
		tmpBlock *model.Block
		err      error
		state    *blockassembly_api.StateMessage
	)

finished:
	for {
		switch {
		case time.Now().After(deadline):
			t.Fatalf("Timeout waiting for block height %d", expectedBlock.Height)
		default:
			tmpBlock, err = td.BlockchainClient.GetBlockByHeight(td.Ctx, expectedBlock.Height)
			if err == nil {
				break finished
			}

			if !errors.Is(err, errors.ErrBlockNotFound) {
				t.Fatalf("Failed to get block at height %d: %v", expectedBlock.Height, err)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	assert.Equal(t, expectedBlock.Header.Hash().String(), tmpBlock.Header.Hash().String(),
		"Block hash mismatch at height %d", expectedBlock.Height)

	for state == nil || state.CurrentHeight < expectedBlock.Height {
		state, err = td.BlockAssemblyClient.GetBlockAssemblyState(td.Ctx)
		require.NoError(t, err)

		if time.Now().After(deadline) {
			t.Logf("Timeout waiting for block height %d", expectedBlock.Height)
			t.FailNow()
		}

		time.Sleep(10 * time.Millisecond)
	}

	require.LessOrEqual(t, expectedBlock.Height, state.CurrentHeight, "Expected block assembly to reach height %d or higher", expectedBlock.Height)

	if len(skipVerifyChain) > 0 && skipVerifyChain[0] {
		return
	}

	previousBlockHash := expectedBlock.Header.HashPrevBlock

	for height := expectedBlock.Height - 1; height > 0; height-- {
		getBlockByHeight, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, height)
		require.NoError(t, err)

		require.Equal(t, previousBlockHash.String(), getBlockByHeight.Header.Hash().String(), "Block hash mismatch at height %d", height)

		previousBlockHash = getBlockByHeight.Header.HashPrevBlock
	}
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
		options.WithDeleteAt(100),
		options.WithAllowOverwrite(true),
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
		options.WithDeleteAt(100),
		options.WithAllowOverwrite(true),
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

func (td *TestDaemon) ResetServiceManagerContext(t *testing.T) {
	err := td.d.ServiceManager.ResetContext()
	require.NoError(t, err)
}

func WaitForHealthLiveness(ports []int, timeout time.Duration) error {
	timeoutElapsed := time.After(timeout)

	var err error

	for _, port := range ports {
		healthReadinessEndpoint := fmt.Sprintf("http://localhost:%d/health/readiness", port)

	out:
		for {
			select {
			case <-timeoutElapsed:
				return errors.NewError("health check failed for port %d after timeout: %v", port, timeout, err)
			default:
				_, err = util.DoHTTPRequest(context.Background(), healthReadinessEndpoint, nil)
				if err != nil {
					time.Sleep(100 * time.Millisecond)

					continue
				}

				break out
			}
		}
	}

	return nil
}

func (td *TestDaemon) CreateAndSendTxs(t *testing.T, parentTx *bt.Tx, count int) ([]*bt.Tx, []*chainhash.Hash, error) {
	transactions := make([]*bt.Tx, count)
	currentParent := parentTx
	txHashes := make([]*chainhash.Hash, 0)

	for i := 0; i < count; i++ {
		utxo := &bt.UTXO{
			TxIDHash:      currentParent.TxIDChainHash(),
			Vout:          uint32(0),
			LockingScript: currentParent.Outputs[0].LockingScript,
			Satoshis:      currentParent.Outputs[0].Satoshis,
		}

		newTx := bt.NewTx()

		err := newTx.FromUTXOs(utxo)
		require.NoError(t, err)

		outputAmount := currentParent.Outputs[0].Satoshis - 1000 // minus 1000 satoshis for fee
		if outputAmount <= 0 {
			return transactions, nil, errors.NewProcessingError("insufficient funds for next transaction", nil)
		}

		err = newTx.AddP2PKHOutputFromPubKeyBytes(td.privKey.PubKey().SerialiseCompressed(), outputAmount)
		if err != nil {
			return transactions, nil, errors.NewProcessingError("Error adding output to transaction", err)
		}

		err = newTx.FillAllInputs(td.Ctx, &unlocker.Getter{PrivateKey: td.privKey})
		require.NoError(t, err)

		_, err = td.DistributorClient.SendTransaction(td.Ctx, newTx)
		require.NoError(t, err)

		td.Logger.Infof("Transaction %d sent: %s", i+1, newTx.TxID())

		transactions[i] = newTx
		txHashes = append(txHashes, newTx.TxIDChainHash())
		currentParent = newTx
	}

	return transactions, txHashes, nil
}

type daemonDependency struct {
	name string
	port int
}

// nolint:unused
func calculateDependencies(t *testing.T, tSettings []*settings.Settings) []daemonDependency {
	dependencies := make([]daemonDependency, 5)

	// Blockchain store
	url := tSettings[0].BlockChain.StoreURL

	port, err := strconv.Atoi(url.Port())
	if err != nil {
		t.Fatalf("Failed to parse store port: %v", err)
	}

	dependencies = append(dependencies, daemonDependency{"postgres", port})

	// Kafka
	url = tSettings[0].Kafka.BlocksConfig

	port, err = strconv.Atoi(url.Port())
	if err != nil {
		t.Fatalf("Failed to parse store port: %v", err)
	}

	dependencies = append(dependencies, daemonDependency{"kafka-shared", port})

	// Aerospike
	for i, s := range tSettings {
		url = s.UtxoStore.UtxoStore

		port, err = strconv.Atoi(url.Port())
		if err != nil {
			t.Fatalf("Failed to parse store port: %v", err)
		}

		dependencies = append(dependencies, daemonDependency{"aerospike-" + strconv.Itoa(i+1), port})
	}

	return dependencies
}

func StartDaemonDependencies(t *testing.T, removeDataDir bool, dependencies []daemonDependency) tc.ComposeStack {
	var (
		err     error
		compose tc.ComposeStack
	)

	identifier := tc.StackIdentifier(fmt.Sprintf("test-%d", time.Now().UnixNano()))

	if removeDataDir {
		err := os.RemoveAll("./data")
		require.NoError(t, err)
	}

	compose, err = tc.NewDockerComposeWith(tc.WithStackFiles("../../docker-compose-host.yml"), identifier)
	if err != nil {
		t.Fatalf("Failed to create docker network: %v", err)
	}

	services := make([]string, len(dependencies))
	for i, dependency := range dependencies {
		services[i] = dependency.name
	}

	if err := compose.Up(t.Context(), tc.RunServices(services...)); err != nil {
		t.Fatalf("Failed to start docker network: %v", err)
	}

	ports := make([]int, len(dependencies))
	for i, dependency := range dependencies {
		ports[i] = dependency.port
	}

	// Wait for dependent services to become ready
	if err := testutil.WaitForPortsReady(t.Context(), "localhost", ports, 5*time.Second, 100*time.Millisecond); err != nil {
		// If the wait fails (timeout), stop the docker stack before failing the test
		log.Printf("Services failed to start, attempting to stop docker stack...")

		downCtx, downCancel := context.WithTimeout(t.Context(), 30*time.Second)

		defer downCancel()

		if downErr := compose.Down(downCtx, tc.RemoveOrphans(true)); downErr != nil {
			log.Printf("Error stopping docker stack after port wait failure: %v", downErr)
		}

		t.Fatalf("Failed waiting for service ports: %v", err)
	}

	// even tho the ports are 'ready', if you try to connect to aerospike you might see:
	// Node C81A9166430781A (127.0.0.1:3200) is not yet fully initialized
	// time.Sleep(1 * time.Second)

	return compose
}

func StopDaemonDependencies(ctx context.Context, compose tc.ComposeStack) {
	if compose == nil {
		log.Printf("No docker stack to stop.")
		return
	}

	log.Printf("Attempting to stop docker stack...")

	// Inside StopDaemonDependencies
	downCtx, downCancel := context.WithTimeout(context.Background(), 60*time.Second) // Use a separate timeout for cleanup
	defer downCancel()

	downErr := compose.Down(downCtx, tc.RemoveOrphans(true))
	if downErr != nil {
		log.Printf("Error stopping docker stack: %v. Will still attempt container and port checks.", downErr)
	} else {
		log.Printf("Docker stack stopped successfully (according to compose.Down).")
	}

	// ----> NEW: Wait for containers to disappear <----
	// Assuming project name is 'test', derived from 'test/docker-compose-host.yml'
	projectName := "test"
	containerWaitTimeout := 60 * time.Second // Timeout for waiting for containers to be gone
	containerCheckInterval := 2 * time.Second
	// Use a separate context for this wait, derived from Background
	containerWaitCtx, containerWaitCancel := context.WithTimeout(context.Background(), containerWaitTimeout)
	defer containerWaitCancel()

	log.Printf("Checking if containers for project '%s' are gone...", projectName)

	if err := testutil.WaitForDockerComposeProjectDown(containerWaitCtx, projectName, containerWaitTimeout, containerCheckInterval); err != nil {
		// Log potentially more severe error, but don't necessarily fail the test run here
		log.Printf("ERROR: %v", err)
	} else {
		log.Printf("Container check passed for project '%s'.", projectName)
	}
	// ----> END NEW <----

	// Wait for the ports used by the services to become free (existing code)
	portsToCheck := []int{15432, 19092, 3100, 3200, 3300}
	waitTimeout := 30 * time.Second
	waitInterval := 500 * time.Millisecond

	log.Printf("Calling WaitForPortsFree (timeout %s, interval %s)...", waitTimeout, waitInterval)
	waitCtx, cancelWait := context.WithTimeout(context.Background(), waitTimeout)

	defer cancelWait()

	if err := testutil.WaitForPortsFree(waitCtx, "localhost", portsToCheck, waitTimeout, waitInterval); err != nil {
		log.Printf("Warning during WaitForPortsFree: %v", err)
	} else {
		log.Printf("Confirmed dependent service ports are free.")
	}

	log.Printf("StopDaemonDependencies finished.")
}

// CreateParentTransactions creates a single transaction with multiple outputs from a parent transaction.
// Each output can then be spent concurrently by child transactions.
// count specifies how many outputs to create in the transaction.
func (td *TestDaemon) CreateParentTransactionWithNOutputs(t *testing.T, parentTx *bt.Tx, count int) (*bt.Tx, error) {
	// Create a new transaction
	newTx := bt.NewTx()

	// Add input from parent transaction using UTXO
	utxo := &bt.UTXO{
		TxIDHash:      parentTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: parentTx.Outputs[0].LockingScript,
		Satoshis:      parentTx.Outputs[0].Satoshis,
	}
	err := newTx.FromUTXOs(utxo)

	if err != nil {
		return nil, errors.NewProcessingError("failed to add input", err)
	}

	// Calculate satoshis per output, leaving some for fees
	// Reserve 1000 satoshis for fees
	//nolint:gosec
	totalSatoshis := parentTx.Outputs[0].Satoshis - 1000
	satoshisPerOutput := totalSatoshis / uint64(count) //nolint:gosec

	// Create the specified number of outputs, all using the same key
	for i := 0; i < count; i++ {
		// Add output using TestDaemon's key
		err = newTx.AddP2PKHOutputFromPubKeyBytes(td.privKey.PubKey().SerialiseCompressed(), satoshisPerOutput)
		if err != nil {
			return nil, errors.NewProcessingError("failed to add output", err)
		}
	}

	// Fill all inputs (signs the transaction)
	err = newTx.FillAllInputs(td.Ctx, &unlocker.Getter{PrivateKey: td.privKey})
	if err != nil {
		return nil, errors.NewProcessingError("failed to sign transaction", err)
	}

	// Send the transaction
	response, err := td.DistributorClient.SendTransaction(td.Ctx, newTx)
	require.NoError(t, err)

	require.Equal(t, len(response), 1)

	td.Logger.Infof("Created parent transaction with %d outputs: %s, error: %v", count, newTx.TxID(), response[0].Error)

	// Wait a bit for the transaction to be processed
	time.Sleep(1 * time.Second)

	return newTx, nil
}

// CreateAndSendTxsConcurrently creates and sends transactions concurrently using multiple goroutines
func (td *TestDaemon) CreateAndSendTxsConcurrently(t *testing.T, parentTx *bt.Tx) ([]*bt.Tx, []*chainhash.Hash, error) {
	transactions := make([]*bt.Tx, len(parentTx.Outputs))
	txHashes := make([]*chainhash.Hash, len(parentTx.Outputs))

	resultChan := make(chan struct {
		index int
		tx    *bt.Tx
	}, len(parentTx.Outputs))
	errorChan := make(chan error, 1)

	var wg sync.WaitGroup
	// Create a goroutine for each output to spend
	for index := 0; index < len(parentTx.Outputs); index++ {
		// for index := 0; index < 10; index++ {
		wg.Add(1)

		go func(index int) {
			defer wg.Done()

			//nolint:gosec
			utxo := &bt.UTXO{
				TxIDHash:      parentTx.TxIDChainHash(),
				Vout:          uint32(index),
				LockingScript: parentTx.Outputs[index].LockingScript,
				Satoshis:      parentTx.Outputs[index].Satoshis,
			}

			newTx := bt.NewTx()
			if err := newTx.FromUTXOs(utxo); err != nil {
				errorChan <- errors.NewProcessingError("Error creating transaction from UTXO", err)
			}

			outputAmount := parentTx.Outputs[index].Satoshis - 1000 // minus 1000 satoshis for fee

			// Add two outputs to allow for further spending
			// splitAmount := outputAmount / 2
			if err := newTx.AddP2PKHOutputFromPubKeyBytes(td.privKey.PubKey().SerialiseCompressed(), outputAmount); err != nil {
				errorChan <- errors.NewProcessingError("Error adding first output to transaction", err)
			}

			if err := newTx.FillAllInputs(td.Ctx, &unlocker.Getter{PrivateKey: td.privKey}); err != nil {
				errorChan <- errors.NewProcessingError("Error filling inputs", err)
			}

			if _, err := td.DistributorClient.SendTransaction(td.Ctx, newTx); err != nil {
				errorChan <- errors.NewProcessingError("Error sending transaction", err)
			}

			resultChan <- struct {
				index int
				tx    *bt.Tx
			}{index: index, tx: newTx}
		}(index)
	}

	// Start a goroutine to close the result channel when all work is done
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	for index := 0; index < len(parentTx.Outputs); index++ {
		result := <-resultChan
		transactions[result.index] = result.tx
		txHashes[result.index] = result.tx.TxIDChainHash()
		td.Logger.Infof("Transaction %d sent: %s", result.index+1, result.tx.TxID())
	}

	return transactions, txHashes, nil
}

func (td *TestDaemon) GetPrivateKey(t *testing.T) *bec.PrivateKey {
	privKey := td.privKey
	return privKey
}
