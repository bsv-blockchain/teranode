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

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/pkg/go-chaincfg"
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
	"github.com/bitcoin-sv/teranode/test/utils/transactions"
	"github.com/bitcoin-sv/teranode/test/utils/wait"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
	"go.opentelemetry.io/otel"
	otelprop "go.opentelemetry.io/otel/propagation"
)

const (
	blockHashMismatch         = "Block hash mismatch at height %d"
	failedGettingSubtree      = "Failed to get subtree"
	failedParsingSubtreeBytes = "Failed to parse subtree bytes"
	failedParsingStorePort    = "Failed to parse store port: %v"
)

// TestDaemon is a struct that holds the test daemon instance and its dependencies.
type TestDaemon struct {
	AssetURL              string
	BlockAssemblyClient   *blockassembly.Client
	BlockValidationClient *blockvalidation.Client
	BlockchainClient      blockchain.ClientI
	Ctx                   context.Context
	DistributorClient     *distributor.Distributor
	Logger                ulogger.Logger
	PropagationClient     *propagation.Client
	Settings              *settings.Settings
	SubtreeStore          blob.Store
	UtxoStore             utxo.Store
	composeDependencies   tc.ComposeStack
	ctxCancel             context.CancelFunc
	d                     *Daemon
	privKey               *bec.PrivateKey
	rpcURL                *url.URL
}

// TestOptions defines the options for creating a test daemon instance.
type TestOptions struct {
	EnableFullLogging       bool
	EnableLegacy            bool
	EnableP2P               bool
	EnableRPC               bool
	EnableValidator         bool
	SettingsContext         string
	SettingsOverrideFunc    func(*settings.Settings)
	SkipRemoveDataDir       bool
	StartDaemonDependencies bool
	UseTracing              bool
}

// JSONError represents a JSON error response from the RPC server.
type JSONError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Error implements the error interface for JSONError.
func (je *JSONError) Error() string {
	return fmt.Sprintf("code: %d, message: %s", je.Code, je.Message)
}

// NewTestDaemon creates a new TestDaemon instance with the provided options.
func NewTestDaemon(t *testing.T, opts TestOptions) *TestDaemon {
	ctx, cancel := context.WithCancel(context.Background())

	var (
		composeDependencies tc.ComposeStack
		appSettings         *settings.Settings
	)

	if opts.SettingsContext != "" {
		appSettings = settings.NewSettings(opts.SettingsContext)
	} else {
		appSettings = settings.NewSettings() // This reads gocore.Config and applies sensible defaults
	}

	path := filepath.Join("data", appSettings.ClientName)
	if strings.HasPrefix(opts.SettingsContext, "dev.system.test") {
		// a bit hacky. Ideally, all stores sit under data/${ClientName}
		path = "data"
	}

	if !opts.SkipRemoveDataDir {
		absPath, err := filepath.Abs(path)
		require.NoError(t, err)

		t.Logf("Removing data directory: %s", absPath)
		err = os.RemoveAll(absPath)
		require.NoError(t, err)
	}

	// if opts.StartDaemonDependencies {
	// composeDependencies = StartDaemonDependencies(ctx, t, !opts.SkipRemoveDataDir, calculateDependencies(t, opts.Settings))
	// }

	appSettings.ChainCfgParams = &chaincfg.RegressionNetParams
	appSettings.ChainCfgParams.CoinbaseMaturity = 1

	absPath, err := filepath.Abs(path)
	require.NoError(t, err)
	t.Logf("Creating data directory: %s", absPath)

	err = os.MkdirAll(absPath, 0755)
	require.NoError(t, err)

	quorumPath := appSettings.SubtreeValidation.QuorumPath
	require.NotNil(t, quorumPath, "No subtree_quorum_path specified")

	err = os.MkdirAll(quorumPath, 0755)
	require.NoError(t, err)

	// Override with test settings...
	appSettings.Asset.CentrifugeDisable = true
	appSettings.UtxoStore.DBTimeout = 500 * time.Second
	appSettings.LocalTestStartFromState = "RUNNING"
	appSettings.SubtreeValidation.TxMetaCacheEnabled = false
	appSettings.ProfilerAddr = ""
	appSettings.RPC.CacheEnabled = false
	appSettings.P2P.DHTUsePrivate = true

	// Override with test settings...
	if opts.SettingsOverrideFunc != nil {
		opts.SettingsOverrideFunc(appSettings)
	}

	readyCh := make(chan struct{})

	var (
		logger        ulogger.Logger
		loggerFactory Option
	)

	if opts.EnableFullLogging {
		logger = ulogger.New(appSettings.ClientName)
		loggerFactory = WithLoggerFactory(func(serviceName string) ulogger.Logger {
			return ulogger.New(appSettings.ClientName+"-"+serviceName, ulogger.WithLevel("DEBUG"))
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

	WaitForPortsFree(t, ctx, appSettings)

	go d.Start(logger, services, appSettings, readyCh)

	select {
	case <-readyCh:
		t.Logf("Daemon %s started successfully", appSettings.ClientName)
	case <-time.After(30 * time.Second):
		t.Fatalf("Daemon %s failed to start within 30s", appSettings.ClientName)
	}

	ports := []int{appSettings.HealthCheckPort}
	require.NoError(t, WaitForHealthLiveness(ports, 10*time.Second))

	var blockchainClient blockchain.ClientI

	blockchainClient, err = blockchain.NewClient(ctx, logger, appSettings, "test")
	require.NoError(t, err)

	var blockAssemblyClient *blockassembly.Client

	blockAssemblyClient, err = blockassembly.NewClient(ctx, logger, appSettings)
	require.NoError(t, err)

	var propagationClient *propagation.Client

	propagationClient, err = propagation.NewClient(ctx, logger, appSettings)
	require.NoError(t, err)

	var blockValidationClient *blockvalidation.Client

	blockValidationClient, err = blockvalidation.NewClient(ctx, logger, appSettings, "test")
	require.NoError(t, err)

	var distributorClient *distributor.Distributor

	distributorClient, err = distributor.NewDistributor(ctx, logger, appSettings,
		distributor.WithBackoffDuration(500*time.Millisecond),
		distributor.WithRetryAttempts(10),
		distributor.WithFailureTolerance(0),
	)
	require.NoError(t, err)

	var w *wif.WIF
	w, err = wif.DecodeWIF(appSettings.BlockAssembly.MinerWalletPrivateKeys[0])
	require.NoError(t, err)

	privKey := w.PrivKey

	var subtreeStore blob.Store

	subtreeStore, err = d.daemonStores.GetSubtreeStore(logger, appSettings)
	require.NoError(t, err)

	var utxoStore utxo.Store

	utxoStore, err = d.daemonStores.GetUtxoStore(ctx, logger, appSettings)
	require.NoError(t, err)

	assert.NotNil(t, blockchainClient)
	assert.NotNil(t, blockAssemblyClient)
	assert.NotNil(t, propagationClient)
	assert.NotNil(t, blockValidationClient)
	assert.NotNil(t, subtreeStore)
	assert.NotNil(t, utxoStore)
	assert.NotNil(t, distributorClient)

	return &TestDaemon{
		AssetURL:              fmt.Sprintf("http://localhost:%d", appSettings.Asset.HTTPPort),
		BlockAssemblyClient:   blockAssemblyClient,
		BlockValidationClient: blockValidationClient,
		BlockchainClient:      blockchainClient,
		Ctx:                   ctx,
		DistributorClient:     distributorClient,
		Logger:                logger,
		PropagationClient:     propagationClient,
		Settings:              appSettings,
		SubtreeStore:          subtreeStore,
		UtxoStore:             utxoStore,
		composeDependencies:   composeDependencies,
		ctxCancel:             cancel,
		d:                     d,
		privKey:               privKey,
		rpcURL:                appSettings.RPC.RPCListenerURL,
	}
}

// Stop stops the TestDaemon instance and cleans up resources.
func (td *TestDaemon) Stop(t *testing.T, skipTracerShutdown ...bool) {
	if err := td.d.Stop(skipTracerShutdown...); err != nil {
		t.Errorf("Failed to stop daemon %s: %v", td.Settings.ClientName, err)
	}

	WaitForPortsFree(t, td.Ctx, td.Settings)

	td.ctxCancel()

	t.Logf("Daemon %s stopped successfully", td.Settings.ClientName)
}

// StopDaemonDependencies stops the daemon dependencies if they were started.
func (td *TestDaemon) StopDaemonDependencies() {
	StopDaemonDependencies(td.Ctx, td.composeDependencies)
}

// WaitForPortsFree waits for the specified ports to be free on localhost.
func WaitForPortsFree(t *testing.T, ctx context.Context, settings *settings.Settings) {
	require.NoError(t, wait.ForPortsFree(ctx, "localhost", GetPorts(settings), 30*time.Second, 100*time.Millisecond))
}

// GetPorts returns a slice of ports from the provided settings.
func GetPorts(appSettings *settings.Settings) []int {
	ports := []int{
		getPortFromString(appSettings.Asset.CentrifugeListenAddress),
		getPortFromString(appSettings.Asset.HTTPListenAddress),
		getPortFromString(appSettings.Block.PersisterHTTPListenAddress),
		getPortFromString(appSettings.BlockAssembly.GRPCListenAddress),
		getPortFromString(appSettings.BlockChain.GRPCListenAddress),
		getPortFromString(appSettings.BlockChain.HTTPListenAddress),
		getPortFromString(appSettings.BlockValidation.GRPCListenAddress),
		getPortFromString(appSettings.Validator.GRPCListenAddress),
		getPortFromString(appSettings.Validator.HTTPListenAddress),
		getPortFromString(appSettings.P2P.GRPCListenAddress),
		getPortFromString(appSettings.P2P.HTTPListenAddress),
		getPortFromString(appSettings.Coinbase.GRPCListenAddress),
		getPortFromString(appSettings.SubtreeValidation.GRPCListenAddress),
		getPortFromString(appSettings.Legacy.GRPCListenAddress),
		getPortFromString(appSettings.Propagation.HTTPListenAddress),
		getPortFromString(appSettings.Propagation.GRPCListenAddress),
		getPortFromString(appSettings.Faucet.HTTPListenAddress),
		getPortFromURL(appSettings.RPC.RPCListenerURL),
	}

	// remove all where port == 0
	ports = removeZeros(ports)

	return ports
}

// removeZeros removes all zero values from the slice of ports.
func removeZeros(ports []int) []int {
	var result []int

	for _, port := range ports {
		if port != 0 {
			result = append(result, port)
		}
	}

	return result
}

// getPortFromString extracts the port number from a string address.
// This works for IPV4 and IPV6 addresses, as well as simple hostnames.
func getPortFromString(address string) int {
	if address == "" {
		return 0
	}

	lastColon := strings.LastIndex(address, ":")
	if lastColon == -1 || lastColon == len(address)-1 {
		return 0
	}

	portString := address[lastColon+1:]

	port, err := strconv.Atoi(portString)
	if err != nil {
		return 0
	}

	return port
}

// getPortFromURL extracts the port number from a URL.
func getPortFromURL(url *url.URL) int {
	port, err := strconv.Atoi(url.Port())
	if err != nil {
		return 0
	}

	return port
}

// CallRPC calls the RPC method with the given parameters and returns the response as a string.
func (td *TestDaemon) CallRPC(ctx context.Context, method string, params []interface{}) (string, error) {
	// Create the request payload
	requestBody, err := json.Marshal(map[string]interface{}{
		"method": method,
		"params": params,
	})
	td.Logger.Infof("Request: %s", string(requestBody))

	if err != nil {
		return "", errors.NewProcessingError("failed to marshal request body", err)
	}

	// Create the HTTP request with context
	var req *http.Request

	req, err = http.NewRequestWithContext(ctx, "POST", td.rpcURL.String(), bytes.NewBuffer(requestBody))
	if err != nil {
		return "", errors.NewProcessingError("failed to create request", err)
	}

	// Set the appropriate headers
	req.SetBasicAuth("bitcoin", "bitcoin")
	req.Header.Set("Content-Type", "application/json")

	// Inject OpenTelemetry trace context into HTTP headers
	otel.GetTextMapPropagator().Inject(ctx, otelprop.HeaderCarrier(req.Header))

	// Perform the request
	client := &http.Client{}

	var resp *http.Response

	resp, err = client.Do(req)
	if err != nil {
		return "", errors.NewProcessingError("failed to perform request", err)
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	// Check the status code
	if resp.StatusCode != http.StatusOK {
		return "", errors.NewProcessingError("expected status code 200, got %v", resp.StatusCode)
	}

	// Read the response body
	var body []byte

	body, err = io.ReadAll(resp.Body)
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

	if err = json.Unmarshal(body, &jsonResponse); err != nil {
		return string(body), errors.NewProcessingError("failed to parse response JSON", err)
	}

	if jsonResponse.Error != nil {
		return string(body), errors.NewProcessingError("rpc returned error", jsonResponse.Error)
	}

	// Return the response as a string
	return string(body), nil
}

// VerifyBlockByHeight verifies that the block at the given height matches the expected block.
func (td *TestDaemon) VerifyBlockByHeight(t *testing.T, expectedBlock *model.Block, height uint32) {
	tmpBlock, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, height)
	require.NoError(t, err, "Failed to get block at height %d", height)
	assert.Equal(t, expectedBlock.Header.Hash().String(), tmpBlock.Header.Hash().String(), blockHashMismatch, height)
}

// VerifyBlockByHash verifies that the block at the given hash matches the expected block.
func (td *TestDaemon) VerifyBlockByHash(t *testing.T, expectedBlock *model.Block, hash *chainhash.Hash) {
	tmpBlock, err := td.BlockchainClient.GetBlock(td.Ctx, hash)
	require.NoError(t, err, "Failed to get block at hash %s", hash)
	assert.Equal(t, expectedBlock.Header.Hash().String(), tmpBlock.Header.Hash().String(),
		"Block hash mismatch at hash %s", hash)
}

// VerifyConflictingInSubtrees verifies that the expected conflicting transactions are present in the subtree with the given hash.
func (td *TestDaemon) VerifyConflictingInSubtrees(t *testing.T, subtreeHash *chainhash.Hash, expectedConflicts ...*bt.Tx) {
	latestSubtreeReader, err := td.SubtreeStore.GetIoReader(td.Ctx, subtreeHash[:], fileformat.FileTypeSubtree)
	require.NoError(t, err, failedGettingSubtree)

	var latestSubtree *util.Subtree

	latestSubtree, err = util.NewSubtreeFromReader(latestSubtreeReader)
	_ = latestSubtreeReader.Close() // Ensure the reader is closed after use

	require.NoError(t, err, failedParsingSubtreeBytes)

	require.Len(t, latestSubtree.ConflictingNodes, len(expectedConflicts),
		"Unexpected number of conflicting nodes in subtree")

	for _, conflict := range expectedConflicts {
		// conflicting txs are not in order
		assert.True(t, slices.Contains(latestSubtree.ConflictingNodes, *conflict.TxIDChainHash()),
			"Expected conflicting node %s not found in subtree", conflict.String())
	}
}

// VerifyConflictingInUtxoStore verifies that the expected conflicting transactions are marked as conflicting in the UTXO store.
func (td *TestDaemon) VerifyConflictingInUtxoStore(t *testing.T, conflictValue bool, expectedConflicts ...*bt.Tx) {
	for _, conflict := range expectedConflicts {
		readTx, err := td.UtxoStore.Get(td.Ctx, conflict.TxIDChainHash())
		require.NoError(t, err, "Failed to get transaction %s", conflict.String())
		assert.Equal(t, conflictValue, readTx.Conflicting,
			"Expected transaction %s to be marked as conflicting", conflict.String())
	}
}

// VerifyNotInBlockAssembly checks that the given transactions are not present in the block assembly candidate's subtrees.
func (td *TestDaemon) VerifyNotInBlockAssembly(t *testing.T, txs ...*bt.Tx) {
	// get a mining candidate and check the subtree does not contain the given transactions
	candidate, err := td.BlockAssemblyClient.GetMiningCandidate(td.Ctx, true)
	require.NoError(t, err)

	for _, subtreeHash := range candidate.SubtreeHashes {
		var subtreeReader io.ReadCloser

		subtreeReader, err = td.SubtreeStore.GetIoReader(td.Ctx, subtreeHash, fileformat.FileTypeSubtree)
		require.NoError(t, err, failedGettingSubtree)

		var subtree *util.Subtree

		subtree, err = util.NewSubtreeFromReader(subtreeReader)

		_ = subtreeReader.Close()

		// Ensure the reader is closed after use
		require.NoError(t, err, failedParsingSubtreeBytes)

		for _, tx := range txs {
			hash := *tx.TxIDChainHash()
			found := subtree.HasNode(hash)
			assert.False(t, found, "Expected subtree to not contain transaction %s", hash.String())
		}
	}
}

// VerifyInBlockAssembly checks that the given transactions are present in the block assembly candidate's subtrees exactly once.
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
		var subtreeReader io.ReadCloser

		subtreeReader, err = td.SubtreeStore.GetIoReader(td.Ctx, subtreeHash, fileformat.FileTypeSubtree)
		require.NoError(t, err, failedGettingSubtree)

		var subtree *util.Subtree

		subtree, err = util.NewSubtreeFromReader(subtreeReader)
		_ = subtreeReader.Close() // Ensure the reader is closed after use

		require.NoError(t, err, failedParsingSubtreeBytes)

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

// CreateTransaction creates a new transaction with a single input from the parent transaction.
func (td *TestDaemon) CreateTransaction(t *testing.T, parentTx *bt.Tx, useInput ...uint64) *bt.Tx {
	tx := bt.NewTx()

	var parentOutput uint64

	if len(useInput) > 0 {
		parentOutput = useInput[0]
	} else {
		useParentOutput, _ := rand.Int(rand.Reader, big.NewInt(int64(len(parentTx.Outputs))))

		if useParentOutput.Int64() == int64(len(parentTx.Outputs)-1) {
			// if the last input was selected (the OP_RETURN output), use the first output instead
			useParentOutput = big.NewInt(0)
		}

		parentOutput = useParentOutput.Uint64()
	}

	// convert to uint32
	useParentOutputUint32, err := util.SafeUint64ToUint32(parentOutput)
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
	var numOutputs *big.Int

	numOutputs, err = rand.Int(rand.Reader, big.NewInt(10))
	require.NoError(t, err)

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

// CreateTransactionFromMultipleInputs creates a new transaction with multiple inputs from the provided parent transactions.
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

// CreateTransactionWithOptions creates a new transaction with configurable options
// At least one parent transaction must be provided using WithParentTx or WithParentTxs
func (td *TestDaemon) CreateTransactionWithOptions(t *testing.T, options ...transactions.TxOption) *bt.Tx {
	var opts []transactions.TxOption

	opts = append(opts, transactions.WithPrivateKey(td.privKey)) // Put this as the first option so it is used as the fallback
	opts = append(opts, options...)                              // Add the other options, which may override the fallback if WithPrivateKey was specified

	return transactions.Create(t, opts...)
}

// MineToMaturityAndGetSpendableCoinbaseTx mines blocks to maturity and returns the spendable coinbase transaction.
func (td *TestDaemon) MineToMaturityAndGetSpendableCoinbaseTx(t *testing.T, ctx context.Context) *bt.Tx {
	_, err := td.CallRPC(ctx, "generate", []any{uint32(td.Settings.ChainCfgParams.CoinbaseMaturity + 1)})
	require.NoError(t, err)

	var lastBlock *model.Block

	lastBlock, err = td.BlockchainClient.GetBlockByHeight(ctx, uint32(td.Settings.ChainCfgParams.CoinbaseMaturity+1))
	require.NoError(t, err)

	td.WaitForBlockHeight(t, lastBlock, 10*time.Second)

	var block1 *model.Block

	block1, err = td.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)

	coinbaseTx := block1.CoinbaseTx

	return coinbaseTx
}

// MineAndWait mines the specified number of blocks and waits for them to be added to the blockchain.
func (td *TestDaemon) MineAndWait(t *testing.T, blockCount uint32) *model.Block {
	// Get the current block height
	_, meta, err := td.BlockchainClient.GetBestBlockHeader(td.Ctx)
	require.NoError(t, err)

	_, err = td.CallRPC(td.Ctx, "generate", []any{blockCount})
	require.NoError(t, err)

	endHeight := meta.Height + blockCount

	var lastBlock *model.Block

	lastBlock, err = td.BlockchainClient.GetBlockByHeight(td.Ctx, endHeight)
	require.NoError(t, err)

	return lastBlock
}

// CreateTestBlock creates a test block with the given previous block, nonce, and transactions.
func (td *TestDaemon) CreateTestBlock(t *testing.T, previousBlock *model.Block, nonce uint32, txs ...*bt.Tx) (*util.Subtree, *model.Block) {
	// Create and save the subtree with the double spend tx
	subtree, err := createAndSaveSubtrees(td.Ctx, td.SubtreeStore, txs)
	require.NoError(t, err)

	var address *bscript.Address

	address, err = bscript.NewAddressFromPublicKey(td.privKey.PubKey(), true)
	require.NoError(t, err)

	var coinbaseTx *bt.Tx

	coinbaseTx, err = model.CreateCoinbase(previousBlock.Height+1, 50e8, "test", []string{address.AddressString})
	require.NoError(t, err)

	var merkleRoot *chainhash.Hash

	merkleRoot, err = subtree.RootHashWithReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) // nolint:gosec
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

// WaitForBlockHeight waits for the blockchain to reach the specified block height and verifies the block.
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
		blockHashMismatch, expectedBlock.Height)

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
		var getBlockByHeight *model.Block

		getBlockByHeight, err = td.BlockchainClient.GetBlockByHeight(td.Ctx, height)
		require.NoError(t, err)

		require.Equal(t, previousBlockHash.String(), getBlockByHeight.Header.Hash().String(), blockHashMismatch, height)

		previousBlockHash = getBlockByHeight.Header.HashPrevBlock
	}
}

// createAndSaveSubtrees creates a new subtree with the given transactions and saves it to the subtree store.
func createAndSaveSubtrees(ctx context.Context, subtreeStore blob.Store, txs []*bt.Tx) (*util.Subtree, error) {
	subtree, err := util.NewIncompleteTreeByLeafCount(len(txs) + 1)
	if err != nil {
		return nil, err
	}

	subtreeData := util.NewSubtreeData(subtree)
	subtreeMeta := util.NewSubtreeMeta(subtree)

	err = subtree.AddCoinbaseNode()
	if err != nil {
		return nil, err
	}

	for i, tx := range txs {
		err = subtree.AddNode(*tx.TxIDChainHash(), uint64(i), uint64(i)) // nolint:gosec
		if err != nil {
			return nil, err
		}

		// add the transaction to the subtree data, with the index of the node, which is i+1, skipping the coinbase node
		err = subtreeData.AddTx(tx, i+1)
		if err != nil {
			return nil, err
		}

		parentTxHashes := make([]chainhash.Hash, len(tx.Inputs))
		for j, input := range tx.Inputs {
			// get the parent tx hash
			parentTxHashes[j] = *input.PreviousTxIDChainHash()
		}

		// add the transaction to the subtree meta, with the index of the node, which is i+1, skipping the coinbase node
		if err = subtreeMeta.SetTxInpointsFromTx(tx); err != nil {
			return nil, err
		}
	}

	if err = storeSubtreeFiles(ctx, subtreeStore, subtree, subtreeData, subtreeMeta); err != nil {
		return nil, err
	}

	return subtree, nil
}

// storeSubtreeFiles serializes and stores the subtree, subtree data, and subtree meta in the provided subtree store.
func storeSubtreeFiles(ctx context.Context, subtreeStore blob.Store, subtree *util.Subtree, subtreeData *util.SubtreeData, subtreeMeta *util.SubtreeMeta) error {
	subtreeBytes, err := subtree.Serialize()
	if err != nil {
		return err
	}

	err = subtreeStore.Set(
		ctx,
		subtree.RootHash()[:],
		fileformat.FileTypeSubtreeToCheck,
		subtreeBytes,
		options.WithDeleteAt(100),
		options.WithAllowOverwrite(true),
	)
	if err != nil {
		return err
	}

	subtreeDataBytes, err := subtreeData.Serialize()
	if err != nil {
		return err
	}

	err = subtreeStore.Set(
		ctx,
		subtreeData.RootHash()[:],
		fileformat.FileTypeSubtreeData,
		subtreeDataBytes,
		options.WithDeleteAt(100),
		options.WithAllowOverwrite(true),
	)
	if err != nil {
		return err
	}

	subtreeMetaBytes, err := subtreeMeta.Serialize()
	if err != nil {
		return err
	}

	err = subtreeStore.Set(
		ctx,
		subtree.RootHash()[:],
		fileformat.FileTypeSubtreeMeta,
		subtreeMetaBytes,
		options.WithDeleteAt(100),
		options.WithAllowOverwrite(true),
	)
	if err != nil {
		return err
	}

	return nil
}

// ResetServiceManagerContext resets the ServiceManager context to allow for a fresh start.
func (td *TestDaemon) ResetServiceManagerContext(t *testing.T) {
	err := td.d.ServiceManager.ResetContext()
	require.NoError(t, err)
}

// WaitForHealthLiveness waits for the health readiness endpoint of the given ports to respond within the specified timeout.
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

// CreateAndSendTxs creates and sends a specified number of transactions based on the provided parent transaction.
func (td *TestDaemon) CreateAndSendTxs(t *testing.T, parentTx *bt.Tx, count int) ([]*bt.Tx, []*chainhash.Hash, error) {
	rawTransactions := make([]*bt.Tx, count)
	currentParent := parentTx
	txHashes := make([]*chainhash.Hash, 0)

	for i := 0; i < count; i++ {
		existingUtxo := &bt.UTXO{
			TxIDHash:      currentParent.TxIDChainHash(),
			Vout:          uint32(0),
			LockingScript: currentParent.Outputs[0].LockingScript,
			Satoshis:      currentParent.Outputs[0].Satoshis,
		}

		newTx := bt.NewTx()

		err := newTx.FromUTXOs(existingUtxo)
		require.NoError(t, err)

		outputAmount := currentParent.Outputs[0].Satoshis - 1000 // minus 1000 satoshis for fee
		if outputAmount <= 0 {
			return rawTransactions, nil, errors.NewProcessingError("insufficient funds for next transaction", nil)
		}

		err = newTx.AddP2PKHOutputFromPubKeyBytes(td.privKey.PubKey().SerialiseCompressed(), outputAmount)
		if err != nil {
			return rawTransactions, nil, errors.NewProcessingError("error adding output to transaction", err)
		}

		err = newTx.FillAllInputs(td.Ctx, &unlocker.Getter{PrivateKey: td.privKey})
		require.NoError(t, err)

		_, err = td.DistributorClient.SendTransaction(td.Ctx, newTx)
		require.NoError(t, err)

		td.Logger.Infof("Transaction %d sent: %s", i+1, newTx.TxID())

		rawTransactions[i] = newTx
		txHashes = append(txHashes, newTx.TxIDChainHash())
		currentParent = newTx
	}

	return rawTransactions, txHashes, nil
}

// daemonDependency represents a dependency for the daemon, including its name and port.
type daemonDependency struct {
	name string
	port int
}

// calculateDependencies calculates the dependencies required for the daemon based on the provided app settings.
// nolint:unused // This function is used to calculate the dependencies for the daemon.
func calculateDependencies(t *testing.T, appSettings []*settings.Settings) []daemonDependency {
	dependencies := make([]daemonDependency, 5)

	// Blockchain store
	blockchainURL := appSettings[0].BlockChain.StoreURL

	port, err := strconv.Atoi(blockchainURL.Port())
	if err != nil {
		t.Fatalf(failedParsingStorePort, err)
	}

	dependencies = append(dependencies, daemonDependency{"postgres", port})

	// Kafka
	kafkaURL := appSettings[0].Kafka.BlocksConfig

	port, err = strconv.Atoi(kafkaURL.Port())
	if err != nil {
		t.Fatalf(failedParsingStorePort, err)
	}

	dependencies = append(dependencies, daemonDependency{"kafka-shared", port})

	// Aerospike
	for i, s := range appSettings {
		aeroURL := s.UtxoStore.UtxoStore

		port, err = strconv.Atoi(aeroURL.Port())
		if err != nil {
			t.Fatalf(failedParsingStorePort, err)
		}

		dependencies = append(dependencies, daemonDependency{"aerospike-" + strconv.Itoa(i+1), port})
	}

	return dependencies
}

// StartDaemonDependencies starts the required dependencies for the daemon using Docker Compose.
func StartDaemonDependencies(t *testing.T, removeDataDir bool, dependencies []daemonDependency) tc.ComposeStack {
	var (
		err     error
		compose tc.ComposeStack
	)

	identifier := tc.StackIdentifier(fmt.Sprintf("test-%d", time.Now().UnixNano()))

	if removeDataDir {
		err = os.RemoveAll("./data")
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

	if err = compose.Up(t.Context(), tc.RunServices(services...)); err != nil {
		t.Fatalf("Failed to start docker network: %v", err)
	}

	ports := make([]int, len(dependencies))
	for i, dependency := range dependencies {
		ports[i] = dependency.port
	}

	// Wait for dependent services to become ready
	if err = wait.ForPortsReady(t.Context(), "localhost", ports, 5*time.Second, 100*time.Millisecond); err != nil {
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

// StopDaemonDependencies stops the Docker Compose stack and checks if the containers are gone and ports are free.
func StopDaemonDependencies(_ context.Context, compose tc.ComposeStack) {
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

	if err := wait.ForDockerComposeProjectDown(containerWaitCtx, projectName, containerWaitTimeout, containerCheckInterval); err != nil {
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

	log.Printf("Calling ForPortsFree (timeout %s, interval %s)...", waitTimeout, waitInterval)
	waitCtx, cancelWait := context.WithTimeout(context.Background(), waitTimeout)

	defer cancelWait()

	if err := wait.ForPortsFree(waitCtx, "localhost", portsToCheck, waitTimeout, waitInterval); err != nil {
		log.Printf("Warning during ForPortsFree: %v", err)
	} else {
		log.Printf("Confirmed dependent service ports are free.")
	}

	log.Printf("StopDaemonDependencies finished.")
}

// CreateParentTransactionWithNOutputs creates a single transaction with multiple outputs from a parent transaction.
// Each output can then be spent concurrently by child transactions.
// count specifies how many outputs to create in the transaction.
func (td *TestDaemon) CreateParentTransactionWithNOutputs(t *testing.T, parentTx *bt.Tx, count int) (*bt.Tx, error) {
	// Create a new transaction
	newTx := bt.NewTx()

	// Add input from parent transaction using UTXO
	existingUtxo := &bt.UTXO{
		TxIDHash:      parentTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: parentTx.Outputs[0].LockingScript,
		Satoshis:      parentTx.Outputs[0].Satoshis,
	}
	err := newTx.FromUTXOs(existingUtxo)

	if err != nil {
		return nil, errors.NewProcessingError("failed to add input", err)
	}

	// Calculate satoshis per output, leaving some for fee
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
	var response []*distributor.ResponseWrapper

	response, err = td.DistributorClient.SendTransaction(td.Ctx, newTx)
	require.NoError(t, err)

	require.Equal(t, len(response), 1)

	td.Logger.Infof("Created parent transaction with %d outputs: %s, error: %v", count, newTx.TxID(), response[0].Error)

	// Wait a bit for the transaction to be processed
	time.Sleep(1 * time.Second)

	return newTx, nil
}

// CreateAndSendTxsConcurrently creates and sends transactions concurrently using multiple goroutines
func (td *TestDaemon) CreateAndSendTxsConcurrently(_ *testing.T, parentTx *bt.Tx) ([]*bt.Tx, []*chainhash.Hash, error) {
	existingTransactions := make([]*bt.Tx, len(parentTx.Outputs))
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
			existingUtxo := &bt.UTXO{
				TxIDHash:      parentTx.TxIDChainHash(),
				Vout:          uint32(index),
				LockingScript: parentTx.Outputs[index].LockingScript,
				Satoshis:      parentTx.Outputs[index].Satoshis,
			}

			newTx := bt.NewTx()
			if err := newTx.FromUTXOs(existingUtxo); err != nil {
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
		existingTransactions[result.index] = result.tx
		txHashes[result.index] = result.tx.TxIDChainHash()
		td.Logger.Infof("Transaction %d sent: %s", result.index+1, result.tx.TxID())
	}

	return existingTransactions, txHashes, nil
}

func (td *TestDaemon) CreateAndSendTxsForAllParentOutputs(t *testing.T, parentTx *bt.Tx) ([]*bt.Tx, []*chainhash.Hash, error) {
	return td.CreateAndSendTxs(t, parentTx, len(parentTx.Outputs))
}

// GetPrivateKey retrieves the private key used by the TestDaemon for signing transactions.
func (td *TestDaemon) GetPrivateKey(_ *testing.T) *bec.PrivateKey {
	privKey := td.privKey
	return privKey
}

// LogJSON logs the given data as formatted JSON with a label.
func (td *TestDaemon) LogJSON(t *testing.T, label string, data interface{}) {
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		t.Errorf("Error marshaling JSON: %v", err)
		return
	}

	t.Logf("\n%s:\n%s", label, string(jsonData))
}
