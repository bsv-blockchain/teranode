package testdaemon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockvalidation"
	"github.com/bitcoin-sv/teranode/services/propagation"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/distributor"
	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
)

type TestContainer struct {
	Compose    tc.ComposeStack
	Ctx        context.Context
	Identifier tc.StackIdentifier
	Logger     *ulogger.ErrorTestLogger
}

type TestContainersConfig struct {
	ComposeFile string
}

func NewTestContainer(t *testing.T, config TestContainersConfig) (*TestContainer, error) {
	identifier := tc.StackIdentifier(fmt.Sprintf("test-%d", time.Now().UnixNano()))
	ctx, cancel := context.WithCancel(context.Background())

	compose, err := tc.NewDockerComposeWith(tc.WithStackFiles(config.ComposeFile), identifier)
	require.NoError(t, err)

	container := &TestContainer{
		Compose:    compose,
		Ctx:        ctx,
		Identifier: identifier,
		Logger:     ulogger.NewErrorTestLogger(t, cancel),
	}

	require.NoError(t, compose.Up(ctx))

	// Add small delay to allow services to start
	time.Sleep(20 * time.Second)

	return container, nil
}

// TestDaemonClients holds a set of clients for a specific node configuration
type TestClient struct {
	BlockchainClient      blockchain.ClientI
	BlockAssemblyClient   *blockassembly.Client
	PropagationClient     *propagation.Client
	BlockValidationClient *blockvalidation.Client
	SubtreeStore          blob.Store
	UtxoStore             utxo.Store
	DistributorClient     *distributor.Distributor
	Settings              *settings.Settings
	RPCURL                *url.URL
}

// GetNodeClients returns a set of clients configured for a specific node
func (tc *TestContainer) GetNodeClients(t *testing.T, settingsContext string) *TestClient {
	tSettings := settings.NewSettings(settingsContext)
	clients := &TestClient{Settings: tSettings}

	blockchainClient, err := blockchain.NewClient(tc.Ctx, tc.Logger, tSettings, "test")
	require.NoError(t, err)

	clients.BlockchainClient = blockchainClient

	blockAssemblyClient, err := blockassembly.NewClient(tc.Ctx, tc.Logger, tSettings)
	require.NoError(t, err)

	clients.BlockAssemblyClient = blockAssemblyClient

	propagationClient, err := propagation.NewClient(tc.Ctx, tc.Logger, tSettings)
	require.NoError(t, err)

	clients.PropagationClient = propagationClient

	blockValidationClient, err := blockvalidation.NewClient(tc.Ctx, tc.Logger, tSettings, "test")
	require.NoError(t, err)

	clients.BlockValidationClient = blockValidationClient

	distributorClient, err := distributor.NewDistributor(tc.Ctx, tc.Logger, tSettings,
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)
	require.NoError(t, err)

	clients.DistributorClient = distributorClient

	subtreeStore, err := daemon.GetSubtreeStore(tc.Logger, tSettings)
	require.NoError(t, err)

	clients.SubtreeStore = subtreeStore

	utxoStore, err := daemon.GetUtxoStore(tc.Ctx, tc.Logger, tSettings)
	require.NoError(t, err)

	clients.UtxoStore = utxoStore

	clients.RPCURL = tSettings.RPC.RPCListenerURL

	return clients
}

// callrpc on a test client
func (tc *TestClient) CallRPC(t *testing.T, method string, params []interface{}) (string, error) {
	// Create the request payload
	requestBody, err := json.Marshal(map[string]interface{}{
		"method": method,
		"params": params,
	})
	t.Logf("Request: %s", string(requestBody))

	if err != nil {
		return "", errors.NewProcessingError("failed to marshal request body", err)
	}

	// Create the HTTP request
	req, err := http.NewRequest("POST", tc.RPCURL.String(), bytes.NewBuffer(requestBody))
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

// stop a node
func (tc *TestContainer) StopNode(t *testing.T, nodeName string) {
	node, err := tc.Compose.ServiceContainer(tc.Ctx, nodeName)
	require.NoError(t, err)

	require.NoError(t, node.Stop(tc.Ctx, nil))
}
