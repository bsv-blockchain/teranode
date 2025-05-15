package testcontainers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockvalidation"
	"github.com/bitcoin-sv/teranode/services/propagation"
	distributor "github.com/bitcoin-sv/teranode/services/rpc"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/testutil"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
)

type TestContainer struct {
	Config     TestContainersConfig
	Compose    tc.ComposeStack
	Ctx        context.Context
	Identifier tc.StackIdentifier
	Logger     *ulogger.ErrorTestLogger
}

type TestContainersConfig struct {
	Path               string
	ComposeFile        string
	Profiles           []string
	HealthServicePorts []ServicePort
	ServicesToWaitFor  []ServicePort
}

type ServicePort struct {
	ServiceName string
	Port        int
	IsMapped    bool
}

func NewTestContainer(t *testing.T, config TestContainersConfig) (*TestContainer, error) {
	err := os.RemoveAll(filepath.Join(config.Path, "data"))
	require.NoError(t, err)

	// Wait for directory removal to complete
	err = helper.WaitForDirRemoval(filepath.Join(config.Path, "data"), 2*time.Second)
	require.NoError(t, err)

	// Ensure the required Docker mount paths exist
	requiredDirs := []string{
		filepath.Join(config.Path, "data/test/default/legacy/svnode1-data"),
		filepath.Join(config.Path, "data/test/default/legacy/svnode2-data"),
		filepath.Join(config.Path, "data/test/default/legacy/svnode3-data"),
		filepath.Join(config.Path, "data/aerospike1/logs"),
		filepath.Join(config.Path, "data/aerospike2/logs"),
		filepath.Join(config.Path, "data/aerospike3/logs"),
	}
	for _, dir := range requiredDirs {
		err = os.MkdirAll(dir, 0755)
		require.NoError(t, err)
	}

	identifier := tc.StackIdentifier(fmt.Sprintf("test-%d", time.Now().UnixNano()))
	ctx, cancel := context.WithCancel(context.Background())

	compose, err := tc.NewDockerComposeWith(tc.WithStackFiles(filepath.Join(config.Path, config.ComposeFile)), tc.WithProfiles(config.Profiles...), identifier)
	require.NoError(t, err)

	container := &TestContainer{
		Config:     config,
		Compose:    compose,
		Ctx:        ctx,
		Identifier: identifier,
		Logger:     ulogger.NewErrorTestLogger(t, cancel),
	}

	require.NoError(t, compose.Up(ctx))

	services := compose.Services()
	t.Logf("Services: %v", services)

	ports := getPorts(ctx, t, compose, config.HealthServicePorts)
	require.NoError(t, daemon.WaitForHealthLiveness(ports, 10*time.Second))

	ports = getPorts(ctx, t, compose, config.ServicesToWaitFor)
	require.NoError(t, testutil.WaitForPortsReady(ctx, "localhost", ports, 10*time.Second, 1*time.Second))

	return container, nil
}

func getPorts(ctx context.Context, t *testing.T, compose tc.ComposeStack, servicePorts []ServicePort) []int {
	ports := []int{}

	for _, servicePort := range servicePorts {
		t1, err := compose.ServiceContainer(ctx, servicePort.ServiceName)
		if err != nil {
			t.Logf("Failed to get service container for %s: %v", servicePort.ServiceName, err)
			continue
		}

		var mappedPort int

		if servicePort.IsMapped {
			p, err := t1.MappedPort(ctx, nat.Port(strconv.Itoa(servicePort.Port)))
			require.NoError(t, err)

			mappedPort = p.Int()
		} else {
			mappedPort = servicePort.Port
		}

		ports = append(ports, mappedPort)
	}

	return ports
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

	ds := daemon.DaemonStores{}

	subtreeStore, err := ds.GetSubtreeStore(tc.Logger, tSettings)
	require.NoError(t, err)

	clients.SubtreeStore = subtreeStore

	utxoStore, err := ds.GetUtxoStore(tc.Ctx, tc.Logger, tSettings)
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

	type JSONError struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}

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

// start a node
func (tc *TestContainer) StartNode(t *testing.T, nodeName string) {
	node, err := tc.Compose.ServiceContainer(tc.Ctx, nodeName)
	require.NoError(t, err)

	require.NoError(t, node.Start(tc.Ctx))
	time.Sleep(10 * time.Second)
}

func (tc *TestContainer) Cleanup(t *testing.T) {
	err := tc.Compose.Down(tc.Ctx)
	require.NoError(t, err)

	// Wait for configured health check and service ports to be free before continuing
	portsToWaitFor := []int{}

	// Add ports from HealthServicePorts
	for _, healthService := range tc.Config.HealthServicePorts {
		portsToWaitFor = append(portsToWaitFor, healthService.Port)
	}

	// Add ports from ServicesToWaitFor
	for _, service := range tc.Config.ServicesToWaitFor {
		portsToWaitFor = append(portsToWaitFor, service.Port)
	}

	if len(portsToWaitFor) > 0 {
		// t.Logf("Waiting for host ports %v to become available after compose down...", portsToWaitFor)
		err = helper.WaitForPortsToBeAvailable(tc.Ctx, portsToWaitFor, 30*time.Second)
		require.NoError(t, err, "Ports %v did not become available after cleanup", portsToWaitFor)
	} else {
		t.Logf("No health check or fixed mapped ports found in config to check during cleanup.")
	}
}

func WaitForHealthLiveness(port int, timeout time.Duration) error {
	healthReadinessEndpoint := fmt.Sprintf("http://localhost:%d/health/readiness", port)
	timeoutElapsed := time.After(timeout)

	var err error

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

			return nil
		}
	}
}
