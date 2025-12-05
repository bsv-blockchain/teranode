package utils

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	primitives "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/errors"
	ba "github.com/bsv-blockchain/teranode/services/blockassembly"
	bc "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/propagation"
	"github.com/bsv-blockchain/teranode/settings"
	bhttp "github.com/bsv-blockchain/teranode/stores/blob/http"
	bcs "github.com/bsv-blockchain/teranode/stores/blockchain"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	stubs "github.com/bsv-blockchain/teranode/test/utils/stubs"
	"github.com/bsv-blockchain/teranode/test/utils/tconfig"
	"github.com/bsv-blockchain/teranode/test/utils/tstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Teranode test environment is a compose stack of 3 teranode instances.
type TeranodeTestEnv struct {
	TConfig              tconfig.TConfig
	Context              context.Context
	Compose              tc.ComposeStack
	ComposeSharedStorage tstore.TStoreClient
	Nodes                []TeranodeTestClient
	LegacyNodes          []SVNodeTestClient
	Logger               ulogger.Logger
	Cancel               context.CancelFunc
	Daemon               daemon.Daemon
}

type TeranodeTestClient struct {
	Name                string
	BlockchainClient    bc.ClientI
	BlockassemblyClient ba.Client
	PropagationClient   *propagation.Client
	ClientBlockstore    *bhttp.HTTPStore
	ClientSubtreestore  *bhttp.HTTPStore
	UtxoStore           *utxostore.Store
	CoinbaseClient      *stubs.CoinbaseClient
	AssetURL            string
	RPCURL              string
	IPAddress           string
	SVNodeIPAddress     string
	Settings            *settings.Settings
	BlockChainDB        bcs.Store
}

type SVNodeTestClient struct {
	Name      string
	IPAddress string
}

const (
	testEnvKey    = "TEST_ENV"
	containerMode = "container"
)

// NewTeraNodeTestEnv creates a new test environment with the provided Compose file paths.
func NewTeraNodeTestEnv(c tconfig.TConfig) *TeranodeTestEnv {
	logger := ulogger.New("e2eTestRun", ulogger.WithLevel(c.Suite.LogLevel))
	ctx, cancel := context.WithCancel(context.Background())

	return &TeranodeTestEnv{
		TConfig: c,
		Context: ctx,
		Logger:  logger,
		Cancel:  cancel,
	}
}

// SetupDockerNodes initializes Docker Compose with the provided environment settings.
func (t *TeranodeTestEnv) SetupDockerNodes() error {
	testRunMode := os.Getenv(testEnvKey) == containerMode
	if !testRunMode {
		identifier := tc.StackIdentifier(t.TConfig.Suite.TestID)
		compose, err := tc.NewDockerComposeWith(tc.WithStackFiles(t.TConfig.LocalSystem.Composes...), identifier)

		if err != nil {
			return err
		}

		// Set TEST_ID in environment settings
		envSettings := t.TConfig.Teranode.SettingsMap()
		envSettings["TEST_ID"] = t.TConfig.Suite.TestID

		// Create test directory if it doesn't exist
		testDir := fmt.Sprintf("./data/test/%s", t.TConfig.Suite.TestID)
		if err := os.MkdirAll(testDir, 0755); err != nil {
			return err
		}

		if err := compose.WithEnv(envSettings).Up(t.Context); err != nil {
			return err
		}

		t.Compose = compose

		// Setup shared volume client for local docker-compose
		if err := t.setupSharedStorageClient(); err != nil {
			return err
		}

		mapSettings := t.TConfig.Teranode.SettingsMap()
		for key, val := range mapSettings {
			settings := settings.NewSettings(val)
			nodeName := strings.ReplaceAll(key, "SETTINGS_CONTEXT_", "teranode")
			svNodeName := strings.ReplaceAll(nodeName, "tera", "sv")
			t.Nodes = append(t.Nodes, TeranodeTestClient{
				Name:     nodeName,
				Settings: settings,
			})
			t.LegacyNodes = append(t.LegacyNodes, SVNodeTestClient{
				Name: svNodeName,
			})

			// os.Setenv("SETTINGS_CONTEXT", "")
		}
	}

	return nil
}

func (t *TeranodeTestEnv) setupSharedStorageClient() error {
	teststorageServiceName := "teststorage"
	teststorageInternalPort := nat.Port(fmt.Sprintf("%d/tcp", 50051))
	teststorageMappedPort, _ := t.GetMappedPort(teststorageServiceName, teststorageInternalPort)
	teststorageURL := fmt.Sprintf("localhost:%d", teststorageMappedPort.Int())

	conn, err := grpc.NewClient(teststorageURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	// defer conn.Close()
	t.ComposeSharedStorage = tstore.NewTStoreClient(conn)

	return nil
}

// InitializeNodeClients sets up all the necessary client connections for the nodes.
func (t *TeranodeTestEnv) InitializeTeranodeTestClients() error {
	if err := t.setupBlobStores(); err != nil {
		return err
	}

	// t.Nodes = make([]TeranodeTestClient, 3)
	for i := 0; i < 3; i++ {
		node := &t.Nodes[i]
		node.CoinbaseClient = stubs.NewCoinbaseClient()

		t.Logger.Infof("Initializing node %s", node.Name)

		if err := t.GetContainerIPAddress(node); err != nil {
			return err
		}

		if t.TConfig.Suite.IsLegacyTest {
			svNode := &t.LegacyNodes[i]
			if err := t.GetLegacyContainerIPAddress(svNode); err != nil {
				return err
			}
		}

		if err := t.setupRPCURL(node); err != nil {
			return err
		}

		if err := t.setupAssetURL(node); err != nil {
			return err
		}

		if err := t.setupBlockchainClient(node); err != nil {
			return err
		}

		if err := t.setupBlockassemblyClient(node); err != nil {
			return err
		}

		if err := t.setupPropagationClient(node); err != nil {
			return err
		}

		if err := t.setupStores(node); err != nil {
			return err
		}
	}

	return nil
}

func (t *TeranodeTestEnv) GetContainerIPAddress(node *TeranodeTestClient) error {
	if t.Compose != nil {
		if t.Compose != nil {
			service, err := t.Compose.ServiceContainer(t.Context, node.Name)
			if err != nil {
				return err
			}

			ipAddress, err := service.ContainerIP(t.Context)
			if err != nil {
				return err
			}

			node.IPAddress = ipAddress

			return nil
		}

		return nil
	}

	return nil
}

func (t *TeranodeTestEnv) GetLegacyContainerIPAddress(node *SVNodeTestClient) error {
	if t.Compose != nil {
		if t.Compose != nil {
			service, err := t.Compose.ServiceContainer(t.Context, node.Name)
			if err != nil {
				return err
			}

			ipAddress, err := service.ContainerIP(t.Context)
			if err != nil {
				return err
			}

			node.IPAddress = ipAddress

			return nil
		}

		return nil
	}

	return nil
}

func (t *TeranodeTestEnv) getServiceAddress(serviceName string, port string) (string, error) {
	if os.Getenv(testEnvKey) == containerMode {
		// In container mode, use service names directly
		return fmt.Sprintf("%s:%s", serviceName, port), nil
	}
	// In local mode, use localhost with mapped ports
	mappedPort, err := t.GetMappedPort(serviceName, nat.Port(port))
	if err != nil {
		t.Logger.Errorf("Failed to get mapped port", "error", err)
		return "", err
	}

	return fmt.Sprintf("localhost:%s", mappedPort.Port()), nil
}

func getPortFromAddress(address string) (string, error) {
	split := strings.Split(address, ":")
	if len(split) < 2 {
		return "", errors.NewConfigurationError("error parsing grpc url:", address)
	}

	return split[1], nil
}

func (t *TeranodeTestEnv) setupRPCURL(node *TeranodeTestClient) error {
	port := node.Settings.RPC.RPCListenerURL.Port()
	t.Logger.Infof("rpc port: %s", port)

	rpcPort := fmt.Sprintf("%s/tcp", port)

	rpcURL, err := t.getServiceAddress(node.Name, rpcPort)
	if err != nil {
		return errors.NewConfigurationError("error getting rpc url:", err)
	}

	node.RPCURL = rpcURL

	return nil
}

func (t *TeranodeTestEnv) setupAssetURL(node *TeranodeTestClient) error {
	assetPort := fmt.Sprintf("%d/tcp", node.Settings.Asset.HTTPPort)

	assetURL, err := t.getServiceAddress(node.Name, assetPort)
	if err != nil {
		return errors.NewConfigurationError("error getting asset url:", err)
	}

	node.AssetURL = assetURL

	return nil
}

func (t *TeranodeTestEnv) setupBlockchainClient(node *TeranodeTestClient) error {
	port, err := getPortFromAddress(node.Settings.BlockChain.GRPCAddress)
	if err != nil {
		return errors.NewConfigurationError("error parsing blockchain grpc url:", err)
	}

	blockchainGRPCPort := fmt.Sprintf("%s/tcp", port)

	blockchainAddress, err := t.getServiceAddress(node.Name, blockchainGRPCPort)
	if err != nil {
		return errors.NewConfigurationError("error getting blockchain grpc address:", err)
	}

	blockchainClient, err := bc.NewClientWithAddress(t.Context, t.Logger, node.Settings, blockchainAddress, "test")
	if err != nil {
		return errors.NewConfigurationError("error creating blockchain client", err)
	}

	node.BlockchainClient = blockchainClient

	return nil
}

func (t *TeranodeTestEnv) setupBlockassemblyClient(node *TeranodeTestClient) error {
	port, err := getPortFromAddress(node.Settings.BlockAssembly.GRPCAddress)
	if err != nil {
		return errors.NewConfigurationError("error parsing blockassembly grpc url:", err)
	}

	blockassemblyGRPCPort := fmt.Sprintf("%s/tcp", port)

	blockassemblyAddress, err := t.getServiceAddress(node.Name, blockassemblyGRPCPort)
	if err != nil {
		return errors.NewConfigurationError("error getting blockassembly grpc address:", err)
	}

	blockassemblyClient, err := ba.NewClientWithAddress(t.Context, t.Logger, node.Settings, blockassemblyAddress)
	if err != nil {
		return errors.NewConfigurationError("error creating blockassembly client", err)
	}

	node.BlockassemblyClient = *blockassemblyClient

	return nil
}

func (t *TeranodeTestEnv) setupPropagationClient(node *TeranodeTestClient) error {
	if os.Getenv(testEnvKey) != containerMode {
		// we have multiple addresses separated by |
		propagationServers := node.Settings.Propagation.GRPCAddresses

		t.Logger.Infof("propagationServers: %s", propagationServers)

		var mappedAddresses []string

		for _, addr := range propagationServers {
			// Split host:port
			parts := strings.Split(addr, ":")
			if len(parts) != 2 {
				return errors.NewConfigurationError("invalid address format:", addr)
			}

			// Extract node name from host (e.g., "node1" from "node1:8084")
			nodeName := parts[0]
			port := parts[1]

			// Get mapped port for this node's grpc port
			mappedPort, err := t.GetMappedPort(nodeName, nat.Port(fmt.Sprintf("%s/tcp", port)))
			if err != nil {
				return errors.NewConfigurationError("error getting mapped port:", err)
			}

			// Create new address with localhost and mapped port
			mappedAddr := fmt.Sprintf("localhost:%s", mappedPort.Port())
			mappedAddresses = append(mappedAddresses, mappedAddr)
		}

		// Join all addresses back with | separator
		node.Settings.Propagation.GRPCAddresses = mappedAddresses
	}

	propagationClient, err := propagation.NewClient(t.Context, t.Logger, node.Settings)
	if err != nil {
		return errors.NewConfigurationError("error creating propagation client:", err)
	}

	node.PropagationClient = propagationClient

	return nil
}

// Setup all blob http store client for blockstore and subtreestore
func (t *TeranodeTestEnv) setupBlobStores() error {
	if len(t.TConfig.Teranode.URLBlobBlockstores) != len(t.TConfig.Teranode.Contexts) {
		t.Logger.Warnf("inconsistent length of teranodes contexts[%v]  and blob block store[%v]", len(t.TConfig.Teranode.Contexts), len(t.TConfig.Teranode.URLBlobBlockstores))
		return nil
	}

	if len(t.TConfig.Teranode.URLBlobSubtreestores) != len(t.TConfig.Teranode.Contexts) {
		t.Logger.Warnf("inconsistent length of teranodes contexts[%v]  and blob subtree store[%v]", len(t.TConfig.Teranode.Contexts), len(t.TConfig.Teranode.URLBlobSubtreestores))
		return nil
	}

	for i := range t.TConfig.Teranode.Contexts {
		node := &t.Nodes[i]

		// Set the blob blockstore client
		blobblockstoreServiceName := fmt.Sprintf("blobblockstore%v", i+1)
		blobblockstoreInternalPort := nat.Port(fmt.Sprintf("%d/tcp", 8080))
		blobblockstoreMappedPort, err := t.GetMappedPort(blobblockstoreServiceName, blobblockstoreInternalPort)

		if err != nil {
			return err
		}

		blobBlockstoreURL := fmt.Sprintf("localhost:%d", blobblockstoreMappedPort.Int())
		bbURL, err := url.Parse(fmt.Sprintf("http://%v", blobBlockstoreURL))

		if err != nil {
			return err
		}

		node.ClientBlockstore, err = bhttp.New(t.Logger, bbURL)
		if err != nil {
			return err
		}

		if eCode, _, err := node.ClientBlockstore.Health(context.Background(), true); eCode != http.StatusOK || err != nil {
			return errors.NewProcessingError("unhealthy blob block store http client, error : %v", err.Error())
		}

		// Set the blob subtree client
		blobSubtreestoreServiceName := fmt.Sprintf("blobsubtreestore%v", i+1)
		blobSubtreestoreInternalPort := nat.Port(fmt.Sprintf("%d/tcp", 8080))
		blobSubtreestoreMappedPort, err := t.GetMappedPort(blobSubtreestoreServiceName, blobSubtreestoreInternalPort)

		if err != nil {
			return err
		}

		blobSubtreestoreURL := fmt.Sprintf("localhost:%d", blobSubtreestoreMappedPort.Int())
		bsURL, err := url.Parse(fmt.Sprintf("http://%v", blobSubtreestoreURL))

		if err != nil {
			return err
		}

		node.ClientSubtreestore, err = bhttp.New(t.Logger, bsURL)
		if err != nil {
			return err
		}

		if eCode, _, err := node.ClientSubtreestore.Health(context.Background(), true); eCode != http.StatusOK || err != nil {
			return errors.NewProcessingError("unhealthy blob subtree store http client")
		}
	}

	return nil
}

func (t *TeranodeTestEnv) setupStores(node *TeranodeTestClient) error {
	isContainerMode := os.Getenv(testEnvKey) == containerMode

	var (
		utxoStoreURL, blockchainStoreURL *url.URL
		err                              error
	)

	if !isContainerMode {
		// Get utxoStoreURL from node settings
		utxoStoreURL = node.Settings.UtxoStore.UtxoStore

		// Extract aerospike host and port
		hostParts := strings.Split(utxoStoreURL.Host, ":")
		if len(hostParts) != 2 {
			return errors.NewConfigurationError("invalid aerospike host format:", utxoStoreURL.Host)
		}

		aerospikeHost := hostParts[0]
		aerospikePort := hostParts[1]

		// Get the mapped port for the aerospike instance
		mappedPort, err := t.GetMappedPort(aerospikeHost, nat.Port(aerospikePort+"/tcp"))
		if err != nil {
			t.Logger.Errorf("Failed to get mapped port for %s", aerospikeHost, "error", err)
			return err
		}

		// Create new URL with localhost and mapped port
		utxoStoreURL.Host = fmt.Sprintf("localhost:%s", mappedPort.Port())

		// Handle the externalStore parameter
		values := utxoStoreURL.Query()
		if externalStore := values.Get("externalStore"); externalStore != "" {
			// Parse the externalStore URL
			// externalStoreURL, err := url.Parse(externalStore)
			if err != nil {
				return errors.NewConfigurationError("error parsing external store url:", err)
			}

			// Modify the path to point to the correct test directory
			relativePath := fmt.Sprintf("./%s/%s/%s/external", t.TConfig.LocalSystem.DataDir, t.TConfig.Suite.TestID, node.Name)
			t.Logger.Infof("Modified externalStore path: %s", relativePath)

			// Manually construct the query string to avoid encoding issues
			queryParts := []string{}

			for k, v := range values {
				if k == "externalStore" {
					queryParts = append(queryParts, fmt.Sprintf("%s=file://%s", k, relativePath))
				} else {
					for _, val := range v {
						queryParts = append(queryParts, fmt.Sprintf("%s=%s", k, val))
					}
				}
			}

			utxoStoreURL.RawQuery = strings.Join(queryParts, "&")
		}

		t.Logger.Infof("utxoStoreURL: %s", utxoStoreURL.String())

		blockchainStoreURL = node.Settings.BlockChain.StoreURL
		// the url is like postgres://miner1:miner1@postgres:5432/teranode1
		// we need to change it to postgres://miner1:miner1@localhost:5432/teranode1
		// 5432 should be the mapped port
		hostParts = strings.Split(blockchainStoreURL.Host, ":")
		if len(hostParts) != 2 {
			return errors.NewConfigurationError("invalid postgres host format:", blockchainStoreURL.Host)
		}

		postgresHost := hostParts[0]
		postgresPort := hostParts[1]

		// Get the mapped port for the postgres instance
		mappedPort, err = t.GetMappedPort(postgresHost, nat.Port(postgresPort+"/tcp"))
		if err != nil {
			t.Logger.Errorf("Failed to get mapped port for %s", postgresHost, "error", err)
			return err
		}

		// Create new URL with localhost and mapped port
		blockchainStoreURL.Host = fmt.Sprintf("localhost:%s", mappedPort.Port())
	} else {
		// In container mode, use the mounted volume paths
		// nodePath := fmt.Sprintf("/app/data/%s", node.Name) (this was ineffectual in the original code)

		// For UTXO store, we use the node's settings but modify the external store path
		utxoStoreURL = node.Settings.UtxoStore.UtxoStore

		// Get the query values
		values := utxoStoreURL.Query()

		// Update the externalStore path to point to our test container's path
		nodePath := fmt.Sprintf("/app/data/%s", node.Name)
		values.Set("externalStore", fmt.Sprintf("file://%s/external", nodePath))

		blockchainStoreURL = node.Settings.BlockChain.StoreURL
	}

	// Create UTXO store
	utxoStore, err := utxostore.New(t.Context, t.Logger, node.Settings, utxoStoreURL)
	if err != nil {
		return errors.NewConfigurationError("error creating utxostore:", err)
	}

	node.UtxoStore = utxoStore

	// Create blockchain store
	blockchainStore, err := bcs.NewStore(t.Logger, blockchainStoreURL, node.Settings)
	if err != nil {
		return errors.NewConfigurationError("error creating blockchain store", err)
	}

	node.BlockChainDB = blockchainStore

	return nil
}

// GetMappedPort retrieves the mapped port for a service running in Docker Compose.
func (t *TeranodeTestEnv) GetMappedPort(nodeName string, port nat.Port) (nat.Port, error) {
	if t.Compose != nil {
		node, err := t.Compose.ServiceContainer(t.Context, nodeName)
		if err != nil {
			return "", err
		}

		mappedPort, err := node.MappedPort(t.Context, port)
		if err != nil {
			return "", err
		}

		return mappedPort, nil
	}

	return "", nil
}

// StopDockerNodes stops the Docker Compose services and removes volumes.
func (t *TeranodeTestEnv) StopDockerNodes() error {
	if t != nil && t.Compose != nil {
		if err := t.Compose.Down(t.Context); err != nil {
			return err
		}
	}

	return nil
}

// RestartDockerNodes restarts the Docker Compose services.
func (t *TeranodeTestEnv) RestartDockerNodes(envSettings map[string]string) error {
	if t.Compose != nil {
		if err := t.Compose.Down(t.Context); err != nil {
			return err
		}

		identifier := tc.StackIdentifier(t.TConfig.Suite.TestID)

		compose, err := tc.NewDockerComposeWith(tc.WithStackFiles(t.TConfig.LocalSystem.Composes...), identifier)
		if err != nil {
			return err
		}

		if err := compose.WithEnv(envSettings).Up(t.Context); err != nil {
			return err
		}

		t.Compose = compose

		time.Sleep(30 * time.Second)

		nodeNames := []string{"teranode1", "teranode2", "teranode3"}

		order := []string{"SETTINGS_CONTEXT_1", "SETTINGS_CONTEXT_2", "SETTINGS_CONTEXT_3"}
		for idx, key := range order {
			settings := settings.NewSettings(envSettings[key])
			t.Nodes[idx].Name = nodeNames[idx]
			t.Nodes[idx].Settings = settings
			t.Logger.Infof("Settings context: %s", envSettings[key])
			t.Logger.Infof("Node name: %s", nodeNames[idx])
			t.Logger.Infof("Node settings: %s", t.Nodes[idx].Settings)
		}
	}

	return nil
}

// StartNode starts a specific TeraNode by name.
func (t *TeranodeTestEnv) StartNode(nodeName string) error {
	if t.Compose != nil {
		node, err := t.Compose.ServiceContainer(t.Context, nodeName)
		if err != nil {
			return err
		}

		if err := node.Start(t.Context); err != nil {
			return err
		}

		nodeNames := []string{"teranode1", "teranode2", "teranode3"}

		for idx := range nodeNames {
			t.Nodes[idx].Name = nodeNames[idx]
			t.Logger.Infof("Node name: %s", nodeNames[idx])
			t.Logger.Infof("Node settings: %s", t.Nodes[idx].Settings)
		}
	}

	return nil
}

// StopNode stops a specific TeraNode by name.
func (t *TeranodeTestEnv) StopNode(nodeName string) error {
	if t.Compose != nil {
		node, err := t.Compose.ServiceContainer(t.Context, nodeName)
		if err != nil {
			return err
		}

		if err := node.Stop(t.Context, nil); err != nil {
			return err
		}
	}

	return nil
}

// Helper function to make host addresses from ports.
func makeHostAddressFromPort(port string) string {
	return fmt.Sprintf("localhost:%s", port)
}

func (tc *TeranodeTestClient) CreateAndSendTx(t *testing.T, ctx context.Context, parentTx *bt.Tx) (*bt.Tx, error) {
	logger := ulogger.New("e2eTestRun", ulogger.WithLevel("INFO"))

	privateKey, err := primitives.PrivateKeyFromWif(tc.Settings.BlockAssembly.MinerWalletPrivateKeys[0])
	require.NoError(t, err)

	publicKey := privateKey.PubKey().Compressed()

	utxo := &bt.UTXO{
		TxIDHash:      parentTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: parentTx.Outputs[0].LockingScript,
		Satoshis:      parentTx.Outputs[0].Satoshis,
	}

	newTx := bt.NewTx()

	err = newTx.FromUTXOs(utxo)
	require.NoError(t, err)

	err = newTx.AddP2PKHOutputFromPubKeyBytes(publicKey, 10000)
	require.NoError(t, err)

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	require.NoError(t, err)

	err = tc.PropagationClient.ProcessTransaction(ctx, newTx)
	require.NoError(t, err)

	logger.Infof("Transaction sent: %s", newTx.TxID())

	return newTx, nil
}

// CreateAndSendTxs creates and sends a chain of transactions, where each transaction spends from the previous one
func (tc *TeranodeTestClient) CreateAndSendTxs(t *testing.T, ctx context.Context, parentTx *bt.Tx, count int) ([]*bt.Tx, []*chainhash.Hash, error) {
	logger := ulogger.New("e2eTestRun", ulogger.WithLevel("INFO"))

	privateKey, err := primitives.PrivateKeyFromWif(tc.Settings.BlockAssembly.MinerWalletPrivateKeys[0])
	require.NoError(t, err)

	publicKey := privateKey.PubKey().Compressed()

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

		err = newTx.FromUTXOs(utxo)
		require.NoError(t, err)

		outputAmount := currentParent.Outputs[0].Satoshis - 1000 // minus 1000 satoshis for fee
		if outputAmount <= 0 {
			return transactions, nil, errors.NewProcessingError("insufficient funds for next transaction", nil)
		}

		err = newTx.AddP2PKHOutputFromPubKeyBytes(publicKey, outputAmount)
		if err != nil {
			return transactions, nil, errors.NewProcessingError("Error adding output to transaction", err)
		}

		err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
		require.NoError(t, err)

		err = tc.PropagationClient.ProcessTransaction(ctx, newTx)
		require.NoError(t, err)

		logger.Infof("Transaction %d sent: %s", i+1, newTx.TxID())

		transactions[i] = newTx
		txHashes = append(txHashes, newTx.TxIDChainHash())
		currentParent = newTx
	}

	return transactions, txHashes, nil
}

func (tc *TeranodeTestClient) CreateAndSendTxsConcurrently(t *testing.T, ctx context.Context, parentTx *bt.Tx, count int) ([]*bt.Tx, []*chainhash.Hash, error) {
	logger := ulogger.New("e2eTestRun", ulogger.WithLevel("INFO"))

	privateKey, err := primitives.PrivateKeyFromWif(tc.Settings.BlockAssembly.MinerWalletPrivateKeys[0])
	require.NoError(t, err)

	publicKey := privateKey.PubKey().Compressed()

	// Step 1: Create a splitting transaction with count outputs
	splitTx := bt.NewTx()
	err = splitTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      parentTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: parentTx.Outputs[0].LockingScript,
		Satoshis:      parentTx.Outputs[0].Satoshis,
	})
	require.NoError(t, err)

	// Calculate amount for each output, reserving some for fees
	//nolint:gosec
	splitAmount := (parentTx.Outputs[0].Satoshis - uint64(count*1000)) / uint64(count)
	require.Greater(t, splitAmount, uint64(1000), "insufficient funds to split")

	// Add count outputs to split transaction
	for i := 0; i < count; i++ {
		err = splitTx.AddP2PKHOutputFromPubKeyBytes(publicKey, splitAmount)
		require.NoError(t, err)
	}

	err = splitTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	require.NoError(t, err)

	// Send the split transaction
	err = tc.PropagationClient.ProcessTransaction(ctx, splitTx)
	require.NoError(t, err)

	logger.Infof("Split transaction sent: %s", splitTx.TxID())

	// Step 2: Wait a bit for the split tx to be processed
	time.Sleep(1 * time.Second)

	// Step 3: Create and send transactions concurrently
	var wg sync.WaitGroup

	transactions := make([]*bt.Tx, count)
	txHashes := make([]*chainhash.Hash, count)
	errChan := make(chan error, count)

	for i := 0; i < count; i++ {
		wg.Add(1)

		go func(index int) {
			defer wg.Done()

			// Create a new transaction spending one of the split outputs
			//nolint:gosec
			utxo := &bt.UTXO{
				TxIDHash:      splitTx.TxIDChainHash(),
				Vout:          uint32(index),
				LockingScript: splitTx.Outputs[index].LockingScript,
				Satoshis:      splitTx.Outputs[index].Satoshis,
			}

			newTx := bt.NewTx()

			err := newTx.FromUTXOs(utxo)
			if err != nil {
				errChan <- errors.NewProcessingError("error creating tx %d: %v", index, err)
				return
			}

			// Create a single output with slightly less satoshis (fee)
			err = newTx.AddP2PKHOutputFromPubKeyBytes(publicKey, splitAmount-1000)
			if err != nil {
				errChan <- errors.NewProcessingError("error adding output to tx %d: %v", index, err)
				return
			}

			err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
			if err != nil {
				errChan <- errors.NewProcessingError("error filling inputs for tx %d: %v", index, err)
				return
			}

			err = tc.PropagationClient.ProcessTransaction(ctx, newTx)
			if err != nil {
				errChan <- errors.NewProcessingError("error sending tx %d: %v", index, err)
				return
			}

			logger.Infof("Concurrent transaction %d sent: %s", index+1, newTx.TxID())
			transactions[index] = newTx
			txHashes[index] = newTx.TxIDChainHash()
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(errChan)

	// Check for any errors
	select {
	case err := <-errChan:
		if err != nil {
			return nil, nil, err
		}
	default:
	}

	return transactions, txHashes, nil
}
