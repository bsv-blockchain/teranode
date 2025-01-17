package utils

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/errors"
	ba "github.com/bitcoin-sv/teranode/services/blockassembly"
	bc "github.com/bitcoin-sv/teranode/services/blockchain"
	cb "github.com/bitcoin-sv/teranode/services/coinbase"
	"github.com/bitcoin-sv/teranode/settings"
	blob "github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	bcs "github.com/bitcoin-sv/teranode/stores/blockchain"
	utxostore "github.com/bitcoin-sv/teranode/stores/utxo/aerospike"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/bitcoin-sv/teranode/ulogger"
	distributor "github.com/bitcoin-sv/teranode/util/distributor"
	"github.com/docker/go-connections/nat"
	"github.com/ordishs/gocore"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
)

// Teranode test environment is a compose stack of 3 teranode instances.
type TeranodeTestEnv struct {
	TConfig tconfig.TConfig
	Context context.Context
	Compose tc.ComposeStack
	Nodes   []TeranodeTestClient
	Logger  ulogger.Logger
	Cancel  context.CancelFunc
	Daemon  daemon.Daemon
}

type TeranodeTestClient struct {
	Name                string
	SettingsContext     string
	CoinbaseClient      cb.Client
	BlockchainClient    bc.ClientI
	BlockassemblyClient ba.Client
	DistributorClient   distributor.Distributor
	Blockstore          blob.Store
	SubtreeStore        blob.Store
	BlockstoreURL       *url.URL
	UtxoStore           *utxostore.Store
	SubtreesKafkaURL    *url.URL
	AssetURL            string
	RPCURL              string
	IPAddress           string
	Settings            *settings.Settings
	BlockChainDB        bcs.Store
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

		// time.Sleep(30 * time.Second)

		t.Compose = compose
		mapSettings := t.TConfig.Teranode.SettingsMap()

		for key, val := range mapSettings {
			settings := settings.NewSettings(val)
			nodeName := strings.ReplaceAll(key, "SETTINGS_CONTEXT_", "teranode")
			t.Nodes = append(t.Nodes, TeranodeTestClient{
				SettingsContext: val,
				Name:            nodeName,
				Settings:        settings,
			})
			t.Logger.Infof("Settings context: %s", envSettings[key])
			t.Logger.Infof("Node name: %s", nodeName)
			t.Logger.Infof("Node settings: %s", t.Nodes[len(t.Nodes)-1].Settings)
			os.Setenv("SETTINGS_CONTEXT", "")
		}
	}

	return nil
}

// InitializeNodeClients sets up all the necessary client connections for the nodes.
func (t *TeranodeTestEnv) InitializeTeranodeTestClients() error {
	for i := range t.Nodes {
		node := &t.Nodes[i]
		t.Logger.Infof("Initializing node %s", node.Name)
		t.Logger.Infof("Settings context: %s", node.SettingsContext)

		if err := t.GetContainerIPAddress(node); err != nil {
			return err
		}

		if err := t.setupCoinbaseClient(node); err != nil {
			return err
		}

		if err := t.setupBlockchainClient(node); err != nil {
			return err
		}

		if err := t.setupBlockassemblyClient(node); err != nil {
			return err
		}

		if err := t.setupDistributorClient(node); err != nil {
			return err
		}

		if err := t.setupStores(node); err != nil {
			return err
		}

		if err := t.setupRPCURL(node); err != nil {
			return err
		}

		if err := t.setupAssetURL(node); err != nil {
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
			t.Logger.Infof("Node %s IP address: %s", node.Name, ipAddress)

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
	port, err := getPortFromAddress(node.Settings.RPC.RPCListenerURL)
	if err != nil {
		return err
	}

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

func (t *TeranodeTestEnv) setupCoinbaseClient(node *TeranodeTestClient) error {
	port, err := getPortFromAddress(node.Settings.Coinbase.GRPCAddress)
	if err != nil {
		return err
	}

	coinbaseGRPCPort := fmt.Sprintf("%s/tcp", port)
	coinbaseGrpcAddress, err := t.getServiceAddress(node.Name, coinbaseGRPCPort)
	if err != nil {
		return errors.NewConfigurationError("error getting coinbase grpc address:", err)
	}

	coinbaseClient, err := cb.NewClientWithAddress(t.Context, t.Logger, node.Settings, coinbaseGrpcAddress)
	if err != nil {
		return errors.NewConfigurationError("error creating coinbase client:", err)
	}

	node.CoinbaseClient = *coinbaseClient

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

func (t *TeranodeTestEnv) setupDistributorClient(node *TeranodeTestClient) error {
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

	distributorClient, err := distributor.NewDistributor(t.Context, t.Logger, node.Settings)
	if err != nil {
		return errors.NewConfigurationError("error creating distributor client:", err)
	}

	node.DistributorClient = *distributorClient

	return nil
}

func (t *TeranodeTestEnv) setupStores(node *TeranodeTestClient) error {
	isContainerMode := os.Getenv(testEnvKey) == containerMode

	var (
		blockStoreURL, subtreeStoreURL, utxoStoreURL, blockchainStoreURL *url.URL
		err                                                              error
	)

	if !isContainerMode {
		// In local mode, use test runner context for relative paths
		settingsContext := fmt.Sprintf("docker.%s.test.context.testrunner", node.Name)

		var found bool

		blockStoreURL, err, found = gocore.Config().GetURL(fmt.Sprintf("blockstore.%s", settingsContext))
		if err != nil || !found {
			return errors.NewConfigurationError("error getting blockstore url:", err)
		}

		// Modify the path to include test ID
		// Original path is like: file://./../../data/test/teranode1/blockstore
		// We want: file://./../../data/test/<test-id>/teranode1/blockstore
		// if blockStoreURL.Scheme == "file" {
		path := blockStoreURL.Path
		// Insert test ID into the path
		parts := strings.Split(path, "/test/")
		if len(parts) == 2 {
			path = fmt.Sprintf("%s/test/%s/%s", parts[0], t.TConfig.Suite.TestID, parts[1])
			t.Logger.Infof("Modified blockstore path: %s", path)
			blockStoreURL.Path = path
		}
		// }

		subtreeStoreURL, err, found = gocore.Config().GetURL(fmt.Sprintf("subtreestore.%s", settingsContext))
		if err != nil || !found {
			return errors.NewConfigurationError("error getting subtreestore url:", err)
		}

		// Do the same for subtree store
		// if subtreeStoreURL.Scheme == "file" {

		path = subtreeStoreURL.Path

		parts = strings.Split(path, "/test/")
		if len(parts) == 2 {
			path = fmt.Sprintf("%s/test/%s/%s", parts[0], t.TConfig.Suite.TestID, parts[1])
			t.Logger.Infof("Modified subtreestore path: %s", path)
			subtreeStoreURL.Path = path
		}
		// }

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
		nodePath := fmt.Sprintf("/app/data/%s", node.Name)

		blockStoreURL, err = url.Parse(fmt.Sprintf("file://%s/blockstore", nodePath))
		if err != nil {
			return errors.NewConfigurationError("error parsing blockstore url:", err)
		}

		subtreeStoreURL, err = url.Parse(fmt.Sprintf("file://%s/subtreestore", nodePath))
		if err != nil {
			return errors.NewConfigurationError("error parsing subtreestore url:", err)
		}

		// For UTXO store, we use the node's settings but modify the external store path
		utxoStoreURL = node.Settings.UtxoStore.UtxoStore

		// Get the query values
		values := utxoStoreURL.Query()

		// Update the externalStore path to point to our test container's path
		nodePath = fmt.Sprintf("/app/data/%s", node.Name)
		values.Set("externalStore", fmt.Sprintf("file://%s/external", nodePath))

		blockchainStoreURL = node.Settings.BlockChain.StoreURL
	}

	// Create blockstore
	blockStore, err := blob.NewStore(t.Logger, blockStoreURL)
	if err != nil {
		return errors.NewConfigurationError("error creating blockstore:", err)
	}

	node.Blockstore = blockStore
	node.BlockstoreURL = blockStoreURL

	// Create subtree store
	subtreeStore, err := blob.NewStore(t.Logger, subtreeStoreURL, options.WithHashPrefix(2))
	if err != nil {
		return errors.NewConfigurationError("error creating subtreestore:", err)
	}

	node.SubtreeStore = subtreeStore

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
	if t.Compose != nil {
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
			t.Nodes[idx].SettingsContext = envSettings[key]
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
		order := []string{"SETTINGS_CONTEXT_1", "SETTINGS_CONTEXT_2", "SETTINGS_CONTEXT_3"}

		for idx := range order {
			settings := settings.NewSettings(t.Nodes[idx].SettingsContext)
			t.Nodes[idx].Name = nodeNames[idx]
			t.Nodes[idx].Settings = settings
			t.Logger.Infof("Settings context: %s", t.Nodes[idx].SettingsContext)
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
