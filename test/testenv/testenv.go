package testenv

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	ba "github.com/bitcoin-sv/ubsv/services/blockassembly"
	bc "github.com/bitcoin-sv/ubsv/services/blockchain"
	cb "github.com/bitcoin-sv/ubsv/services/coinbase"
	"github.com/bitcoin-sv/ubsv/settings"
	blob "github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	bcs "github.com/bitcoin-sv/ubsv/stores/blockchain"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo/aerospike"
	"github.com/bitcoin-sv/ubsv/ulogger"
	distributor "github.com/bitcoin-sv/ubsv/util/distributor"
	"github.com/docker/go-connections/nat"
	"github.com/ordishs/gocore"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
)

// Teranode test environment is a compose stack of 3 teranode instances.
type TeranodeTestEnv struct {
	ComposeFilePaths []string
	Context          context.Context
	Compose          tc.ComposeStack
	Nodes            []TeranodeTestClient
	Logger           ulogger.Logger
	Cancel           context.CancelFunc
}

type TeranodeTestClient struct {
	Name                   string
	SettingsContext        string
	CoinbaseClient         cb.Client
	BlockchainClient       bc.ClientI
	BlockassemblyClient    ba.Client
	DistributorClient      distributor.Distributor
	Blockstore             blob.Store
	SubtreeStore           blob.Store
	BlockstoreURL          *url.URL
	UtxoStore              *utxostore.Store
	SubtreesKafkaURL       *url.URL
	RPCURL                 string
	IPAddress              string
	Settings               *settings.Settings
	BlockChainDB           bcs.Store
	DefaultSettingsContext string
}

// NewTeraNodeTestEnv creates a new test environment with the provided Compose file paths.
func NewTeraNodeTestEnv(composeFilePaths []string) *TeranodeTestEnv {
	var logLevelStr, _ = gocore.Config().Get("logLevel.docker.test", "ERROR")
	logger := ulogger.New("e2eTestRun", ulogger.WithLevel(logLevelStr))
	ctx, cancel := context.WithCancel(context.Background())

	return &TeranodeTestEnv{
		ComposeFilePaths: composeFilePaths,
		Context:          ctx,
		Logger:           logger,
		Cancel:           cancel,
	}
}

// SetupDockerNodes initializes Docker Compose with the provided environment settings.
func (t *TeranodeTestEnv) SetupDockerNodes(envSettings map[string]string) error {
	var testRunMode, _ = gocore.Config().Get("test_run_mode.docker", "ci")
	if testRunMode == "ci" {
		identifier := tc.StackIdentifier("e2e")
		compose, err := tc.NewDockerComposeWith(tc.WithStackFiles(t.ComposeFilePaths...), identifier)

		if err != nil {
			return err
		}

		if err := compose.WithEnv(envSettings).Up(t.Context); err != nil {
			return err
		}

		// time.Sleep(30 * time.Second)

		t.Compose = compose

		nodeNames := []string{"ubsv1", "ubsv2", "ubsv3"}
		order := []string{"SETTINGS_CONTEXT_1", "SETTINGS_CONTEXT_2", "SETTINGS_CONTEXT_3"}
		defaultSettings := []string{"docker.ubsv1.test", "docker.ubsv2.test", "docker.ubsv3.test"}

		for idx, key := range order {
			os.Setenv("SETTINGS_CONTEXT", envSettings[key])
			t.Nodes = append(t.Nodes, TeranodeTestClient{
				DefaultSettingsContext: defaultSettings[idx],
				SettingsContext:        envSettings[key],
				Name:                   nodeNames[len(t.Nodes)],
				Settings:               settings.NewSettings(),
			})

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

func (t *TeranodeTestEnv) setupRPCURL(node *TeranodeTestClient) error {
	rpcPort := "9292/tcp"
	rpcMappedPort, err := t.GetMappedPort(node.Name, nat.Port(rpcPort))

	if err != nil {
		return errors.NewConfigurationError("error getting rpc mapped port:", err)
	}

	node.RPCURL = fmt.Sprintf("http://%s", makeHostAddressFromPort(rpcMappedPort.Port()))

	return nil
}

func (t *TeranodeTestEnv) setupCoinbaseClient(node *TeranodeTestClient) error {
	coinbaseGRPCPort := "8093/tcp"

	coinbaseMappedPort, err := t.GetMappedPort(node.Name, nat.Port(coinbaseGRPCPort))
	if err != nil {
		return errors.NewConfigurationError("error getting coinbase mapped port:", err)
	}

	coinbaseClient, err := cb.NewClientWithAddress(t.Context, t.Logger, makeHostAddressFromPort(coinbaseMappedPort.Port()))
	if err != nil {
		return errors.NewConfigurationError("error creating coinbase client:", err)
	}

	node.CoinbaseClient = *coinbaseClient

	return nil
}

func (t *TeranodeTestEnv) setupBlockchainClient(node *TeranodeTestClient) error {
	blockchainGRPCPort := "8087/tcp"
	blockchainMappedPort, err := t.GetMappedPort(node.Name, nat.Port(blockchainGRPCPort))

	if err != nil {
		return errors.NewConfigurationError("error getting blockchain mapped port", err)
	}

	blockchainClient, err := bc.NewClientWithAddress(t.Context, t.Logger, node.Settings, makeHostAddressFromPort(blockchainMappedPort.Port()), "test")
	if err != nil {
		return errors.NewConfigurationError("error creating blockchain client", err)
	}

	node.BlockchainClient = blockchainClient

	return nil
}

func (t *TeranodeTestEnv) setupBlockassemblyClient(node *TeranodeTestClient) error {
	blockassemblyGRPCPort := "8085/tcp"

	blockassemblyMappedPort, err := t.GetMappedPort(node.Name, nat.Port(blockassemblyGRPCPort))
	if err != nil {
		return errors.NewConfigurationError("error getting blockassembly mapped port", err)
	}

	blockassemblyClient, err := ba.NewClientWithAddress(t.Context, t.Logger, node.Settings, makeHostAddressFromPort(blockassemblyMappedPort.Port()))
	if err != nil {
		return errors.NewConfigurationError("error creating blockassembly client", err)
	}

	node.BlockassemblyClient = *blockassemblyClient

	return nil
}

func (t *TeranodeTestEnv) setupDistributorClient(node *TeranodeTestClient) error {
	distributorGRPCPort := "8084/tcp"
	distributorMappedPort, err := t.GetMappedPort(node.Name, nat.Port(distributorGRPCPort))

	if err != nil {
		return errors.NewConfigurationError("error getting distributor mapped port:", err)
	}

	distributorClient, err := distributor.NewDistributorFromAddress(t.Context, t.Logger, node.Settings, makeHostAddressFromPort(distributorMappedPort.Port()))
	if err != nil {
		return errors.NewConfigurationError("error creating distributor client:", err)
	}

	node.DistributorClient = *distributorClient

	return nil
}

func (t *TeranodeTestEnv) setupStores(node *TeranodeTestClient) error {
	blockStoreURL, err, found := gocore.Config().GetURL(fmt.Sprintf("blockstore.%s.context.testrunner", node.DefaultSettingsContext))
	// filePathMappedVolume, err, found := gocore.Config().GetURL(fmt.Sprintf("filePathMappedVolume.%s", node.SettingsContext))
	if err != nil {
		return errors.NewConfigurationError("error getting mapped volume url:", err)
	}

	if !found {
		return errors.NewConfigurationError("error getting mapped volume url:", err)
	}

	t.Logger.Infof("blockStoreURL: %s", blockStoreURL.String())

	blockStore, err := blob.NewStore(t.Logger, blockStoreURL)
	if err != nil {
		return errors.NewConfigurationError("error creating blockstore:", err)
	}

	node.Blockstore = blockStore
	node.BlockstoreURL = blockStoreURL

	subtreeStoreURL, err, found := gocore.Config().GetURL(fmt.Sprintf("subtreestore.%s.context.testrunner", node.SettingsContext))
	t.Logger.Infof("subtreeStoreURL: %s", subtreeStoreURL.String())

	if err != nil {
		return errors.NewConfigurationError("error getting subtreestore url:", err)
	}

	if !found {
		return errors.NewConfigurationError("error getting subtreestore url:", err)
	}

	subtreeStore, err := blob.NewStore(t.Logger, subtreeStoreURL, options.WithHashPrefix(2))
	if err != nil {
		return errors.NewConfigurationError("error creating subtreestore:", err)
	}

	node.SubtreeStore = subtreeStore

	utxoStoreURL, err, _ := gocore.Config().GetURL(fmt.Sprintf("utxostore.%s.context.testrunner", node.DefaultSettingsContext))
	if err != nil {
		return errors.NewConfigurationError("error creating utxostore", err)
	}

	t.Logger.Infof("utxoStoreURL: %s", utxoStoreURL.String())
	node.UtxoStore, err = utxostore.New(t.Context, t.Logger, node.Settings, utxoStoreURL)

	if err != nil {
		return errors.NewConfigurationError("error creating utxostore", err)
	}

	blockchainStoreURL, err, _ := gocore.Config().GetURL(fmt.Sprintf("blockchain_store.%s.context.testrunner", node.DefaultSettingsContext))

	if err != nil {
		t.Logger.Errorf("Error bcs: %v", err)
	}

	t.Logger.Infof("blockchainStoreURL: %s", blockchainStoreURL.String())

	blockchainStore, err := bcs.NewStore(t.Logger, blockchainStoreURL)
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
	if t != nil && t.Compose != nil && t.Context != nil {
		return t.Compose.Down(t.Context)
	}

	return nil
}

// RestartDockerNodes restarts the Docker Compose services.
func (t *TeranodeTestEnv) RestartDockerNodes(envSettings map[string]string) error {
	if t.Compose != nil {
		if err := t.Compose.Down(t.Context); err != nil {
			return err
		}

		identifier := tc.StackIdentifier("e2e")

		compose, err := tc.NewDockerComposeWith(tc.WithStackFiles(t.ComposeFilePaths...), identifier)
		if err != nil {
			return err
		}

		if err := compose.WithEnv(envSettings).Up(t.Context); err != nil {
			return err
		}

		t.Compose = compose

		time.Sleep(30 * time.Second)
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

		time.Sleep(10 * time.Second)
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
