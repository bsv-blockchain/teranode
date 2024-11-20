package test_framework

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	ba "github.com/bitcoin-sv/ubsv/services/blockassembly"
	bc "github.com/bitcoin-sv/ubsv/services/blockchain"
	cb "github.com/bitcoin-sv/ubsv/services/coinbase"
	blob "github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo/aerospike"
	"github.com/bitcoin-sv/ubsv/ulogger"
	distributor "github.com/bitcoin-sv/ubsv/util/distributor"
	"github.com/docker/go-connections/nat"
	"github.com/ordishs/gocore"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
)

type BitcoinTestFramework struct {
	ComposeFilePaths []string
	Context          context.Context
	Compose          tc.ComposeStack
	Nodes            []BitcoinNode
	Logger           ulogger.Logger
	Cancel           context.CancelFunc
}

type BitcoinNode struct {
	NodeName            string
	SettingsContext     string
	CoinbaseClient      cb.Client
	BlockchainClient    bc.ClientI
	BlockassemblyClient ba.Client
	DistributorClient   distributor.Distributor
	BlockChainDB        blockchain_store.Store
	Blockstore          blob.Store
	SubtreeStore        blob.Store
	BlockstoreURL       *url.URL
	UtxoStore           *utxostore.Store
	SubtreesKafkaURL    *url.URL
	RPCURL              string
}

func NewBitcoinTestFramework(composeFilePaths []string) *BitcoinTestFramework {
	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("testRun", ulogger.WithLevel(logLevelStr))
	ctx, cancel := context.WithCancel(context.Background())

	return &BitcoinTestFramework{
		ComposeFilePaths: composeFilePaths,
		Context:          ctx,
		Logger:           logger,
		Cancel:           cancel,
	}
}

func (b *BitcoinTestFramework) SetupNodes(m map[string]string) error {
	if len(m) == 0 {
		return errors.NewConfigurationError("no settings map provided")
	}

	var testRunMode, _ = gocore.Config().Get("test_run_mode", "ci")

	b.Logger.Infof("testRunMode: %s", testRunMode)
	b.Logger.Infof("ComposeFilePaths: %v", b.ComposeFilePaths)
	b.Logger.Infof("m: %v", m)

	if testRunMode == "ci" {
		identifier := tc.StackIdentifier("e2e")

		// compose, err := tc.NewDockerCompose(b.ComposeFilePaths...)
		compose, err := tc.NewDockerComposeWith(tc.WithStackFiles(b.ComposeFilePaths...), identifier)

		if err != nil {
			return err
		}

		if err := compose.WithEnv(m).Up(b.Context); err != nil {
			return err
		}

		time.Sleep(30 * time.Second)

		b.Compose = compose

		order := []string{"SETTINGS_CONTEXT_1", "SETTINGS_CONTEXT_2", "SETTINGS_CONTEXT_3"}
		names := []string{"ubsv1", "ubsv2", "ubsv3"}

		for _, key := range order {
			b.Nodes = append(b.Nodes, BitcoinNode{
				SettingsContext: m[key],
				NodeName:        names[len(b.Nodes)],
			})
		}
	}

	return nil
}

// nolint: gocognit
func (b *BitcoinTestFramework) GetClientHandles() error {
	logger := b.Logger

	logger.Infof("nodes: %v", b.Nodes)

	for i, node := range b.Nodes {
		// coinbaseGrpcAddress, ok := gocore.Config().Get(fmt.Sprintf("coinbase_grpcAddress.%s", node.SettingsContext))
		// coinbaseGrpcPort

		// if !ok {
		// 	return errors.NewConfigurationError("no coinbase_grpcAddress setting found")
		// }

		coinbaseGRPCPort := "8093/tcp"
		coinbaseMappedPort, err := b.GetMappedPort(node.NodeName, nat.Port(coinbaseGRPCPort))

		if err != nil {
			return errors.NewConfigurationError("error getting mapped port %v", coinbaseMappedPort, err)
		}

		coinbaseClient, err := cb.NewClientWithAddress(b.Context, logger, makeHostAddressFromPort(coinbaseMappedPort.Port()))
		if err != nil {
			return errors.NewConfigurationError("error creating coinbase client %v", coinbaseMappedPort, err)
		}

		b.Nodes[i].CoinbaseClient = *coinbaseClient

		// blockchainGrpcAddress, ok := gocore.Config().Get(fmt.Sprintf("blockchain_grpcAddress.%s", node.SettingsContext))
		// if !ok {
		// 	return errors.NewConfigurationError("no blockchain_grpcAddress setting found")
		// }
		blockchainGRPCPort := "8087/tcp"
		blockchainMappedPort, err := b.GetMappedPort(node.NodeName, nat.Port(blockchainGRPCPort))

		if err != nil {
			return errors.NewConfigurationError("error getting mapped port %v", blockchainMappedPort, err)
		}

		blockchainClient, err := bc.NewClientWithAddress(b.Context, logger, makeHostAddressFromPort(blockchainMappedPort.Port()), "test")
		if err != nil {
			return errors.NewConfigurationError("error creating blockchain client %v", blockchainMappedPort, err)
		}

		b.Nodes[i].BlockchainClient = blockchainClient

		// blockassemblyGrpcAddress, ok := gocore.Config().Get(fmt.Sprintf("blockassembly_grpcAddress.%s", node.SettingsContext))
		// if !ok {
		// 	return errors.NewConfigurationError("no blockassembly_grpcAddress setting found")
		// }

		blockassemblyGRPCPort := "8085/tcp"
		blockassemblyMappedPort, err := b.GetMappedPort(node.NodeName, nat.Port(blockassemblyGRPCPort))

		if err != nil {
			return errors.NewConfigurationError("error getting mapped port %v", blockassemblyMappedPort, err)
		}

		blockassemblyClient, err := ba.NewClientWithAddress(b.Context, logger, makeHostAddressFromPort(blockassemblyMappedPort.Port()))
		if err != nil {
			return errors.NewServiceError("error creating blockassembly client %v", blockassemblyMappedPort, err)
		}

		b.Nodes[i].BlockassemblyClient = *blockassemblyClient

		// propagationGrpcAddress, ok := gocore.Config().Get(fmt.Sprintf("propagation_grpcAddress.%s", node.SettingsContext))
		// if !ok {
		// 	return errors.NewConfigurationError("no propagation_grpcAddress setting found")
		// }

		propagationGRPCPort := "8084/tcp"
		propagationMappedPort, _ := b.GetMappedPort(node.NodeName, nat.Port(propagationGRPCPort))

		if err != nil {
			return errors.NewConfigurationError("error getting mapped port %v", propagationMappedPort, err)
		}

		distributorClient, err := distributor.NewDistributorFromAddress(b.Context, logger, makeHostAddressFromPort(propagationMappedPort.Port()))
		if err != nil {
			return errors.NewConfigurationError("error creating distributor client %v", propagationMappedPort, err)
		}

		b.Nodes[i].DistributorClient = *distributorClient

		// subtreesKafkaURL, err, ok := gocore.Config().GetURL(fmt.Sprintf("kafka_subtreesConfig.%s.run", node.SettingsContext))
		// if err != nil {
		// 	return errors.NewConfigurationError("no kafka_subtreesConfig setting found")
		// }

		// if !ok {
		// 	return errors.NewConfigurationError("no kafka_subtreesConfig setting found")
		// }

		// kafkaURL, _ := url.Parse(strings.Replace(subtreesKafkaURL.String(), "kafka-shared", "localhost", 1))
		// kafkaURL, _ = url.Parse(strings.Replace(kafkaURL.String(), "9092", "19093", 1))
		// b.Nodes[i].SubtreesKafkaURL = kafkaURL
		// blockchainStoreURL, _, _ := gocore.Config().GetURL(fmt.Sprintf("blockchain_store.%s", node.SettingsContext))

		// blockchainStore, err := blockchain_store.NewStore(logger, blockchainStoreURL)
		// if err != nil {
		// 	return errors.NewConfigurationError("error creating blockchain store %w", err)
		// }

		// b.Nodes[i].BlockChainDB = blockchainStore

		blockStoreURL, err, found := gocore.Config().GetURL(fmt.Sprintf("blockstore.%s.run", node.SettingsContext))
		if err != nil {
			return errors.NewConfigurationError("error getting blockstore url %v", blockStoreURL, err)
		}

		if !found {
			return errors.NewConfigurationError("blockstore config not found")
		}

		blockStore, err := blob.NewStore(logger, blockStoreURL)
		if err != nil {
			return errors.NewConfigurationError("error creating blockstore %v", blockStoreURL, err)
		}

		b.Nodes[i].Blockstore = blockStore
		b.Nodes[i].BlockstoreURL = blockStoreURL

		subtreeStoreURL, err, found := gocore.Config().GetURL(fmt.Sprintf("subtreestore.%s.run", node.SettingsContext))
		if err != nil {
			return errors.NewConfigurationError("error getting subtreestore url %v", subtreeStoreURL, err)
		}

		if !found {
			return errors.NewConfigurationError("subtreestore config not found")
		}

		subtreeStore, err := blob.NewStore(logger, subtreeStoreURL, options.WithHashPrefix(2))
		if err != nil {
			return errors.NewConfigurationError("error creating subtreestore %v", subtreeStoreURL, err)
		}

		b.Nodes[i].SubtreeStore = subtreeStore

		logger.Infof("Settings_context: %s", node.SettingsContext)

		utxoStoreURL, err, _ := gocore.Config().GetURL(fmt.Sprintf("utxostore.%s.run", node.SettingsContext))
		if err != nil {
			return errors.NewConfigurationError("error creating utxostore %v", utxoStoreURL, err)
		}

		logger.Infof("utxoStoreUrl: %s", utxoStoreURL.String())
		b.Nodes[i].UtxoStore, err = utxostore.New(b.Context, logger, utxoStoreURL)

		if err != nil {
			return errors.NewConfigurationError("error creating utxostore %v", utxoStoreURL, err)
		}

		// rpcURL, ok := gocore.Config().Get(fmt.Sprintf("rpc_listener_url.%s", node.SettingsContext))
		// if !ok {
		// 	return errors.NewConfigurationError("no rpc_listener_url setting found")
		// }

		// rpcPort := strings.Replace(rpcURL, ":", "", 1)
		// b.Nodes[i].RPCURL = fmt.Sprintf("http://localhost:%d%s", i+1, rpcPort)
	}

	return nil
}

// StopNodes stops the nodes with docker-compose down operation.
func (b *BitcoinTestFramework) StopNodes() error {
	if b.Compose != nil {
		// Stop the Docker Compose services
		if err := b.Compose.Down(b.Context); err != nil {
			return err
		}
	}

	return nil
}

func (b *BitcoinTestFramework) StopNodesWithRmVolume() error {
	defer func() {
		if r := recover(); r != nil {
			if b != nil && b.Logger != nil {
				b.Logger.Errorf("Recovered from panic: %v", r)
			}
		}
	}()

	if b.Compose != nil {
		if err := b.Compose.Down(b.Context, tc.RemoveVolumes(true)); err != nil {
			return err
		}
	}

	return nil
}

// Restart the nodes with docker-compose down operation.
func (b *BitcoinTestFramework) RestartNodes(m map[string]string) error {
	if b.Compose != nil {
		// Stop the Docker Compose services
		if err := b.Compose.Down(b.Context); err != nil {
			return err
		}

		identifier := tc.StackIdentifier("e2e")

		b.Compose, _ = tc.NewDockerComposeWith(tc.WithStackFiles(b.ComposeFilePaths...), identifier)
		if err := b.Compose.WithEnv(m).Up(b.Context); err != nil {
			return err
		}

		// Wait for the services to be ready
		time.Sleep(30 * time.Second)
	}

	return nil
}

// StartNode starts a particular node.
func (b *BitcoinTestFramework) StartNode(nodeName string) error {
	if b.Compose != nil {
		// Stop the Docker Compose services
		node, err := b.Compose.ServiceContainer(b.Context, nodeName)
		if err != nil {
			return err
		}

		err = node.Start(b.Context)
		if err != nil {
			return err
		}

		time.Sleep(10 * time.Second)

	}

	return nil
}

func (b *BitcoinTestFramework) GetMappedPort(nodeName string, port nat.Port) (nat.Port, error) {
	if b.Compose != nil {
		// Stop the Docker Compose services
		node, err := b.Compose.ServiceContainer(b.Context, nodeName)
		if err != nil {
			return "", err
		}

		mappedPort, err := node.MappedPort(b.Context, port)
		if err != nil {
			return "", err
		}

		return mappedPort, nil
	}

	return "", nil
}

// StopNode stops a particular node.
func (b *BitcoinTestFramework) StopNode(nodeName string) error {
	if b.Compose != nil {
		// Stop the Docker Compose services
		node, err := b.Compose.ServiceContainer(b.Context, nodeName)
		if err != nil {
			return err
		}

		err = node.Stop(b.Context, nil)
		if err != nil {
			return err
		}
	}

	return nil
}

func makeHostAddressFromPort(port string) string {
	return fmt.Sprintf("localhost:%s", port)
}

// getHostAddress returns the host equivalent address for the given ubsv service.
func getHostAddress(input string) string {
	// Split the input string by ":" to separate the prefix and the port
	parts := strings.Split(input, ":")

	if len(parts) != 2 {
		// Handle unexpected input format
		return ""
	}

	// Extract the suffix after the "-"
	suffix := parts[0][len(parts[0])-1:] // get the last character after "-"
	port := parts[1]

	// Construct the desired output
	return fmt.Sprintf("localhost:%s%s", suffix, port)
}
