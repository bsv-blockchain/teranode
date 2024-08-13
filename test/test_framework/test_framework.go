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
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo/sql"
	"github.com/bitcoin-sv/ubsv/ulogger"
	distributor "github.com/bitcoin-sv/ubsv/util/distributor"
	"github.com/ordishs/gocore"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
)

type BitcoinTestFramework struct {
	ComposeFilePaths []string
	Context          context.Context
	Compose          tc.ComposeStack
	Nodes            []BitcoinNode
	Logger           ulogger.Logger
}

type BitcoinNode struct {
	SETTINGS_CONTEXT    string
	CoinbaseClient      cb.Client
	BlockchainClient    bc.ClientI
	BlockassemblyClient ba.Client
	DistributorClient   distributor.Distributor
	BlockChainDB        blockchain_store.Store
	Blockstore          blob.Store
	SubtreeStore        blob.Store
	BlockstoreUrl       *url.URL
	UtxoStore           *utxostore.Store
	SubtreesKafkaURL    *url.URL
	RPC_URL             string
}

func NewBitcoinTestFramework(composeFilePaths []string) *BitcoinTestFramework {
	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("testRun", ulogger.WithLevel(logLevelStr))
	return &BitcoinTestFramework{
		ComposeFilePaths: composeFilePaths,
		Context:          context.Background(),
		Logger:           logger,
	}
}

// SetupNodes starts the nodes with docker-compose up operation.
// The settings map is used to pass the environment variables to the docker-compose services.
func (b *BitcoinTestFramework) SetupNodes(m map[string]string) error {
	var testRunMode, _ = gocore.Config().Get("test_run_mode", "ci")
	logger := b.Logger

	if testRunMode == "ci" {
		// Start the nodes with docker-compose up operation
		compose, err := tc.NewDockerCompose(b.ComposeFilePaths...)
		if err != nil {
			return err
		}

		if err := compose.WithEnv(m).Up(b.Context); err != nil {
			return err
		}

		// Wait for the services to be ready
		time.Sleep(30 * time.Second)

		b.Compose = compose
	}

	order := []string{"SETTINGS_CONTEXT_1", "SETTINGS_CONTEXT_2", "SETTINGS_CONTEXT_3"}
	for _, key := range order {
		b.Nodes = append(b.Nodes, BitcoinNode{
			SETTINGS_CONTEXT: m[key],
		})
	}

	for i, node := range b.Nodes {
		coinbaseGrpcAddress, ok := gocore.Config().Get(fmt.Sprintf("coinbase_grpcAddress.%s", node.SETTINGS_CONTEXT))
		fmt.Println(coinbaseGrpcAddress)
		if !ok {
			return errors.NewConfigurationError("no coinbase_grpcAddress setting found")
		}
		coinbaseClient, err := cb.NewClientWithAddress(b.Context, logger, getHostAddress(coinbaseGrpcAddress))
		if err != nil {
			return errors.NewConfigurationError("error creating coinbase client %w", err)
		}
		b.Nodes[i].CoinbaseClient = *coinbaseClient

		blockchainGrpcAddress, ok := gocore.Config().Get(fmt.Sprintf("blockchain_grpcAddress.%s", node.SETTINGS_CONTEXT))
		if !ok {
			return errors.NewConfigurationError("no blockchain_grpcAddress setting found")
		}
		blockchainClient, err := bc.NewClientWithAddress(b.Context, logger, getHostAddress(blockchainGrpcAddress))
		if err != nil {
			return errors.NewConfigurationError("error creating blockchain client %w", err)
		}
		b.Nodes[i].BlockchainClient = blockchainClient

		blockassembly_grpcAddress, ok := gocore.Config().Get(fmt.Sprintf("blockassembly_grpcAddress.%s", node.SETTINGS_CONTEXT))
		if !ok {
			return errors.NewConfigurationError("no blockassembly_grpcAddress setting found")
		}
		blockassemblyClient, err := ba.NewClientWithAddress(b.Context, logger, getHostAddress(blockassembly_grpcAddress))
		if err != nil {
			return errors.NewServiceError("error creating blockassembly client %w", err)
		}
		b.Nodes[i].BlockassemblyClient = *blockassemblyClient

		propagation_grpcAddress, ok := gocore.Config().Get(fmt.Sprintf("propagation_grpcAddress.%s", node.SETTINGS_CONTEXT))
		if !ok {
			return errors.NewConfigurationError("no propagation_grpcAddress setting found")
		}
		distributorClient, err := distributor.NewDistributorFromAddress(b.Context, logger, getHostAddress(propagation_grpcAddress))
		if err != nil {
			return errors.NewConfigurationError("error creating distributor client %w", err)
		}
		b.Nodes[i].DistributorClient = *distributorClient

		subtreesKafkaUrl, err, ok := gocore.Config().GetURL(fmt.Sprintf("kafka_subtreesConfig.%s.run", node.SETTINGS_CONTEXT))
		if err != nil {
			return errors.NewConfigurationError("no kafka_subtreesConfig setting found")
		}
		if !ok {
			return errors.NewConfigurationError("no kafka_subtreesConfig setting found")
		}
		kafkaURL, _ := url.Parse(strings.Replace(subtreesKafkaUrl.String(), "kafka-shared", "localhost", 1))
		kafkaURL, _ = url.Parse(strings.Replace(kafkaURL.String(), "9092", "19093", 1))

		b.Nodes[i].SubtreesKafkaURL = kafkaURL

		blockchainStoreURL, _, _ := gocore.Config().GetURL(fmt.Sprintf("blockchain_store.%s", node.SETTINGS_CONTEXT))
		blockchainStore, err := blockchain_store.NewStore(logger, blockchainStoreURL)
		if err != nil {
			return errors.NewConfigurationError("error creating blockchain store %w", err)
		}
		b.Nodes[i].BlockChainDB = blockchainStore

		blockStoreUrl, err, found := gocore.Config().GetURL(fmt.Sprintf("blockstore.%s.run", node.SETTINGS_CONTEXT))
		if err != nil {
			return errors.NewConfigurationError("error getting blockstore url %w", err)
		}
		if !found {
			return errors.NewConfigurationError("blockstore config not found")
		}
		blockStore, err := blob.NewStore(logger, blockStoreUrl)
		if err != nil {
			return errors.NewConfigurationError("error creating blockstore %w", err)
		}
		b.Nodes[i].Blockstore = blockStore
		b.Nodes[i].BlockstoreUrl = blockStoreUrl

		subtreeStoreUrl, err, found := gocore.Config().GetURL(fmt.Sprintf("subtreestore.%s.run", node.SETTINGS_CONTEXT))
		if err != nil {
			return errors.NewConfigurationError("error getting subtreestore url %w", err)
		}
		if !found {
			return errors.NewConfigurationError("subtreestore config not found")
		}
		subtreeStore, err := blob.NewStore(logger, subtreeStoreUrl)
		if err != nil {
			return errors.NewConfigurationError("error creating subtreestore %w", err)
		}
		b.Nodes[i].SubtreeStore = subtreeStore

		utxoStoreUrl, err, _ := gocore.Config().GetURL(fmt.Sprintf("utxostore.%s.run", node.SETTINGS_CONTEXT))
		logger.Infof("utxoStoreUrl", utxoStoreUrl.String())
		b.Nodes[i].UtxoStore, _ = utxostore.New(b.Context, logger, utxoStoreUrl)
		if err != nil {
			return errors.NewConfigurationError("error creating utxostore %w", err)
		}

		rpcUrl, ok := gocore.Config().Get(fmt.Sprintf("rpc_listener_url.%s", node.SETTINGS_CONTEXT))
		//remove : from the prefix
		rpcPort := strings.Replace(rpcUrl, ":", "", 1)
		if !ok {
			return errors.NewConfigurationError("no rpc_listener_url setting found")
		}
		b.Nodes[i].RPC_URL = fmt.Sprintf("http://localhost:%d%s", i+1, rpcPort)
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
	if b.Compose != nil {
		// Stop the Docker Compose services
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

		b.Compose, _ = tc.NewDockerCompose(b.ComposeFilePaths...)
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
