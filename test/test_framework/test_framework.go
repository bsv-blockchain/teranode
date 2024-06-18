package test_framework

import (
	"context"
	"fmt"
	"strings"
	"time"

	ba "github.com/bitcoin-sv/ubsv/services/blockassembly"
	bc "github.com/bitcoin-sv/ubsv/services/blockchain"
	cb "github.com/bitcoin-sv/ubsv/services/coinbase"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
)

type BitcoinTestFramework struct {
	ComposeFilePaths []string
	Context          context.Context
	Compose          tc.ComposeStack
	Nodes            []BitcoinNode
}

type BitcoinNode struct {
	SETTINGS_CONTEXT    string
	CoinbaseClient      cb.Client
	BlockchainClient    bc.ClientI
	BlockassemblyClient ba.Client
}

func NewBitcoinTestFramework(composeFilePaths []string) *BitcoinTestFramework {
	return &BitcoinTestFramework{
		ComposeFilePaths: composeFilePaths,
		Context:          context.Background(),
	}
}

// StopNodes starts the nodes with docker-compose up operation.
func (b *BitcoinTestFramework) SetupNodes(m map[string]string) error {
	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("txblast", ulogger.WithLevel(logLevelStr))

	compose, err := tc.NewDockerCompose(b.ComposeFilePaths...)
	if err != nil {
		return err
	}

	if err := compose.WithEnv(m).Up(b.Context); err != nil {
		return err
	}

	// Wait for the services to be ready
	time.Sleep(10 * time.Second)

	b.Compose = compose

	for setting := range m {
		b.Nodes = append(b.Nodes, BitcoinNode{
			SETTINGS_CONTEXT: m[setting],
		})
	}

	for i, node := range b.Nodes {
		coinbaseGrpcAddress, ok := gocore.Config().Get(fmt.Sprintf("coinbase_grpcAddress.%s", node.SETTINGS_CONTEXT))
		fmt.Println(coinbaseGrpcAddress)
		if !ok {
			return fmt.Errorf("no coinbase_grpcAddress setting found")
		}
		coinbaseClient, err := cb.NewClientWithAddress(b.Context, logger, getHostAddress(coinbaseGrpcAddress))
		if err != nil {
			return err
		}
		b.Nodes[i].CoinbaseClient = *coinbaseClient

		blockchainGrpcAddress, ok := gocore.Config().Get(fmt.Sprintf("blockchain_grpcAddress.%s", node.SETTINGS_CONTEXT))
		if !ok {
			return fmt.Errorf("no blockchain_grpcAddress setting found")
		}
		blockchainClient, err := bc.NewClientWithAddress(b.Context, logger, getHostAddress(blockchainGrpcAddress))
		if err != nil {
			return err
		}
		b.Nodes[i].BlockchainClient = blockchainClient

		blockassembly_grpcAddress, ok := gocore.Config().Get(fmt.Sprintf("blockassembly_grpcAddress.%s", node.SETTINGS_CONTEXT))
		if !ok {
			return fmt.Errorf("no blockassembly_grpcAddress setting found")
		}
		blockassemblyClient := ba.NewClientWithAddress(b.Context, logger, getHostAddress(blockassembly_grpcAddress))
		b.Nodes[i].BlockassemblyClient = *blockassemblyClient
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

// StopNodes starts a particular node.
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

	}
	return nil
}

// StopNodes stops a particular node.
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
