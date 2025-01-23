package generate

import (
	"fmt"
	"os"
	"strings"
	"text/template"

	"github.com/spf13/cobra"
)

const (
	parentTemplate = `
version: '3.8'

x-teranode-base:
  &teranode-base
  image: teranode:latest
  depends_on:
    - postgres
  networks:
    - teranode-network
  volumes:
    - ./data:/app/data
  expose:
    - 8081-8093

services:
  teranode-builder:
    image: teranode:latest
    build:
      context: .
      dockerfile: local.Dockerfile
      args:
        BASE_IMG: 434394763103.dkr.ecr.eu-north-1.amazonaws.com/ubsv:base-build-db1a6f0
        RUN_IMG: 434394763103.dkr.ecr.eu-north-1.amazonaws.com/ubsv:base-run-db1a6f0
    networks:
      - teranode-network
    entrypoint: ["pwd"]

  postgres:
    container_name: postgres
    image: postgres:latest
    networks:
      - teranode-network
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: postgres
    ports:
      - 5432:5432
    volumes:
      - ./cmd/testUtil/compose/generator/scripts/postgres/init.sql:/docker-entrypoint-initdb.d/init.sql
      - ./data/postgres:/var/lib/postgresql/data
    healthcheck:
      test: [ "CMD-SHELL", "pg_isready -U postgres" ]
      interval: 5s
      timeout: 5s
      retries: 5

  p2p-bootstrap-1:
    build:
      context: ./modules/p2pBootstrap
      dockerfile: local.Dockerfile
    environment:
      P2P_BOOTSTRAP_PORT: 9901
      SETTINGS_CONTEXT: "docker.ci.teranode1"
    networks:
      - teranode-network
    ports:
      - "19901:9901"
{{ .Services }}
networks:
  teranode-network:
    name: my-teranode-network
`
)

type ServiceConfig struct {
	Index                      int
	ServiceName                string
	StartCoinbase              bool
	MineInitialBlocks          bool
	MineInitialBlocksCount     int
	MinerWalletPrivateKeys     string
	CoinbaseWalletPrivateKey   string
	StartBootstrap             bool
	FeatureBootstrap           bool
	StartP2P                   bool
	FeatureLibP2P              bool
	P2PPrivateKey              string
	P2PPeerID                  string
	P2PStaticPeers             string
	Ports                      string
	P2PDhtUsePrivate           bool
	AssetHTTPAddress           string
	AssetGRPCAddress           string
	P2PBootstrapAddresses      string
	PropagationHTTPAddresses   string
	BlockchainGRPCAddress      string
	PropagationGRPCAddresses   string
	PropagationQUICAddresses   string
	BlockAssemblyGRPCAddress   string
	BlockValidationHTTPAddress string
	CoinbaseAssetGRPCAddress   string
	CoinbaseGRPCAddress        string
	CoinbaseP2PPrivateKey      string
	CoinbaseP2PPeerID          string
	CoinbaseP2PStaticPeers     string
	SubtreeStore               string
	TxStore                    string
	ValidatorGRPCAddress       string
}

type TxBlasterConfig struct {
	Index                    int
	ServiceName              string
	TeranodeDependencies     []string
	AssetHTTPAddress         string
	PropagationGRPCAddresses string
	CoinbaseGRPCAddress      string
	CoinbaseAssetGRPCAddress string
	PropagationQUICAddresses string
}

type TxBlasterConfigs struct {
	TxBlasterConfigs []TxBlasterConfig
}
type ServiceConfigs struct {
	TeranodeServices  []ServiceConfig
	TxBlasterServices []TxBlasterConfig
}

var numNodes int

const asset_apiPrefix = "/api/v1"

func AddGenerateCommand(rootCmd *cobra.Command) {
	var generateCmd = &cobra.Command{
		Use:   "generate",
		Short: "Generate Docker Compose file",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("Generate executed with %d numNodes\n", numNodes)
			err := Generate()
			if err != nil {
				fmt.Println("Error generating compose file:", err)
			}
		},
	}

	generateCmd.Flags().IntVarP(&numNodes, "numNodes", "n", 3, "Number of nodes")
	rootCmd.AddCommand(generateCmd)
}

func Generate() error {
	// Create an array of private keys for the miner wallet
	privateKeys := []string{
		"L56TgyTpDdvL3W24SMoALYotibToSCySQeo4pThLKxw6EFR6f93Q",
		"KyAwSjuXZNgj78w3W7mR1fVMbPFu2heaCJJkWK5Yy58NZ4xafV6k",
		"L32b9pdNPxXhqEA5VvzMb8wQ1NPpRXFmViFgEewjkVg8ZgH23b6w",
		"KzDoV5r6iYFMdNRoh8xYhDq5MT5rYdVgEVASADMxgg59nLEGShNP",
		"KwoDrk3rchNCX5YAmbPrGu1m7kVkGVEz8bWXhjkdLJ8gcTUTk6B8",
		"L2Gi71RNwnzX5Symkpdt1sHzyQrL9WKHyYrJr9fK5tohdH9H19ke",
	}
	slicedPrivateKeys := privateKeys[:numNodes]

	// Generate addresses for propagation
	allPropagationAddresses := generatePropagationGRPCAddresses(numNodes)

	// Add DBScript to the init.sql file
	err := writeInitSQLFile(numNodes)
	if err != nil {
		fmt.Println("Error writing init.sql file:", err)
		return err
	}

	// Initialize service configurations for each service type
	var teranodeNodes []ServiceConfig

	var txBlasterNodes []TxBlasterConfig

	// Generate configurations for each service type
	for i := 0; i < numNodes; i++ {
		teranodeNodes = append(teranodeNodes, generateTERANODEConfig(i+1, slicedPrivateKeys, allPropagationAddresses))
	}

	for i := 0; i < numNodes; i++ {
		txBlasterNodes = append(txBlasterNodes, generateTxBlasterConfig(i+1, numNodes, allPropagationAddresses))
	}

	// Combine all service configurations
	var allServiceConfigs ServiceConfigs
	allServiceConfigs.TeranodeServices = append(allServiceConfigs.TeranodeServices, teranodeNodes...)
	allServiceConfigs.TxBlasterServices = append(allServiceConfigs.TxBlasterServices, txBlasterNodes...)

	// Merge individual service configurations into one compose file
	err = generateComposeFile(allServiceConfigs)
	if err != nil {
		fmt.Println("Error generating compose file:", err)
		return err
	}

	return nil
}

func generateTERANODEConfig(index int, privateKeys []string, propagationAddress string) ServiceConfig {
	return ServiceConfig{
		ServiceName:                fmt.Sprintf("teranode-%d", index),
		Index:                      index,
		StartCoinbase:              true,
		MineInitialBlocks:          index == 1,
		MineInitialBlocksCount:     300,
		MinerWalletPrivateKeys:     strings.Join(privateKeys, "|"),
		CoinbaseWalletPrivateKey:   privateKeys[index-1],
		StartBootstrap:             false,
		FeatureBootstrap:           false,
		StartP2P:                   true,
		FeatureLibP2P:              true,
		P2PDhtUsePrivate:           true,
		BlockchainGRPCAddress:      fmt.Sprintf("teranode-%d:8087", index),
		P2PBootstrapAddresses:      "/dns4/p2p-bootstrap-1/tcp/9901/p2p/12D3KooWS43tBXaGewmskvL1B82KccLP5JafTvreiJNbHCbZhDnh",
		Ports:                      fmt.Sprintf("%d8081-%d8092:8081-8092", index, index),
		PropagationGRPCAddresses:   propagationAddress,
		PropagationHTTPAddresses:   fmt.Sprintf("teranode-%d:8833", index),
		AssetHTTPAddress:           fmt.Sprintf("http://teranode-%d:8090%s", index, asset_apiPrefix),
		AssetGRPCAddress:           fmt.Sprintf("teranode-%d:8091", index),
		BlockAssemblyGRPCAddress:   fmt.Sprintf("teranode-%d:8085", index),
		BlockValidationHTTPAddress: fmt.Sprintf("teranode-%d:8188", index),
		CoinbaseAssetGRPCAddress:   fmt.Sprintf("teranode-%d:8091", index),
		CoinbaseGRPCAddress:        fmt.Sprintf("teranode-%d:8093", index),
		ValidatorGRPCAddress:       fmt.Sprintf("teranode-%d:8081", index),
		SubtreeStore:               fmt.Sprintf("file:///data/teranode%d/subtreestore", index),
		TxStore:                    fmt.Sprintf("file:///data/teranode%d/txstore", index),
	}
}

func generateTxBlasterConfig(index int, numNodes int, propagationAddress string) TxBlasterConfig {
	var dependsOn []string
	for n := 1; n <= numNodes; n++ {
		dependsOn = append(dependsOn, fmt.Sprintf("teranode-%d", n))
	}

	return TxBlasterConfig{
		ServiceName:              fmt.Sprintf("tx-blaster-%d", index),
		Index:                    index,
		TeranodeDependencies:     dependsOn,
		AssetHTTPAddress:         fmt.Sprintf("http://teranode-%d:8090%s", index, asset_apiPrefix),
		PropagationGRPCAddresses: propagationAddress,
		CoinbaseGRPCAddress:      fmt.Sprintf("teranode-%d:8093", index),
		CoinbaseAssetGRPCAddress: fmt.Sprintf("teranode-%d:8091", index),
	}
}

func generateComposeFile(serviceConfigs ServiceConfigs) error {
	// Parse the parent template
	tmpl, err := template.New("compose").Parse(parentTemplate)
	if err != nil {
		return err
	}

	// Execute the template with service configurations
	var sb strings.Builder
	if err := tmpl.Execute(&sb, map[string]interface{}{
		"Services": generateServices(serviceConfigs),
	}); err != nil {
		return err
	}

	// Write the composed configuration to a file or print it
	fmt.Println(sb.String())
	//nolint:gosec // G306: Expect WriteFile permissions to be 0600 or less (gosec)
	if err := os.WriteFile("../../../docker-compose-generated.yml", []byte(sb.String()), 0644); err != nil {
		return err
	}

	return nil
}

func generateServices(serviceConfigs ServiceConfigs) string {
	var sb strings.Builder

	for _, config := range serviceConfigs.TeranodeServices {
		switch {
		case strings.HasPrefix(config.ServiceName, "teranode"):
			teranodeConfigTemplate, err := template.ParseFiles("generator/templates/teranodeService.tmpl")
			if err != nil {
				return err.Error()
			}

			if err := teranodeConfigTemplate.ExecuteTemplate(&sb, "teranodeService", config); err != nil {
				fmt.Println("Error executing teranode template:", err)
				continue
			}
		}
	}

	for _, config := range serviceConfigs.TxBlasterServices {
		switch {
		case strings.HasPrefix(config.ServiceName, "tx-blaster"):
			txBlasterConfigTemplate, err := template.ParseFiles("generator/templates/txBlasterService.tmpl")
			if err != nil {
				return err.Error()
			}

			if err := txBlasterConfigTemplate.ExecuteTemplate(&sb, "txBlasterService", config); err != nil {
				fmt.Println("Error executing txBlaster template:", err)
				continue
			}
		}
	}

	return sb.String()
}

func generatePropagationGRPCAddresses(numNodes int) string {
	var addresses []string
	for i := 1; i <= numNodes; i++ {
		addresses = append(addresses, fmt.Sprintf("teranode-%d:8084", i))
	}

	return strings.Join(addresses, " | ")
}

func writeInitSQLFile(numNodes int) error {
	initSQLFile := "./generator/scripts/postgres/init.sql"

	file, err := os.OpenFile(initSQLFile, os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	defer file.Close()

	for i := 1; i <= numNodes; i++ {
		_, _ = file.WriteString(fmt.Sprintf("CREATE DATABASE coinbase%d;\n", i))
		_, _ = file.WriteString(fmt.Sprintf("CREATE DATABASE blockchain%d;\n", i))
		_, _ = file.WriteString(fmt.Sprintf("CREATE DATABASE txmeta%d;\n", i))
		_, _ = file.WriteString(fmt.Sprintf("CREATE DATABASE utxostore%d;\n", i))
	}

	return nil
}
