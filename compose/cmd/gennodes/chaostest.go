package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// chaos-test (github.com/galt-tr/chaos-test) drives a fleet described by an
// inventory.json. Its schema lives in bsv-regtest/topology/inventory.go; the
// structs below mirror only the fields this stack can fill. The orchestrator
// runs on the stack's compose network (multinode.sh sim up), so the plain URLs
// are in-network and the host* URLs are the ports gennodes already publishes.
//
// Deliberately absent: minerWIF and keys.json. Both carry private keys, so
// multinode.sh fills minerWIF from PK1 and copies chaos-test's own dev
// keys.json at sim-up time instead of writing them into the generated tree.

const (
	chaosTestDir         = "chaostest"
	chaosTestHealthPort  = 8000
	chaosTestPropHTTP    = 8833
	chaosTestP2PPort     = 9905
	chaosTestKafkaBroker = "kafka-shared:9092"
)

type ctInventory struct {
	Project  string               `json:"project"`
	Network  string               `json:"network"`
	Networks ctNetworks           `json:"networks"`
	Nodes    []ctNode             `json:"nodes"`
	Services map[string]ctService `json:"services"`
	RPCUser  string               `json:"rpcUser"`
	RPCPass  string               `json:"rpcPass"`
	MinerWIF string               `json:"minerWIF"`
}

// ctNetworks names the compose network for chaos-test's p2p plane. The
// stack has a single network, so partitions must use iptables rather than a
// network disconnect, which would also cut the node off from its stores.
type ctNetworks struct {
	ChaosName     string `json:"chaosnetName"`
	PartitionMode string `json:"partitionMode"`
}

type ctNode struct {
	Kind               string `json:"kind"`
	Name               string `json:"name"`
	Index              int    `json:"index"`
	PeerID             string `json:"peerID"`
	P2PAddr            string `json:"p2pAddr"`
	RPCURL             string `json:"rpcURL"`
	AssetURL           string `json:"assetURL"`
	PropagationURL     string `json:"propagationURL"`
	HealthURL          string `json:"healthURL"`
	HostRPCURL         string `json:"hostRPCURL"`
	HostAssetURL       string `json:"hostAssetURL"`
	HostBase           int    `json:"hostBase"`
	KafkaRejectedTx    string `json:"kafkaRejectedTx"`
	KafkaInvalidBlocks string `json:"kafkaInvalidBlocks"`
	Container          string `json:"container"`
}

type ctService struct {
	URL       string `json:"url"`
	Container string `json:"container,omitempty"`
}

func buildChaosTestInventory(s spec, networkName string) ctInventory {
	inv := ctInventory{
		Project:  "teranode-multinode",
		Network:  "regtest",
		Networks: ctNetworks{ChaosName: networkName, PartitionMode: "iptables"},
		Services: map[string]ctService{
			"kafka": {URL: chaosTestKafkaBroker, Container: "kafka-shared-multinode"},
		},
		RPCUser: "bitcoin",
		RPCPass: "bitcoin",
	}

	for _, nd := range s.Nodes {
		name := fmt.Sprintf("teranode%d", nd.Index)
		// All-in-one: one container answers everything. Split: RPC and health
		// live in -core (also the container chaos pause/stop targets), asset
		// and propagation in their own siblings.
		rpcHost, assetHost, propHost, p2pHost := name, name, name, name
		container := name + "-multinode"
		if !s.AllInOne {
			rpcHost, assetHost, propHost, p2pHost = name+"-core", name+"-asset", name+"-propagation", name+"-p2p"
			container = name + "-core-multinode"
		}

		inv.Nodes = append(inv.Nodes, ctNode{
			Kind:               "teranode",
			Name:               name,
			Index:              nd.Index,
			PeerID:             nd.PeerID,
			P2PAddr:            fmt.Sprintf("/dns/%s/tcp/%d/p2p/%s", p2pHost, chaosTestP2PPort, nd.PeerID),
			RPCURL:             fmt.Sprintf("http://%s:%d", rpcHost, rpcContPort),
			AssetURL:           fmt.Sprintf("http://%s:%d/api/v1", assetHost, dashboardContPort),
			PropagationURL:     fmt.Sprintf("http://%s:%d", propHost, chaosTestPropHTTP),
			HealthURL:          fmt.Sprintf("http://%s:%d/health", rpcHost, chaosTestHealthPort),
			HostRPCURL:         fmt.Sprintf("http://localhost:%d", nd.RPCPort),
			HostAssetURL:       fmt.Sprintf("http://localhost:%d/api/v1", nd.DashboardPort),
			HostBase:           nd.HostBase,
			KafkaRejectedTx:    "rejectedtx-" + name,
			KafkaInvalidBlocks: "invalid-blocks-" + name,
			Container:          container,
		})
	}

	return inv
}

// writeChaosTestInventory writes <outDir>/chaostest/inventory.json. The
// network name follows the compose template's
// teranode-network-multinode-${TEST_ID:-default}.
func writeChaosTestInventory(outDir string, s spec) error {
	testID := os.Getenv("TEST_ID")
	if testID == "" {
		testID = "default"
	}

	dir := filepath.Join(outDir, chaosTestDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	b, err := json.MarshalIndent(buildChaosTestInventory(s, "teranode-network-multinode-"+testID), "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filepath.Join(dir, "inventory.json"), append(b, '\n'), 0o644) //nolint:gosec // generated config, no secrets
}
