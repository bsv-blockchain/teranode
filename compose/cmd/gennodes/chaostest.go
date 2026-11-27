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
	Hub      *ctHub               `json:"hub,omitempty"`
	Alert    *ctAlert             `json:"alert,omitempty"`
	RPCUser  string               `json:"rpcUser"`
	RPCPass  string               `json:"rpcPass"`
	MinerWIF string               `json:"minerWIF"`
}

// ctNetworks names the compose networks for chaos-test's planes. Without
// -chaostest the stack has a single network, so partitions must use iptables
// rather than a network disconnect, which would also cut the node off from
// its stores. With it, chaosnet carries only p2p and the datahubs, and
// chaos-test's own network-disconnect partitions work as on its fleet.
type ctNetworks struct {
	Chaos         string `json:"chaosnet,omitempty"`
	Alert         string `json:"alertnet,omitempty"`
	ChaosName     string `json:"chaosnetName"`
	AlertName     string `json:"alertnetName,omitempty"`
	PartitionMode string `json:"partitionMode,omitempty"`

	// Not read by chaos-test: the addresses multinode.sh attaches the
	// orchestrator (alertnet), arcade and merkle-service (chaosnet) with.
	OrchestratorAlertIP string `json:"orchestratorAlertIP,omitempty"`
	ArcadeChaosIP       string `json:"arcadeChaosIP,omitempty"`
	MerkleChaosIP       string `json:"merkleChaosIP,omitempty"`
}

type ctHub struct {
	AlertIP   string `json:"alertIP"`
	PeerID    string `json:"peerID"`
	AlertAddr string `json:"alertAddr"`
	APIURL    string `json:"apiURL"`
	HostAPI   string `json:"hostAPIURL"`
	Container string `json:"container"`
}

type ctAlert struct {
	Topic          string   `json:"topic"`
	ProtocolID     string   `json:"protocolID"`
	GenesisPubKeys []string `json:"genesisPubKeys"`
	Bootstrap      string   `json:"bootstrap"`
}

type ctNode struct {
	Kind               string `json:"kind"`
	Name               string `json:"name"`
	Index              int    `json:"index"`
	ChaosIP            string `json:"chaosIP,omitempty"`
	AlertIP            string `json:"alertIP,omitempty"`
	PeerID             string `json:"peerID"`
	AlertPeerID        string `json:"alertPeerID,omitempty"`
	AlertAddr          string `json:"alertAddr,omitempty"`
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

	LegacyAddr  string     `json:"legacyAddr,omitempty"`  // teranode: chaosIP:18444 when SV nodes follow it
	LegacyPeers []string   `json:"legacyPeers,omitempty"` // svnode: its -connect targets
	Sidecar     *ctSidecar `json:"sidecar,omitempty"`     // svnode: its alert sidecar
}

type ctSidecar struct {
	Name      string `json:"name"`
	Container string `json:"container"`
	APIURL    string `json:"apiURL"`
	HostAPI   string `json:"hostAPIURL"`
}

type ctService struct {
	URL       string `json:"url"`
	Container string `json:"container,omitempty"`
}

func buildChaosTestInventory(s spec, testID string) ctInventory {
	networkName := "teranode-network-multinode-" + testID
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

	if c := s.Chaos; c != nil {
		inv.Networks = ctNetworks{
			Chaos:               c.ChaosSubnet,
			Alert:               c.AlertSubnet,
			ChaosName:           "teranode-chaosnet-multinode-" + testID,
			AlertName:           "teranode-alertnet-multinode-" + testID,
			OrchestratorAlertIP: orchestratorAlertIP,
			ArcadeChaosIP:       arcadeChaosIP,
			MerkleChaosIP:       merkleChaosIP,
		}
		inv.Hub = &ctHub{
			AlertIP:   c.HubIP,
			PeerID:    c.HubPeerID,
			AlertAddr: c.HubAddr,
			APIURL:    fmt.Sprintf("http://alert-system:%d", hubAPIPort),
			HostAPI:   fmt.Sprintf("http://localhost:%d", hubAPIPort),
			Container: "alert-system-multinode",
		}
		inv.Alert = &ctAlert{Topic: alertTopic, ProtocolID: alertProtocolID, GenesisPubKeys: c.genesis, Bootstrap: c.HubAddr}
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

		p2pAddr := fmt.Sprintf("/dns/%s/tcp/%d/p2p/%s", p2pHost, chaosTestP2PPort, nd.PeerID)
		var alertAddr, legacyAddr string
		if s.Chaos != nil {
			p2pAddr = fmt.Sprintf("/ip4/%s/tcp/%d/p2p/%s", nd.ChaosIP, chaosTestP2PPort, nd.PeerID)
			alertAddr = nd.AlertAddr()
			if len(s.Chaos.SVNodes) > 0 {
				legacyAddr = fmt.Sprintf("%s:%d", nd.ChaosIP, legacyPort)
			}
		}

		inv.Nodes = append(inv.Nodes, ctNode{
			Kind:               "teranode",
			Name:               name,
			Index:              nd.Index,
			ChaosIP:            nd.ChaosIP,
			AlertIP:            nd.AlertIP,
			PeerID:             nd.PeerID,
			AlertPeerID:        nd.AlertPeerID,
			AlertAddr:          alertAddr,
			P2PAddr:            p2pAddr,
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
			LegacyAddr:         legacyAddr,
		})
	}

	if s.Chaos != nil {
		for _, sv := range s.Chaos.SVNodes {
			inv.Nodes = append(inv.Nodes, ctNode{
				Kind:        "svnode",
				Name:        sv.Name(),
				Index:       sv.Index,
				ChaosIP:     sv.ChaosIP,
				AlertIP:     sv.AlertIP,
				AlertPeerID: sv.AlertPeerID,
				AlertAddr:   sv.AlertAddr(),
				RPCURL:      fmt.Sprintf("http://%s:%d", sv.Name(), svRPCPort),
				HostRPCURL:  fmt.Sprintf("http://localhost:%d", sv.RPCHostPort()),
				HostBase:    sv.HostBase,
				Container:   sv.Name() + "-multinode",
				LegacyPeers: sv.Connect,
				Sidecar: &ctSidecar{
					Name:      sv.SidecarName(),
					Container: sv.SidecarName() + "-multinode",
					APIURL:    fmt.Sprintf("http://%s:%d", sv.SidecarName(), hubAPIPort),
					HostAPI:   fmt.Sprintf("http://localhost:%d", sv.APIHostPort()),
				},
			})
		}
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

	b, err := json.MarshalIndent(buildChaosTestInventory(s, testID), "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filepath.Join(dir, "inventory.json"), append(b, '\n'), 0o644) //nolint:gosec // generated config, no secrets
}
