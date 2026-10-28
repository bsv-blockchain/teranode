package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
)

// -chaostest lays the stack out the way chaos-test's bsv-regtest fleet is, so
// its bundled scenarios run here unchanged:
//
//   - chaosnet: node-to-node p2p and datahub traffic on pinned IPs. chaos-test
//     partitions a node by disconnecting it from this network and reconnecting
//     it with the same IP; the stores and Kafka stay on teranode-network, so a
//     partitioned node keeps running.
//   - alertnet: the alert-system p2p network (teranode 9908, hub 9906). The
//     subnet is the one bsv-regtest uses: libp2p classifies it as public, so
//     the alert library's private-IP gater lets it through.
//   - alert-system: the hub, bootstrap peer and canonical alert store.
//
// Alert identities come from alert_keys.json, a committed pool of throwaway
// Ed25519 keys like peer_keys.json. The genesis public keys are passed in
// (multinode.sh reads them from chaos-test's keys.json), because the
// orchestrator signs alerts with the matching private keys.

//go:embed alert_keys.json
var alertKeysJSON []byte

const (
	chaosSubnet     = "10.200.0.0/24"
	alertSubnet     = "192.0.0.128/26"
	alertGateway    = "192.0.0.129"
	hubAlertIP      = "192.0.0.130"
	hubAlertPort    = 9906
	nodeAlertPort   = 9908
	hubAPIPort      = 3000
	alertTopic      = "bitcoin_alert_system_regtest"
	alertProtocolID = "/bitcoin/alert-system/1.0.0"

	// Addresses the multinode.sh add-ons attach with: the orchestrator joins
	// alertnet (its alert host pushes to nodes), arcade joins chaosnet (it
	// follows the nodes' p2p and datahubs).
	orchestratorAlertIP = "192.0.0.180"
	arcadeChaosIP       = "10.200.0.40"
)

type alertKey struct {
	Name       string `json:"name"`
	PeerID     string `json:"peer_id"`
	PrivateKey string `json:"private_key_hex"`
}

type chaosPlanes struct {
	ChaosSubnet  string
	AlertSubnet  string
	AlertGateway string
	HubIP        string
	HubPeerID    string
	HubAddr      string
	// GenesisKeys is the pipe-separated list teranode's alert_genesis_keys takes.
	GenesisKeys string
	genesis     []string
	hubKey      string
}

func chaosIP(idx int) string { return fmt.Sprintf("10.200.0.%d", 10+idx) }
func alertIP(idx int) string { return fmt.Sprintf("192.0.0.%d", 140+idx) }

// applyChaosPlanes switches s to the chaos-test layout. All-in-one only: the
// split topology spreads p2p, asset and RPC over separate containers, and
// chaos-test partitions one container per node.
func applyChaosPlanes(s *spec, genesis []string) error {
	if !s.AllInOne {
		return errors.NewError("-chaostest needs the all-in-one topology (-allinone=1)")
	}
	if len(genesis) < 3 {
		return errors.NewError("-chaostest needs at least 3 alert genesis public keys, got %d", len(genesis))
	}

	var pool []alertKey
	if err := json.Unmarshal(alertKeysJSON, &pool); err != nil {
		return err
	}
	byName := make(map[string]alertKey, len(pool))
	for _, k := range pool {
		byName[k.Name] = k
	}
	hub, ok := byName["hub"]
	if !ok {
		return errors.NewError("alert key pool has no hub key")
	}

	for i := range s.Nodes {
		nd := &s.Nodes[i]
		k, ok := byName[fmt.Sprintf("teranode%d", nd.Index)]
		if !ok {
			return errors.NewError("alert key pool has no key for teranode%d", nd.Index)
		}
		nd.ChaosIP = chaosIP(nd.Index)
		nd.AlertIP = alertIP(nd.Index)
		nd.AlertPeerID = k.PeerID
		nd.AlertPrivateKey = k.PrivateKey
	}
	for i := range s.Nodes {
		var peers []string
		for _, other := range s.Nodes {
			if other.Index != s.Nodes[i].Index {
				peers = append(peers, fmt.Sprintf("/ip4/%s/tcp/${P2P_PORT}/p2p/%s", other.ChaosIP, other.PeerID))
			}
		}
		s.Nodes[i].StaticPeersChaos = strings.Join(peers, " | ")
	}

	s.Chaos = &chaosPlanes{
		ChaosSubnet:  chaosSubnet,
		AlertSubnet:  alertSubnet,
		AlertGateway: alertGateway,
		HubIP:        hubAlertIP,
		HubPeerID:    hub.PeerID,
		HubAddr:      fmt.Sprintf("/ip4/%s/tcp/%d/p2p/%s", hubAlertIP, hubAlertPort, hub.PeerID),
		GenesisKeys:  strings.Join(genesis, " | "),
		genesis:      genesis,
		hubKey:       hub.PrivateKey,
	}
	return nil
}

func (n node) AlertAddr() string {
	return fmt.Sprintf("/ip4/%s/tcp/%d/p2p/%s", n.AlertIP, nodeAlertPort, n.AlertPeerID)
}

// writeAlertHubConfig writes <outDir>/alert-system/config.json, the
// go-alert-system config bsv-regtest gives its hub: bootstraps from
// teranode1's alert peer and verifies nothing over RPC.
func writeAlertHubConfig(outDir string, s spec) error {
	dir := filepath.Join(outDir, "alert-system")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	cfg := map[string]any{
		"alert_processing_interval": "5m",
		"alert_webhook_url":         "",
		"bitcoin_config_path":       "",
		"datastore": map[string]any{
			"auto_migrate": true,
			"debug":        false,
			"engine":       "sqlite",
			"password":     "",
			"sqlite":       map[string]any{"database_path": "/data/alert.db", "shared": false},
			"table_prefix": "alert_system",
		},
		"disable_rpc_verification": true,
		"environment":              "local",
		"genesis_keys":             s.Chaos.genesis,
		"log_level":                "debug",
		"log_output_file":          "",
		"p2p": map[string]any{
			"alert_system_protocol_id":   alertProtocolID,
			"allow_private_ip_addresses": false,
			"bootstrap_peer":             s.Nodes[0].AlertAddr(),
			"ip":                         "0.0.0.0",
			"peer_discovery_interval":    "15s",
			"port":                       fmt.Sprint(hubAlertPort),
			"private_key":                s.Chaos.hubKey,
			"topic_name":                 alertTopic,
		},
		"request_logging": true,
		"rpc_connections": []map[string]string{
			{"host": fmt.Sprintf("http://teranode1:%d", rpcContPort), "user": "bitcoin", "password": "bitcoin"},
		},
		"web_server": map[string]any{
			"idle_timeout": "60s", "port": fmt.Sprint(hubAPIPort), "read_timeout": "15s", "write_timeout": "15s",
		},
	}
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	// Carries the hub's throwaway regtest identity, like settings_multinode.conf
	// carries the nodes' p2p keys.
	return os.WriteFile(filepath.Join(dir, "config.json"), append(b, '\n'), 0o644) //nolint:gosec // generated regtest config
}
