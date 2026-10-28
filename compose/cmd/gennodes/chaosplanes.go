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
//   - svnodeN (-svnodes, up to maxSVNodes): Bitcoin SV nodes following the
//     teranodes over the legacy (Bitcoin wire) protocol on chaosnet, each with
//     a go-alert-system sidecar on alertnet that applies alerts over RPC.
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
	// alertnet (its alert host pushes to nodes), arcade and merkle-service
	// join chaosnet (they follow the nodes' p2p and datahubs).
	orchestratorAlertIP = "192.0.0.180"
	arcadeChaosIP       = "10.200.0.40"
	merkleChaosIP       = "10.200.0.41"

	// SV nodes: the image and settings bsv-regtest runs. maxSVNodes is the
	// sidecar keys in alert_keys.json.
	maxSVNodes     = 2
	legacyPort     = 18444
	svRPCPort      = 18332
	svHostBaseRoot = 40000 // svnodeN publishes RPC on base+332, its sidecar's API on base+300
	svHostStride   = 1000
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
	SVNodes     []svNode
	genesis     []string
	hubKey      string
}

type svNode struct {
	Index       int
	ChaosIP     string
	AlertIP     string // its sidecar's
	AlertPeerID string
	alertKey    string
	HostBase    int
	// Connect is the -connect list: every teranode's legacy service and the
	// other SV nodes. SV Node downloads blocks only from peers it dialled.
	Connect []string
	// Strict turns SV Node's standard-output policy on (acceptnonstdoutputs=0),
	// as bsv-regtest does for its second SV node; svnode-dust-policy relies on
	// one node of each kind.
	Strict bool
}

func (sv svNode) Name() string        { return fmt.Sprintf("svnode%d", sv.Index) }
func (sv svNode) SidecarName() string { return fmt.Sprintf("alert-svnode%d", sv.Index) }
func (sv svNode) RPCHostPort() int    { return sv.HostBase + 332 }
func (sv svNode) APIHostPort() int    { return sv.HostBase + 300 }
func (sv svNode) AlertAddr() string {
	return fmt.Sprintf("/ip4/%s/tcp/%d/p2p/%s", sv.AlertIP, hubAlertPort, sv.AlertPeerID)
}

func chaosIP(idx int) string   { return fmt.Sprintf("10.200.0.%d", 10+idx) }
func alertIP(idx int) string   { return fmt.Sprintf("192.0.0.%d", 140+idx) }
func svChaosIP(idx int) string { return fmt.Sprintf("10.200.0.%d", 20+idx) }
func svAlertIP(idx int) string { return fmt.Sprintf("192.0.0.%d", 160+idx) }

// applyChaosPlanes switches s to the chaos-test layout. All-in-one only: the
// split topology spreads p2p, asset and RPC over separate containers, and
// chaos-test partitions one container per node.
func applyChaosPlanes(s *spec, genesis []string, svCount int) error {
	if !s.AllInOne {
		return errors.NewError("-chaostest needs the all-in-one topology (-allinone=1)")
	}
	if svCount < 0 || svCount > maxSVNodes {
		return errors.NewError("-svnodes must be in [0, %d], got %d", maxSVNodes, svCount)
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

	var svs []svNode
	for j := 1; j <= svCount; j++ {
		k, ok := byName[fmt.Sprintf("alert-svnode%d", j)]
		if !ok {
			return errors.NewError("alert key pool has no key for alert-svnode%d", j)
		}
		sv := svNode{
			Index:       j,
			ChaosIP:     svChaosIP(j),
			AlertIP:     svAlertIP(j),
			AlertPeerID: k.PeerID,
			alertKey:    k.PrivateKey,
			HostBase:    svHostBaseRoot + (j-1)*svHostStride,
			Strict:      j == 2,
		}
		for _, nd := range s.Nodes {
			sv.Connect = append(sv.Connect, fmt.Sprintf("%s:%d", nd.ChaosIP, legacyPort))
		}
		for o := 1; o <= svCount; o++ {
			if o != j {
				sv.Connect = append(sv.Connect, fmt.Sprintf("%s:%d", svChaosIP(o), legacyPort))
			}
		}
		svs = append(svs, sv)
	}

	s.Chaos = &chaosPlanes{
		ChaosSubnet:  chaosSubnet,
		AlertSubnet:  alertSubnet,
		AlertGateway: alertGateway,
		HubIP:        hubAlertIP,
		HubPeerID:    hub.PeerID,
		HubAddr:      fmt.Sprintf("/ip4/%s/tcp/%d/p2p/%s", hubAlertIP, hubAlertPort, hub.PeerID),
		GenesisKeys:  strings.Join(genesis, " | "),
		SVNodes:      svs,
		genesis:      genesis,
		hubKey:       hub.PrivateKey,
	}
	return nil
}

func (n node) AlertAddr() string {
	return fmt.Sprintf("/ip4/%s/tcp/%d/p2p/%s", n.AlertIP, nodeAlertPort, n.AlertPeerID)
}

// writeAlertConfigs writes the go-alert-system configs bsv-regtest uses:
// <outDir>/alert-system/config.json for the hub (bootstraps from teranode1's
// alert peer) and alert-svnodeN.json for each SV node's sidecar (bootstraps
// from the hub, applies alerts to its SV node over RPC). None verifies over
// RPC at start.
func writeAlertConfigs(outDir string, s spec) error {
	dir := filepath.Join(outDir, "alert-system")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	hub := alertSystemConfig(s.Chaos.genesis, s.Nodes[0].AlertAddr(), s.Chaos.hubKey,
		fmt.Sprintf("http://teranode1:%d", rpcContPort))
	if err := writeJSON(filepath.Join(dir, "config.json"), hub); err != nil {
		return err
	}
	for _, sv := range s.Chaos.SVNodes {
		cfg := alertSystemConfig(s.Chaos.genesis, s.Chaos.HubAddr, sv.alertKey,
			fmt.Sprintf("http://%s:%d", sv.Name(), svRPCPort))
		if err := writeJSON(filepath.Join(dir, sv.SidecarName()+".json"), cfg); err != nil {
			return err
		}
	}
	return nil
}

func alertSystemConfig(genesis []string, bootstrap, privateKey, rpcHost string) map[string]any {
	return map[string]any{
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
		"genesis_keys":             genesis,
		"log_level":                "debug",
		"log_output_file":          "",
		"p2p": map[string]any{
			"alert_system_protocol_id":   alertProtocolID,
			"allow_private_ip_addresses": false,
			"bootstrap_peer":             bootstrap,
			"ip":                         "0.0.0.0",
			"peer_discovery_interval":    "15s",
			"port":                       fmt.Sprint(hubAlertPort),
			"private_key":                privateKey,
			"topic_name":                 alertTopic,
		},
		"request_logging": true,
		"rpc_connections": []map[string]string{
			{"host": rpcHost, "user": "bitcoin", "password": "bitcoin"},
		},
		"web_server": map[string]any{
			"idle_timeout": "60s", "port": fmt.Sprint(hubAPIPort), "read_timeout": "15s", "write_timeout": "15s",
		},
	}
}

// writeJSON writes a generated config. Alert configs carry throwaway regtest
// identities, like settings_multinode.conf carries the nodes' p2p keys.
func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644) //nolint:gosec // generated regtest config
}

// writeSVNodeConfigs writes <outDir>/svnode/svnodeN.conf, bsv-regtest's
// bitcoin.conf for a regtest SV node following the teranodes.
func writeSVNodeConfigs(outDir string, s spec) error {
	if len(s.Chaos.SVNodes) == 0 {
		return nil
	}
	dir := filepath.Join(outDir, "svnode")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for _, sv := range s.Chaos.SVNodes {
		if err := renderToFile("templates/svnode.conf.tmpl", filepath.Join(dir, sv.Name()+".conf"), sv); err != nil {
			return err
		}
	}
	return nil
}
