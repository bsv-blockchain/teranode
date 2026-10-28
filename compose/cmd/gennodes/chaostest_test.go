package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChaosTestInventory_AllInOne(t *testing.T) {
	keys, err := loadKeys(3)
	require.NoError(t, err)
	s := buildSpec(keys)

	inv := buildChaosTestInventory(s, "default")

	require.Equal(t, "regtest", inv.Network)
	require.Equal(t, "teranode-network-multinode-default", inv.Networks.ChaosName)
	require.Equal(t, "iptables", inv.Networks.PartitionMode)
	require.Empty(t, inv.MinerWIF, "private keys are filled at sim-up time, never generated")
	require.Len(t, inv.Nodes, 3)

	n2 := inv.Nodes[1]
	require.Equal(t, "teranode2", n2.Name)
	require.Equal(t, "teranode2-multinode", n2.Container)
	require.Equal(t, "http://teranode2:9292", n2.RPCURL)
	require.Equal(t, "http://teranode2:8090/api/v1", n2.AssetURL)
	require.Equal(t, "http://teranode2:8000/health", n2.HealthURL)
	// Host ports must match what the compose template publishes.
	require.Equal(t, "http://localhost:23292", n2.HostRPCURL)
	require.Equal(t, "http://localhost:22090/api/v1", n2.HostAssetURL)
	require.Equal(t, "rejectedtx-teranode2", n2.KafkaRejectedTx)
	require.Equal(t, "invalid-blocks-teranode2", n2.KafkaInvalidBlocks)
	require.Equal(t, keys[1].PeerID, n2.PeerID)
}

func TestChaosTestInventory_Split(t *testing.T) {
	keys, err := loadKeys(3)
	require.NoError(t, err)
	s := buildSpec(keys)
	s.AllInOne = false

	n1 := buildChaosTestInventory(s, "net").Nodes[0]
	require.Equal(t, "teranode1-core-multinode", n1.Container)
	require.Equal(t, "http://teranode1-core:9292", n1.RPCURL)
	require.Equal(t, "http://teranode1-asset:8090/api/v1", n1.AssetURL)
	require.Equal(t, "http://teranode1-propagation:8833", n1.PropagationURL)
	require.Equal(t, "http://teranode1-core:8000/health", n1.HealthURL)
	require.Equal(t, "/dns/teranode1-p2p/tcp/9905/p2p/"+n1.PeerID, n1.P2PAddr)
}

func TestWriteAll_WritesChaosTestInventory(t *testing.T) {
	t.Setenv("TEST_ID", "ci42")
	keys, err := loadKeys(3)
	require.NoError(t, err)

	dir := t.TempDir()
	require.NoError(t, writeAll(dir, buildSpec(keys)))

	b, err := os.ReadFile(filepath.Join(dir, "chaostest", "inventory.json"))
	require.NoError(t, err)

	var inv ctInventory
	require.NoError(t, json.Unmarshal(b, &inv))
	require.Equal(t, "teranode-network-multinode-ci42", inv.Networks.ChaosName)
	require.Len(t, inv.Nodes, 3)
}

var testGenesis = []string{"02aa", "02bb", "02cc"}

func TestChaosPlanes_Inventory(t *testing.T) {
	keys, err := loadKeys(3)
	require.NoError(t, err)
	s := buildSpec(keys)
	require.NoError(t, applyChaosPlanes(&s, testGenesis))

	inv := buildChaosTestInventory(s, "default")
	require.Equal(t, "teranode-chaosnet-multinode-default", inv.Networks.ChaosName)
	require.Equal(t, "teranode-alertnet-multinode-default", inv.Networks.AlertName)
	require.Empty(t, inv.Networks.PartitionMode, "chaosnet carries only p2p, so chaos-test's own disconnect partitions apply")
	require.NotNil(t, inv.Hub)
	require.NotNil(t, inv.Alert)
	require.Equal(t, testGenesis, inv.Alert.GenesisPubKeys)
	require.Equal(t, inv.Hub.AlertAddr, inv.Alert.Bootstrap)

	n2 := inv.Nodes[1]
	require.Equal(t, "10.200.0.12", n2.ChaosIP)
	require.Equal(t, "192.0.0.142", n2.AlertIP)
	require.Equal(t, "/ip4/10.200.0.12/tcp/9905/p2p/"+keys[1].PeerID, n2.P2PAddr)
	require.Equal(t, "/ip4/192.0.0.142/tcp/9908/p2p/"+n2.AlertPeerID, n2.AlertAddr)
	// Control plane stays on teranode-network.
	require.Equal(t, "http://teranode2:9292", n2.RPCURL)
}

func TestChaosPlanes_Rejected(t *testing.T) {
	keys, err := loadKeys(3)
	require.NoError(t, err)

	s := buildSpec(keys)
	s.AllInOne = false
	require.Error(t, applyChaosPlanes(&s, testGenesis), "split topology")

	s = buildSpec(keys)
	require.Error(t, applyChaosPlanes(&s, testGenesis[:2]), "too few genesis keys")
}

func TestChaosPlanes_WriteAll(t *testing.T) {
	keys, err := loadKeys(3)
	require.NoError(t, err)
	s := buildSpec(keys)
	require.NoError(t, applyChaosPlanes(&s, testGenesis))

	dir := t.TempDir()
	require.NoError(t, writeAll(dir, s))

	compose, err := os.ReadFile(filepath.Join(dir, "docker-compose-multinode.yml"))
	require.NoError(t, err)
	require.Contains(t, string(compose), "teranode-chaosnet-multinode-${TEST_ID:-default}")
	require.Contains(t, string(compose), "alert-system-multinode")
	require.Contains(t, string(compose), "ipv4_address: 10.200.0.13")
	// Partitions leak through the docker host unless both planes are internal.
	require.Equal(t, 2, strings.Count(string(compose), "internal: true"))

	settings, err := os.ReadFile(filepath.Join(dir, "settings_multinode.conf"))
	require.NoError(t, err)
	require.Contains(t, string(settings), "startAlert.docker.teranode1                         = true")
	require.Contains(t, string(settings), "asset_httpAddress.docker.teranode1                  = http://10.200.0.11:")
	require.Contains(t, string(settings), "/ip4/10.200.0.12/tcp/${P2P_PORT}/p2p/"+keys[1].PeerID)
	require.NotContains(t, string(settings), "/dns/teranode2/tcp/")
	// A partition must not cut a node's own services apart.
	require.Contains(t, string(settings), "blockchain_grpcAddress.docker.teranode1             = localhost:${BLOCKCHAIN_GRPC_PORT}")
	require.Contains(t, string(settings), "propagation_grpcAddresses.docker.teranode2          = localhost:${PROPAGATION_GRPC_PORT}")

	hub, err := os.ReadFile(filepath.Join(dir, "alert-system", "config.json"))
	require.NoError(t, err)
	var cfg struct {
		GenesisKeys []string `json:"genesis_keys"`
		P2P         struct {
			BootstrapPeer string `json:"bootstrap_peer"`
		} `json:"p2p"`
	}
	require.NoError(t, json.Unmarshal(hub, &cfg))
	require.Equal(t, testGenesis, cfg.GenesisKeys)
	require.Equal(t, s.Nodes[0].AlertAddr(), cfg.P2P.BootstrapPeer)
}

func TestChaosPlanes_DefaultUnchanged(t *testing.T) {
	keys, err := loadKeys(3)
	require.NoError(t, err)

	dir := t.TempDir()
	require.NoError(t, writeAll(dir, buildSpec(keys)))

	compose, err := os.ReadFile(filepath.Join(dir, "docker-compose-multinode.yml"))
	require.NoError(t, err)
	require.NotContains(t, string(compose), "chaosnet")
	require.NotContains(t, string(compose), "alert-system")
	_, err = os.Stat(filepath.Join(dir, "alert-system"))
	require.True(t, os.IsNotExist(err))
}

func TestChaosPlanes_HubWaitsForTeranode1(t *testing.T) {
	keys, err := loadKeys(3)
	require.NoError(t, err)
	s := buildSpec(keys)
	require.NoError(t, applyChaosPlanes(&s, testGenesis))

	dir := t.TempDir()
	require.NoError(t, writeAll(dir, s))
	compose, err := os.ReadFile(filepath.Join(dir, "docker-compose-multinode.yml"))
	require.NoError(t, err)
	require.Contains(t, string(compose), "until nc -z 192.0.0.141 9908")
}
