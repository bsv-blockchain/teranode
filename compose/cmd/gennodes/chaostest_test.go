package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChaosTestInventory_AllInOne(t *testing.T) {
	keys, err := loadKeys(3)
	require.NoError(t, err)
	s := buildSpec(keys)

	inv := buildChaosTestInventory(s, "teranode-network-multinode-default")

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
