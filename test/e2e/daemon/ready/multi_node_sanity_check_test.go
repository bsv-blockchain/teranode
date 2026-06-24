package smoke

import (
	"testing"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/stretchr/testify/assert"
)

func TestMultiNodeSendSanity(t *testing.T) {
	createNode := func(t *testing.T, nodeNumber int) *daemon.TestDaemon {
		return daemon.NewTestDaemon(t, daemon.TestOptions{
			EnableP2P:         true,
			UTXOStoreType:     "aerospike",
			SkipRemoveDataDir: nodeNumber > 1,
			// EnableDebugLogging: true,
			PreserveDataDir: true,
			SettingsOverrideFunc: func(s *settings.Settings) {
				test.MultiNodeSettings(nodeNumber)(s)
				s.BlockAssembly.InitialMerkleItemsPerSubtree = 1024
				s.BlockAssembly.MaximumMerkleItemsPerSubtree = 32768
			},
			FSMState: blockchain.FSMStateRUNNING,
		})
	}

	node1 := createNode(t, 1)
	defer node1.Stop(t)

	node2 := createNode(t, 2)
	defer node2.Stop(t)

	assert.NotEqual(t, node1.Settings.ClientName, node2.Settings.ClientName)
	assert.NotEqual(t, node1.Settings.BlockChain.GRPCAddress, node2.Settings.BlockChain.GRPCAddress)
	assert.NotEqual(t, node1.Settings.BlockChain.StoreURL, node2.Settings.BlockChain.StoreURL)
	assert.NotEqual(t, node1.Settings.SubtreeValidation.SubtreeStore, node2.Settings.SubtreeValidation.SubtreeStore)
}
