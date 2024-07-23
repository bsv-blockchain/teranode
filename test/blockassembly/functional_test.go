////go:build e2eTest

// How to run this test:
// $ `make smoketests test=blockassembly.TestNode
// Other variations are:
// $ `make smoketests test=blockassembly.TestNode no-build=1
package test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockassembly/blockassembly_api"
	tf "github.com/bitcoin-sv/ubsv/test/test_framework"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
)

var (
	framework   *tf.BitcoinTestFramework
	settingsMap map[string]string
)

func TestMain(m *testing.M) {
	setupBitcoinTestFramework()
	defer tearDownBitcoinTestFramework()

	m.Run()

	// os.Exit(0)
}

func setupBitcoinTestFramework() {
	framework = tf.NewBitcoinTestFramework([]string{"../../docker-compose.yml", "../../docker-compose.e2etest.override.yml"})
	settingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.blockassemblyTest",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv2.blockassemblyTest",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv3.blockassemblyTest",
	}
	if err := framework.SetupNodes(settingsMap); err != nil {
		fmt.Printf("Error setting up nodes: %v\n", err)
		os.Exit(1)
	}
}

func tearDownBitcoinTestFramework() {
	if err := framework.StopNodesWithRmVolume(); err != nil {
		fmt.Printf("Error stopping nodes: %v\n", err)
	}
	// _ = os.RemoveAll("../../data")
}

func newTx(lockTime uint32) *bt.Tx {
	tx := bt.NewTx()
	tx.LockTime = lockTime
	return tx
}

func TestNode(t *testing.T) {
	blockassemblyNode0 := framework.Nodes[0].BlockassemblyClient
	blockchainClientNode0 := framework.Nodes[0].BlockchainClient
	var hashes []*chainhash.Hash

	blockchainSubscription, err := blockchainClientNode0.Subscribe(framework.Context, "test")
	if err != nil {
		t.Errorf("error subscribing to blockchain service: %v", err)
		return
	}

	go func() {
		for {
			select {
			case <-framework.Context.Done():
				return
			case notification := <-blockchainSubscription:
				if notification.Type == model.NotificationType_Subtree {
					hashes = append(hashes, notification.Hash)
				}
			}
		}
	}()

	utxoStoreNode0 := framework.Nodes[0].UtxoStore

	for i := uint64(0); i < 100; i++ {
		newTX := newTx(uint32(i))
		newTx := &blockassembly_api.AddTxRequest{
			Txid:     newTX.TxIDChainHash()[:],
			Fee:      i,
			Size:     i,
			Locktime: newTX.LockTime,
			Utxos:    nil,
		}
		_, err := blockassemblyNode0.BlockAssemblyAPIClient().AddTx(framework.Context, newTx)
		if err != nil {
			t.Errorf("Error adding tx: %v", err)
		}
		_, _ = utxoStoreNode0.Create(framework.Context, newTX, 0)
	}

	m, err := blockassemblyNode0.GetMiningCandidate(framework.Context)
	if err != nil {
		t.Errorf("Error getting mining candidate: %v", err)
	}
	assert.Equal(t, uint(0x3), uint(m.SubtreeCount))
	time.Sleep(10 * time.Second)
	assert.Equal(t, uint(3), uint(len(hashes)))
	// _, err = helper.MineBlockWithCandidate(framework.Context, blockassemblyNode0, m, logger)
	// assert.Nil(t, err, "Error mining block")
}
