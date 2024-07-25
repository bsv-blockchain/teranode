package tna

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	tf "github.com/bitcoin-sv/ubsv/test/test_framework"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

var (
	framework *tf.BitcoinTestFramework
)

func newTx(lockTime uint32) *bt.Tx {
	tx := bt.NewTx()
	tx.LockTime = lockTime
	return tx
}

func TestMain(m *testing.M) {
	setupBitcoinTestFramework()
	//defer tearDownBitcoinTestFramework()

	m.Run()

	// os.Exit(exitCode)
}

func setupBitcoinTestFramework() {
	framework = tf.NewBitcoinTestFramework([]string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.override.yml"})
	m := map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.tna1Test",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv2.tna1Test",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv3.tna1Test",
	}
	if err := framework.SetupNodes(m); err != nil {
		fmt.Printf("Error setting up nodes: %v\n", err)
		os.Exit(1)
	}
}

func tearDownBitcoinTestFramework() {
	if err := framework.StopNodes(); err != nil {
		fmt.Printf("Error stopping nodes: %v\n", err)
	}
	_ = os.RemoveAll("../../data")
}

func TestBroadcastNewTxAllNodes(t *testing.T) {
	// Test setup
	ctx := context.Background()
	blockchainClientNode0 := framework.Nodes[0].BlockchainClient
	baClient0 := framework.Nodes[0].BlockassemblyClient
	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("test", ulogger.WithLevel(logLevelStr))
	var hashes []*chainhash.Hash

	blockchainSubscription, err := blockchainClientNode0.Subscribe(ctx, "test-broadcast-pow")
	if err != nil {
		t.Errorf("error subscribing to blockchain service: %v", err)
		return
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case notification := <-blockchainSubscription:
				if notification.Type == model.NotificationType_Subtree {
					hashes = append(hashes, notification.Hash)
					fmt.Println("Length of hashes:", len(hashes))

				} else {
					fmt.Println("other notifications than subtrees")
					fmt.Println(notification.Type)
				}
			}
		}
	}()

	hashesTx, err := helper.CreateAndSendRawTxs(ctx, framework.Nodes[0], 20)
	if err != nil {
		t.Errorf("Failed to create and send raw txs: %v", err)
	}
	fmt.Printf("Hashes in created block: %v\n", hashesTx)

	_, errMining := helper.MineBlock(ctx, baClient0, logger)
	if errMining != nil {
		t.Errorf("Failed to create block  %v", errMining)
	}

	// _, errMiningCandidate := helper.GetMiningCandidate(ctx, baClient0, logger)
	// if errMiningCandidate != nil {
	// 	t.Errorf("Failed to create and send raw txs: %v", errMiningCandidate)
	// }

	time.Sleep(10 * time.Second)

	if len(hashes) > 0 {
		fmt.Println("First element of hashes:", hashes[0])
	} else {
		fmt.Println("hashes is empty!")
	}

	fmt.Println("subtree notification received")

	subtreeHash := hashes[0].String()

	// subtree1, err1 := framework.Nodes[1].SubtreeStore.Get(ctx, subtreeHash[:])
	// if err1 != nil {
	// 	t.Errorf("Error getting subtree: %v %v", err1, subtree1)
	// }

	// subtree2, err2 := framework.Nodes[2].SubtreeStore.Get(ctx, subtreeHash[:])
	// if err2 != nil {
	// 	t.Errorf("Error getting subtree: %v %v", err2, subtree2)
	// }

	baseDir := "../../data"

	// Search inside ubsv1, ubsv2 and ubsv3 subfolders
	for i := 1; i <= 3; i++ {

		subDir := fmt.Sprintf("ubsv%d/subtreestore", i)
		fmt.Println(subDir)
		filePath := filepath.Join(baseDir, subDir, subtreeHash)
		fmt.Println(filePath)
		if _, err := os.Stat(filePath); err == nil {
			fmt.Printf("Subtree %s exists.\n", filePath)
		} else if os.IsNotExist(err) {
			fmt.Printf("Subtree %s doesn't exists %s.\n", subtreeHash, subDir)
		} else {
			fmt.Printf("Error checking the file %s in %s: %v\n", subtreeHash, subDir, err)
		}
	}
}
