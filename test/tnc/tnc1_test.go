package tnc

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/bitcoin-sv/ubsv/model"
	tf "github.com/bitcoin-sv/ubsv/test/test_framework"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

var (
	framework *tf.BitcoinTestFramework
)

//func newTx(lockTime uint32) *bt.Tx {
//	tx := bt.NewTx()
//	tx.LockTime = lockTime
//	return tx
//}

func TestMain(m *testing.M) {
	setupBitcoinTestFramework()
	//defer tearDownBitcoinTestFramework()

	m.Run()

	// os.Exit(exitCode)
}

func setupBitcoinTestFramework() {
	framework = tf.NewBitcoinTestFramework([]string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.override.yml"})
	m := map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.tnc1Test",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv2.tnc1Test",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv3.tnc1Test",
	}
	if err := framework.SetupNodes(m); err != nil {
		fmt.Printf("Error setting up nodes: %v\n", err)
		os.Exit(1)
	}
}

//func tearDownBitcoinTestFramework() {
//	if err := framework.StopNodes(); err != nil {
//		fmt.Printf("Error stopping nodes: %v\n", err)
//	}
//	_ = os.RemoveAll("../../data")
//}

// func TestCalcMerkleRootAllTxs(t *testing.T) {
// 	ctx := context.Background()
// 	ba0 := framework.Nodes[0].BlockassemblyClient
// 	mc0, _ := ba0.GetMiningCandidate(ctx)
// 	mc0.
// 		ba1 := framework.Nodes[1].BlockassemblyClient
// 	mc1, _ := ba1.GetMiningCandidate(ctx)
// 	mp1 := mc1.MerkleProof
// 	ba2 := framework.Nodes[2].BlockassemblyClient
// 	mc2, _ := ba2.GetMiningCandidate(ctx)
// 	mp2 := mc2.MerkleProof

// }

func TestCandidateContainsAllTxs(t *testing.T) {
	ctx := context.Background()
	node0 := framework.Nodes[0]
	blockchainClientNode0 := node0.BlockchainClient
	var hashes []*chainhash.Hash

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("test", ulogger.WithLevel(logLevelStr))

	blockchainSubscription, err := blockchainClientNode0.Subscribe(ctx, "test-tnc1")
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

	_, errTXs := helper.SendTXsWithDistributor(ctx, framework.Nodes[0], logger)
	if errTXs != nil {
		t.Errorf("Failed to send txs with distributor: %v", errTXs)
	}

	if len(hashes) > 0 {
		fmt.Println("First element of hashes:", hashes[0])
	} else {
		t.Errorf("No subtrees detected: cannot calculate Merkleproofs")
	}

	fmt.Println("num of subtrees:", len(hashes))

	mc0, err0 := helper.GetMiningCandidate(ctx, framework.Nodes[0].BlockassemblyClient, logger)
	mc1, err1 := helper.GetMiningCandidate(ctx, framework.Nodes[1].BlockassemblyClient, logger)
	mc2, err2 := helper.GetMiningCandidate(ctx, framework.Nodes[2].BlockassemblyClient, logger)
	mp0 := utils.ReverseAndHexEncodeSlice(mc0.GetMerkleProof()[0])
	mp1 := utils.ReverseAndHexEncodeSlice(mc1.GetMerkleProof()[0])
	mp2 := utils.ReverseAndHexEncodeSlice(mc2.GetMerkleProof()[0])

	fmt.Println("Merkleproofs:")
	fmt.Println(mp0)
	fmt.Println(mp1)
	fmt.Println(mp2)

	if mp0 != mp1 || mp1 != mp2 {
		t.Errorf("Merkle proofs are different")
	}

	// Calculate MerkleProof for other TXs sent
	if err0 != nil {
		t.Errorf("Failed to get mining candidate 0: %v", err0)
	}
	if err1 != nil {
		t.Errorf("Failed to get mining candidate 1: %v", err1)
	}
	if err2 != nil {
		t.Errorf("Failed to get mining candidate 2: %v", err2)
	}

}

func TestCheckHashPrevBlockCandidate(t *testing.T) {
	ctx := context.Background()
	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("test", ulogger.WithLevel(logLevelStr))
	ba := framework.Nodes[0].BlockassemblyClient
	bc := framework.Nodes[0].BlockchainClient

	_, errTXs := helper.SendTXsWithDistributor(ctx, framework.Nodes[0], logger)
	if errTXs != nil {
		t.Errorf("Failed to send txs with distributor: %v", errTXs)
	}

	block0bytes, errMine0 := helper.MineBlock(ctx, framework.Nodes[0].BlockassemblyClient, logger)
	if errMine0 != nil {
		t.Errorf("Failed to mine block: %v", errMine0)
	}

	mc0, errmc0 := ba.GetMiningCandidate(ctx)
	if errmc0 != nil {
		t.Errorf("error getting mining candidate: %v", errmc0)
	}

	bestBlockheader, _, errBbh := bc.GetBestBlockHeader(ctx)
	if errBbh != nil {
		t.Errorf("error getting best block header: %v", errBbh)
	}

	block0, err0 := framework.Nodes[0].BlockchainClient.GetBlock(ctx, (*chainhash.Hash)(block0bytes))
	if err0 != nil {
		t.Errorf("Failed to get block: %v", err0)
	}

	prevHash, errHash := chainhash.NewHash(mc0.PreviousHash)

	if errHash != nil {
		t.Errorf("error getting previous hash: %v", errHash)
	}

	if bestBlockheader.String() != block0.Header.String() || bestBlockheader.String() != prevHash.String() {
		t.Errorf("Teranode working on incorrect prevHash")
	}
}
