//go:build tnatests

package tna

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/test/setup"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/libsv/go-bt/v2/chainhash"
)

type TNA1TestSuite struct {
	setup.BitcoinTestSuite
}

func (suite *TNA1TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.tna1Test",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv2.tna1Test",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv3.tna1Test",
	}
}

func (suite *TNA1TestSuite) SetupTest() {
	suite.InitSuite()
	suite.BitcoinTestSuite.SetupTestWithCustomSettings(suite.SettingsMap)
}

func (suite *TNA1TestSuite) TestBroadcastNewTxAllNodes() {
	// Test setup
	ctx := context.Background()
	t := suite.T()
	framework := suite.Framework
	blockchainClientNode0 := framework.Nodes[0].BlockchainClient
	var hashes []*chainhash.Hash
	var found int

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
					hash, err := chainhash.NewHash(notification.Hash)
					require.NoError(t, err)
					hashes = append(hashes, hash)
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

	time.Sleep(2 * time.Second)

	if len(hashes) > 0 {
		fmt.Println("First element of hashes:", hashes[0])
	} else {
		fmt.Println("hashes is empty!")
	}

	fmt.Println("subtree notification received")

	baseDir := "../../data"

	fmt.Println("num of subtrees:", len(hashes))

	// Search inside ubsv1, ubsv2 and ubsv3 subfolders
	for _, subtreeHash := range hashes {
		fmt.Println("Subtree hash:", subtreeHash)
		for i := 2; i <= 3; i++ {

			subDir := fmt.Sprintf("ubsv%d/subtreestore", i)
			fmt.Println(subDir)
			filePath := filepath.Join(baseDir, subDir, subtreeHash.String())
			fmt.Println(filePath)
			if _, err := os.Stat(filePath); err == nil {
				fmt.Printf("Subtree %s exists.\n", filePath)
				found += 1
			} else if os.IsNotExist(err) {
				fmt.Printf("Subtree %s doesn't exists %s.\n", subtreeHash.String(), subDir)
			} else {
				fmt.Printf("Error checking the file %s in %s: %v\n", subtreeHash.String(), subDir, err)
			}
		}
	}

	if found == 0 {
		t.Errorf("Test failed, no subtree found")
	}
}

func TestTNA1TestSuite(t *testing.T) {
	suite.Run(t, new(TNA1TestSuite))
}
