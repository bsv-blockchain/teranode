package netsync

import (
	"context"
	"fmt"
	"testing"

	"github.com/bitcoin-sv/ubsv/services/legacy/testdata"
	"github.com/stretchr/testify/require"
)

func TestSyncManager_createTxMap(t *testing.T) {
	// Define test cases with block file paths and expected lengths of the txMap
	testCases := []struct {
		name             string
		blockFilePath    string
		expectedTxMapLen int
	}{
		{
			name:             "Block1",
			blockFilePath:    "../testdata/00000000000000000ad4cd15bbeaf6cb4583c93e13e311f9774194aadea87386.bin",
			expectedTxMapLen: 563,
		},
		{
			name:             "Block2",
			blockFilePath:    "../testdata/00000000000000000488eecd93d6f3767b1ba38668200a6a5349af2e0d4fad3f.bin",
			expectedTxMapLen: 1355,
		},
		{
			name:             "Block3",
			blockFilePath:    "../testdata/000000000000000009631dd3dd7357675d8a1f8925be5e7851c68255531ac5fb.bin",
			expectedTxMapLen: 900,
		},
		{
			name:             "Block4",
			blockFilePath:    "../testdata/0000000000000000015594853418b4093c4be4ad8b77fec88b5400feb3268fc4.bin",
			expectedTxMapLen: 484,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			block, err := testdata.ReadBlockFromFile(tc.blockFilePath)
			require.NoError(t, err)

			sm := &SyncManager{}

			txMap, err := sm.createTxMap(block)
			require.NoError(t, err)
			require.Len(t, txMap, tc.expectedTxMapLen)
		})
	}
}

func TestSyncManager_prepareTxsPerLevel(t *testing.T) {
	testCases := []struct {
		name             string
		blockFilePath    string
		expectedTxMapLen int
	}{
		{
			name:             "Block1",
			blockFilePath:    "../testdata/00000000000000000ad4cd15bbeaf6cb4583c93e13e311f9774194aadea87386.bin",
			expectedTxMapLen: 563,
		},
		// {
		// 	name:             "Block2",
		// 	blockFilePath:    "../testdata/00000000000000000488eecd93d6f3767b1ba38668200a6a5349af2e0d4fad3f.bin",
		// 	expectedTxMapLen: 1355,
		// },
		// {
		// 	name:             "Block3",
		// 	blockFilePath:    "../testdata/000000000000000009631dd3dd7357675d8a1f8925be5e7851c68255531ac5fb.bin",
		// 	expectedTxMapLen: 900,
		// },
		// {
		// 	name:             "Block4",
		// 	blockFilePath:    "../testdata/0000000000000000015594853418b4093c4be4ad8b77fec88b5400feb3268fc4.bin",
		// 	expectedTxMapLen: 484,
		// },
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			block, err := testdata.ReadBlockFromFile(tc.blockFilePath)
			require.NoError(t, err)

			sm := &SyncManager{}
			//sm.utxoStore = utxostore.New(ulogger.TestLogger{})
			txMap, err := sm.createTxMap(block)
			require.NoError(t, err)
			require.Len(t, txMap, tc.expectedTxMapLen)

			for _, wireTx := range block.Transactions() {
				txHash := *wireTx.Hash()
				// extend transaction
				if txWrapper, found := txMap[txHash]; found {
					tx := txWrapper.tx

					for _, input := range tx.Inputs {
						prevTxHash := *input.PreviousTxIDChainHash()
						if _, found := txMap[prevTxHash]; found {
							txWrapper.someParentsInBlock = true
						}
					}
				}

			}

			maxLevel, blockTXsPerLevel := sm.prepareTxsPerLevel(context.Background(), block, txMap)
			fmt.Println("Max Level: ", maxLevel)
			// for level, txs := range blockTXsPerLevel {
			// 	fmt.Printf("Level %d: %d transactions\n", level, len(txs))
			// }
			allParents := 0
			for i := range blockTXsPerLevel {
				allParents += len(blockTXsPerLevel[i])
			}
			fmt.Println("all Parents: ", allParents)
		})
	}
}
