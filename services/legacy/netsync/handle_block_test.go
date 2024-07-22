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
			blockFilePath:    "../testdata/00000000000000000ad4cd15bbeaf6cb4583c93e13e311f9774194aadea87386.hex",
			expectedTxMapLen: 563,
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
			blockFilePath:    "../testdata/00000000000000000ad4cd15bbeaf6cb4583c93e13e311f9774194aadea87386.hex",
			expectedTxMapLen: 563,
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

			maxLevel, blockTXsPerLevel := sm.prepareTxsPerLevel(context.Background(), block, txMap)
			fmt.Println("Max Level: ", maxLevel)
			for level, txs := range blockTXsPerLevel {
				fmt.Printf("Level %d: %d transactions\n", level, len(txs))
			}
		})
	}
}
