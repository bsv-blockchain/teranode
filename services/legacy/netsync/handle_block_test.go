package netsync

import (
	"context"
	"fmt"
	"testing"

	"github.com/bitcoin-sv/ubsv/services/legacy/testdata"
	"github.com/stretchr/testify/require"
)

func TestSyncManager_createTxMap(t *testing.T) {
	// t.Run("TestSyncManager_createTxMap", func(t *testing.T) {

	// })

	block, err := testdata.ReadBlockFromFile("../testdata/00000000000000000ad4cd15bbeaf6cb4583c93e13e311f9774194aadea87386.hex")
	require.NoError(t, err)

	sm := &SyncManager{}

	txMap, err := sm.createTxMap(block)
	require.NoError(t, err)
	require.Len(t, txMap, 563)
}

func TestSyncManager_prepareTxsPerLevel1(t *testing.T) {
	// t.Run("TestSyncManager_prepareTxsPerLevel1", func(t *testing.T) {

	// })

	// Decode the hex string to bytes
	// Deserialize the bytes into a Block
	block, err := testdata.ReadBlockFromFile("../testdata/00000000000000000ad4cd15bbeaf6cb4583c93e13e311f9774194aadea87386.hex")
	require.NoError(t, err)

	sm := &SyncManager{}

	txMap, err := sm.createTxMap(block)
	require.NoError(t, err)
	require.Len(t, txMap, 563)

	maxLevel, blockTXsPerLevel := sm.prepareTxsPerLevel(context.Background(), block, txMap)
	fmt.Println(maxLevel)
	fmt.Println(len(blockTXsPerLevel))

}
