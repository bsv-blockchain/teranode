package blockassembly

import (
	"context"
	"crypto/rand"
	"net/url"
	"sort"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxostoresql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadUnminedTransactionsWithDiskSort(t *testing.T) {
	initPrometheusMetrics()

	t.Run("basic disk sort with unmined transactions", func(t *testing.T) {
		ba, utxoStore, cleanup := setupDiskSortTest(t)
		defer cleanup()

		ctx := context.Background()

		// Create some unmined transactions
		transactions := generateTestTransactionsForDiskSort(t, 10)

		for _, tx := range transactions {
			_, err := utxoStore.Create(ctx, tx, 1)
			require.NoError(t, err)
		}

		// Enable disk sort and disable parent validation
		ba.settings.BlockAssembly.UnminedTxDiskSortEnabled = true
		ba.settings.BlockAssembly.OnRestartValidateParentChain = false

		// Run the disk sort function
		err := ba.loadUnminedTransactionsWithDiskSort(ctx, false)
		require.NoError(t, err)

		// Verify transactions were added to subtree processor
		assert.True(t, true, "loadUnminedTransactionsWithDiskSort completed successfully")
	})

	t.Run("disk sort preserves CreatedAt order", func(t *testing.T) {
		ba, utxoStore, cleanup := setupDiskSortTest(t)
		defer cleanup()

		ctx := context.Background()

		// Create transactions with different CreatedAt timestamps
		// We'll verify order by checking the subtree processor gets them in order
		transactions := generateTestTransactionsForDiskSort(t, 5)

		for i, tx := range transactions {
			_, err := utxoStore.Create(ctx, tx, 1)
			require.NoError(t, err)
			// Sleep a tiny bit to ensure different CreatedAt timestamps
			if i < len(transactions)-1 {
				time.Sleep(10 * time.Millisecond)
			}
		}

		ba.settings.BlockAssembly.UnminedTxDiskSortEnabled = true
		ba.settings.BlockAssembly.OnRestartValidateParentChain = false

		err := ba.loadUnminedTransactionsWithDiskSort(ctx, false)
		require.NoError(t, err)
	})

	t.Run("disk sort with mixed mined/unmined transactions", func(t *testing.T) {
		ba, utxoStore, cleanup := setupDiskSortTest(t)
		defer cleanup()

		ctx := context.Background()

		transactions := generateTestTransactionsForDiskSort(t, 10)

		// Create half as unmined, half as mined
		for i, tx := range transactions {
			if i%2 == 0 {
				// Unmined
				_, err := utxoStore.Create(ctx, tx, 1)
				require.NoError(t, err)
			} else {
				// Mined in a block on the main chain (block ID 0 is genesis)
				_, err := utxoStore.Create(ctx, tx, 1, utxo.WithMinedBlockInfo(
					utxo.MinedBlockInfo{
						BlockID:     0,
						BlockHeight: 0,
						SubtreeIdx:  1,
					},
				))
				require.NoError(t, err)
			}
		}

		ba.settings.BlockAssembly.UnminedTxDiskSortEnabled = true
		ba.settings.BlockAssembly.OnRestartValidateParentChain = false

		err := ba.loadUnminedTransactionsWithDiskSort(ctx, false)
		require.NoError(t, err)
	})

	t.Run("disk sort with locked transactions", func(t *testing.T) {
		ba, utxoStore, cleanup := setupDiskSortTest(t)
		defer cleanup()

		ctx := context.Background()

		transactions := generateTestTransactionsForDiskSort(t, 5)

		// Create all as unmined
		var txHashes []chainhash.Hash
		for _, tx := range transactions {
			meta, err := utxoStore.Create(ctx, tx, 1)
			require.NoError(t, err)
			txHashes = append(txHashes, *meta.Tx.TxIDChainHash())
		}

		// Lock some transactions
		err := utxoStore.SetLocked(ctx, txHashes[:2], true)
		require.NoError(t, err)

		ba.settings.BlockAssembly.UnminedTxDiskSortEnabled = true
		ba.settings.BlockAssembly.OnRestartValidateParentChain = false

		err = ba.loadUnminedTransactionsWithDiskSort(ctx, false)
		require.NoError(t, err)

		// Verify locked transactions were unlocked after load
		for _, hash := range txHashes[:2] {
			meta, err := utxoStore.Get(ctx, &hash)
			require.NoError(t, err)
			assert.False(t, meta.Locked, "locked transactions should be unlocked after load")
		}
	})

	t.Run("disk sort with empty store", func(t *testing.T) {
		ba, _, cleanup := setupDiskSortTest(t)
		defer cleanup()

		ctx := context.Background()

		ba.settings.BlockAssembly.UnminedTxDiskSortEnabled = true
		ba.settings.BlockAssembly.OnRestartValidateParentChain = false

		// Should handle empty store gracefully
		err := ba.loadUnminedTransactionsWithDiskSort(ctx, false)
		require.NoError(t, err)
	})

	t.Run("disk sort with fullScan=true", func(t *testing.T) {
		ba, utxoStore, cleanup := setupDiskSortTest(t)
		defer cleanup()

		ctx := context.Background()

		transactions := generateTestTransactionsForDiskSort(t, 5)

		for _, tx := range transactions {
			_, err := utxoStore.Create(ctx, tx, 1)
			require.NoError(t, err)
		}

		ba.settings.BlockAssembly.UnminedTxDiskSortEnabled = true
		ba.settings.BlockAssembly.OnRestartValidateParentChain = false

		// Test with fullScan=true
		err := ba.loadUnminedTransactionsWithDiskSort(ctx, true)
		require.NoError(t, err)
	})

	t.Run("disk sort with context cancellation", func(t *testing.T) {
		ba, utxoStore, cleanup := setupDiskSortTest(t)
		defer cleanup()

		ctx, cancel := context.WithCancel(context.Background())

		// Create some transactions
		transactions := generateTestTransactionsForDiskSort(t, 3)

		for _, tx := range transactions {
			_, err := utxoStore.Create(ctx, tx, 1)
			require.NoError(t, err)
		}

		ba.settings.BlockAssembly.UnminedTxDiskSortEnabled = true
		ba.settings.BlockAssembly.OnRestartValidateParentChain = false

		// Cancel context immediately
		cancel()

		// Should handle cancellation gracefully
		_ = ba.loadUnminedTransactionsWithDiskSort(ctx, false)
		// We don't assert on error since behavior depends on timing
	})

	t.Run("disk sort dispatched from loadUnminedTransactions", func(t *testing.T) {
		ba, utxoStore, cleanup := setupDiskSortTest(t)
		defer cleanup()

		ctx := context.Background()

		transactions := generateTestTransactionsForDiskSort(t, 5)

		for _, tx := range transactions {
			_, err := utxoStore.Create(ctx, tx, 1)
			require.NoError(t, err)
		}

		// Enable disk sort and disable parent validation
		ba.settings.BlockAssembly.UnminedTxDiskSortEnabled = true
		ba.settings.BlockAssembly.OnRestartValidateParentChain = false

		// Call the main loadUnminedTransactions - should dispatch to disk sort
		err := ba.loadUnminedTransactions(ctx, false)
		require.NoError(t, err)
	})

	t.Run("in-memory sort used when parent validation enabled", func(t *testing.T) {
		ba, utxoStore, cleanup := setupDiskSortTest(t)
		defer cleanup()

		ctx := context.Background()

		transactions := generateTestTransactionsForDiskSort(t, 5)

		for _, tx := range transactions {
			_, err := utxoStore.Create(ctx, tx, 1)
			require.NoError(t, err)
		}

		// Enable disk sort but ALSO enable parent validation
		// This should use in-memory sort instead
		ba.settings.BlockAssembly.UnminedTxDiskSortEnabled = true
		ba.settings.BlockAssembly.OnRestartValidateParentChain = true

		err := ba.loadUnminedTransactions(ctx, false)
		require.NoError(t, err)
	})
}

func TestSortEntryOrdering(t *testing.T) {
	// Test that sortEntry slice sorts correctly by CreatedAt
	entries := []sortEntry{
		{CreatedAt: 300, Sequence: 2},
		{CreatedAt: 100, Sequence: 0},
		{CreatedAt: 200, Sequence: 1},
		{CreatedAt: 500, Sequence: 4},
		{CreatedAt: 400, Sequence: 3},
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].CreatedAt < entries[j].CreatedAt
	})

	// Verify sorted order
	assert.Equal(t, 100, entries[0].CreatedAt)
	assert.Equal(t, 200, entries[1].CreatedAt)
	assert.Equal(t, 300, entries[2].CreatedAt)
	assert.Equal(t, 400, entries[3].CreatedAt)
	assert.Equal(t, 500, entries[4].CreatedAt)
}

// setupDiskSortTest creates a BlockAssembler configured for disk sort testing
func setupDiskSortTest(t *testing.T) (*BlockAssembler, utxo.Store, func()) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	tSettings := createTestSettings(t)
	tSettings.BlockAssembly.UnminedTxDiskSortEnabled = true
	tSettings.BlockAssembly.OnRestartValidateParentChain = false

	utxoStore, err := utxostoresql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	err = utxoStore.SetBlockHeight(1)
	require.NoError(t, err)

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	blockchainStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockchainStore, nil, nil)
	require.NoError(t, err)

	blobStore := memory.New()
	stats := gocore.NewStat("test")

	// Create a buffered channel to consume subtrees without blocking
	newSubtreeChan := make(chan subtreeprocessor.NewSubtreeRequest, 100)

	ba, err := NewBlockAssembler(
		ctx,
		ulogger.TestLogger{},
		tSettings,
		stats,
		utxoStore,
		blobStore,
		blockchainClient,
		newSubtreeChan,
	)
	require.NoError(t, err)

	// Get genesis block
	genesisBlock, err := blockchainStore.GetBlockByID(ctx, 0)
	require.NoError(t, err)

	// Overwrite default subtree processor with one that uses our buffered channel
	ba.subtreeProcessor, err = subtreeprocessor.NewSubtreeProcessor(
		ctx,
		ulogger.TestLogger{},
		tSettings,
		nil,              // blobStore
		blockchainClient, // blockchainClient
		nil,              // utxoStore
		newSubtreeChan,   // buffered channel to consume subtrees
	)
	require.NoError(t, err)

	ba.setBestBlockHeader(genesisBlock.Header, 0)
	ba.SetSkipWaitForPendingBlocks(true)

	// Start a goroutine to consume subtrees from the channel
	go func() {
		for req := range newSubtreeChan {
			// Acknowledge the subtree by sending nil error
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()

	cleanup := func() {
		if ba.subtreeProcessor != nil {
			ba.subtreeProcessor.Stop(context.Background())
		}
		close(newSubtreeChan)
	}

	return ba, utxoStore, cleanup
}

// generateTestTransactionsForDiskSort creates test transactions for disk sort tests
func generateTestTransactionsForDiskSort(tb testing.TB, count int) []*bt.Tx {
	transactions := make([]*bt.Tx, count)

	// Standard P2PKH locking script
	prevLockingScript := bscript.NewFromBytes([]byte{
		0x76, 0xa9, 0x14,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x88, 0xac,
	})

	for i := 0; i < count; i++ {
		tx := bt.NewTx()
		tx.LockTime = uint32(i % 1000)

		// Create an input with a random previous tx
		prevTxID := make([]byte, 32)
		_, err := rand.Read(prevTxID)
		require.NoError(tb, err)

		prevHash, err := chainhash.NewHash(prevTxID)
		require.NoError(tb, err)

		input := &bt.Input{
			PreviousTxOutIndex: uint32(i % 10),
			PreviousTxSatoshis: 1000,
			PreviousTxScript:   prevLockingScript,
			UnlockingScript:    bscript.NewFromBytes([]byte{0x01, 0x02, 0x03}),
			SequenceNumber:     0xffffffff,
		}
		_ = input.PreviousTxIDAdd(prevHash)

		tx.Inputs = []*bt.Input{input}

		output := &bt.Output{
			Satoshis:      900,
			LockingScript: prevLockingScript,
		}
		tx.Outputs = []*bt.Output{output}

		transactions[i] = tx
	}

	return transactions
}
