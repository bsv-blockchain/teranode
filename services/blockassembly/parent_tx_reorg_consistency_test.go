package blockassembly

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	utxostoresql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParentTransactionNETCalculationDuringReset tests that the NET calculation
// correctly handles parent transactions that appear in BOTH moveBack and moveForward blocks.
// This is a critical scenario during EDA reorgs.
func TestParentTransactionNETCalculationDuringReset(t *testing.T) {
	t.Run("parent in BOTH moveBack and moveForward should NOT be marked unmined", func(t *testing.T) {
		ctx := context.Background()
		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := utxostoresql.New(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), utxoStoreURL)
		require.NoError(t, err)

		err = utxoStore.SetBlockHeight(150)
		require.NoError(t, err)

		blobStore := blob_memory.New()

		// Create a proper parent transaction using the helper from tests
		parentTx := bt.NewTx()
		parentTx.LockTime = 0

		// Add input
		input := &bt.Input{
			PreviousTxOutIndex: 0,
			PreviousTxSatoshis: 5000000000,
			SequenceNumber:     0xffffffff,
			UnlockingScript:    bscript.NewFromBytes([]byte{}),
		}
		_ = input.PreviousTxIDAdd(&chainhash.Hash{})
		parentTx.Inputs = []*bt.Input{input}

		// Add output with simple script
		parentTx.Outputs = []*bt.Output{
			{
				Satoshis:      100000,
				LockingScript: bscript.NewFromBytes([]byte{0x76, 0xa9, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x88, 0xac}),
			},
		}

		// Add parent to UTXO store and mark as mined
		_, err = utxoStore.Create(ctx, parentTx, 1)
		require.NoError(t, err)

		// Get the actual hash
		parentTxHash := parentTx.TxIDChainHash()

		err = utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*parentTxHash}, true)
		require.NoError(t, err)

		// Verify parent starts as mined (unmined_since should be 0)
		parentMetaBefore, err := utxoStore.Get(ctx, parentTxHash)
		require.NoError(t, err)
		require.Equal(t, uint32(0), parentMetaBefore.UnminedSince, "Parent should start as mined")

		// Helper to create subtree with transaction
		createSubtreeWithTx := func(txHash *chainhash.Hash) (*chainhash.Hash, []byte, error) {
			st, err := subtree.NewTreeByLeafCount(64)
			if err != nil {
				return nil, nil, err
			}
			_ = st.AddCoinbaseNode()
			err = st.AddSubtreeNode(subtree.Node{
				Hash:        *txHash,
				Fee:         100,
				SizeInBytes: 250,
			})
			if err != nil {
				return nil, nil, err
			}
			stBytes, err := st.Serialize()
			return st.RootHash(), stBytes, err
		}

		// Create moveBack block containing parent
		_, moveBackSubtreeBytes, err := createSubtreeWithTx(parentTxHash)
		require.NoError(t, err)

		// Use a unique hash for moveBack subtree storage
		moveBackSubtreeHash, err := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
		require.NoError(t, err)

		err = blobStore.Set(ctx, moveBackSubtreeHash[:], fileformat.FileTypeSubtree, moveBackSubtreeBytes)
		require.NoError(t, err)
		err = blobStore.Set(ctx, moveBackSubtreeHash[:], fileformat.FileTypeSubtreeMeta, []byte{})
		require.NoError(t, err)

		moveBackBlock := &model.Block{
			Height: 1,
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &chainhash.Hash{},
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      1000000000,
				Bits:           model.NBit{},
				Nonce:          1,
			},
			CoinbaseTx: &bt.Tx{},
			Subtrees:   []*chainhash.Hash{moveBackSubtreeHash},
		}

		// Create moveForward block ALSO containing parent (this is the KEY test scenario)
		_, moveForwardSubtreeBytes, err := createSubtreeWithTx(parentTxHash)
		require.NoError(t, err)

		// Use a different unique hash for moveForward subtree storage
		moveForwardSubtreeHash, err := chainhash.NewHashFromStr("2222222222222222222222222222222222222222222222222222222222222222")
		require.NoError(t, err)

		err = blobStore.Set(ctx, moveForwardSubtreeHash[:], fileformat.FileTypeSubtree, moveForwardSubtreeBytes)
		require.NoError(t, err)
		err = blobStore.Set(ctx, moveForwardSubtreeHash[:], fileformat.FileTypeSubtreeMeta, []byte{})
		require.NoError(t, err)

		moveForwardBlock := &model.Block{
			Height: 1,
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &chainhash.Hash{},
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      1000000001,
				Bits:           model.NBit{},
				Nonce:          2,
			},
			CoinbaseTx: &bt.Tx{},
			Subtrees:   []*chainhash.Hash{moveForwardSubtreeHash},
		}

		// Extract move Back transactions (NET calculation logic)
		moveBackBlocksWithMeta := []*blockWithMeta{
			{block: moveBackBlock, meta: &model.BlockHeaderMeta{Invalid: false}},
		}
		moveForwardBlocksWithMeta := []*blockWithMeta{
			{block: moveForwardBlock, meta: &model.BlockHeaderMeta{Invalid: false}},
		}

		settings := test.CreateBaseTestSettings(t)

		// Build moveForwardTxMap (this is what reset() does)
		moveForwardTxMap := make(map[chainhash.Hash]bool)
		for _, blockWithMeta := range moveForwardBlocksWithMeta {
			if blockWithMeta.meta.Invalid {
				t.Logf("Skipping invalid moveForward block")
				continue
			}

			blockSubtrees, err := blockWithMeta.block.GetSubtrees(ctx, ulogger.TestLogger{}, blobStore, settings.Block.GetAndValidateSubtreesConcurrency)
			if err != nil {
				t.Errorf("Failed to get subtrees for moveForward block: %v", err)
				continue
			}

			for _, st := range blockSubtrees {
				for _, node := range st.Nodes {
					if !node.Hash.IsEqual(subtree.CoinbasePlaceholderHash) {
						moveForwardTxMap[node.Hash] = true
					}
				}
			}
		}

		t.Logf("moveForwardTxMap contains parent: %v", moveForwardTxMap[*parentTxHash])
		require.True(t, moveForwardTxMap[*parentTxHash], "Parent should be in moveForwardTxMap")

		// Build moveBackTxs (NET calculation)
		var moveBackTxs []chainhash.Hash
		for _, blockWithMeta := range moveBackBlocksWithMeta {
			if blockWithMeta.meta.Invalid {
				t.Logf("Skipping invalid moveBack block")
				continue
			}

			blockSubtrees, err := blockWithMeta.block.GetSubtrees(ctx, ulogger.TestLogger{}, blobStore, settings.Block.GetAndValidateSubtreesConcurrency)
			if err != nil {
				t.Errorf("Failed to get subtrees for moveBack block: %v", err)
				continue
			}

			for _, st := range blockSubtrees {
				for _, node := range st.Nodes {
					if !node.Hash.IsEqual(subtree.CoinbasePlaceholderHash) {
						// Only add if NOT in moveForward (NET calculation)
						if !moveForwardTxMap[node.Hash] {
							moveBackTxs = append(moveBackTxs, node.Hash)
						}
					}
				}
			}
		}

		t.Logf("moveBackTxs count: %d", len(moveBackTxs))
		assert.Empty(t, moveBackTxs, "Parent should be filtered out (in both blocks)")

		// Simulate marking transactions as unmined
		if len(moveBackTxs) > 0 {
			err = utxoStore.MarkTransactionsOnLongestChain(ctx, moveBackTxs, false)
			require.NoError(t, err)
		}

		// Verify parent state
		parentMetaAfter, err := utxoStore.Get(ctx, parentTxHash)
		require.NoError(t, err)

		// CRITICAL ASSERTION: Parent should STAY mined because it's in moveForward
		if parentMetaAfter.UnminedSince > 0 {
			t.Errorf("❌ BUG DETECTED: Parent is in BOTH moveBack and moveForward, but was marked as unmined (unmined_since=%v)",
				parentMetaAfter.UnminedSince)
			t.Errorf("This indicates NET calculation failed - parent was NOT filtered from moveBackTxs")
			t.Errorf("Likely cause: moveForwardTxMap incomplete (Invalid block skip or GetSubtrees failure)")
		} else {
			t.Logf("✅ PASS: Parent correctly stayed mined (unmined_since=0)")
		}

		assert.Equal(t, uint32(0), parentMetaAfter.UnminedSince, "Parent in BOTH blocks should stay mined")
	})

	t.Run("rapid state transitions - parent in multiple competing blocks at same height", func(t *testing.T) {
		ctx := context.Background()
		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := utxostoresql.New(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), utxoStoreURL)
		require.NoError(t, err)

		// Set current block height so unmined_since gets set properly
		err = utxoStore.SetBlockHeight(100)
		require.NoError(t, err)

		// Create parent transaction
		parentTx := bt.NewTx()
		parentTx.LockTime = 0

		input := &bt.Input{
			PreviousTxOutIndex: 0,
			PreviousTxSatoshis: 5000000000,
			SequenceNumber:     0xffffffff,
			UnlockingScript:    bscript.NewFromBytes([]byte{}),
		}
		_ = input.PreviousTxIDAdd(&chainhash.Hash{})
		parentTx.Inputs = []*bt.Input{input}

		parentTx.Outputs = []*bt.Output{
			{
				Satoshis:      100000,
				LockingScript: bscript.NewFromBytes([]byte{0x76, 0xa9, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x88, 0xac}),
			},
		}

		// Add parent to UTXO store
		_, err = utxoStore.Create(ctx, parentTx, 1)
		require.NoError(t, err)

		parentTxHash := parentTx.TxIDChainHash()

		// Initial state: parent is unmined
		err = utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*parentTxHash}, false)
		require.NoError(t, err)

		// Simulate rapid EDA scenario:
		// Block A mined → parent mined
		t.Logf("Block A: Mark parent as mined")
		err = utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*parentTxHash}, true)
		require.NoError(t, err)

		meta1, _ := utxoStore.Get(ctx, parentTxHash)
		t.Logf("After Block A: unmined_since=%v", meta1.UnminedSince)

		// Block B wins → parent unmined
		t.Logf("Block B wins: Mark parent as unmined")
		err = utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*parentTxHash}, false)
		require.NoError(t, err)

		meta2, _ := utxoStore.Get(ctx, parentTxHash)
		t.Logf("After Block B: unmined_since=%v", meta2.UnminedSince)

		// Block C wins → parent mined again
		t.Logf("Block C wins: Mark parent as mined again")
		err = utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*parentTxHash}, true)
		require.NoError(t, err)

		meta3, _ := utxoStore.Get(ctx, parentTxHash)
		t.Logf("After Block C: unmined_since=%v", meta3.UnminedSince)

		// Block D wins → parent unmined yet again
		t.Logf("Block D wins: Mark parent as unmined yet again")
		err = utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*parentTxHash}, false)
		require.NoError(t, err)

		metaFinal, _ := utxoStore.Get(ctx, parentTxHash)
		t.Logf("Final state: unmined_since=%v", metaFinal.UnminedSince)

		// Verify parent ended up in correct final state (unmined)
		assert.Greater(t, metaFinal.UnminedSince, uint32(0), "Parent should be unmined after Block D wins")

		t.Logf("✅ PASS: Rapid state transitions handled correctly")
	})

	t.Run("concurrent MarkTransactionsOnLongestChain and query - simulate SQL timing race", func(t *testing.T) {
		ctx := context.Background()
		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := utxostoresql.New(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), utxoStoreURL)
		require.NoError(t, err)

		err = utxoStore.SetBlockHeight(200)
		require.NoError(t, err)

		// Create parent transaction
		parentTx := bt.NewTx()
		parentTx.LockTime = 0

		input := &bt.Input{
			PreviousTxOutIndex: 0,
			PreviousTxSatoshis: 5000000000,
			SequenceNumber:     0xffffffff,
			UnlockingScript:    bscript.NewFromBytes([]byte{}),
		}
		_ = input.PreviousTxIDAdd(&chainhash.Hash{})
		parentTx.Inputs = []*bt.Input{input}

		parentTx.Outputs = []*bt.Output{
			{
				Satoshis:      100000,
				LockingScript: bscript.NewFromBytes([]byte{0x76, 0xa9, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x88, 0xac}),
			},
		}

		// Add parent to UTXO store and mark as MINED initially
		_, err = utxoStore.Create(ctx, parentTx, 1)
		require.NoError(t, err)

		parentTxHash := parentTx.TxIDChainHash()

		err = utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*parentTxHash}, true)
		require.NoError(t, err)

		// Verify parent is mined
		metaBefore, err := utxoStore.Get(ctx, parentTxHash)
		require.NoError(t, err)
		require.Equal(t, uint32(0), metaBefore.UnminedSince, "Parent should start as mined")

		// Simulate the race condition:
		// Thread 1: Mark as unmined
		// Thread 2: Query unmined transactions (immediately, might read stale data)

		done := make(chan bool)
		var queryResult uint32

		// Start query in goroutine (simulates loadUnminedTransactions)
		go func() {
			// Try to query while UPDATE might be in flight
			meta, err := utxoStore.Get(ctx, parentTxHash)
			if err == nil {
				queryResult = meta.UnminedSince
			}
			done <- true
		}()

		// Mark as unmined (this is the UPDATE that might not be visible yet)
		err = utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*parentTxHash}, false)
		require.NoError(t, err)

		// Wait for concurrent query to complete
		<-done

		// Check final state
		metaAfter, err := utxoStore.Get(ctx, parentTxHash)
		require.NoError(t, err)

		t.Logf("Concurrent query saw: unmined_since=%v", queryResult)
		t.Logf("Final state: unmined_since=%v", metaAfter.UnminedSince)

		// The race: concurrent query might have seen unmined_since=0 (stale)
		// But final state should be unmined_since=200
		if queryResult == 0 && metaAfter.UnminedSince > 0 {
			t.Errorf("❌ RACE CONDITION DETECTED: Concurrent query read stale data (saw 0, actual is %v)", metaAfter.UnminedSince)
			t.Errorf("This is the bug! loadUnminedTransactions() could skip parent if it reads during UPDATE")
		}

		assert.Greater(t, metaAfter.UnminedSince, uint32(0), "Parent should be unmined")
		t.Logf("✅ Test completed - checking for race")
	})

	t.Run("parent ONLY in moveBack - should be marked unmined", func(t *testing.T) {
		ctx := context.Background()
		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := utxostoresql.New(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), utxoStoreURL)
		require.NoError(t, err)

		err = utxoStore.SetBlockHeight(150)
		require.NoError(t, err)

		blobStore := blob_memory.New()

		// Create parent transaction
		parentTx := bt.NewTx()
		parentTx.LockTime = 0

		input := &bt.Input{
			PreviousTxOutIndex: 0,
			PreviousTxSatoshis: 5000000000,
			SequenceNumber:     0xffffffff,
			UnlockingScript:    bscript.NewFromBytes([]byte{}),
		}
		_ = input.PreviousTxIDAdd(&chainhash.Hash{})
		parentTx.Inputs = []*bt.Input{input}

		parentTx.Outputs = []*bt.Output{
			{
				Satoshis:      100000,
				LockingScript: bscript.NewFromBytes([]byte{0x76, 0xa9, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x88, 0xac}),
			},
		}

		_, err = utxoStore.Create(ctx, parentTx, 1)
		require.NoError(t, err)

		parentTxHash := parentTx.TxIDChainHash()

		// Start with parent as mined
		err = utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*parentTxHash}, true)
		require.NoError(t, err)

		// Create subtree with parent
		st, err := subtree.NewTreeByLeafCount(64)
		require.NoError(t, err)
		_ = st.AddCoinbaseNode()
		err = st.AddSubtreeNode(subtree.Node{
			Hash:        *parentTxHash,
			Fee:         100,
			SizeInBytes: 250,
		})
		require.NoError(t, err)

		stBytes, err := st.Serialize()
		require.NoError(t, err)

		stHash, err := chainhash.NewHashFromStr("4444444444444444444444444444444444444444444444444444444444444444")
		require.NoError(t, err)

		err = blobStore.Set(ctx, stHash[:], fileformat.FileTypeSubtree, stBytes)
		require.NoError(t, err)
		err = blobStore.Set(ctx, stHash[:], fileformat.FileTypeSubtreeMeta, []byte{})
		require.NoError(t, err)

		// Parent ONLY in moveBack
		moveBackBlock := &model.Block{
			Height: 1,
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &chainhash.Hash{},
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      1500000000,
				Bits:           model.NBit{},
				Nonce:          10,
			},
			CoinbaseTx: &bt.Tx{},
			Subtrees:   []*chainhash.Hash{stHash},
		}

		// NO moveForward blocks
		moveBackBlocksWithMeta := []*blockWithMeta{
			{block: moveBackBlock, meta: &model.BlockHeaderMeta{Invalid: false}},
		}

		settings := test.CreateBaseTestSettings(t)

		// Process moveBack only (parent should be added to moveBackTxs)
		var moveBackTxs []chainhash.Hash
		for _, blockWithMeta := range moveBackBlocksWithMeta {
			blockSubtrees, err := blockWithMeta.block.GetSubtrees(ctx, ulogger.TestLogger{}, blobStore, settings.Block.GetAndValidateSubtreesConcurrency)
			require.NoError(t, err)

			for _, st := range blockSubtrees {
				for _, node := range st.Nodes {
					if !node.Hash.IsEqual(subtree.CoinbasePlaceholderHash) {
						moveBackTxs = append(moveBackTxs, node.Hash)
					}
				}
			}
		}

		t.Logf("moveBackTxs count: %d (should be 1 - parent only)", len(moveBackTxs))
		require.Len(t, moveBackTxs, 1, "Should have exactly 1 transaction (parent)")

		// Mark as unmined
		err = utxoStore.MarkTransactionsOnLongestChain(ctx, moveBackTxs, false)
		require.NoError(t, err)

		// Verify parent is now unmined
		parentMetaAfter, err := utxoStore.Get(ctx, parentTxHash)
		require.NoError(t, err)

		t.Logf("After moveBack: unmined_since=%v", parentMetaAfter.UnminedSince)

		if parentMetaAfter.UnminedSince == 0 {
			t.Errorf("❌ BUG: Parent ONLY in moveBack should be unmined, but unmined_since=0")
		}

		assert.Greater(t, parentMetaAfter.UnminedSince, uint32(0), "Parent only in moveBack should be unmined")
		t.Logf("✅ PASS: Parent only in moveBack correctly marked unmined")
	})

	t.Run("parent ONLY in moveForward - should stay mined", func(t *testing.T) {
		ctx := context.Background()
		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := utxostoresql.New(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), utxoStoreURL)
		require.NoError(t, err)

		err = utxoStore.SetBlockHeight(150)
		require.NoError(t, err)

		blobStore := blob_memory.New()

		// Create parent transaction
		parentTx := bt.NewTx()
		parentTx.LockTime = 0

		input := &bt.Input{
			PreviousTxOutIndex: 0,
			PreviousTxSatoshis: 5000000000,
			SequenceNumber:     0xffffffff,
			UnlockingScript:    bscript.NewFromBytes([]byte{}),
		}
		_ = input.PreviousTxIDAdd(&chainhash.Hash{})
		parentTx.Inputs = []*bt.Input{input}

		parentTx.Outputs = []*bt.Output{
			{
				Satoshis:      100000,
				LockingScript: bscript.NewFromBytes([]byte{0x76, 0xa9, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x88, 0xac}),
			},
		}

		_, err = utxoStore.Create(ctx, parentTx, 1)
		require.NoError(t, err)

		parentTxHash := parentTx.TxIDChainHash()

		// Start with parent as mined
		err = utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*parentTxHash}, true)
		require.NoError(t, err)

		// Create subtree with parent
		st, err := subtree.NewTreeByLeafCount(64)
		require.NoError(t, err)
		_ = st.AddCoinbaseNode()
		err = st.AddSubtreeNode(subtree.Node{
			Hash:        *parentTxHash,
			Fee:         100,
			SizeInBytes: 250,
		})
		require.NoError(t, err)

		stBytes, err := st.Serialize()
		require.NoError(t, err)

		stHash, err := chainhash.NewHashFromStr("5555555555555555555555555555555555555555555555555555555555555555")
		require.NoError(t, err)

		err = blobStore.Set(ctx, stHash[:], fileformat.FileTypeSubtree, stBytes)
		require.NoError(t, err)
		err = blobStore.Set(ctx, stHash[:], fileformat.FileTypeSubtreeMeta, []byte{})
		require.NoError(t, err)

		// Parent ONLY in moveForward
		moveForwardBlock := &model.Block{
			Height: 1,
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &chainhash.Hash{},
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      1600000000,
				Bits:           model.NBit{},
				Nonce:          20,
			},
			CoinbaseTx: &bt.Tx{},
			Subtrees:   []*chainhash.Hash{stHash},
		}

		// Process with NO moveBack blocks
		moveForwardBlocksWithMeta := []*blockWithMeta{
			{block: moveForwardBlock, meta: &model.BlockHeaderMeta{Invalid: false}},
		}

		settings := test.CreateBaseTestSettings(t)

		// Build moveForwardTxMap
		moveForwardTxMap := make(map[chainhash.Hash]bool)
		for _, blockWithMeta := range moveForwardBlocksWithMeta {
			blockSubtrees, err := blockWithMeta.block.GetSubtrees(ctx, ulogger.TestLogger{}, blobStore, settings.Block.GetAndValidateSubtreesConcurrency)
			require.NoError(t, err)

			for _, st := range blockSubtrees {
				for _, node := range st.Nodes {
					if !node.Hash.IsEqual(subtree.CoinbasePlaceholderHash) {
						moveForwardTxMap[node.Hash] = true
					}
				}
			}
		}

		t.Logf("moveForwardTxMap contains parent: %v", moveForwardTxMap[*parentTxHash])
		require.True(t, moveForwardTxMap[*parentTxHash], "Parent should be in moveForwardTxMap")

		// With no moveBack blocks, moveBackTxs should be empty
		var moveBackTxs []chainhash.Hash
		t.Logf("moveBackTxs count: %d (should be 0 - no moveBack blocks)", len(moveBackTxs))

		// No MarkTransactionsOnLongestChain call (nothing to unmine)

		// Verify parent stayed mined
		parentMetaAfter, err := utxoStore.Get(ctx, parentTxHash)
		require.NoError(t, err)

		t.Logf("After moveForward only: unmined_since=%v", parentMetaAfter.UnminedSince)

		if parentMetaAfter.UnminedSince > 0 {
			t.Errorf("❌ BUG: Parent ONLY in moveForward should stay mined, but unmined_since=%v", parentMetaAfter.UnminedSince)
		}

		assert.Equal(t, uint32(0), parentMetaAfter.UnminedSince, "Parent only in moveForward should stay mined")
		t.Logf("✅ PASS: Parent only in moveForward correctly stayed mined")
	})
}
