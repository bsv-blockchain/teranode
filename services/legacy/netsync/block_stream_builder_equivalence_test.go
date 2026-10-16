package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// batchBuildSubtrees is the reference this test proves the streaming builder
// against: it builds every subtree, its transaction data and its inpoint
// metadata from a fixed list of transactions all at once, the way the
// pre-streaming code did, rather than one transaction at a time as
// blockStreamBuilder does. txs[0] must be the block's coinbase. It is never
// itself stored — AddCoinbaseNode fills slot zero with a placeholder, exactly
// as the streaming builder does — so both paths start from the same
// placeholder rather than one substituting the real coinbase id and the other
// not.
func batchBuildSubtrees(txs []*bt.Tx, maxItems int) ([]*subtreepkg.Subtree, []*subtreepkg.Data, []*subtreepkg.Meta, error) {
	subtreeSize, k, finalLeaves, err := partitionLegacyBlock(len(txs), maxItems)
	if err != nil {
		return nil, nil, nil, err
	}

	trees := make([]*subtreepkg.Subtree, k)
	datas := make([]*subtreepkg.Data, k)
	metas := make([]*subtreepkg.Meta, k)

	next := 1 // txs[0] is the coinbase; AddCoinbaseNode below consumes slot zero instead

	for i := 0; i < k; i++ {
		capacity := subtreeSize
		if i == k-1 && k > 1 && finalLeaves < subtreeSize {
			capacity = finalLeaves
		}

		st, err := subtreepkg.NewIncompleteTreeByLeafCount(capacity)
		if err != nil {
			return nil, nil, nil, err
		}

		data := subtreepkg.NewSubtreeData(st)
		meta := subtreepkg.NewSubtreeMeta(st)

		if i == 0 {
			if err := st.AddCoinbaseNode(); err != nil {
				return nil, nil, nil, err
			}
		}

		for st.Length() < capacity {
			tx := txs[next]
			hash := tx.TxIDChainHash()
			nodeIdx := st.Length()

			// Same three calls, same order, same fee-zero stamp as
			// blockStreamBuilder.AddTx (block_stream_builder.go) — this
			// function's only job is to be that logic run over a slice instead
			// of a stream, so it must not diverge from it in any way that
			// would make a real bug in the streaming path look correct.
			if err := st.AddNode(*hash, 0, uint64(tx.Size())); err != nil {
				return nil, nil, nil, err
			}

			if err := data.AddTx(tx, nodeIdx); err != nil {
				return nil, nil, nil, err
			}

			if err := meta.SetTxInpointsFromTx(tx); err != nil {
				return nil, nil, nil, err
			}

			next++
		}

		trees[i] = st
		datas[i] = data
		metas[i] = meta
	}

	return trees, datas, metas, nil
}

// TestBlockStreamBuilder_MatchesBatchBuild gates one property only: that
// streaming the same transactions one at a time produces the same partitioning
// and ordering as building them from a slice all at once. Its oracle,
// batchBuildSubtrees, is a second transcription of the builder's own AddTx logic
// run over a slice instead of a stream (see that function's comment above), not
// an independent implementation, so a root hash alone cannot prove even that
// much — two subtrees can share a root while carrying different fee or size
// fields — hence every comparison below is on Serialize() output, not on the
// root.
//
// This test does NOT prove equivalence with the production createSubtrees path
// (handle_block.go), and never exercises it. One thing it is blind to: both
// sides here stamp a zero fee, whereas createSubtrees computes a real fee
// whenever legacyOutpointOnly is off, which is its default.
func TestBlockStreamBuilder_MatchesBatchBuild(t *testing.T) {
	for _, tc := range []struct {
		name     string
		txCount  int
		maxItems int
	}{
		{"one short subtree", 5, 8},
		{"single subtree, exactly full", 8, 8},
		// The only shape in this table (and, per review, in the whole package)
		// where the final subtree comes out of the real builder exactly full
		// rather than short: it is the sole case that exercises the
		// accumulator's plain-root branch for a last subtree end to end
		// through newBlockStreamBuilder's own isLast computation, rather than
		// through a hand-built subtree in merkle_accumulator_test.go that
		// bypasses that computation entirely.
		{"two full subtrees", 16, 8},
		{"two full subtrees plus a short final one", 20, 8},
		{"mainnet subtree size, three subtrees", 9000, 4096}, // 4096 is the live setting, not the 1048576 code default
	} {
		t.Run(tc.name, func(t *testing.T) {
			cb := coinbaseTx(t)

			txs := make([]*bt.Tx, tc.txCount)
			hashes := make([]*chainhash.Hash, tc.txCount)
			txs[0] = cb
			hashes[0] = cb.TxIDChainHash()

			for i := 1; i < tc.txCount; i++ {
				txs[i], hashes[i] = streamTx(t, i)
			}

			var streamedTrees []*subtreepkg.Subtree

			var streamedData []*subtreepkg.Data

			var streamedMeta []*subtreepkg.Meta

			emit := func(index int, st *subtreepkg.Subtree, data *subtreepkg.Data, meta *subtreepkg.Meta) error {
				streamedTrees = append(streamedTrees, st)
				streamedData = append(streamedData, data)
				streamedMeta = append(streamedMeta, meta)

				return nil
			}

			b, err := newBlockStreamBuilder(tc.txCount, tc.maxItems, cb, emit, newDedupMap(tc.txCount))
			require.NoError(t, err)

			for i := 1; i < tc.txCount; i++ {
				require.NoError(t, b.AddTx(txs[i], hashes[i]))
			}

			streamRoot, streamHashes, err := b.Finish()
			require.NoError(t, err)

			batchTrees, batchData, batchMeta, err := batchBuildSubtrees(txs, tc.maxItems)
			require.NoError(t, err)

			batchRoot, err := referenceRootFromSubtrees(batchTrees, cb.TxIDChainHash(), uint64(cb.Size()))
			require.NoError(t, err)

			require.Equal(t, len(batchTrees), len(streamedTrees), "same number of subtrees")
			require.Equal(t, batchRoot.String(), streamRoot.String(), "same merkle root")
			require.Len(t, streamHashes, len(batchTrees))

			for i := range batchTrees {
				wantTree, err := batchTrees[i].Serialize()
				require.NoError(t, err)
				gotTree, err := streamedTrees[i].Serialize()
				require.NoError(t, err)
				require.Equal(t, wantTree, gotTree, "subtree %d bytes must match the batch build", i)

				wantData, err := batchData[i].Serialize()
				require.NoError(t, err)
				gotData, err := streamedData[i].Serialize()
				require.NoError(t, err)
				require.Equal(t, wantData, gotData, "subtree %d data bytes must match the batch build", i)

				wantMeta, err := batchMeta[i].Serialize()
				require.NoError(t, err)
				gotMeta, err := streamedMeta[i].Serialize()
				require.NoError(t, err)
				require.Equal(t, wantMeta, gotMeta, "subtree %d meta bytes must match the batch build", i)

				require.Equal(t, batchTrees[i].RootHash().String(), streamHashes[i].String(),
					"subtree %d root hash must match the batch build", i)
			}
		})
	}
}
