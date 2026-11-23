package subtreeprocessor

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// At utxostore_laneMigration shadow every path that moves a transaction into or around block
// assembly's pool keeps its own lane position with it, so the lane bitmaps of the subtrees built
// from the pool hold it: the dequeue and drain paths, the re-chain after a conflict removal, a
// block moved back in a reorg, and the restart reload. At off none is kept.

// lanePoolProcessor is a subtree processor of 4-node subtrees, its UTXO store an in-memory SQLite,
// on the disk tx map or the in-memory one, at shadow or off. Start is not called, so the test
// drives it directly.
func lanePoolProcessor(t *testing.T, disk, shadow bool) (*SubtreeProcessor, utxo.Store) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 4
	tSettings.BlockAssembly.UseDynamicSubtreeSize = false

	if shadow {
		tSettings.UtxoStore.LaneMigration = settings.LaneMigrationShadow
	}

	utxoStoreURL, err := url.Parse("sqlitememory:///lanepool")
	require.NoError(t, err)

	utxoStore, err := sql.New(t.Context(), ulogger.TestLogger{}, tSettings, utxoStoreURL)
	require.NoError(t, err)

	newSubtreeChan := make(chan NewSubtreeRequest, 64)
	done := make(chan struct{})

	go func() {
		for {
			select {
			case req := <-newSubtreeChan:
				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-done:
				return
			}
		}
	}()

	t.Cleanup(func() { close(done) })

	var opts []Options
	if disk {
		opts = append(opts, WithTxMapDirs([]string{t.TempDir(), t.TempDir()}))
	}

	stp, err := NewSubtreeProcessor(t.Context(), ulogger.TestLogger{}, tSettings, memory.New(), nil, utxoStore, newSubtreeChan, opts...)
	require.NoError(t, err)

	if disk {
		require.NotNil(t, stp.diskTxMap, "the processor runs on the disk tx map")
	}

	return stp, utxoStore
}

func lanePoolTxHash(i int) chainhash.Hash {
	return chainhash.HashH([]byte{0x1a, 0x7e, byte(i), byte(i >> 8)})
}

func lanePoolInpoints(i int) *subtreepkg.TxInpoints {
	p := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{chainhash.HashH([]byte{0x9a, byte(i)})}, []uint32{1, 0})

	return &p
}

// requirePoolPositions checks the lane position the pool keeps for each tx.
func requirePoolPositions(t *testing.T, stp *SubtreeProcessor, want map[chainhash.Hash]uint64) {
	t.Helper()

	for hash, position := range want {
		require.True(t, stp.currentTxMap.Exists(hash), "tx %s is in the pool", hash)
		require.Equal(t, position, stp.currentTxMap.Position(hash), "tx %s", hash)
	}
}

func TestPoolKeepsLanePositionThroughConflictRemoval(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(map[bool]string{false: "in-memory map", true: "disk map"}[disk], func(t *testing.T) {
			stp, _ := lanePoolProcessor(t, disk, true)

			want := make(map[chainhash.Hash]uint64)

			// two chained subtrees and a current one: coinbase plus three, four, then two
			for i := 0; i < 9; i++ {
				hash := lanePoolTxHash(i)
				position := lane.Pack(uint8(3+i%2), uint64(100+i))

				if i == 4 {
					position = 0 // a tx that reached block assembly without one
				}

				require.NoError(t, stp.addNodeAt(subtreepkg.Node{Hash: hash, Fee: 1, SizeInBytes: 1}, lanePoolInpoints(i), position, true))
				want[hash] = position
			}

			require.Len(t, stp.chainedSubtrees, 2)

			// Removing a tx from the first chained subtree re-chains every subtree after it.
			removed := lanePoolTxHash(1)
			require.NoError(t, stp.removeTxFromSubtrees(context.Background(), removed))
			delete(want, removed)

			require.False(t, stp.currentTxMap.Exists(removed))
			requirePoolPositions(t, stp, want)
		})
	}
}

func TestPoolKeepsLanePositionThroughReload(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(map[bool]string{false: "in-memory map", true: "disk map"}[disk], func(t *testing.T) {
			stp, _ := lanePoolProcessor(t, disk, true)

			want := make(map[chainhash.Hash]uint64)
			txs := make([]*utxo.UnminedTransaction, 0, 6)

			for i := 0; i < 6; i++ {
				hash := lanePoolTxHash(100 + i)
				position := lane.Pack(9, uint64(5_000+i))

				if i == 2 {
					position = 0
				}

				txs = append(txs, &utxo.UnminedTransaction{
					Node:         &subtreepkg.Node{Hash: hash, Fee: 1, SizeInBytes: 1},
					TxInpoints:   lanePoolInpoints(i),
					LanePosition: position,
				})
				want[hash] = position
			}

			require.NoError(t, stp.AddNodesDirectly(txs[:4], true))

			// the reload's one-at-a-time path
			for _, tx := range txs[4:] {
				require.NoError(t, stp.AddDirectlyAt(tx.Node, tx.TxInpoints, tx.LanePosition, true, false))
			}

			requirePoolPositions(t, stp, want)
		})
	}
}

func TestPoolKeepsLanePositionThroughBlockMovementDequeue(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(map[bool]string{false: "in-memory map", true: "disk map"}[disk], func(t *testing.T) {
			stp, _ := lanePoolProcessor(t, disk, true)

			want := make(map[chainhash.Hash]uint64)

			batch := func(start int) ([]subtreepkg.Node, []*subtreepkg.TxInpoints, []uint64) {
				nodes := make([]subtreepkg.Node, 3)
				inpoints := make([]*subtreepkg.TxInpoints, 3)
				positions := make([]uint64, 3)

				for i := range nodes {
					hash := lanePoolTxHash(start + i)
					nodes[i] = subtreepkg.Node{Hash: hash, Fee: 1, SizeInBytes: 1}
					inpoints[i] = lanePoolInpoints(start + i)
					positions[i] = lane.Pack(4, uint64(start+i+1))
					want[hash] = positions[i]
				}

				return nodes, inpoints, positions
			}

			// the sequential drain during a block movement
			nodes, inpoints, positions := batch(200)
			require.Equal(t, Admitted, stp.queue.enqueuePositionedBatchBounded(nodes, inpoints, positions, false))
			require.NoError(t, stp.dequeueDuringBlockMovement(nil, nil, nil, true))

			// the parallel drain after a block, with one batch without positions
			nodes, inpoints, positions = batch(300)
			plainNodes, plainInpoints, _ := batch(400)

			for _, node := range plainNodes {
				want[node.Hash] = 0
			}

			_, err := stp.addDrainedBatches(context.Background(), []*TxBatch{
				{nodes: nodes, txInpoints: inpoints, positions: positions},
				{nodes: plainNodes, txInpoints: plainInpoints},
			}, &deferredBlockDrain{})
			require.NoError(t, err)

			requirePoolPositions(t, stp, want)
		})
	}
}

// laneMoveBackBlock stores the txs as a block's one subtree, with the coinbase placeholder first,
// and returns the block and what moveBackBlockBulkBuild is handed for it.
func laneMoveBackBlock(t *testing.T, stp *SubtreeProcessor, hashes []chainhash.Hash) (*model.Block, [][]subtreepkg.Node, [][]subtreepkg.TxInpoints) {
	t.Helper()

	st, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	for _, hash := range hashes {
		require.NoError(t, st.AddNode(hash, 1, 1))
	}

	inpoints := make([]subtreepkg.TxInpoints, len(st.Nodes))
	for i := range hashes {
		inpoints[i+1] = *lanePoolInpoints(i)
	}

	block := &model.Block{Header: prevBlockHeader, Subtrees: []*chainhash.Hash{st.RootHash()}}

	return block, [][]subtreepkg.Node{st.Nodes}, [][]subtreepkg.TxInpoints{inpoints}
}

// createWithLanePosition creates a tx in the UTXO store with position, 0 for none.
func createWithLanePosition(t *testing.T, store utxo.Store, seed byte, position uint64) chainhash.Hash {
	t.Helper()

	parentHash := chainhash.Hash{0x7a, seed}

	tx := bt.NewTx()
	tx.Inputs = []*bt.Input{{PreviousTxOutIndex: 0, PreviousTxSatoshis: 2000, UnlockingScript: bscript.NewFromBytes([]byte{})}}
	require.NoError(t, tx.Inputs[0].PreviousTxIDAdd(&parentHash))
	require.NoError(t, tx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000+uint64(seed)))

	opts := []utxo.CreateOption{utxo.WithCreateOnly()}
	if position != 0 {
		opts = append(opts, utxo.WithLanePosition(position))
	}

	_, _, err := store.SpendAndCreate(t.Context(), tx, 0, opts...)
	require.NoError(t, err)

	return *tx.TxIDChainHash()
}

// laneReadRecordingStore records the fields each BatchDecorate asks for.
type laneReadRecordingStore struct {
	utxo.Store
	reads [][]fields.FieldName
}

func (s *laneReadRecordingStore) BatchDecorate(ctx context.Context, items []*utxo.UnresolvedMetaData, fieldNames ...fields.FieldName) error {
	s.reads = append(s.reads, fieldNames)

	return s.Store.BatchDecorate(ctx, items, fieldNames...)
}

func TestPoolKeepsLanePositionThroughMoveBack(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(map[bool]string{false: "in-memory map", true: "disk map"}[disk], func(t *testing.T) {
			stp, store := lanePoolProcessor(t, disk, true)

			first := createWithLanePosition(t, store, 1, lane.Pack(12, 3_000))
			second := createWithLanePosition(t, store, 2, 0)
			gone := lanePoolTxHash(500) // in no store: its record was pruned

			block, nodes, inpoints := laneMoveBackBlock(t, stp, []chainhash.Hash{first, second, gone})

			_, _, err := stp.moveBackBlockBulkBuild(context.Background(), block, true, nil, nil, nodes, inpoints, nil)
			require.NoError(t, err)

			requirePoolPositions(t, stp, map[chainhash.Hash]uint64{first: lane.Pack(12, 3_000), second: 0, gone: 0})
		})
	}
}

func TestPoolKeepsNoLanePositionAtOff(t *testing.T) {
	stp, store := lanePoolProcessor(t, false, false)
	recording := &laneReadRecordingStore{Store: store}
	stp.utxoStore = recording

	first := createWithLanePosition(t, store, 3, lane.Pack(12, 4_000))

	block, nodes, inpoints := laneMoveBackBlock(t, stp, []chainhash.Hash{first})

	_, _, err := stp.moveBackBlockBulkBuild(context.Background(), block, true, nil, nil, nodes, inpoints, nil)
	require.NoError(t, err)

	require.Zero(t, stp.currentTxMap.Position(first), "at off the pool keeps no position")
	require.Empty(t, recording.reads, "at off a move-back reads nothing from the UTXO store for lanes")
}

// TestPoolKeepsLanePositionThroughTheSequentialCopyAfterABlock: after a block, fewer pool txs than
// blockassembly_parallelSetIfNotExistsThreshold are copied into the new half of the double buffer
// one at a time, on our own block's path and on the remainder path of a peer's block. Each keeps
// its lane position, on both maps; at off none is copied.
func TestPoolKeepsLanePositionThroughTheSequentialCopyAfterABlock(t *testing.T) {
	for _, disk := range []bool{false, true} {
		for _, shadow := range []bool{true, false} {
			name := map[bool]string{false: "in-memory map", true: "disk map"}[disk] + map[bool]string{true: ", shadow", false: ", off"}[shadow]

			t.Run(name, func(t *testing.T) {
				for _, path := range []string{"own block", "peer block remainder"} {
					stp, _ := lanePoolProcessor(t, disk, shadow)
					require.Greater(t, stp.settings.BlockAssembly.ParallelSetIfNotExistsThreshold, 3)

					// the half the block found the pool in, holding three txs with positions
					var old TxInpointsMap = NewSplitTxInpointsMap(16)

					if disk {
						m, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: []string{t.TempDir()}})
						require.NoError(t, err)
						t.Cleanup(func() { _ = m.Close() })

						old = m
					}

					st, err := subtreepkg.NewTreeByLeafCount(4)
					require.NoError(t, err)

					want := make(map[chainhash.Hash]uint64)

					for i := 0; i < 3; i++ {
						hash := lanePoolTxHash(600 + i)
						position := lane.Pack(11, uint64(70+i))

						require.NoError(t, st.AddNode(hash, 1, 1))
						old.SetAt(hash, lanePoolInpoints(i), position)

						want[hash] = position
						if !shadow {
							want[hash] = 0
						}
					}

					block := &model.Block{Header: prevBlockHeader}

					if path == "own block" {
						require.NoError(t, stp.processOwnBlockSubtreeNodes(block, st.Nodes, old, 0, nil, true))
					} else {
						require.NoError(t, stp.processRemainderTxHashes(context.Background(), []*subtreepkg.Subtree{st}, frozenEmptyHashSet(), nil, nil, old, true))
					}

					requirePoolPositions(t, stp, want)
				}
			})
		}
	}
}

// frozenEmptyHashSet is a block's transaction set holding none of the pool's txs.
func frozenEmptyHashSet() *SortedHashSet {
	s := NewSortedHashSet(0)
	_ = s.Freeze(1)

	return s
}

// TestPoolKeepsLanePositionAcrossMapKinds: when a disk half of the double buffer cannot be
// created, a block movement copies the pool between a disk map and an in-memory map, one way or
// the other. At shadow each tx keeps its lane position across the copy.
func TestPoolKeepsLanePositionAcrossMapKinds(t *testing.T) {
	for _, toDisk := range []bool{false, true} {
		t.Run(map[bool]string{false: "disk to in-memory", true: "in-memory to disk"}[toDisk], func(t *testing.T) {
			stp, _ := lanePoolProcessor(t, toDisk, true)

			var src TxInpointsMap = NewSplitTxInpointsMap(16)

			if !toDisk {
				m, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: []string{t.TempDir()}})
				require.NoError(t, err)
				t.Cleanup(func() { _ = m.Close() })

				src = m
			}

			nodes := make([]subtreepkg.Node, 0, 4)
			want := make(map[chainhash.Hash]uint64)

			for i := 0; i < 4; i++ {
				hash := lanePoolTxHash(700 + i)
				position := lane.Pack(12, uint64(300+i))

				src.SetAt(hash, lanePoolInpoints(i), position)
				nodes = append(nodes, subtreepkg.Node{Hash: hash, Fee: 1, SizeInBytes: 1})
				want[hash] = position
			}

			wasInserted := make([]bool, len(nodes))
			require.NoError(t, stp.parallelGetAndSetIfNotExists(nodes, src, 0, 2, wasInserted))

			for i := range wasInserted {
				require.True(t, wasInserted[i])
			}

			requirePoolPositions(t, stp, want)
		})
	}
}
