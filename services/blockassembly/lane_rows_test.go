package blockassembly

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockassembly/mining"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blobOptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// The lane gate for the blocks this node mines (lanes Phase 1, P2b). The transactions reach block
// assembly through the columnar gRPC handler with their lane positions, as the validator sends
// them at shadow; block assembly seals them into subtrees, stores each with its lane bitmaps, and
// stores the block it mines with its lane map rows, read back here from a real blockchain store.

// laneRowsReader reads a block's stored lane map rows (stores/blockchain/sql).
type laneRowsReader interface {
	LaneMapRows(ctx context.Context, blockID uint32) ([]lane.MapRow, error)
}

// lastOpts is the StoreBlockOptions of the last AddBlock.
func (c *addBlockOptionsSpyClient) lastOpts() *options.StoreBlockOptions {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.opts
}

// ownLaneTxs is the block's transactions after its coinbase: four subtrees' worth at four nodes a
// subtree, so the candidate is two complete subtrees. One has no position, which block assembly
// counts and still mines, and lanes 3 and 9 each have a hole.
//
// The second subtree's first node, ownLaneTxs[3], is a real transaction with a position: only a
// block's first subtree starts with the coinbase placeholder.
var ownLaneTxs = []uint64{
	lane.Pack(3, 10), lane.Pack(3, 11), lane.Pack(9, 500),
	lane.Pack(3, 12), lane.Pack(9, 502), 0, lane.Pack(3, 14),
}

// ownLaneBlock is what mining a block from ownLaneTxs left behind.
type ownLaneBlock struct {
	ctx          context.Context
	block        *model.Block
	addBlockOpts *addBlockOptionsSpyClient
	rows         []lane.MapRow
	subtreeStore *memory.Memory
}

// mineOwnLaneBlock runs block assembly at shadow or off on the blockchain store at storeURL, hands
// it ownLaneTxs with their positions through AddTxBatchColumnar, mines the candidate and returns the
// block, the AddBlock options and the rows the store holds for it.
func mineOwnLaneBlock(t *testing.T, storeURL string, shadow bool) *ownLaneBlock {
	t.Helper()

	initPrometheusMetrics()

	common := testutil.NewCommonTestSetup(t)

	const subtreeSize = 4
	common.Settings.BlockAssembly.InitialMerkleItemsPerSubtree = subtreeSize
	common.Settings.BlockAssembly.MinimumMerkleItemsPerSubtree = subtreeSize
	common.Settings.BlockAssembly.SubmitMiningSolutionWaitForResponse = true

	if shadow {
		common.Settings.UtxoStore.LaneMigration = settings.LaneMigrationShadow
	}

	parsed, err := url.Parse(storeURL)
	require.NoError(t, err)

	store, err := blockchainstore.NewStore(common.Logger, parsed, common.Settings)
	require.NoError(t, err)

	realBlockchainClient, err := blockchain.NewLocalClient(common.Logger, common.Settings, store, nil, nil)
	require.NoError(t, err)

	spy := &addBlockOptionsSpyClient{ClientI: realBlockchainClient}
	subtreeStore := memory.New()

	ctx, cancel := context.WithCancel(common.Ctx)

	utxoStore := testutil.NewSQLiteMemoryUTXOStore(ctx, common.Logger, common.Settings, t)
	_ = utxoStore.SetBlockHeight(123)

	server := New(common.Logger, common.Settings, nil, utxoStore, subtreeStore, spy)
	server.SetSkipWaitForPendingBlocks(true)
	require.NoError(t, server.Init(ctx))
	require.NoError(t, realBlockchainClient.Run(ctx, "blockassembly-lane-test"))

	t.Cleanup(func() {
		cancel()
		_ = server.Stop(context.Background())

		if server.blockAssembler != nil {
			server.blockAssembler.Wait()
		}
	})

	require.NoError(t, server.blockAssembler.Start(ctx))
	require.Eventually(t, func() bool {
		return server.blockAssembler.GetCurrentRunningState() == StateRunning
	}, 5*time.Second, 50*time.Millisecond, "block assembler did not reach Running state")

	_, err = server.AddTxBatchColumnar(ctx, ownLaneColumnarBatch(t))
	require.NoError(t, err)

	var candidate *model.MiningCandidate

	require.Eventually(t, func() bool {
		candidate, err = server.GetMiningCandidate(ctx, &blockassembly_api.GetMiningCandidateRequest{IncludeSubtrees: true})
		if err != nil || candidate == nil || len(candidate.SubtreeHashes) != 2 {
			return false
		}

		for _, subtreeHash := range candidate.SubtreeHashes {
			if _, getErr := subtreeStore.Get(ctx, subtreeHash, fileformat.FileTypeSubtree); getErr != nil {
				return false
			}
		}

		return true
	}, 10*time.Second, 50*time.Millisecond, "the two complete subtrees were not stored")

	solution, err := mining.Mine(ctx, server.settings, candidate, nil)
	require.NoError(t, err)

	resp, err := server.SubmitMiningSolution(ctx, &blockassembly_api.SubmitMiningSolutionRequest{
		Id:         candidate.Id,
		Nonce:      solution.Nonce,
		Time:       solution.Time,
		Version:    solution.Version,
		CoinbaseTx: solution.Coinbase,
	})
	require.NoError(t, err)
	require.True(t, resp.Ok)

	bestHeader, _, err := realBlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	block, err := realBlockchainClient.GetBlock(ctx, bestHeader.Hash())
	require.NoError(t, err)

	_, meta, err := realBlockchainClient.GetBlockHeader(ctx, bestHeader.Hash())
	require.NoError(t, err)

	reader, ok := store.(laneRowsReader)
	require.True(t, ok, "the blockchain store reads lane map rows")

	rows, err := reader.LaneMapRows(ctx, meta.ID)
	require.NoError(t, err)

	return &ownLaneBlock{ctx: ctx, block: block, addBlockOpts: spy, rows: rows, subtreeStore: subtreeStore}
}

// ownLaneColumnarBatch is ownLaneTxs as the validator's client sends them at shadow.
func ownLaneColumnarBatch(t *testing.T) *blockassembly_api.AddTxBatchColumnarRequest {
	t.Helper()

	req := &blockassembly_api.AddTxBatchColumnarRequest{
		ParentTxOffsets:   []uint32{0},
		VoutIdxsTxOffsets: []uint32{0},
		LanePositions:     ownLaneTxs,
	}

	for i := range ownLaneTxs {
		txid := chainhash.HashH(fmt.Appendf(nil, "own-lane-block-tx-%d", i))
		parent := chainhash.HashH(fmt.Appendf(nil, "own-lane-block-parent-%d", i))

		req.TxidsPacked = append(req.TxidsPacked, txid[:]...)
		req.Fees = append(req.Fees, uint64(1000+i))
		req.Sizes = append(req.Sizes, 250)
		req.ParentTxHashesPacked = append(req.ParentTxHashesPacked, parent[:]...)
		req.ParentTxOffsets = append(req.ParentTxOffsets, uint32(i+1))
		req.VoutIdxsPacked = append(req.VoutIdxsPacked, 1, 0)
		req.VoutIdxsTxOffsets = append(req.VoutIdxsTxOffsets, uint32(len(req.VoutIdxsPacked)))
	}

	return req
}

// wantOwnLaneRows is the rows ownLaneTxs' members make, lane by lane.
func wantOwnLaneRows(t *testing.T) []lane.MapRow {
	t.Helper()

	byLane := map[uint8]*roaring64.Bitmap{}

	for _, v := range ownLaneTxs {
		if v == 0 {
			continue
		}

		l, p := lane.Unpack(v)
		if byLane[l] == nil {
			byLane[l] = roaring64.New()
		}

		byLane[l].Add(p)
	}

	var rows []lane.MapRow

	for _, l := range []uint8{3, 9} {
		laneRows, err := lane.BuildMapRows(l, byLane[l])
		require.NoError(t, err)

		rows = append(rows, laneRows...)
	}

	return rows
}

// ownLaneMetric sums a lane metric's counters, or its histograms' observation counts, over the
// series whose labels include every one given.
func ownLaneMetric(t *testing.T, metric string, labels map[string]string) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	var sum float64

	for _, f := range families {
		if f.GetName() != metric {
			continue
		}

		for _, m := range f.GetMetric() {
			matched := 0

			for _, l := range m.GetLabel() {
				if want, ok := labels[l.GetName()]; ok && want == l.GetValue() {
					matched++
				}
			}

			if matched != len(labels) {
				continue
			}

			if m.GetCounter() != nil {
				sum += m.GetCounter().GetValue()
			}

			if m.GetHistogram() != nil {
				sum += float64(m.GetHistogram().GetSampleCount())
			}
		}
	}

	return sum
}

func TestOwnBlockLaneRows(t *testing.T) {
	stores := map[string]func(t *testing.T) string{
		"sqlite": func(*testing.T) string { return "sqlitememory:///" },
		"postgres": func(t *testing.T) string {
			connStr, teardown, err := postgres.SetupTestPostgresContainer()
			require.NoError(t, err)
			t.Cleanup(func() { _ = teardown() })

			return connStr
		},
	}

	for name, storeURL := range stores {
		t.Run(name, func(t *testing.T) {
			t.Run("shadow: the block's rows equal its members per lane", func(t *testing.T) {
				own := map[string]string{"block": lane.BlockOwn}
				fewer := map[string]string{"block": lane.BlockOwn, "direction": lane.MismatchFewer}
				unpositioned := map[string]string{"service": laneBitmapsService}

				builds := ownLaneMetric(t, "teranode_lane_gate_seconds", map[string]string{"block": lane.BlockOwn, "stage": lane.GateStageBuild})
				inserts := ownLaneMetric(t, "teranode_lane_gate_seconds", map[string]string{"block": lane.BlockOwn, "stage": lane.GateStageInsert})
				mismatches := ownLaneMetric(t, "teranode_lane_row_count_mismatches_total", fewer)
				withoutPosition := ownLaneMetric(t, "teranode_lane_txs_without_position_total", unpositioned)
				missing := ownLaneMetric(t, "teranode_lane_missing_bitmaps_total", own)

				mined := mineOwnLaneBlock(t, storeURL(t), true)

				require.Equal(t, uint64(len(ownLaneTxs)+1), mined.block.TransactionCount, "the tx without a position is mined")

				opts := mined.addBlockOpts.lastOpts()
				require.True(t, opts.LaneMapRowsOwnBlock, "the rows go as our own block's")
				require.NotEmpty(t, opts.LaneMapRows)

				want := wantOwnLaneRows(t)
				require.Len(t, mined.rows, len(want))

				for i := range want {
					same, err := lane.SameRow(want[i], mined.rows[i])
					require.NoError(t, err)
					require.True(t, same, "row %d: want %+v, stored %+v", i, want[i], mined.rows[i])
				}

				for _, subtreeHash := range mined.block.Subtrees {
					data, err := mined.subtreeStore.Get(mined.ctx, subtreeHash[:], fileformat.FileTypeSubtreeLaneBitmaps)
					require.NoError(t, err, "subtree %s is stored with its lane bitmaps", subtreeHash)

					bitmaps, err := lane.DecodeSubtreeBitmaps(data)
					require.NoError(t, err)
					require.Equal(t, *subtreeHash, bitmaps.SubtreeHash)
				}

				require.Equal(t, builds+1, ownLaneMetric(t, "teranode_lane_gate_seconds", map[string]string{"block": lane.BlockOwn, "stage": lane.GateStageBuild}))
				require.Equal(t, inserts+1, ownLaneMetric(t, "teranode_lane_gate_seconds", map[string]string{"block": lane.BlockOwn, "stage": lane.GateStageInsert}), "the store times the insert under the own label")
				require.Equal(t, mismatches+1, ownLaneMetric(t, "teranode_lane_row_count_mismatches_total", fewer), "six members for seven txs besides the coinbase")
				require.Equal(t, withoutPosition+1, ownLaneMetric(t, "teranode_lane_txs_without_position_total", unpositioned))
				require.Equal(t, missing, ownLaneMetric(t, "teranode_lane_missing_bitmaps_total", own))
			})

			t.Run("off: nothing written or sent", func(t *testing.T) {
				own := map[string]string{"block": lane.BlockOwn}
				missing := ownLaneMetric(t, "teranode_lane_missing_bitmaps_total", own)
				builds := ownLaneMetric(t, "teranode_lane_gate_seconds", own)

				// the client sends positions, as a validator at shadow would; block assembly at off keeps none
				mined := mineOwnLaneBlock(t, storeURL(t), false)

				require.Equal(t, missing, ownLaneMetric(t, "teranode_lane_missing_bitmaps_total", own), "at off nothing is looked for")
				require.Equal(t, builds, ownLaneMetric(t, "teranode_lane_gate_seconds", own), "nor timed")

				require.Equal(t, uint64(len(ownLaneTxs)+1), mined.block.TransactionCount)

				opts := mined.addBlockOpts.lastOpts()
				require.Nil(t, opts.LaneMapRows)
				require.False(t, opts.LaneMapRowsOwnBlock)
				require.Empty(t, mined.rows)

				for _, subtreeHash := range mined.block.Subtrees {
					_, err := mined.subtreeStore.Get(mined.ctx, subtreeHash[:], fileformat.FileTypeSubtreeLaneBitmaps)
					require.Error(t, err, "no lane bitmaps file at off")
				}
			})
		})
	}
}

// TestOwnLaneBitmapsKeepsOnlyRecentSubtrees: the kept bitmaps of a subtree go once it was last
// stored more than ownLaneBitmapsKeepBlocks blocks ago, and storing it again keeps it.
func TestOwnLaneBitmapsKeepsOnlyRecentSubtrees(t *testing.T) {
	var kept ownLaneBitmaps

	hashOf := func(i uint32) *chainhash.Hash {
		h := chainhash.HashH(binary.LittleEndian.AppendUint32(nil, i))
		return &h
	}

	kept.put(*hashOf(1), lane.LaneBitmaps{}, 100)
	kept.put(*hashOf(2), lane.LaneBitmaps{}, 100)

	require.True(t, kept.touch(*hashOf(2), 105), "stored again five blocks later")

	kept.put(*hashOf(3), lane.LaneBitmaps{}, 100+ownLaneBitmapsKeepBlocks+1)

	_, missing := kept.get([]*chainhash.Hash{hashOf(1)})
	require.Equal(t, 1, missing, "last stored more than the margin ago")

	_, missing = kept.get([]*chainhash.Hash{hashOf(2), hashOf(3)})
	require.Zero(t, missing)
	require.Equal(t, 2, kept.len())
	require.False(t, kept.touch(*hashOf(1), 200))
}

// setOrderStore records the file type and delete-at height of each Set, in order.
type setOrderStore struct {
	blob.Store

	mu   sync.Mutex
	sets []fileformat.FileType
	dahs map[fileformat.FileType]uint32
}

func (s *setOrderStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...blobOptions.FileOption) error {
	s.mu.Lock()
	s.sets = append(s.sets, fileType)

	if s.dahs == nil {
		s.dahs = map[fileformat.FileType]uint32{}
	}

	s.dahs[fileType] = blobOptions.NewFileOptions(opts...).DAH
	s.mu.Unlock()

	return s.Store.Set(ctx, key, fileType, value, opts...)
}

func (s *setOrderStore) index(fileType fileformat.FileType) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, ft := range s.sets {
		if ft == fileType {
			return i
		}
	}

	return -1
}

// TestStoreSubtreeDataWritesLaneBitmapsBeforeTheSubtree: at shadow a stored subtree's lane bitmaps
// file holds its members' positions, is written before the subtree file with the same delete-at
// height, and is kept in memory for a block mined from it, on both pool maps and whether or not a
// subtree meta file is written. A member without a position is counted and left out.
func TestStoreSubtreeDataWritesLaneBitmapsBeforeTheSubtree(t *testing.T) {
	positions := []uint64{lane.Pack(2, 40), 0, lane.Pack(2, 42)}

	for _, tc := range []struct {
		name string
		disk bool
		meta bool
	}{
		{"in-memory map, with meta", false, true},
		{"disk map, with meta", true, true},
		{"in-memory map, without meta", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, memoryStore := setupServerWithSettings(t, func(s *settings.Settings) {
				s.UtxoStore.LaneMigration = settings.LaneMigrationShadow
				s.BlockAssembly.StoreTxInpointsForSubtreeMeta = tc.meta
			})

			store := &setOrderStore{Store: memoryStore}
			server.subtreeStore = store

			var txMap subtreeprocessor.TxInpointsMap = subtreeprocessor.NewSplitTxInpointsMap(4)

			if tc.disk {
				disk, err := subtreeprocessor.NewDiskTxMap(subtreeprocessor.DiskTxMapOptions{BasePaths: []string{t.TempDir()}})
				require.NoError(t, err)
				t.Cleanup(func() { _ = disk.Close() })

				txMap = disk
			}

			st, err := subtreepkg.NewTreeByLeafCount(4)
			require.NoError(t, err)
			require.NoError(t, st.AddCoinbaseNode())

			for i, position := range positions {
				hash := chainhash.HashH(fmt.Appendf(nil, "lane-seal-%s-%d", tc.name, i))
				require.NoError(t, st.AddNode(hash, 1, 1))
				txMap.SetAt(hash, singleParentInpointsPtr(chainhash.HashH([]byte{byte(i)}), 0), position)
			}

			unpositioned := map[string]string{"service": laneBitmapsService}
			before := ownLaneMetric(t, "teranode_lane_txs_without_position_total", unpositioned)

			subtreeDone, allDone, err := server.storeSubtreeData(t.Context(), subtreeprocessor.NewSubtreeRequest{Subtree: st, ParentTxMap: txMap}, make(chan *subtreeRetrySend, 10))
			require.NoError(t, err)
			require.True(t, <-subtreeDone)
			<-allDone

			bitmapsAt, subtreeAt := store.index(fileformat.FileTypeSubtreeLaneBitmaps), store.index(fileformat.FileTypeSubtree)
			require.GreaterOrEqual(t, bitmapsAt, 0, "the lane bitmaps file is written")
			require.Less(t, bitmapsAt, subtreeAt, "before the subtree file")
			require.Equal(t, store.dahs[fileformat.FileTypeSubtree], store.dahs[fileformat.FileTypeSubtreeLaneBitmaps], "with the subtree's delete-at height")

			data, err := memoryStore.Get(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtreeLaneBitmaps)
			require.NoError(t, err)

			bitmaps, err := lane.DecodeSubtreeBitmaps(data)
			require.NoError(t, err)
			require.Equal(t, []uint64{40, 42}, bitmaps.Lanes[2].ToArray())

			kept, missing := server.ownLaneBitmaps.get([]*chainhash.Hash{st.RootHash()})
			require.Zero(t, missing)
			require.Equal(t, []uint64{40, 42}, kept[0][2].ToArray())

			require.Equal(t, before+1, ownLaneMetric(t, "teranode_lane_txs_without_position_total", unpositioned))

			// The same subtree stored again keeps its bitmaps and counts nothing again.
			_, _, err = server.storeSubtreeData(t.Context(), subtreeprocessor.NewSubtreeRequest{Subtree: st, ParentTxMap: txMap}, make(chan *subtreeRetrySend, 10))
			require.ErrorIs(t, err, errors.ErrBlobAlreadyExists)
			require.Equal(t, before+1, ownLaneMetric(t, "teranode_lane_txs_without_position_total", unpositioned))
			require.Equal(t, 1, server.ownLaneBitmaps.len())
		})
	}
}

// TestStoreSubtreeDataAtOffWritesNoLaneBitmaps: at off a stored subtree has no lane bitmaps file and
// nothing is kept, even when the pool holds positions.
func TestStoreSubtreeDataAtOffWritesNoLaneBitmaps(t *testing.T) {
	server, memoryStore := setupServerWithSettings(t, func(*settings.Settings) {})

	txMap := subtreeprocessor.NewSplitTxInpointsMap(4)

	st, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	hash := chainhash.HashH([]byte("lane-seal-off"))
	require.NoError(t, st.AddNode(hash, 1, 1))
	txMap.SetAt(hash, singleParentInpointsPtr(chainhash.HashH([]byte("lane-seal-off-parent")), 0), lane.Pack(2, 1))

	subtreeDone, allDone, err := server.storeSubtreeData(t.Context(), subtreeprocessor.NewSubtreeRequest{Subtree: st, ParentTxMap: txMap}, make(chan *subtreeRetrySend, 10))
	require.NoError(t, err)
	require.True(t, <-subtreeDone)
	<-allDone

	_, err = memoryStore.Get(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtreeLaneBitmaps)
	require.Error(t, err)
	require.Zero(t, server.ownLaneBitmaps.len())
}

// failingPositionMap is a pool map whose position reads fail, as a disk map's do when a record is
// lost, and that refuses the read that records the error on the map.
type failingPositionMap struct {
	subtreeprocessor.TxInpointsMap
}

func (failingPositionMap) PositionWithErr(chainhash.Hash) (uint64, error) {
	return 0, errors.NewStorageError("disk tx map: read failed")
}

func (failingPositionMap) Position(chainhash.Hash) uint64 {
	panic("the storage worker must not record a read error on the pool map")
}

// TestSubtreeLanePositionsReadsWithoutRecordingErrors: the storage worker reads positions with
// PositionWithErr, so a failed read leaves no error on the pool map for the processor to take for
// its own; the tx is counted as without a position.
func TestSubtreeLanePositionsReadsWithoutRecordingErrors(t *testing.T) {
	st, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	require.NoError(t, st.AddNode(chainhash.HashH([]byte("lane-failing-read")), 1, 1))

	laneBits := subtreeLanePositions(st, failingPositionMap{TxInpointsMap: subtreeprocessor.NewSplitTxInpointsMap(1)})
	require.Equal(t, 1, laneBits.Unpositioned())
}
