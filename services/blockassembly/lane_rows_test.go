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
	txmap "github.com/bsv-blockchain/go-tx-map"
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

// ownLaneServer is block assembly running at shadow or off on the blockchain store at storeURL,
// with the pieces the own-block tests read.
type ownLaneServer struct {
	ctx          context.Context
	server       *BlockAssembly
	client       blockchain.ClientI
	spy          *addBlockOptionsSpyClient
	store        blockchainstore.Store
	subtreeStore *memory.Memory
}

// startOwnLaneServer starts block assembly at shadow or off on the blockchain store at storeURL.
func startOwnLaneServer(t *testing.T, storeURL string, shadow bool) *ownLaneServer {
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

	return &ownLaneServer{ctx: ctx, server: server, client: realBlockchainClient, spy: spy, store: store, subtreeStore: subtreeStore}
}

// candidateOfTwoSubtrees hands the server ownLaneTxs through AddTxBatchColumnar and returns the
// candidate once its two complete subtrees are stored.
func (o *ownLaneServer) candidateOfTwoSubtrees(t *testing.T) *model.MiningCandidate {
	t.Helper()

	_, err := o.server.AddTxBatchColumnar(o.ctx, ownLaneColumnarBatch(t))
	require.NoError(t, err)

	var candidate *model.MiningCandidate

	require.Eventually(t, func() bool {
		candidate, err = o.server.GetMiningCandidate(o.ctx, &blockassembly_api.GetMiningCandidateRequest{IncludeSubtrees: true})
		if err != nil || candidate == nil || len(candidate.SubtreeHashes) != 2 {
			return false
		}

		for _, subtreeHash := range candidate.SubtreeHashes {
			if _, getErr := o.subtreeStore.Get(o.ctx, subtreeHash, fileformat.FileTypeSubtree); getErr != nil {
				return false
			}
		}

		return true
	}, 10*time.Second, 50*time.Millisecond, "the two complete subtrees were not stored")

	return candidate
}

// mineOwnLaneBlock runs block assembly at shadow or off on the blockchain store at storeURL, hands
// it ownLaneTxs with their positions through AddTxBatchColumnar, mines the candidate and returns the
// block, the AddBlock options and the rows the store holds for it.
func mineOwnLaneBlock(t *testing.T, storeURL string, shadow bool) *ownLaneBlock {
	t.Helper()

	o := startOwnLaneServer(t, storeURL, shadow)
	ctx, server, realBlockchainClient, spy, store, subtreeStore := o.ctx, o.server, o.client, o.spy, o.store, o.subtreeStore

	candidate := o.candidateOfTwoSubtrees(t)

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

	// two blocks later both are still kept: three heights are
	kept.put(*hashOf(4), lane.LaneBitmaps{}, 100+ownLaneBitmapsKeepBlocks)

	_, missing := kept.get([]*chainhash.Hash{hashOf(1), hashOf(2)})
	require.Zero(t, missing, "stored ownLaneBitmapsKeepBlocks blocks ago, still kept")

	require.True(t, kept.touch(*hashOf(2), 105), "stored again five blocks later")

	kept.put(*hashOf(3), lane.LaneBitmaps{}, 100+ownLaneBitmapsKeepBlocks+1)

	_, missing = kept.get([]*chainhash.Hash{hashOf(1)})
	require.Equal(t, 1, missing, "last stored more than the margin ago")

	_, missing = kept.get([]*chainhash.Hash{hashOf(2), hashOf(3)})
	require.Zero(t, missing)
	require.Equal(t, 3, kept.len())
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
	s.mu.Unlock()

	if err := s.Store.Set(ctx, key, fileType, value, opts...); err != nil {
		return err
	}

	// only a write the store took sets the file's delete-at height
	s.mu.Lock()

	if s.dahs == nil {
		s.dahs = map[fileformat.FileType]uint32{}
	}

	s.dahs[fileType] = blobOptions.NewFileOptions(opts...).DAH
	s.mu.Unlock()

	return nil
}

func (s *setOrderStore) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, dah uint32, opts ...blobOptions.FileOption) error {
	// as the file store does for a file that is not there
	if exists, err := s.Store.Exists(ctx, key, fileType, opts...); err == nil && !exists {
		return errors.ErrNotFound
	}

	s.mu.Lock()

	if s.dahs == nil {
		s.dahs = map[fileformat.FileType]uint32{}
	}

	s.dahs[fileType] = dah
	s.mu.Unlock()

	return s.Store.SetDAH(ctx, key, fileType, dah, opts...)
}

func (s *setOrderStore) dah(fileType fileformat.FileType) uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.dahs[fileType]
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

	txMap := subtreeprocessor.NewSplitTxInpointsMap(1)
	txMap.Set(st.Nodes[1].Hash, singleParentInpointsPtr(chainhash.HashH([]byte("lane-failing-parent")), 0))

	laneBits, lost := subtreeLanePositions(st, failingPositionMap{TxInpointsMap: txMap}, nil, nil)
	require.Zero(t, lost)
	require.Equal(t, 1, laneBits.Unpositioned())
}

// laneSealSubtree is a subtree of three positioned txs, after the coinbase placeholder, and the
// in-memory pool map that holds them.
func laneSealSubtree(t *testing.T, name string) (*subtreepkg.Subtree, subtreeprocessor.TxInpointsMap) {
	t.Helper()

	txMap := subtreeprocessor.NewSplitTxInpointsMap(4)

	st, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	for i := 0; i < 3; i++ {
		hash := chainhash.HashH(fmt.Appendf(nil, "lane-existing-%s-%d", name, i))
		require.NoError(t, st.AddNode(hash, 1, 1))
		txMap.SetAt(hash, singleParentInpointsPtr(chainhash.HashH([]byte{byte(i), 0x77}), 0), lane.Pack(4, uint64(20+i)))
	}

	// the root is computed once here, as the processor does before it sends a subtree, so test
	// goroutines that read it while a storage worker does never race on its lazy cache
	_ = st.RootHash()

	return st, txMap
}

// TestStoreSubtreeDataKeepsAnExistingSubtreesLaneBitmaps: a subtree the store already holds keeps
// its lane bitmaps and their file lives as long as the subtree, whether its bitmaps are kept in
// memory (stored again by this process) or not (after a restart, or first stored by subtree
// validation).
func TestStoreSubtreeDataKeepsAnExistingSubtreesLaneBitmaps(t *testing.T) {
	shadow := func(s *settings.Settings) { s.UtxoStore.LaneMigration = settings.LaneMigrationShadow }

	t.Run("kept, stored again at a later height", func(t *testing.T) {
		server, memoryStore := setupServerWithSettings(t, shadow)
		store := &setOrderStore{Store: memoryStore}
		server.subtreeStore = store

		st, txMap := laneSealSubtree(t, "kept")

		subtreeDone, allDone, err := server.storeSubtreeData(t.Context(), subtreeprocessor.NewSubtreeRequest{Subtree: st, ParentTxMap: txMap}, make(chan *subtreeRetrySend, 10))
		require.NoError(t, err)
		require.True(t, <-subtreeDone)
		<-allDone

		require.NoError(t, server.blockAssembler.utxoStore.SetBlockHeight(130))

		_, _, err = server.storeSubtreeData(t.Context(), subtreeprocessor.NewSubtreeRequest{Subtree: st, ParentTxMap: txMap}, make(chan *subtreeRetrySend, 10))
		require.ErrorIs(t, err, errors.ErrBlobAlreadyExists)

		require.Equal(t, store.dah(fileformat.FileTypeSubtree), store.dah(fileformat.FileTypeSubtreeLaneBitmaps), "the bitmaps file lives as long as the subtree")
		require.Equal(t, uint32(130)+server.settings.GlobalBlockHeightRetention, store.dah(fileformat.FileTypeSubtreeLaneBitmaps))
	})

	t.Run("kept, its file lost", func(t *testing.T) {
		server, memoryStore := setupServerWithSettings(t, shadow)
		store := &setOrderStore{Store: memoryStore}
		server.subtreeStore = store

		st, txMap := laneSealSubtree(t, "lost")

		subtreeDone, allDone, err := server.storeSubtreeData(t.Context(), subtreeprocessor.NewSubtreeRequest{Subtree: st, ParentTxMap: txMap}, make(chan *subtreeRetrySend, 10))
		require.NoError(t, err)
		require.True(t, <-subtreeDone)
		<-allDone

		// the file's first write failed, or it was lost, while the bitmaps stayed kept
		require.NoError(t, memoryStore.Del(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtreeLaneBitmaps))

		_, _, err = server.storeSubtreeData(t.Context(), subtreeprocessor.NewSubtreeRequest{Subtree: st, ParentTxMap: txMap}, make(chan *subtreeRetrySend, 10))
		require.ErrorIs(t, err, errors.ErrBlobAlreadyExists)

		exists, err := memoryStore.Exists(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtreeLaneBitmaps)
		require.NoError(t, err)
		require.True(t, exists, "storing the subtree again writes its lost lane bitmaps file")
		require.Equal(t, store.dah(fileformat.FileTypeSubtree), store.dah(fileformat.FileTypeSubtreeLaneBitmaps))
	})

	t.Run("not kept, the files already stored", func(t *testing.T) {
		server, memoryStore := setupServerWithSettings(t, shadow)
		store := &setOrderStore{Store: memoryStore}
		server.subtreeStore = store

		st, txMap := laneSealSubtree(t, "restart")

		subtreeBytes, err := st.Serialize()
		require.NoError(t, err)
		require.NoError(t, memoryStore.Set(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

		old, err := lane.EncodeSubtreeBitmaps(lane.SubtreeBitmaps{SubtreeHash: *st.RootHash(), Lanes: lane.LaneBitmaps{}})
		require.NoError(t, err)
		require.NoError(t, memoryStore.Set(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtreeLaneBitmaps, old, blobOptions.WithDeleteAt(5)))

		_, _, err = server.storeSubtreeData(t.Context(), subtreeprocessor.NewSubtreeRequest{Subtree: st, ParentTxMap: txMap}, make(chan *subtreeRetrySend, 10))
		require.ErrorIs(t, err, errors.ErrBlobAlreadyExists)

		kept, missing := server.ownLaneBitmaps.get([]*chainhash.Hash{st.RootHash()})
		require.Zero(t, missing, "the bitmaps are built from the pool and kept")
		require.Equal(t, []uint64{20, 21, 22}, kept[0][4].ToArray())
		require.Equal(t, store.dah(fileformat.FileTypeSubtree), store.dah(fileformat.FileTypeSubtreeLaneBitmaps), "the old file lives as long as the subtree")
	})
}

// TestOwnLaneRowsWaitsForBitmapsStillBeingStored: a block listing a subtree whose storage is still
// running gets its rows once the bitmaps are kept; a subtree still being stored past the wait
// counts as timed out, and one no storage is running for as absent.
func TestOwnLaneRowsWaitsForBitmapsStillBeingStored(t *testing.T) {
	previous := ownLaneBitmapsWait
	ownLaneBitmapsWait = 200 * time.Millisecond

	t.Cleanup(func() { ownLaneBitmapsWait = previous })

	server, _ := setupServerWithSettings(t, func(s *settings.Settings) { s.UtxoStore.LaneMigration = settings.LaneMigrationShadow })

	root := chainhash.HashH([]byte("lane-wait-root"))
	block := &model.Block{Header: laneTestHeader(), Subtrees: []*chainhash.Hash{&root}, TransactionCount: 2}

	t.Run("kept while waiting", func(t *testing.T) {
		server.ownLaneBitmaps.expect(root)

		go func() {
			time.Sleep(20 * time.Millisecond)
			server.ownLaneBitmaps.put(root, lane.LaneBitmaps{3: bitmapOfPositions(9)}, 123)
		}()

		rows := server.ownLaneRows(t.Context(), block)
		require.Len(t, rows, 1)
		require.Equal(t, uint8(3), rows[0].Lane)
	})

	for _, tc := range []struct {
		name    string
		pending bool
		reason  string
	}{
		{"still being stored", true, lane.MissingBitmapsTimedOut},
		{"no storage running", false, lane.MissingBitmapsAbsent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			other := chainhash.HashH([]byte("lane-wait-" + tc.name))
			if tc.pending {
				server.ownLaneBitmaps.expect(other)
			}

			labels := map[string]string{"block": lane.BlockOwn, "reason": tc.reason}
			before := ownLaneMetric(t, "teranode_lane_missing_bitmaps_total", labels)

			start := time.Now()
			rows := server.ownLaneRows(t.Context(), &model.Block{Header: laneTestHeader(), Subtrees: []*chainhash.Hash{&other}, TransactionCount: 2})

			require.Nil(t, rows)
			waited := time.Since(start)
			require.GreaterOrEqual(t, waited, ownLaneBitmapsWait, "it waited")
			require.Less(t, waited, ownLaneBitmapsWait+time.Second, "it waited no longer than the bound, with scheduling slack")
			require.Equal(t, before+1, ownLaneMetric(t, "teranode_lane_missing_bitmaps_total", labels))
		})
	}
}

func bitmapOfPositions(positions ...uint64) *roaring64.Bitmap {
	bm := roaring64.New()
	bm.AddMany(positions)

	return bm
}

// laneTestHeader is a block header with every hash set, so the block can name itself in a log.
func laneTestHeader() *model.BlockHeader {
	return &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}
}

// TestOwnLaneBitmapsWaitCountsAgainAsItEnds: a subtree whose storage began during the wait counts
// as timed out when the wait ends, not as absent.
func TestOwnLaneBitmapsWaitCountsAgainAsItEnds(t *testing.T) {
	root := chainhash.HashH([]byte("lane-wait-recount"))
	roots := []*chainhash.Hash{&root}

	t.Run("storage begins during the wait", func(t *testing.T) {
		var kept ownLaneBitmaps

		go func() {
			time.Sleep(20 * time.Millisecond)
			kept.expect(root)
		}()

		_, absent, timedOut := kept.waitFor(t.Context(), roots, 100*time.Millisecond)
		require.Zero(t, absent)
		require.Equal(t, 1, timedOut)
	})
}

// blockingReadMap is a pool map whose reads wait until release is closed, so the storage of a
// subtree that reads it stays in progress.
type blockingReadMap struct {
	subtreeprocessor.TxInpointsMap
	release chan struct{}
}

func (m *blockingReadMap) Get(hash chainhash.Hash) (*subtreepkg.TxInpoints, bool) {
	<-m.release

	return m.TxInpointsMap.Get(hash)
}

// TestSubtreeListenerMarksASubtreeAsBeingStored: a subtree handed to the storage listener is marked
// as being stored, so a block that lists it while its bitmaps are not kept yet counts it as timed
// out, not absent; once its storage ends nothing waits for it and the block gets its rows.
func TestSubtreeListenerMarksASubtreeAsBeingStored(t *testing.T) {
	previous := ownLaneBitmapsWait
	ownLaneBitmapsWait = 50 * time.Millisecond

	t.Cleanup(func() { ownLaneBitmapsWait = previous })

	server, _ := setupServerWithSettings(t, func(s *settings.Settings) { s.UtxoStore.LaneMigration = settings.LaneMigrationShadow })

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	requests := make(chan subtreeprocessor.NewSubtreeRequest, 1)
	go server.runNewSubtreeListener(ctx, requests, make(chan *subtreeRetrySend, 10))

	st, txMap := laneSealSubtree(t, "listener")
	slow := &blockingReadMap{TxInpointsMap: txMap, release: make(chan struct{})}
	errCh := make(chan error, 1)
	requests <- subtreeprocessor.NewSubtreeRequest{Subtree: st, ParentTxMap: slow, ErrChan: errCh}

	require.Eventually(t, func() bool {
		server.ownLaneBitmaps.mu.Lock()
		defer server.ownLaneBitmaps.mu.Unlock()

		_, storing := server.ownLaneBitmaps.pending[*st.RootHash()]

		return storing
	}, 5*time.Second, time.Millisecond, "the listener marks the subtree as being stored")

	timedOut := map[string]string{"block": lane.BlockOwn, "reason": lane.MissingBitmapsTimedOut}
	before := ownLaneMetric(t, "teranode_lane_missing_bitmaps_total", timedOut)

	block := &model.Block{Header: laneTestHeader(), Subtrees: []*chainhash.Hash{st.RootHash()}, TransactionCount: 4}

	require.Nil(t, server.ownLaneRows(ctx, block))
	require.Equal(t, before+1, ownLaneMetric(t, "teranode_lane_missing_bitmaps_total", timedOut), "a subtree still being stored is timed out, not absent")

	close(slow.release)
	require.NoError(t, <-errCh)

	server.ownLaneBitmaps.mu.Lock()
	_, stillStoring := server.ownLaneBitmaps.pending[*st.RootHash()]
	server.ownLaneBitmaps.mu.Unlock()
	require.False(t, stillStoring, "once its storage ends nothing waits for it")

	require.NotEmpty(t, server.ownLaneRows(ctx, block))
}

// The two readers below are for tests; storing a block reads through waitFor.

// get returns the kept bitmaps of each subtree in roots, in order, and how many of them have
// none.
func (o *ownLaneBitmaps) get(roots []*chainhash.Hash) ([]lane.LaneBitmaps, int) {
	o.mu.Lock()
	defer o.mu.Unlock()

	bitmaps := make([]lane.LaneBitmaps, len(roots))
	missing := 0

	for i, root := range roots {
		kept, ok := o.bySubtree[*root]
		if !ok {
			missing++
			continue
		}

		bitmaps[i] = kept.lanes
	}

	return bitmaps, missing
}

// len is how many subtrees have kept bitmaps.
func (o *ownLaneBitmaps) len() int {
	o.mu.Lock()
	defer o.mu.Unlock()

	return len(o.bySubtree)
}

// failingExistingSubtreeStore reports every subtree as stored and fails to move its delete-at
// height, so storing it fails before the lane bitmaps are built.
type failingExistingSubtreeStore struct {
	blob.Store
}

func (failingExistingSubtreeStore) Exists(context.Context, []byte, fileformat.FileType, ...blobOptions.FileOption) (bool, error) {
	return true, nil
}

func (failingExistingSubtreeStore) SetDAH(context.Context, []byte, fileformat.FileType, uint32, ...blobOptions.FileOption) error {
	return errors.NewStorageError("set DAH failed")
}

// TestSubtreeStorageThatFailsStopsTheWait: a subtree whose storage fails before its bitmaps are
// kept is no longer marked as being stored once the storage worker is done with it, so a block
// listing it counts it absent, not timed out.
func TestSubtreeStorageThatFailsStopsTheWait(t *testing.T) {
	previous := ownLaneBitmapsWait
	ownLaneBitmapsWait = 50 * time.Millisecond

	t.Cleanup(func() { ownLaneBitmapsWait = previous })

	server, memoryStore := setupServerWithSettings(t, func(s *settings.Settings) { s.UtxoStore.LaneMigration = settings.LaneMigrationShadow })
	server.subtreeStore = failingExistingSubtreeStore{Store: memoryStore}

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	requests := make(chan subtreeprocessor.NewSubtreeRequest, 1)
	go server.runNewSubtreeListener(ctx, requests, make(chan *subtreeRetrySend, 10))

	st, txMap := laneSealSubtree(t, "failing")
	errCh := make(chan error, 1)
	requests <- subtreeprocessor.NewSubtreeRequest{Subtree: st, ParentTxMap: txMap, ErrChan: errCh}

	require.Error(t, <-errCh, "the storage fails")

	server.ownLaneBitmaps.mu.Lock()
	_, stillStoring := server.ownLaneBitmaps.pending[*st.RootHash()]
	server.ownLaneBitmaps.mu.Unlock()
	require.False(t, stillStoring, "nothing waits for a subtree whose storage is over")

	absent := map[string]string{"block": lane.BlockOwn, "reason": lane.MissingBitmapsAbsent}
	before := ownLaneMetric(t, "teranode_lane_missing_bitmaps_total", absent)

	require.Nil(t, server.ownLaneRows(ctx, &model.Block{Header: laneTestHeader(), Subtrees: []*chainhash.Hash{st.RootHash()}, TransactionCount: 4}))
	require.Equal(t, before+1, ownLaneMetric(t, "teranode_lane_missing_bitmaps_total", absent))
}

// TestStoreSubtreeDataKeepsNoLaneBitmapsWhenThePoolLostMembers: a subtree whose pool half was
// cleared before its storage ran (after a block, while it was queued behind a storage backlog) is
// stored without lane bitmaps, kept or written, with the meta pass or without it; a block listing
// it counts it absent rather than taking bitmaps that miss its members.
func TestStoreSubtreeDataKeepsNoLaneBitmapsWhenThePoolLostMembers(t *testing.T) {
	for _, meta := range []bool{true, false} {
		t.Run(fmt.Sprintf("meta=%t", meta), func(t *testing.T) {
			server, memoryStore := setupServerWithSettings(t, func(s *settings.Settings) {
				s.UtxoStore.LaneMigration = settings.LaneMigrationShadow
				s.BlockAssembly.StoreTxInpointsForSubtreeMeta = meta
			})

			st, txMap := laneSealSubtree(t, fmt.Sprintf("pool-cleared-%t", meta))
			txMap.Clear()

			unpositioned := map[string]string{"service": laneBitmapsService}
			before := ownLaneMetric(t, "teranode_lane_txs_without_position_total", unpositioned)

			subtreeDone, allDone, err := server.storeSubtreeData(t.Context(), subtreeprocessor.NewSubtreeRequest{Subtree: st, ParentTxMap: txMap}, make(chan *subtreeRetrySend, 10))
			require.NoError(t, err)
			require.True(t, <-subtreeDone)
			<-allDone

			_, err = memoryStore.Get(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtree)
			require.NoError(t, err, "the subtree is still stored")

			_, missing := server.ownLaneBitmaps.get([]*chainhash.Hash{st.RootHash()})
			require.Equal(t, 1, missing, "no bitmaps are kept")

			_, err = memoryStore.Get(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtreeLaneBitmaps)
			require.Error(t, err, "no bitmaps file is written")

			require.Equal(t, before, ownLaneMetric(t, "teranode_lane_txs_without_position_total", unpositioned), "the lost members are not counted as txs without a position")
		})
	}
}

// TestSubtreeLanePositionsTakesARemovedTxsBackup: a member removed from the pool while its
// subtree was stored takes its position from the backup beside deletedTxs and is not lost; one in
// neither the pool nor the backup is lost.
func TestSubtreeLanePositionsTakesARemovedTxsBackup(t *testing.T) {
	st, txMap := laneSealSubtree(t, "removed-backup")

	removed, gone := st.Nodes[1].Hash, st.Nodes[2].Hash
	removedInpoints, _ := txMap.Get(removed)
	require.True(t, txMap.Delete(removed))
	require.True(t, txMap.Delete(gone))

	deletedTxs := txmap.NewSyncedMap[chainhash.Hash, subtreepkg.TxInpoints]()
	deletedTxs.Set(removed, *removedInpoints)

	deletedPositions := txmap.NewSyncedMap[chainhash.Hash, uint64]()
	deletedPositions.Set(removed, lane.Pack(4, 20))

	laneBits, lost := subtreeLanePositions(st, txMap, deletedTxs, deletedPositions)
	require.Equal(t, 1, lost)
	require.Equal(t, []uint64{20, 22}, laneBits.Subtree(*st.RootHash()).Lanes[4].ToArray())
}

// failingBitmapsWriteStore refuses every write of a lane bitmaps file.
type failingBitmapsWriteStore struct {
	blob.Store
}

func (s failingBitmapsWriteStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...blobOptions.FileOption) error {
	if fileType == fileformat.FileTypeSubtreeLaneBitmaps {
		return errors.NewStorageError("write refused")
	}

	return s.Store.Set(ctx, key, fileType, value, opts...)
}

// TestStoreSubtreeDataCountsAFailedLaneBitmapsWrite: a lane bitmaps file block assembly cannot write
// is counted as write_failed under block="own"; the subtree is still stored, and its bitmaps are
// still kept for a block this node mines.
func TestStoreSubtreeDataCountsAFailedLaneBitmapsWrite(t *testing.T) {
	server, memoryStore := setupServerWithSettings(t, func(s *settings.Settings) { s.UtxoStore.LaneMigration = settings.LaneMigrationShadow })
	server.subtreeStore = failingBitmapsWriteStore{Store: memoryStore}

	labels := map[string]string{"block": lane.BlockOwn, "reason": lane.MissingBitmapsWriteFailed}
	before := ownLaneMetric(t, "teranode_lane_missing_bitmaps_total", labels)

	st, txMap := laneSealSubtree(t, "write-fails")

	subtreeDone, allDone, err := server.storeSubtreeData(t.Context(), subtreeprocessor.NewSubtreeRequest{Subtree: st, ParentTxMap: txMap}, make(chan *subtreeRetrySend, 10))
	require.NoError(t, err)
	require.True(t, <-subtreeDone, "the subtree is still stored")
	<-allDone

	require.Equal(t, before+1, ownLaneMetric(t, "teranode_lane_missing_bitmaps_total", labels))

	_, missing := server.ownLaneBitmaps.get([]*chainhash.Hash{st.RootHash()})
	require.Zero(t, missing, "its bitmaps are still kept")
}

// TestCandidateKeepsItsSubtreesLaneBitmaps: a subtree a mining candidate still lists keeps its
// bitmaps across blocks that do not rebuild the pool's subtrees, such as coinbase-only blocks, while
// one no candidate lists ages out.
func TestCandidateKeepsItsSubtreesLaneBitmaps(t *testing.T) {
	server, _ := setupServerWithSettings(t, func(s *settings.Settings) { s.UtxoStore.LaneMigration = settings.LaneMigrationShadow })

	listed, _ := laneSealSubtree(t, "candidate-listed")
	unlisted, _ := laneSealSubtree(t, "candidate-unlisted")

	server.ownLaneBitmaps.put(*listed.RootHash(), lane.LaneBitmaps{}, 100)
	server.ownLaneBitmaps.put(*unlisted.RootHash(), lane.LaneBitmaps{}, 100)

	// three coinbase-only blocks later, a candidate still lists the first subtree
	require.NoError(t, server.blockAssembler.utxoStore.SetBlockHeight(103))
	server.keepCandidateLaneBitmaps([]*subtreepkg.Subtree{listed})

	// a new subtree is kept at that height, which drops entries older than the margin
	server.ownLaneBitmaps.put(chainhash.HashH([]byte("candidate-new")), lane.LaneBitmaps{}, 103)

	_, missing := server.ownLaneBitmaps.get([]*chainhash.Hash{listed.RootHash()})
	require.Zero(t, missing, "the listed subtree keeps its bitmaps")

	_, missing = server.ownLaneBitmaps.get([]*chainhash.Hash{unlisted.RootHash()})
	require.Equal(t, 1, missing, "a subtree no candidate lists ages out")
}

// TestServingACandidateKeepsItsSubtreesLaneBitmaps: serving a mining candidate after blocks that did
// not rebuild the pool's subtrees, such as coinbase-only blocks, keeps the bitmaps of the subtrees
// it lists, so a block mined from it still gets its rows.
func TestServingACandidateKeepsItsSubtreesLaneBitmaps(t *testing.T) {
	initPrometheusMetrics()

	o := startOwnLaneServer(t, "sqlitememory:///", true)
	candidate := o.candidateOfTwoSubtrees(t)

	roots := make([]*chainhash.Hash, len(candidate.SubtreeHashes))
	for i, h := range candidate.SubtreeHashes {
		roots[i] = (*chainhash.Hash)(h)
	}

	// three blocks later, none of them rebuilding the pool's subtrees, the candidate is served again
	height := o.server.blockAssembler.utxoStore.GetBlockHeight() + ownLaneBitmapsKeepBlocks + 1
	require.NoError(t, o.server.blockAssembler.utxoStore.SetBlockHeight(height))

	_, err := o.server.GetMiningCandidate(o.ctx, &blockassembly_api.GetMiningCandidateRequest{IncludeSubtrees: true})
	require.NoError(t, err)

	// a subtree kept at the new height drops entries older than the margin
	o.server.ownLaneBitmaps.put(chainhash.HashH([]byte("serving-candidate-new")), lane.LaneBitmaps{}, height)

	_, missing := o.server.ownLaneBitmaps.get(roots)
	require.Zero(t, missing, "the candidate's subtrees keep their bitmaps")
}

// removedMidReadMap holds every member but reads no position for any: the processor removed the
// member, backing it up, between the worker's existence check and its position read.
type removedMidReadMap struct {
	subtreeprocessor.TxInpointsMap
}

func (removedMidReadMap) PositionWithErr(chainhash.Hash) (uint64, error) { return 0, nil }

// reAddedMap holds no member on the first position read of each and its position on the next: a
// re-chain deleted the member, then re-added it and dropped the backup, while the worker read it.
type reAddedMap struct {
	subtreeprocessor.TxInpointsMap
	reads map[chainhash.Hash]int
}

func (m reAddedMap) PositionWithErr(hash chainhash.Hash) (uint64, error) {
	m.reads[hash]++
	if m.reads[hash] == 1 {
		return 0, nil
	}

	return m.TxInpointsMap.Position(hash), nil
}

// TestSubtreeLanePositionsSurvivesRemovalDuringTheRead: a member the processor removes or re-adds
// while a storage worker reads its position keeps that position, taken from the removal backup or
// from the map read again, rather than counting as without one.
func TestSubtreeLanePositionsSurvivesRemovalDuringTheRead(t *testing.T) {
	st, txMap := laneSealSubtree(t, "removal-during-read")

	t.Run("removed after the existence check", func(t *testing.T) {
		deletedPositions := txmap.NewSyncedMap[chainhash.Hash, uint64]()
		for i, node := range st.Nodes[1:] {
			deletedPositions.Set(node.Hash, lane.Pack(4, uint64(20+i)))
		}

		laneBits, lost := subtreeLanePositions(st, removedMidReadMap{TxInpointsMap: txMap}, nil, deletedPositions)
		require.Zero(t, lost)
		require.Zero(t, laneBits.Unpositioned())
		require.Equal(t, []uint64{20, 21, 22}, laneBits.Subtree(*st.RootHash()).Lanes[4].ToArray())
	})

	t.Run("re-added with its backup dropped", func(t *testing.T) {
		laneBits, lost := subtreeLanePositions(st, reAddedMap{TxInpointsMap: txMap, reads: map[chainhash.Hash]int{}}, nil, txmap.NewSyncedMap[chainhash.Hash, uint64]())
		require.Zero(t, lost)
		require.Zero(t, laneBits.Unpositioned())
		require.Equal(t, []uint64{20, 21, 22}, laneBits.Subtree(*st.RootHash()).Lanes[4].ToArray())
	})
}

// TestRemovedLanePositionReadsThePoolAgain: a member the meta pass missed in the pool, whose
// removal backup is already dropped because a re-chain put it back, takes its position from the
// pool map read again.
func TestRemovedLanePositionReadsThePoolAgain(t *testing.T) {
	st, txMap := laneSealSubtree(t, "removed-reread")

	position, known := removedLanePosition(txMap, txmap.NewSyncedMap[chainhash.Hash, uint64](), st.Nodes[2].Hash)
	require.True(t, known)
	require.Equal(t, lane.Pack(4, 21), position)

	position, known = removedLanePosition(nil, nil, st.Nodes[2].Hash)
	require.True(t, known, "without backups the pool's answer is taken")
	require.Zero(t, position)

	// with backups, a member in neither the backup nor the pool has no known position
	require.True(t, txMap.Delete(st.Nodes[2].Hash))

	_, known = removedLanePosition(txMap, txmap.NewSyncedMap[chainhash.Hash, uint64](), st.Nodes[2].Hash)
	require.False(t, known)
}

// TestOwnLaneBitmapsPutKeepsTheNewerHeight: a storage worker that captured an older height, and
// keeps a subtree's bitmaps after a newer store or candidate kept them, leaves the newer height, so
// the entry is not dropped while candidates list it.
func TestOwnLaneBitmapsPutKeepsTheNewerHeight(t *testing.T) {
	var kept ownLaneBitmaps

	root := chainhash.HashH([]byte("newer-height"))

	kept.put(root, lane.LaneBitmaps{}, 103)
	kept.put(root, lane.LaneBitmaps{}, 100) // the older worker finishes last

	kept.put(chainhash.HashH([]byte("newer-height-next")), lane.LaneBitmaps{}, 105)

	_, missing := kept.get([]*chainhash.Hash{&root})
	require.Zero(t, missing, "kept at 103, inside the margin of 105")
}

// reAddedLateMap reads no position for a member on its first two reads and holds it from then on:
// another storage request that shares it dropped its backup, and the processor put it back only
// after the worker had read both the map and the backup.
type reAddedLateMap struct {
	subtreeprocessor.TxInpointsMap
	reads map[chainhash.Hash]int
}

func (m reAddedLateMap) PositionWithErr(hash chainhash.Hash) (uint64, error) {
	m.reads[hash]++
	if m.reads[hash] <= 2 {
		return 0, nil
	}

	return m.TxInpointsMap.Position(hash), nil
}

// TestSubtreeLanePositionsReadsAMemberReAddedAfterEveryRead: a member back in the pool after the
// map, the backup and the map again all missed it is read once more and keeps its position.
func TestSubtreeLanePositionsReadsAMemberReAddedAfterEveryRead(t *testing.T) {
	st, txMap := laneSealSubtree(t, "re-added-late")

	laneBits, lost := subtreeLanePositions(st, reAddedLateMap{TxInpointsMap: txMap, reads: map[chainhash.Hash]int{}}, nil, txmap.NewSyncedMap[chainhash.Hash, uint64]())
	require.Zero(t, lost)
	require.Zero(t, laneBits.Unpositioned())
	require.Equal(t, []uint64{20, 21, 22}, laneBits.Subtree(*st.RootHash()).Lanes[4].ToArray())
}

// TestOwnLaneBitmapsWaitIsOneSecond pins the production bound: storing our block, and so announcing
// it, is held back at most one second for a subtree whose bitmaps are not kept.
func TestOwnLaneBitmapsWaitIsOneSecond(t *testing.T) {
	require.Equal(t, time.Second, ownLaneBitmapsWait)
}

// laneTestPoolMap is a pool map of either kind, the in-memory map or the disk map, for tests that
// must hold on both: each reads a tx's lane position on its own code path.
func laneTestPoolMap(t *testing.T, disk bool) subtreeprocessor.TxInpointsMap {
	t.Helper()

	if !disk {
		return subtreeprocessor.NewSplitTxInpointsMap(4)
	}

	m, err := subtreeprocessor.NewDiskTxMap(subtreeprocessor.DiskTxMapOptions{BasePaths: []string{t.TempDir()}})
	require.NoError(t, err)

	t.Cleanup(func() { _ = m.Close() })

	return m
}

// TestStoreSubtreeDataKeepsNoLaneBitmapsWhenARemovedMembersPositionIsUnknown: a member removed
// from the pool while its subtree was stored takes its position from the backup beside its
// inpoints; if another storage request's cleanup dropped that backup meanwhile, its position
// cannot be told from none, and the subtree is stored without lane bitmaps rather than with
// bitmaps that miss it. The meta is stored either way.
func TestStoreSubtreeDataKeepsNoLaneBitmapsWhenARemovedMembersPositionIsUnknown(t *testing.T) {
	for _, disk := range []bool{false, true} {
		for _, backedUp := range []bool{true, false} {
			t.Run(fmt.Sprintf("disk=%t backedUp=%t", disk, backedUp), func(t *testing.T) {
				server, memoryStore := setupServerWithSettings(t, func(s *settings.Settings) { s.UtxoStore.LaneMigration = settings.LaneMigrationShadow })

				st, err := subtreepkg.NewTreeByLeafCount(4)
				require.NoError(t, err)
				require.NoError(t, st.AddCoinbaseNode())

				txMap := laneTestPoolMap(t, disk)
				deletedTxs := txmap.NewSyncedMap[chainhash.Hash, subtreepkg.TxInpoints]()
				deletedPositions := txmap.NewSyncedMap[chainhash.Hash, uint64]()

				for i := 0; i < 3; i++ {
					hash := chainhash.HashH(fmt.Appendf(nil, "lane-unknown-%t-%t-%d", disk, backedUp, i))
					require.NoError(t, st.AddNode(hash, 1, 1))

					inpoints := singleParentInpointsPtr(chainhash.HashH([]byte{byte(i), 0x78}), 0)

					if i == 1 {
						// removed: its inpoints are backed up, and its position only if no
						// cleanup dropped it
						deletedTxs.Set(hash, *inpoints)

						if backedUp {
							deletedPositions.Set(hash, lane.Pack(4, uint64(20+i)))
						}

						continue
					}

					txMap.SetAt(hash, inpoints, lane.Pack(4, uint64(20+i)))
				}

				_ = st.RootHash()

				subtreeDone, allDone, err := server.storeSubtreeData(t.Context(), subtreeprocessor.NewSubtreeRequest{
					Subtree: st, ParentTxMap: txMap, DeletedTxs: deletedTxs, DeletedPositions: deletedPositions,
				}, make(chan *subtreeRetrySend, 10))
				require.NoError(t, err)
				require.True(t, <-subtreeDone)
				<-allDone

				_, err = memoryStore.Get(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtreeMeta)
				require.NoError(t, err, "the meta is stored")

				kept, missing := server.ownLaneBitmaps.get([]*chainhash.Hash{st.RootHash()})
				if !backedUp {
					require.Equal(t, 1, missing, "no bitmaps are kept")
					return
				}

				require.Zero(t, missing)
				require.Equal(t, []uint64{20, 21, 22}, kept[0][4].ToArray())
			})
		}
	}
}

// TestStoreSubtreeDataWithoutMetaKeepsNodeZerosLanePosition: a later subtree has no coinbase
// placeholder, so its node 0 is a real tx; with no meta written, the positions-only pass keeps that
// tx's position, on both pool maps.
func TestStoreSubtreeDataWithoutMetaKeepsNodeZerosLanePosition(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("disk=%t", disk), func(t *testing.T) {
			server, memoryStore := setupServerWithSettings(t, func(s *settings.Settings) {
				s.UtxoStore.LaneMigration = settings.LaneMigrationShadow
				s.BlockAssembly.StoreTxInpointsForSubtreeMeta = false
			})

			st, err := subtreepkg.NewTreeByLeafCount(4)
			require.NoError(t, err)

			txMap := laneTestPoolMap(t, disk)

			for i := 0; i < 4; i++ {
				hash := chainhash.HashH(fmt.Appendf(nil, "lane-node-zero-%t-%d", disk, i))
				require.NoError(t, st.AddNode(hash, 1, 1))
				txMap.SetAt(hash, singleParentInpointsPtr(chainhash.HashH([]byte{byte(i), 0x79}), 0), lane.Pack(5, uint64(30+i)))
			}

			_ = st.RootHash()

			subtreeDone, allDone, err := server.storeSubtreeData(t.Context(), subtreeprocessor.NewSubtreeRequest{Subtree: st, ParentTxMap: txMap}, make(chan *subtreeRetrySend, 10))
			require.NoError(t, err)
			require.True(t, <-subtreeDone)
			<-allDone

			kept, missing := server.ownLaneBitmaps.get([]*chainhash.Hash{st.RootHash()})
			require.Zero(t, missing)
			require.Equal(t, []uint64{30, 31, 32, 33}, kept[0][5].ToArray(), "node 0 keeps its position")

			stored, err := memoryStore.Get(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtreeLaneBitmaps)
			require.NoError(t, err)

			decoded, err := lane.DecodeSubtreeBitmaps(stored)
			require.NoError(t, err)
			require.Equal(t, []uint64{30, 31, 32, 33}, decoded.Lanes[5].ToArray(), "the stored bitmaps hold node 0 too")
		})
	}
}

// clearedBetweenReadsMap answers, through separate reads, what a pool map answers when it is
// cleared between them: it holds the tx (Exists) but reads no position for it. Its one-read
// LookupPosition gives the coherent answer, that the map does not hold it.
type clearedBetweenReadsMap struct {
	subtreeprocessor.TxInpointsMap
}

func (clearedBetweenReadsMap) Exists(chainhash.Hash) bool { return true }

func (clearedBetweenReadsMap) PositionWithErr(chainhash.Hash) (uint64, error) { return 0, nil }

func (clearedBetweenReadsMap) LookupPosition(chainhash.Hash) (uint64, bool, error) {
	return 0, false, nil
}

// TestRemovedMemberPositionUsesTheOneReadLookup: a removed member with no position backup is
// looked up in the pool with one read, so a map cleared between an existence read and a position
// read does not make its position a known none; the member is lost and the subtree keeps no
// bitmaps.
func TestRemovedMemberPositionUsesTheOneReadLookup(t *testing.T) {
	st, txMap := laneSealSubtree(t, "cleared-between-reads")
	pool := clearedBetweenReadsMap{TxInpointsMap: txMap}

	_, known := removedLanePosition(pool, txmap.NewSyncedMap[chainhash.Hash, uint64](), st.Nodes[1].Hash)
	require.False(t, known)

	_, lost := subtreeLanePositions(st, pool, nil, txmap.NewSyncedMap[chainhash.Hash, uint64]())
	require.Equal(t, 3, lost)
}

// TestCandidateSubtreesOutliveBlocksWithNoCandidateServed: a subtree the latest mining candidate
// lists keeps its bitmaps across blocks that do not rebuild the pool's subtrees even when no
// candidate is served between them, so a subtree kept at a newer height does not drop it; once a
// later candidate no longer lists it, it ages out.
func TestCandidateSubtreesOutliveBlocksWithNoCandidateServed(t *testing.T) {
	var kept ownLaneBitmaps

	inPool := chainhash.HashH([]byte("listed-in-pool"))

	kept.put(inPool, lane.LaneBitmaps{}, 100)
	kept.list([]chainhash.Hash{inPool}, 100)

	// three coinbase-only blocks, no candidate served, then a new subtree is kept at 103
	sealed := chainhash.HashH([]byte("sealed-after-blocks"))
	kept.put(sealed, lane.LaneBitmaps{}, 103)

	_, missing := kept.get([]*chainhash.Hash{&inPool})
	require.Zero(t, missing, "the latest candidate still lists it")

	// a later candidate no longer lists it: it ages out with the next subtree kept
	kept.list([]chainhash.Hash{sealed}, 103)
	kept.put(chainhash.HashH([]byte("later")), lane.LaneBitmaps{}, 106)

	_, missing = kept.get([]*chainhash.Hash{&inPool})
	require.Equal(t, 1, missing)
}

// missingFileDAHStore answers SetDAH for a lane bitmaps file it does not hold with ErrNotFound, as
// the file store does.
type missingFileDAHStore struct {
	blob.Store
}

func (s missingFileDAHStore) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, dah uint32, opts ...blobOptions.FileOption) error {
	if fileType == fileformat.FileTypeSubtreeLaneBitmaps {
		exists, err := s.Exists(ctx, key, fileType)
		if err != nil {
			return err
		}

		if !exists {
			return errors.NewNotFoundError("no lane bitmaps file")
		}
	}

	return s.Store.SetDAH(ctx, key, fileType, dah, opts...)
}

// TestRepairingALaneBitmapsFileCountsNoMemberTwice: a subtree whose lane bitmaps file failed to
// write, stored again, gets the file written from its kept bitmaps; its member without a position
// was counted when the bitmaps were collected and is not counted again.
func TestRepairingALaneBitmapsFileCountsNoMemberTwice(t *testing.T) {
	server, memoryStore := setupServerWithSettings(t, func(s *settings.Settings) { s.UtxoStore.LaneMigration = settings.LaneMigrationShadow })
	server.subtreeStore = failingBitmapsWriteStore{Store: memoryStore}

	st, txMap := laneSealSubtree(t, "repair-file")
	txMap.Set(st.Nodes[2].Hash, singleParentInpointsPtr(chainhash.HashH([]byte("repair-parent")), 0)) // without a position

	unpositioned := map[string]string{"service": laneBitmapsService}
	before := ownLaneMetric(t, "teranode_lane_txs_without_position_total", unpositioned)

	request := subtreeprocessor.NewSubtreeRequest{Subtree: st, ParentTxMap: txMap}

	subtreeDone, allDone, err := server.storeSubtreeData(t.Context(), request, make(chan *subtreeRetrySend, 10))
	require.NoError(t, err)
	require.True(t, <-subtreeDone)
	<-allDone

	require.Equal(t, before+1, ownLaneMetric(t, "teranode_lane_txs_without_position_total", unpositioned))

	// stored again, with a store that takes the file
	server.subtreeStore = missingFileDAHStore{Store: memoryStore}
	server.keepExistingSubtreeLaneBitmaps(t.Context(), request, 1, 10)

	stored, err := memoryStore.Get(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtreeLaneBitmaps)
	require.NoError(t, err, "the file is written")

	decoded, err := lane.DecodeSubtreeBitmaps(stored)
	require.NoError(t, err)
	require.Equal(t, []uint64{20, 22}, decoded.Lanes[4].ToArray())

	require.Equal(t, before+1, ownLaneMetric(t, "teranode_lane_txs_without_position_total", unpositioned), "the member is not counted again")
}

// TestOverlappingStoresOfOneSubtreeKeepItPending: two storage requests of one subtree in flight
// at once keep it as being stored until both end, so one ending without bitmaps does not make a
// block listing it count it absent while the other still runs.
func TestOverlappingStoresOfOneSubtreeKeepItPending(t *testing.T) {
	var kept ownLaneBitmaps

	root := chainhash.HashH([]byte("overlapping-stores"))

	kept.expect(root)
	kept.expect(root)
	kept.settle(root) // the first ends without keeping bitmaps

	_, absent, timedOut := kept.waitFor(t.Context(), []*chainhash.Hash{&root}, time.Millisecond)
	require.Zero(t, absent)
	require.Equal(t, 1, timedOut, "the second is still storing it")

	kept.settle(root)

	_, absent, timedOut = kept.waitFor(t.Context(), []*chainhash.Hash{&root}, time.Millisecond)
	require.Equal(t, 1, absent, "no storage runs once both have ended")
	require.Zero(t, timedOut)
}

// TestAnEmptyCandidateKeepsThePoolsPins: an empty mining candidate, served while a block moves the
// pool, does not unpin the subtrees the last full candidate listed, so coinbase-only blocks after
// it do not age them out.
func TestAnEmptyCandidateKeepsThePoolsPins(t *testing.T) {
	server, _ := setupServerWithSettings(t, func(s *settings.Settings) { s.UtxoStore.LaneMigration = settings.LaneMigrationShadow })

	inPool, _ := laneSealSubtree(t, "empty-candidate")

	require.NoError(t, server.blockAssembler.utxoStore.SetBlockHeight(100))
	server.ownLaneBitmaps.put(*inPool.RootHash(), lane.LaneBitmaps{}, 100)
	server.keepCandidateLaneBitmaps([]*subtreepkg.Subtree{inPool})

	// an empty candidate, then three coinbase-only blocks and a new subtree kept at 103
	server.keepCandidateLaneBitmaps(nil)
	server.ownLaneBitmaps.put(chainhash.HashH([]byte("empty-candidate-later")), lane.LaneBitmaps{}, 103)

	_, missing := server.ownLaneBitmaps.get([]*chainhash.Hash{inPool.RootHash()})
	require.Zero(t, missing, "the pool's subtree keeps its bitmaps")
}

// TestTwoStoresOfOneSubtreeCountItsMembersOnce: two storage requests of one subtree, as a reorg can
// queue, keep its bitmaps twice but count its members without a position once.
func TestTwoStoresOfOneSubtreeCountItsMembersOnce(t *testing.T) {
	server, _ := setupServerWithSettings(t, func(s *settings.Settings) { s.UtxoStore.LaneMigration = settings.LaneMigrationShadow })

	st, txMap := laneSealSubtree(t, "two-stores")
	txMap.Set(st.Nodes[2].Hash, singleParentInpointsPtr(chainhash.HashH([]byte("two-stores-parent")), 0)) // without a position

	unpositioned := map[string]string{"service": laneBitmapsService}
	before := ownLaneMetric(t, "teranode_lane_txs_without_position_total", unpositioned)

	var wg sync.WaitGroup

	for i := 0; i < 2; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			laneBits, lost := subtreeLanePositions(st, txMap, nil, nil)
			require.Zero(t, lost)
			server.storeSubtreeLaneBitmaps(t.Context(), st, laneBits, 1, 10)
		}()
	}

	wg.Wait()

	require.Equal(t, before+1, ownLaneMetric(t, "teranode_lane_txs_without_position_total", unpositioned))
}

// TestKeepingBitmapsWakesAWaitingBlock: a block waiting for a subtree's bitmaps returns as soon as
// they are kept, not when its wait runs out.
func TestKeepingBitmapsWakesAWaitingBlock(t *testing.T) {
	var kept ownLaneBitmaps

	root := chainhash.HashH([]byte("wakes-a-waiter"))
	kept.expect(root)

	done := make(chan time.Duration, 1)

	go func() {
		start := time.Now()
		_, absent, timedOut := kept.waitFor(t.Context(), []*chainhash.Hash{&root}, 10*time.Second)
		require.Zero(t, absent+timedOut)

		done <- time.Since(start)
	}()

	// the waiter is established once it has registered for a change
	require.Eventually(t, func() bool {
		kept.mu.Lock()
		defer kept.mu.Unlock()

		return kept.changed != nil
	}, 5*time.Second, time.Millisecond)

	kept.put(root, lane.LaneBitmaps{}, 1)

	select {
	case waited := <-done:
		require.Less(t, waited, 5*time.Second)
	case <-time.After(5 * time.Second):
		t.Fatal("the waiting block was not woken when its subtree's bitmaps were kept")
	}
}

// dahRecordingStore records each delete-at height given to a lane bitmaps file, and each set
// option asking for an exclusive publish.
type dahRecordingStore struct {
	blob.Store

	mu        sync.Mutex
	dahs      []uint32
	exclusive []bool
}

func (s *dahRecordingStore) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, dah uint32, opts ...blobOptions.FileOption) error {
	if fileType == fileformat.FileTypeSubtreeLaneBitmaps {
		s.mu.Lock()
		s.dahs = append(s.dahs, dah)
		s.mu.Unlock()
	}

	return s.Store.SetDAH(ctx, key, fileType, dah, opts...)
}

func (s *dahRecordingStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...blobOptions.FileOption) error {
	if fileType == fileformat.FileTypeSubtreeLaneBitmaps {
		s.mu.Lock()
		s.exclusive = append(s.exclusive, blobOptions.MergeOptions(nil, opts).ExclusivePublish)
		s.mu.Unlock()
	}

	return s.Store.Set(ctx, key, fileType, value, opts...)
}

// TestAnOlderRefreshDoesNotShortenTheLaneBitmapsFile: a storage request of an existing subtree
// that refreshes its lane bitmaps file with an older delete-at height than one already given
// leaves the file with the later one; the write itself is an exclusive publish.
func TestAnOlderRefreshDoesNotShortenTheLaneBitmapsFile(t *testing.T) {
	server, memoryStore := setupServerWithSettings(t, func(s *settings.Settings) { s.UtxoStore.LaneMigration = settings.LaneMigrationShadow })

	recording := &dahRecordingStore{Store: memoryStore}
	server.subtreeStore = recording

	st, txMap := laneSealSubtree(t, "older-refresh")
	request := subtreeprocessor.NewSubtreeRequest{Subtree: st, ParentTxMap: txMap}

	laneBits, _ := subtreeLanePositions(st, txMap, nil, nil)
	server.storeSubtreeLaneBitmaps(t.Context(), st, laneBits, 1, 300)

	server.keepExistingSubtreeLaneBitmaps(t.Context(), request, 2, 302) // a newer refresh
	server.keepExistingSubtreeLaneBitmaps(t.Context(), request, 1, 301) // an older one, finishing last

	recording.mu.Lock()
	defer recording.mu.Unlock()

	require.Equal(t, []bool{true}, recording.exclusive, "the file is written with an exclusive publish")
	require.Equal(t, []uint32{302}, recording.dahs, "only the later delete-at is applied")
}

// TestAPendingSubtreeKeepsItsFileDAH: a subtree with a storage request still running is not
// dropped by age, so that request, finishing with an older delete-at height, does not lower its
// lane bitmaps file's.
func TestAPendingSubtreeKeepsItsFileDAH(t *testing.T) {
	server, memoryStore := setupServerWithSettings(t, func(s *settings.Settings) { s.UtxoStore.LaneMigration = settings.LaneMigrationShadow })

	recording := &dahRecordingStore{Store: memoryStore}
	server.subtreeStore = recording

	st, txMap := laneSealSubtree(t, "pending-file-dah")
	root := *st.RootHash()

	// an older request has taken the subtree and not finished
	server.ownLaneBitmaps.expect(root)

	// a newer request stores it at height 100, then a subtree kept at 103 would age it out
	laneBits, _ := subtreeLanePositions(st, txMap, nil, nil)
	server.storeSubtreeLaneBitmaps(t.Context(), st, laneBits, 100, 388)
	server.ownLaneBitmaps.put(chainhash.HashH([]byte("pending-file-dah-later")), lane.LaneBitmaps{}, 103)

	// the older request finishes with its older delete-at
	server.keepExistingSubtreeLaneBitmaps(t.Context(), subtreeprocessor.NewSubtreeRequest{Subtree: st, ParentTxMap: txMap}, 99, 387)
	server.ownLaneBitmaps.settle(root)

	recording.mu.Lock()
	defer recording.mu.Unlock()

	require.Empty(t, recording.dahs, "the older delete-at is not applied")
}

// TestAMemberInNeitherThePoolNorTheRemovedTxsIsLost: a member the pool does not hold and that has
// no inpoints backup is lost, even when a position backup for it remains.
func TestAMemberInNeitherThePoolNorTheRemovedTxsIsLost(t *testing.T) {
	st, txMap := laneSealSubtree(t, "neither")
	require.True(t, txMap.Delete(st.Nodes[1].Hash))

	deletedPositions := txmap.NewSyncedMap[chainhash.Hash, uint64]()
	deletedPositions.Set(st.Nodes[1].Hash, lane.Pack(4, 20))

	_, lost := subtreeLanePositions(st, txMap, txmap.NewSyncedMap[chainhash.Hash, subtreepkg.TxInpoints](), deletedPositions)
	require.Equal(t, 1, lost)
}
