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
// them at shadow; block assembly seals them into subtrees, keeping each subtree's lane bitmaps as it
// is sealed, stores each with its lane bitmaps file, and stores the block it mines with its lane
// map rows, read back here from a real blockchain store.

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

	return startOwnLaneServerOn(t, storeURL, shadow, nil)
}

// startOwnLaneServerOn is startOwnLaneServer with its subtree store wrapped by wrap, when given.
func startOwnLaneServerOn(t *testing.T, storeURL string, shadow bool, wrap func(*memory.Memory) blob.Store) *ownLaneServer {
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

	var serverStore blob.Store = subtreeStore
	if wrap != nil {
		serverStore = wrap(subtreeStore)
	}

	server := New(common.Logger, common.Settings, nil, utxoStore, serverStore, spy)
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
// file holds the kept bitmaps, is written before the subtree file with the same delete-at height,
// whether or not a subtree meta file is written. A member without a position is counted once, when
// the bitmaps are kept, and left out.
func TestStoreSubtreeDataWritesLaneBitmapsBeforeTheSubtree(t *testing.T) {
	positions := []uint64{lane.Pack(2, 40), 0, lane.Pack(2, 42)}

	for _, tc := range []struct {
		name string
		meta bool
	}{
		{"with meta", true},
		{"without meta", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, memoryStore := setupServerWithSettings(t, func(s *settings.Settings) {
				s.UtxoStore.LaneMigration = settings.LaneMigrationShadow
				s.BlockAssembly.StoreTxInpointsForSubtreeMeta = tc.meta
			})

			store := &setOrderStore{Store: memoryStore}
			server.subtreeStore = store

			txMap := subtreeprocessor.NewSplitTxInpointsMap(4)

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

			keepSealed(server, st, txMap)

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

			// The same subtree kept and stored again counts nothing again.
			keepSealed(server, st, txMap)

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

// keepSealed keeps st's lane bitmaps, built from the positions txMap holds for its members, as the
// subtree processor does when it seals a subtree.
func keepSealed(server *BlockAssembly, st *subtreepkg.Subtree, txMap subtreeprocessor.TxInpointsMap) {
	builder := lane.NewBitmapsBuilder()

	for _, node := range st.Nodes {
		if node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			continue
		}

		builder.Add(txMap.Position(node.Hash))
	}

	server.ownLaneBitmaps.KeepLaneBitmaps(builder.Subtree(*st.RootHash()), builder.Unpositioned())
}

// TestStoreSubtreeDataKeepsAnExistingSubtreesLaneBitmaps: a subtree the store already holds keeps
// its kept lane bitmaps, and their file lives as long as the subtree, or is written again if lost.
func TestStoreSubtreeDataKeepsAnExistingSubtreesLaneBitmaps(t *testing.T) {
	shadow := func(s *settings.Settings) { s.UtxoStore.LaneMigration = settings.LaneMigrationShadow }

	t.Run("kept, stored again at a later height", func(t *testing.T) {
		server, memoryStore := setupServerWithSettings(t, shadow)
		store := &setOrderStore{Store: memoryStore}
		server.subtreeStore = store

		st, txMap := laneSealSubtree(t, "kept")
		keepSealed(server, st, txMap)

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
		keepSealed(server, st, txMap)

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
		// a subtree whose bitmaps the processor could not build has none to write, and its file is
		// left as it is
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

		_, missing := server.ownLaneBitmaps.get([]*chainhash.Hash{st.RootHash()})
		require.Equal(t, 1, missing, "the storage worker builds no bitmaps")
		require.Zero(t, store.dah(fileformat.FileTypeSubtreeLaneBitmaps), "the old file's delete-at is not moved")
	})
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

// The two readers below are for tests.

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

// failingBitmapsWriteStore refuses the first failures writes of a lane bitmaps file with err, a
// storage error when nil, or every write when failures is negative.
type failingBitmapsWriteStore struct {
	blob.Store

	failures int
	err      error
	// failMeta refuses every subtree meta write too.
	failMeta bool

	mu      sync.Mutex
	refused int
}

func (s *failingBitmapsWriteStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...blobOptions.FileOption) error {
	if s.failMeta && fileType == fileformat.FileTypeSubtreeMeta {
		return errors.NewStorageError("meta write refused")
	}

	if fileType == fileformat.FileTypeSubtreeLaneBitmaps {
		s.mu.Lock()
		refuse := s.failures < 0 || s.refused < s.failures
		if refuse {
			s.refused++
		}
		s.mu.Unlock()

		if refuse {
			if s.err != nil {
				return s.err
			}

			return errors.NewStorageError("write refused")
		}
	}

	return s.Store.Set(ctx, key, fileType, value, opts...)
}

// fastLaneBitmapsRetry shortens the server's lane bitmaps write retry to suit a test.
func fastLaneBitmapsRetry(server *BlockAssembly) {
	server.laneBitmapsRetry.Bound = 200 * time.Millisecond
	server.laneBitmapsRetry.FirstBackoff = time.Millisecond
	server.laneBitmapsRetry.MaxBackoff = 5 * time.Millisecond
}

// TestStoreSubtreeDataRetriesAFailedLaneBitmapsWrite: a failed lane bitmaps file write is retried and
// written before the subtree file; past the bound, or with an error a retry cannot pass, it counts
// as write_failed. The subtree is stored and its bitmaps stay kept either way.
func TestStoreSubtreeDataRetriesAFailedLaneBitmapsWrite(t *testing.T) {
	for _, tc := range []struct {
		name          string
		failures      int
		err           error
		wantFile      bool
		wantAttempts  int
		wantFailed    float64
		wantRetried   float64
		wantExhausted float64
	}{
		{"fails twice then is written", 2, nil, true, 2, 0, 1, 0},
		{"fails past the bound", -1, nil, false, -1, 1, 1, 1},
		{"cannot pass on a retry", -1, errors.NewStorageError("failed to get file name", errors.NewConfigurationError("no deletion scheduler")), false, 1, 1, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, memoryStore := setupServerWithSettings(t, func(s *settings.Settings) { s.UtxoStore.LaneMigration = settings.LaneMigrationShadow })
			fastLaneBitmapsRetry(server)

			if tc.wantFile {
				server.laneBitmapsRetry.Bound = 5 * time.Second // a stalled runner cannot end a retry that must succeed
			}

			order := &setOrderStore{Store: memoryStore}
			store := &failingBitmapsWriteStore{Store: order, failures: tc.failures, err: tc.err}
			server.subtreeStore = store

			writeFailed := map[string]string{"block": lane.BlockOwn, "reason": lane.MissingBitmapsWriteFailed}
			service := map[string]string{"service": laneBitmapsService}
			failedBefore := ownLaneMetric(t, "teranode_lane_missing_bitmaps_total", writeFailed)
			retriedBefore := ownLaneMetric(t, "teranode_lane_bitmaps_write_retries_total", service)
			exhaustedBefore := ownLaneMetric(t, "teranode_lane_bitmaps_write_retries_exhausted_total", service)

			st, txMap := laneSealSubtree(t, "write-fails-"+tc.name)
			keepSealed(server, st, txMap)

			subtreeDone, allDone, err := server.storeSubtreeData(t.Context(), subtreeprocessor.NewSubtreeRequest{Subtree: st, ParentTxMap: txMap}, make(chan *subtreeRetrySend, 10))
			require.NoError(t, err)
			require.True(t, <-subtreeDone, "the subtree is stored")
			<-allDone

			if tc.wantFile {
				bitmapsAt, subtreeAt := order.index(fileformat.FileTypeSubtreeLaneBitmaps), order.index(fileformat.FileTypeSubtree)
				require.GreaterOrEqual(t, bitmapsAt, 0, "the lane bitmaps file is written")
				require.Less(t, bitmapsAt, subtreeAt, "before the subtree file")
				require.Equal(t, order.dah(fileformat.FileTypeSubtree), order.dah(fileformat.FileTypeSubtreeLaneBitmaps), "with the subtree's delete-at height")
			} else {
				exists, err := memoryStore.Exists(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtreeLaneBitmaps)
				require.NoError(t, err)
				require.False(t, exists)
			}

			if tc.wantAttempts > 0 {
				require.Equal(t, tc.wantAttempts, store.refused)
			} else {
				require.Greater(t, store.refused, 1, "it was retried until the bound")
			}

			require.Equal(t, failedBefore+tc.wantFailed, ownLaneMetric(t, "teranode_lane_missing_bitmaps_total", writeFailed))
			require.Equal(t, retriedBefore+tc.wantRetried, ownLaneMetric(t, "teranode_lane_bitmaps_write_retries_total", service))
			require.Equal(t, exhaustedBefore+tc.wantExhausted, ownLaneMetric(t, "teranode_lane_bitmaps_write_retries_exhausted_total", service))

			_, missing := server.ownLaneBitmaps.get([]*chainhash.Hash{st.RootHash()})
			require.Zero(t, missing, "its bitmaps are still kept")
		})
	}
}

// TestAQueuedSubtreeRetryWaitsForTheLaneBitmapsWrite: a subtree whose meta write fails while its lane
// bitmaps write is retrying goes to the retry processor, which also stores the subtree file, only
// once the lane bitmaps file is written.
func TestAQueuedSubtreeRetryWaitsForTheLaneBitmapsWrite(t *testing.T) {
	server, memoryStore := setupServerWithSettings(t, func(s *settings.Settings) {
		s.UtxoStore.LaneMigration = settings.LaneMigrationShadow
		s.BlockAssembly.StoreTxInpointsForSubtreeMeta = true
	})
	server.laneBitmapsRetry.Bound = 5 * time.Second
	server.laneBitmapsRetry.FirstBackoff = 100 * time.Millisecond
	server.laneBitmapsRetry.MaxBackoff = 100 * time.Millisecond

	server.subtreeStore = &failingBitmapsWriteStore{Store: memoryStore, failures: 2, failMeta: true}

	st, txMap := laneSealSubtree(t, "queued-retry")
	keepSealed(server, st, txMap)

	retries := make(chan *subtreeRetrySend, 10)

	subtreeDone, allDone, err := server.storeSubtreeData(t.Context(), subtreeprocessor.NewSubtreeRequest{Subtree: st, ParentTxMap: txMap}, retries)
	require.NoError(t, err)

	select {
	case <-retries:
		exists, err := memoryStore.Exists(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtreeLaneBitmaps)
		require.NoError(t, err)
		require.True(t, exists, "the lane bitmaps file was written before the retry was queued")
	case <-time.After(10 * time.Second):
		t.Fatal("the failed meta write was never queued for a retry")
	}

	require.True(t, <-subtreeDone)
	<-allDone
}

// TestALaneBitmapsRetryStartsNoWriteAfterWaitingOutItsBoundOnTheLock: a retry that waits for the
// file's lock, held by another write of the same stripe, until its bound has run out gives up
// without writing.
func TestALaneBitmapsRetryStartsNoWriteAfterWaitingOutItsBoundOnTheLock(t *testing.T) {
	server, memoryStore := setupServerWithSettings(t, func(s *settings.Settings) { s.UtxoStore.LaneMigration = settings.LaneMigrationShadow })
	server.laneBitmapsRetry.Bound = 400 * time.Millisecond
	server.laneBitmapsRetry.FirstBackoff = 200 * time.Millisecond

	store := &failingBitmapsWriteStore{Store: memoryStore, failures: -1}
	server.subtreeStore = store

	root := chainhash.HashH([]byte("lock-past-bound"))
	server.ownLaneBitmaps.put(root, lane.LaneBitmaps{}, 1)

	service := map[string]string{"service": laneBitmapsService}
	exhaustedBefore := ownLaneMetric(t, "teranode_lane_bitmaps_write_retries_exhausted_total", service)

	written := make(chan struct{})

	go func() {
		defer close(written)

		server.writeLaneBitmapsFile(t.Context(), lane.SubtreeBitmaps{SubtreeHash: root, Lanes: lane.LaneBitmaps{4: bitmapOfPositions(1)}}, 300)
	}()

	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()

		return store.refused == 1
	}, 5*time.Second, time.Millisecond)

	lock := server.ownLaneBitmaps.fileLock(root)
	lock.Lock() // held by another write of the stripe through the retry's bound
	time.Sleep(600 * time.Millisecond)
	lock.Unlock()
	<-written

	store.mu.Lock()
	defer store.mu.Unlock()

	require.Equal(t, 1, store.refused, "no write started after the bound")
	require.Equal(t, exhaustedBefore+1, ownLaneMetric(t, "teranode_lane_bitmaps_write_retries_exhausted_total", service))
}

// refreshDuringRetryStore refuses a lane bitmaps file write at delete-at height 300 until a refresh
// at a later height has written the file, and signals the first refusal.
type refreshDuringRetryStore struct {
	blob.Store

	refusedOnce chan struct{}
	refreshed   chan struct{}
	once        sync.Once
}

func (s *refreshDuringRetryStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...blobOptions.FileOption) error {
	if fileType == fileformat.FileTypeSubtreeLaneBitmaps && blobOptions.NewFileOptions(opts...).DAH == 300 {
		select {
		case <-s.refreshed:
		default:
			s.once.Do(func() { close(s.refusedOnce) })

			return errors.NewStorageError("database restarting")
		}
	}

	return s.Store.Set(ctx, key, fileType, value, opts...)
}

// TestARefreshDuringALaneBitmapsRetryKeepsItsLaterDeleteAt: a storage request of the subtree that
// refreshes its lane bitmaps file to a later delete-at height while a write of the file is in its
// backoff writes the file; the retried write then finds it and leaves the later height.
func TestARefreshDuringALaneBitmapsRetryKeepsItsLaterDeleteAt(t *testing.T) {
	server, memoryStore := setupServerWithSettings(t, func(s *settings.Settings) { s.UtxoStore.LaneMigration = settings.LaneMigrationShadow })
	server.laneBitmapsRetry.Bound = 5 * time.Second
	server.laneBitmapsRetry.FirstBackoff = time.Millisecond
	server.laneBitmapsRetry.MaxBackoff = 5 * time.Millisecond

	order := &setOrderStore{Store: memoryStore}
	store := &refreshDuringRetryStore{Store: order, refusedOnce: make(chan struct{}), refreshed: make(chan struct{})}
	server.subtreeStore = store

	st, txMap := laneSealSubtree(t, "refresh-during-retry")
	root := *st.RootHash()

	keepSealed(server, st, txMap)

	service := map[string]string{"service": laneBitmapsService}
	exhaustedBefore := ownLaneMetric(t, "teranode_lane_bitmaps_write_retries_exhausted_total", service)

	written := make(chan struct{})

	go func() {
		defer close(written)

		server.writeKeptLaneBitmapsFile(t.Context(), root, 1, 300)
	}()

	<-store.refusedOnce
	server.keepExistingSubtreeLaneBitmaps(t.Context(), root, 3, 302) // finds no file, writes it at 302
	close(store.refreshed)
	<-written

	require.Equal(t, uint32(302), order.dah(fileformat.FileTypeSubtreeLaneBitmaps), "the retried write did not lower the refresh's delete-at")
	require.True(t, server.ownLaneBitmaps.fileDAHLater(root, 301))
	require.Equal(t, exhaustedBefore, ownLaneMetric(t, "teranode_lane_bitmaps_write_retries_exhausted_total", service), "the refresh did not wait for the retry to give up")
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
// was counted when the bitmaps were kept and is not counted again.
func TestRepairingALaneBitmapsFileCountsNoMemberTwice(t *testing.T) {
	server, memoryStore := setupServerWithSettings(t, func(s *settings.Settings) { s.UtxoStore.LaneMigration = settings.LaneMigrationShadow })
	fastLaneBitmapsRetry(server)
	server.subtreeStore = &failingBitmapsWriteStore{Store: memoryStore, failures: -1}

	st, txMap := laneSealSubtree(t, "repair-file")
	txMap.Set(st.Nodes[2].Hash, singleParentInpointsPtr(chainhash.HashH([]byte("repair-parent")), 0)) // without a position

	unpositioned := map[string]string{"service": laneBitmapsService}
	before := ownLaneMetric(t, "teranode_lane_txs_without_position_total", unpositioned)

	request := subtreeprocessor.NewSubtreeRequest{Subtree: st, ParentTxMap: txMap}

	keepSealed(server, st, txMap)

	subtreeDone, allDone, err := server.storeSubtreeData(t.Context(), request, make(chan *subtreeRetrySend, 10))
	require.NoError(t, err)
	require.True(t, <-subtreeDone)
	<-allDone

	require.Equal(t, before+1, ownLaneMetric(t, "teranode_lane_txs_without_position_total", unpositioned))

	// stored again, with a store that takes the file
	server.subtreeStore = missingFileDAHStore{Store: memoryStore}
	server.keepExistingSubtreeLaneBitmaps(t.Context(), *st.RootHash(), 1, 10)

	stored, err := memoryStore.Get(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtreeLaneBitmaps)
	require.NoError(t, err, "the file is written")

	decoded, err := lane.DecodeSubtreeBitmaps(stored)
	require.NoError(t, err)
	require.Equal(t, []uint64{20, 22}, decoded.Lanes[4].ToArray())

	require.Equal(t, before+1, ownLaneMetric(t, "teranode_lane_txs_without_position_total", unpositioned), "the member is not counted again")
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

// TestKeepingOneSubtreeTwiceCountsItsMembersOnce: one subtree kept twice, as a re-chain that
// rebuilds it unchanged can, counts its members without a position once.
func TestKeepingOneSubtreeTwiceCountsItsMembersOnce(t *testing.T) {
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

			keepSealed(server, st, txMap)
		}()
	}

	wg.Wait()

	require.Equal(t, before+1, ownLaneMetric(t, "teranode_lane_txs_without_position_total", unpositioned))
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
	root := *st.RootHash()

	keepSealed(server, st, txMap)
	server.writeKeptLaneBitmapsFile(t.Context(), root, 1, 300)

	server.keepExistingSubtreeLaneBitmaps(t.Context(), root, 2, 302) // a newer refresh
	server.keepExistingSubtreeLaneBitmaps(t.Context(), root, 1, 301) // an older one, finishing last

	recording.mu.Lock()
	defer recording.mu.Unlock()

	require.Equal(t, []bool{true}, recording.exclusive, "the file is written with an exclusive publish")
	require.Equal(t, []uint32{302}, recording.dahs, "only the later delete-at is applied")
}

// blockingBitmapsStore holds each lane bitmaps file write until released, and records whether a
// delete-at update of one ran while a write was held.
type blockingBitmapsStore struct {
	blob.Store

	entered chan struct{}
	release chan struct{}

	mu          sync.Mutex
	writing     bool
	dahDuringIO bool
	dahs        []uint32
}

func (s *blockingBitmapsStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...blobOptions.FileOption) error {
	if fileType == fileformat.FileTypeSubtreeLaneBitmaps {
		s.mu.Lock()
		s.writing = true
		s.mu.Unlock()

		close(s.entered)
		<-s.release

		s.mu.Lock()
		s.writing = false
		s.mu.Unlock()
	}

	return s.Store.Set(ctx, key, fileType, value, opts...)
}

func (s *blockingBitmapsStore) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, dah uint32, opts ...blobOptions.FileOption) error {
	if fileType == fileformat.FileTypeSubtreeLaneBitmaps {
		s.mu.Lock()
		s.dahDuringIO = s.dahDuringIO || s.writing
		s.dahs = append(s.dahs, dah)
		s.mu.Unlock()
	}

	return s.Store.SetDAH(ctx, key, fileType, dah, opts...)
}

// TestALaneBitmapsFileWriteAndRefreshDoNotOverlap: a delete-at update of a subtree's lane bitmaps
// file waits for a write of that file in flight, so it cannot land between the write's publish and
// its own delete-at.
func TestALaneBitmapsFileWriteAndRefreshDoNotOverlap(t *testing.T) {
	server, memoryStore := setupServerWithSettings(t, func(s *settings.Settings) { s.UtxoStore.LaneMigration = settings.LaneMigrationShadow })

	store := &blockingBitmapsStore{Store: memoryStore, entered: make(chan struct{}), release: make(chan struct{})}
	server.subtreeStore = store

	root := chainhash.HashH([]byte("write-and-refresh"))
	server.ownLaneBitmaps.put(root, lane.LaneBitmaps{}, 1)

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()
		server.writeLaneBitmapsFile(t.Context(), lane.SubtreeBitmaps{SubtreeHash: root, Lanes: lane.LaneBitmaps{4: bitmapOfPositions(1)}}, 300)
	}()

	<-store.entered

	go func() {
		defer wg.Done()
		require.NoError(t, server.setLaneBitmapsFileDAH(t.Context(), root, 301))
	}()

	time.Sleep(100 * time.Millisecond) // the refresh, unserialized, would run now
	close(store.release)
	wg.Wait()

	store.mu.Lock()
	defer store.mu.Unlock()

	require.False(t, store.dahDuringIO, "the refresh waited for the write")
	require.Equal(t, []uint32{301}, store.dahs)
}

// TestTheRecordedFileDAHIsKeptAndNeverLowered: storing a kept subtree's bitmaps again keeps the
// delete-at height recorded for its file, and a lower one recorded later does not replace it.
func TestTheRecordedFileDAHIsKeptAndNeverLowered(t *testing.T) {
	var kept ownLaneBitmaps

	root := chainhash.HashH([]byte("recorded-file-dah"))

	kept.put(root, lane.LaneBitmaps{}, 1)
	kept.recordFileDAH(root, 300)

	kept.put(root, lane.LaneBitmaps{}, 2) // stored again
	require.True(t, kept.fileDAHLater(root, 299), "the recorded height survives a second keep")

	kept.recordFileDAH(root, 200) // an older one, recorded last
	require.True(t, kept.fileDAHLater(root, 250), "a lower height does not replace it")
}

// TestOwnLaneRowsDoNotWait: storing a block reads its subtrees' kept bitmaps and waits for none; a
// subtree with none kept is counted absent at once and the block is stored without rows.
func TestOwnLaneRowsDoNotWait(t *testing.T) {
	server, _ := setupServerWithSettings(t, func(s *settings.Settings) { s.UtxoStore.LaneMigration = settings.LaneMigrationShadow })

	kept := chainhash.HashH([]byte("lane-no-wait-kept"))
	server.ownLaneBitmaps.put(kept, lane.LaneBitmaps{3: bitmapOfPositions(9)}, 123)

	rows := server.ownLaneRows(t.Context(), &model.Block{Header: laneTestHeader(), Subtrees: []*chainhash.Hash{&kept}, TransactionCount: 2})
	require.Len(t, rows, 1)
	require.Equal(t, uint8(3), rows[0].Lane)

	absent := map[string]string{"block": lane.BlockOwn, "reason": lane.MissingBitmapsAbsent}
	before := ownLaneMetric(t, "teranode_lane_missing_bitmaps_total", absent)

	unkept := chainhash.HashH([]byte("lane-no-wait-unkept"))

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	require.Nil(t, server.ownLaneRows(ctx, &model.Block{Header: laneTestHeader(), Subtrees: []*chainhash.Hash{&kept, &unkept}, TransactionCount: 3}))
	require.NoError(t, ctx.Err(), "it returned without waiting")
	require.Equal(t, before+1, ownLaneMetric(t, "teranode_lane_missing_bitmaps_total", absent))
}

// TestStoreSubtreeDataWithoutKeptBitmapsWritesNoFile: a subtree with no lane bitmaps kept is stored
// without a lane bitmaps file, counted as write_failed; the storage worker does not build bitmaps
// from the pool.
func TestStoreSubtreeDataWithoutKeptBitmapsWritesNoFile(t *testing.T) {
	server, memoryStore := setupServerWithSettings(t, func(s *settings.Settings) { s.UtxoStore.LaneMigration = settings.LaneMigrationShadow })

	writeFailed := map[string]string{"block": lane.BlockOwn, "reason": lane.MissingBitmapsWriteFailed}
	before := ownLaneMetric(t, "teranode_lane_missing_bitmaps_total", writeFailed)

	st, txMap := laneSealSubtree(t, "not-kept")

	subtreeDone, allDone, err := server.storeSubtreeData(t.Context(), subtreeprocessor.NewSubtreeRequest{Subtree: st, ParentTxMap: txMap}, make(chan *subtreeRetrySend, 10))
	require.NoError(t, err)
	require.True(t, <-subtreeDone, "the subtree is stored")
	<-allDone

	_, err = memoryStore.Get(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtreeLaneBitmaps)
	require.Error(t, err)
	require.Zero(t, server.ownLaneBitmaps.len())
	require.Equal(t, before+1, ownLaneMetric(t, "teranode_lane_missing_bitmaps_total", writeFailed))
}

// stuckStorageStore holds every subtree storage at its first store call until release is closed.
type stuckStorageStore struct {
	*memory.Memory
	release chan struct{}
}

func (s *stuckStorageStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...blobOptions.FileOption) (bool, error) {
	select {
	case <-s.release:
	case <-ctx.Done():
		return false, ctx.Err()
	}

	return s.Memory.Exists(ctx, key, fileType, opts...)
}

// TestOwnBlockGetsItsRowsWhileSubtreeStorageIsStuck: a block mined from subtrees whose storage has
// not even begun is stored with complete lane rows: the subtrees' bitmaps were kept when they were
// sealed, not when they were stored.
func TestOwnBlockGetsItsRowsWhileSubtreeStorageIsStuck(t *testing.T) {
	release := make(chan struct{})

	o := startOwnLaneServerOn(t, "sqlitememory:///", true, func(m *memory.Memory) blob.Store {
		return &stuckStorageStore{Memory: m, release: release}
	})

	// runs before the server's cleanup, so its storage workers are not left holding the store
	t.Cleanup(func() { close(release) })

	_, err := o.server.AddTxBatchColumnar(o.ctx, ownLaneColumnarBatch(t))
	require.NoError(t, err)

	var candidate *model.MiningCandidate

	require.Eventually(t, func() bool {
		candidate, err = o.server.GetMiningCandidate(o.ctx, &blockassembly_api.GetMiningCandidateRequest{IncludeSubtrees: true})

		return err == nil && candidate != nil && len(candidate.SubtreeHashes) == 2
	}, 10*time.Second, 50*time.Millisecond, "the candidate lists the two complete subtrees")

	for _, subtreeHash := range candidate.SubtreeHashes {
		exists, existsErr := o.subtreeStore.Exists(o.ctx, subtreeHash, fileformat.FileTypeSubtree)
		require.NoError(t, existsErr)
		require.False(t, exists, "the subtree is not stored")
	}

	solution, err := mining.Mine(o.ctx, o.server.settings, candidate, nil)
	require.NoError(t, err)

	resp, err := o.server.SubmitMiningSolution(o.ctx, &blockassembly_api.SubmitMiningSolutionRequest{
		Id:         candidate.Id,
		Nonce:      solution.Nonce,
		Time:       solution.Time,
		Version:    solution.Version,
		CoinbaseTx: solution.Coinbase,
	})
	require.NoError(t, err)
	require.True(t, resp.Ok)

	opts := o.spy.lastOpts()
	require.True(t, opts.LaneMapRowsOwnBlock)

	want := wantOwnLaneRows(t)
	require.Len(t, opts.LaneMapRows, len(want), "the block goes with complete rows")

	for i := range want {
		same, sameErr := lane.SameRow(want[i], opts.LaneMapRows[i])
		require.NoError(t, sameErr)
		require.True(t, same, "row %d: want %+v, sent %+v", i, want[i], opts.LaneMapRows[i])
	}
}

// TestKeptLaneBitmapsTakeTheUtxoStoreHeight: bitmaps the subtree processor keeps are kept at the
// UTXO store's block height, so they age out by the blocks since then, not from height 0.
func TestKeptLaneBitmapsTakeTheUtxoStoreHeight(t *testing.T) {
	server, _ := setupServerWithSettings(t, func(s *settings.Settings) { s.UtxoStore.LaneMigration = settings.LaneMigrationShadow })
	require.NoError(t, server.blockAssembler.utxoStore.SetBlockHeight(500))

	root := chainhash.HashH([]byte("kept-at-store-height"))
	server.ownLaneBitmaps.KeepLaneBitmaps(lane.SubtreeBitmaps{SubtreeHash: root, Lanes: lane.LaneBitmaps{}}, 0)

	// a subtree kept ownLaneBitmapsKeepBlocks blocks later drops entries kept before 500
	server.ownLaneBitmaps.put(chainhash.HashH([]byte("kept-later")), lane.LaneBitmaps{}, 500+ownLaneBitmapsKeepBlocks)

	_, missing := server.ownLaneBitmaps.get([]*chainhash.Hash{&root})
	require.Zero(t, missing, "kept at 500, inside the margin")
}

// TestWritingOverAnExistingLaneBitmapsFileMovesItsDeleteAt: a lane bitmaps file already in the
// store, written by subtree validation or before a restart, gets the subtree's delete-at height
// when block assembly writes the subtree's bitmaps, so it lives as long as the subtree.
func TestWritingOverAnExistingLaneBitmapsFileMovesItsDeleteAt(t *testing.T) {
	server, memoryStore := setupServerWithSettings(t, func(s *settings.Settings) { s.UtxoStore.LaneMigration = settings.LaneMigrationShadow })
	store := &setOrderStore{Store: memoryStore}
	server.subtreeStore = store

	st, txMap := laneSealSubtree(t, "existing-file")
	keepSealed(server, st, txMap)

	old, err := lane.EncodeSubtreeBitmaps(lane.SubtreeBitmaps{SubtreeHash: *st.RootHash(), Lanes: lane.LaneBitmaps{}})
	require.NoError(t, err)
	require.NoError(t, memoryStore.Set(t.Context(), st.RootHash()[:], fileformat.FileTypeSubtreeLaneBitmaps, old, blobOptions.WithDeleteAt(5)))

	server.writeKeptLaneBitmapsFile(t.Context(), *st.RootHash(), 1, 300)

	require.Equal(t, uint32(300), store.dah(fileformat.FileTypeSubtreeLaneBitmaps))
}
