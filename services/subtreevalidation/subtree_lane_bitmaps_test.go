package subtreevalidation

import (
	"context"
	"sync"
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// recordingStore records the file type and delete-at height of every Set, in order, and can be
// told to refuse one file type.
type recordingStore struct {
	blob.Store

	mu     sync.Mutex
	writes []recordedWrite
	refuse fileformat.FileType
}

type recordedWrite struct {
	fileType fileformat.FileType
	dah      uint32
}

func (s *recordingStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...bloboptions.FileOption) error {
	if fileType == s.refuse {
		return errors.NewStorageError("refused for the test")
	}

	s.mu.Lock()
	s.writes = append(s.writes, recordedWrite{fileType: fileType, dah: bloboptions.NewFileOptions(opts...).DAH})
	s.mu.Unlock()

	return s.Store.Set(ctx, key, fileType, value, opts...)
}

func laneBitmapsSubtree(t *testing.T) (*subtreepkg.Subtree, *subtreepkg.Meta) {
	t.Helper()

	st, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, st.AddNode(chainhash.HashH([]byte{0x01}), 1, 100))
	require.NoError(t, st.AddNode(chainhash.HashH([]byte{0x02}), 1, 100))

	m := subtreepkg.NewSubtreeMeta(st)
	for i := range st.Nodes {
		parent := chainhash.HashH([]byte{byte(i), 0xbb})
		require.NoError(t, m.SetTxInpoints(i, subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{parent}, []uint32{1, 0})))
	}

	return st, m
}

// The lane bitmaps are written before the subtree file, with the same expiry, so a stored subtree
// always has them; with no bitmaps, as at utxostore_laneMigration off, nothing extra is written.
func TestStoreSubtreeFiles_WritesLaneBitmapsBeforeTheSubtree(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name    string
		bitmaps *lane.SubtreeBitmaps
		want    []fileformat.FileType
	}{
		{"off", nil, []fileformat.FileType{fileformat.FileTypeSubtreeMeta, fileformat.FileTypeSubtree}},
		{"shadow", &lane.SubtreeBitmaps{}, []fileformat.FileType{fileformat.FileTypeSubtreeLaneBitmaps, fileformat.FileTypeSubtreeMeta, fileformat.FileTypeSubtree}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, m := laneBitmapsSubtree(t)
			root := st.RootHash()

			store := &recordingStore{Store: blobmemory.New()}

			utxoStore := &utxo.MockUtxostore{}
			utxoStore.On("GetBlockHeight").Return(uint32(100)).Maybe()

			server := &Server{logger: ulogger.TestLogger{}, settings: test.CreateBaseTestSettings(t), subtreeStore: store, utxoStore: utxoStore}

			var bitmaps *lane.SubtreeBitmaps
			if tc.bitmaps != nil {
				bitmaps = &lane.SubtreeBitmaps{SubtreeHash: *root, Lanes: lane.LaneBitmaps{7: bitmapOfPositions(1, 2)}}
			}

			require.NoError(t, server.storeSubtreeFiles(ctx, gocore.NewStat("test"), root, st, m, bitmaps))

			types := make([]fileformat.FileType, 0, len(store.writes))
			for _, w := range store.writes {
				types = append(types, w.fileType)
				require.Equal(t, store.writes[0].dah, w.dah, "every file of the subtree has one expiry")
			}

			require.Equal(t, tc.want, types)

			if bitmaps != nil {
				data, err := store.Get(ctx, root[:], fileformat.FileTypeSubtreeLaneBitmaps)
				require.NoError(t, err)

				got, err := lane.DecodeSubtreeBitmaps(data)
				require.NoError(t, err)
				require.Equal(t, *root, got.SubtreeHash)
				require.Equal(t, []uint64{1, 2}, got.Lanes[7].ToArray())
			}
		})
	}
}

// At shadow nothing acts on lanes: a lane bitmaps file that cannot be written leaves the subtree
// to be stored as it is stored without lanes, and its block goes without rows.
func TestStoreSubtreeFiles_ALaneBitmapsFailureDoesNotStopTheSubtree(t *testing.T) {
	ctx := context.Background()

	st, m := laneBitmapsSubtree(t)
	root := st.RootHash()

	store := &recordingStore{Store: blobmemory.New(), refuse: fileformat.FileTypeSubtreeLaneBitmaps}

	utxoStore := &utxo.MockUtxostore{}
	utxoStore.On("GetBlockHeight").Return(uint32(100)).Maybe()

	server := &Server{logger: ulogger.TestLogger{}, settings: test.CreateBaseTestSettings(t), subtreeStore: store, utxoStore: utxoStore}

	bitmaps := &lane.SubtreeBitmaps{SubtreeHash: *root, Lanes: lane.LaneBitmaps{7: bitmapOfPositions(1)}}
	require.NoError(t, server.storeSubtreeFiles(ctx, gocore.NewStat("test"), root, st, m, bitmaps))

	exists, err := store.Exists(ctx, root[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, exists)
}

// At shadow ValidateSubtreeInternal puts each member's own lane position, from the txmeta cache,
// into the subtree's lane bitmaps, and leaves out and counts a member whose position is unknown,
// here one read from a store that keeps no positions. At off it writes no lane bitmaps.
func TestValidateSubtreeInternal_BuildsLaneBitmapsAtShadow(t *testing.T) {
	for _, mode := range []settings.LaneMigration{settings.LaneMigrationOff, settings.LaneMigrationShadow} {
		t.Run(string(mode), func(t *testing.T) {
			InitPrometheusMetrics()

			utxoStore, validatorClient, txStore, subtreeStore, blockchainClient, deferFunc := setup(t)
			defer deferFunc()

			for _, tx := range []*bt.Tx{tx1, tx2, tx3, tx4} {
				_, _, err := utxoStore.SpendAndCreate(context.Background(), tx, 0, utxo.WithCreateOnly())
				require.NoError(t, err)
			}

			tSettings := test.CreateBaseTestSettings(t)
			tSettings.SubtreeValidation.TxMetaCacheEnabled = true
			tSettings.UtxoStore.LaneMigration = mode

			nilConsumer := &kafka.KafkaConsumerGroup{}

			server, err := New(context.Background(), ulogger.TestLogger{}, tSettings, subtreeStore, txStore, utxoStore, validatorClient, blockchainClient, nilConsumer, nilConsumer, nil, nil)
			require.NoError(t, err)

			cache, ok := server.utxoStore.(*txmetacache.TxMetaCache)
			require.True(t, ok)

			// Three members cached with their positions; tx4 is read from the store.
			positions := map[*bt.Tx]uint64{tx1: lane.Pack(7, 10), tx2: lane.Pack(7, 12), tx3: lane.Pack(3, 1)}
			for tx, position := range positions {
				inpoints, err := subtreepkg.NewTxInpointsFromTx(tx)
				require.NoError(t, err)

				require.NoError(t, cache.SetCache(tx.TxIDChainHash(), &meta.Data{Fee: 1, SizeInBytes: 1, TxInpoints: inpoints, LanePosition: position}))
			}

			st, err := subtreepkg.NewTreeByLeafCount(4)
			require.NoError(t, err)

			for _, h := range []*chainhash.Hash{hash1, hash2, hash3, hash4} {
				require.NoError(t, st.AddNode(*h, 1, 0))
			}

			unpositioned := laneTxsWithoutPosition(t)

			_, err = server.ValidateSubtreeInternal(context.Background(), ValidateSubtree{
				SubtreeHash: *st.RootHash(),
				BaseURL:     "http://localhost:8000",
				TxHashes:    []chainhash.Hash{*hash1, *hash2, *hash3, *hash4},
				FromBlock:   true,
			}, chaincfg.GenesisActivationHeight, nil)
			require.NoError(t, err)

			data, err := subtreeStore.Get(context.Background(), st.RootHash()[:], fileformat.FileTypeSubtreeLaneBitmaps)

			if mode == settings.LaneMigrationOff {
				require.ErrorIs(t, err, errors.ErrNotFound, "nothing is written at off")
				require.Equal(t, unpositioned, laneTxsWithoutPosition(t), "nothing is counted at off")

				return
			}

			require.NoError(t, err)

			got, err := lane.DecodeSubtreeBitmaps(data)
			require.NoError(t, err)
			require.Len(t, got.Lanes, 2)
			require.Equal(t, []uint64{10, 12}, got.Lanes[7].ToArray())
			require.Equal(t, []uint64{1}, got.Lanes[3].ToArray())
			require.Equal(t, unpositioned+1, laneTxsWithoutPosition(t), "tx4 has no position")
		})
	}
}

func bitmapOfPositions(positions ...uint64) *roaring64.Bitmap {
	bm := roaring64.New()
	bm.AddMany(positions)

	return bm
}

// laneTxsWithoutPosition reads the transactions this service has left out of lane bitmaps.
func laneTxsWithoutPosition(t *testing.T) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, f := range families {
		if f.GetName() != "teranode_lane_txs_without_position_total" {
			continue
		}

		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "service" && l.GetValue() == laneBitmapsService {
					return m.GetCounter().GetValue()
				}
			}
		}
	}

	return 0
}
