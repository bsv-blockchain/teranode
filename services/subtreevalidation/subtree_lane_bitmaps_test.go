package subtreevalidation

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// recordingStore records the file type and delete-at height of every Set, in order, and can be
// told to refuse one file type.
type recordingStore struct {
	blob.Store

	mu     sync.Mutex
	writes []recordedWrite
	refuse fileformat.FileType
	// slow is a file type whose write takes slowFor longer.
	slow    fileformat.FileType
	slowFor time.Duration
}

type recordedWrite struct {
	fileType fileformat.FileType
	dah      uint32
}

// Set records each write once it has completed, in the order writes complete.
func (s *recordingStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...bloboptions.FileOption) error {
	if fileType == s.refuse {
		return errors.NewStorageError("refused for the test")
	}

	if fileType == s.slow {
		time.Sleep(s.slowFor)
	}

	err := s.Store.Set(ctx, key, fileType, value, opts...)

	s.mu.Lock()
	s.writes = append(s.writes, recordedWrite{fileType: fileType, dah: bloboptions.NewFileOptions(opts...).DAH})
	s.mu.Unlock()

	return err
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

// The lane bitmaps are written beside the meta and before the subtree file, with the same expiry,
// so a stored subtree always has them and the meta never waits for them; with no bitmaps, as at
// utxostore_laneMigration off, nothing extra is written.
func TestStoreSubtreeFiles_WritesLaneBitmapsBeforeTheSubtree(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name    string
		bitmaps *lane.SubtreeBitmaps
		want    []fileformat.FileType
	}{
		{"off", nil, []fileformat.FileType{fileformat.FileTypeSubtreeMeta, fileformat.FileTypeSubtree}},
		// The bitmaps write is held back, so it finishes after the meta, which does not wait for
		// it, and before the subtree, which does.
		{"shadow", &lane.SubtreeBitmaps{}, []fileformat.FileType{fileformat.FileTypeSubtreeMeta, fileformat.FileTypeSubtreeLaneBitmaps, fileformat.FileTypeSubtree}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, m := laneBitmapsSubtree(t)
			root := st.RootHash()

			store := &recordingStore{Store: blobmemory.New(), slow: fileformat.FileTypeSubtreeLaneBitmaps, slowFor: 100 * time.Millisecond}

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

// A call that fails on its meta write returns only once the bitmaps write beside it has finished,
// so no write outlives the call.
func TestStoreSubtreeFiles_WaitsForTheBitmapsOnAnEarlyReturn(t *testing.T) {
	st, m := laneBitmapsSubtree(t)
	root := st.RootHash()

	store := &recordingStore{Store: blobmemory.New(), refuse: fileformat.FileTypeSubtreeMeta, slow: fileformat.FileTypeSubtreeLaneBitmaps, slowFor: 100 * time.Millisecond}

	utxoStore := &utxo.MockUtxostore{}
	utxoStore.On("GetBlockHeight").Return(uint32(100)).Maybe()

	server := &Server{logger: ulogger.TestLogger{}, settings: test.CreateBaseTestSettings(t), subtreeStore: store, utxoStore: utxoStore}

	bitmaps := &lane.SubtreeBitmaps{SubtreeHash: *root, Lanes: lane.LaneBitmaps{7: bitmapOfPositions(1)}}
	require.Error(t, server.storeSubtreeFiles(context.Background(), gocore.NewStat("test"), root, st, m, bitmaps))

	store.mu.Lock()
	defer store.mu.Unlock()

	require.Len(t, store.writes, 1, "the bitmaps write had finished when the call returned")
	require.Equal(t, fileformat.FileTypeSubtreeLaneBitmaps, store.writes[0].fileType)
}

// A lane bitmaps file already there, from an earlier validation of the same subtree, is kept.
func TestStoreSubtreeFiles_KeepsLaneBitmapsAlreadyThere(t *testing.T) {
	ctx := context.Background()

	st, m := laneBitmapsSubtree(t)
	root := st.RootHash()

	store := blobmemory.New()

	earlier, err := lane.EncodeSubtreeBitmaps(lane.SubtreeBitmaps{SubtreeHash: *root, Lanes: lane.LaneBitmaps{7: bitmapOfPositions(1)}})
	require.NoError(t, err)
	require.NoError(t, store.Set(ctx, root[:], fileformat.FileTypeSubtreeLaneBitmaps, earlier))

	utxoStore := &utxo.MockUtxostore{}
	utxoStore.On("GetBlockHeight").Return(uint32(100)).Maybe()

	server := &Server{logger: ulogger.TestLogger{}, settings: test.CreateBaseTestSettings(t), subtreeStore: store, utxoStore: utxoStore}

	bitmaps := &lane.SubtreeBitmaps{SubtreeHash: *root, Lanes: lane.LaneBitmaps{9: bitmapOfPositions(5)}}

	writeFailed := laneMissingBitmaps(t, lane.MissingBitmapsWriteFailed)

	require.NoError(t, server.storeSubtreeFiles(ctx, gocore.NewStat("test"), root, st, m, bitmaps))

	got, err := store.Get(ctx, root[:], fileformat.FileTypeSubtreeLaneBitmaps)
	require.NoError(t, err)
	require.Equal(t, earlier, got)
	require.Equal(t, writeFailed, laneMissingBitmaps(t, lane.MissingBitmapsWriteFailed), "a file already there is not a failed write")
}

// laneMissingBitmaps reads teranode_lane_missing_bitmaps_total for reason.
func laneMissingBitmaps(t *testing.T, reason string) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, f := range families {
		if f.GetName() != "teranode_lane_missing_bitmaps_total" {
			continue
		}

		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "reason" && l.GetValue() == reason {
					return m.GetCounter().GetValue()
				}
			}
		}
	}

	return 0
}

// At shadow a store read asks for each transaction's own lane position and puts it in the
// subtree's metadata; at off it asks for no position.
func TestProcessTxMetaUsingStore_ReadsTheLanePositionAtShadow(t *testing.T) {
	for _, mode := range []settings.LaneMigration{settings.LaneMigrationOff, settings.LaneMigrationShadow} {
		t.Run(string(mode), func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.UtxoStore.LaneMigration = mode

			hashes := []chainhash.Hash{{0x01}, {0x02}}

			var asked []fields.FieldName

			store := &utxo.MockUtxostore{}
			store.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
				items := args.Get(1).([]*utxo.UnresolvedMetaData)
				for _, f := range args[2:] {
					asked = append(asked, f.([]fields.FieldName)...)
				}

				for _, it := range items {
					it.Data = &meta.Data{Fee: 1, SizeInBytes: 1, LanePosition: lane.Pack(4, uint64(it.Idx)+1)} //nolint:gosec // a test index
				}
			}).Return(nil)

			server := &Server{logger: ulogger.TestLogger{}, settings: tSettings, utxoStore: store}

			txMetaSlice := make([]metaSliceItem, len(hashes))
			missed, err := server.processTxMetaUsingStore(context.Background(), hashes, txMetaSlice, nil, true, false, true)
			require.NoError(t, err)
			require.Zero(t, missed)

			if mode == settings.LaneMigrationOff {
				require.NotContains(t, asked, fields.LanePosition)
				return
			}

			require.Contains(t, asked, fields.LanePosition)
			require.Equal(t, lane.Pack(4, 1), txMetaSlice[0].lanePosition)
			require.Equal(t, lane.Pack(4, 2), txMetaSlice[1].lanePosition)
		})
	}
}

// With subtreevalidation_batch_missing_transactions off, each transaction is read on its own. At
// shadow that read is a one-transaction BatchDecorate asking for GetMeta's fields and the lane
// position, which GetMeta does not read; through the txmeta cache it fills the cache as the
// batched read does. It keeps GetMeta's coinbase flag, which makes a subtree listing a coinbase
// invalid. At off it is GetMeta as before.
func TestProcessTxMetaUsingStore_ReadsTheLanePositionOneByOneAtShadow(t *testing.T) {
	for _, mode := range []settings.LaneMigration{settings.LaneMigrationOff, settings.LaneMigrationShadow} {
		t.Run(string(mode), func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.UtxoStore.LaneMigration = mode

			hashes := []chainhash.Hash{{0x01}, {0x02}}

			var asked []fields.FieldName

			store := &utxo.MockUtxostore{}
			store.On("GetMeta", mock.Anything, mock.Anything, mock.Anything).Return(&meta.Data{Fee: 1, SizeInBytes: 1})

			var mu sync.Mutex

			store.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
				items := args.Get(1).([]*utxo.UnresolvedMetaData)

				mu.Lock()
				defer mu.Unlock()

				require.Len(t, items, 1, "one transaction a read")

				for _, f := range args[2:] {
					asked = append(asked, f.([]fields.FieldName)...)
				}

				items[0].Data = &meta.Data{Fee: 1, SizeInBytes: 1, IsCoinbase: items[0].Hash[0] == 2, LanePosition: lane.Pack(4, uint64(items[0].Hash[0]))}
			}).Return(nil)

			server := &Server{logger: ulogger.TestLogger{}, settings: tSettings, utxoStore: store}

			txMetaSlice := make([]metaSliceItem, len(hashes))
			missed, err := server.processTxMetaUsingStore(context.Background(), hashes, txMetaSlice, nil, false, false, true)
			require.NoError(t, err)
			require.Zero(t, missed)
			require.True(t, txMetaSlice[0].isSet)
			require.True(t, txMetaSlice[1].isSet)

			if mode == settings.LaneMigrationOff {
				store.AssertNotCalled(t, "BatchDecorate", mock.Anything, mock.Anything, mock.Anything)
				require.Zero(t, txMetaSlice[0].lanePosition)

				return
			}

			store.AssertNotCalled(t, "GetMeta", mock.Anything, mock.Anything, mock.Anything)
			require.Contains(t, asked, fields.LanePosition)

			for _, f := range utxo.MetaFields {
				require.Contains(t, asked, f, "everything GetMeta reads is still read")
			}

			require.False(t, txMetaSlice[0].coinbase)
			require.True(t, txMetaSlice[1].coinbase, "the coinbase flag comes through")
			require.Equal(t, lane.Pack(4, 1), txMetaSlice[0].lanePosition)
			require.Equal(t, lane.Pack(4, 2), txMetaSlice[1].lanePosition)
		})
	}
}

// At shadow a one-by-one read that finds nothing, by the store's error, a per-transaction error or
// no data, fails the read as GetMeta's error does at off, and sets nothing.
func TestProcessTxMetaUsingStore_OneByOneReadFailsWhenTheStoreFindsNothingAtShadow(t *testing.T) {
	for name, tc := range map[string]struct {
		returns error
		item    error
		want    error
	}{
		"the store's error":       {returns: errors.NewInvalidArgumentError("store refused"), want: errors.ErrInvalidArgument},
		"a per-transaction error": {item: errors.NewProcessingError("bad record"), want: errors.ErrProcessing},
		"no data":                 {want: errors.ErrTxNotFound},
	} {
		t.Run(name, func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.UtxoStore.LaneMigration = settings.LaneMigrationShadow

			store := &utxo.MockUtxostore{}
			store.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
				args.Get(1).([]*utxo.UnresolvedMetaData)[0].Err = tc.item
			}).Return(tc.returns)

			server := &Server{logger: ulogger.TestLogger{}, settings: tSettings, utxoStore: store}

			txMetaSlice := make([]metaSliceItem, 1)
			_, readErr := server.processTxMetaUsingStore(context.Background(), []chainhash.Hash{{0x01}}, txMetaSlice, nil, false, false, true)
			require.ErrorIs(t, readErr, tc.want, "the store's own reason comes back")
			require.False(t, txMetaSlice[0].isSet)
		})
	}
}

// A one-by-one read the store fails transiently is retried with a fresh request, so what the failed
// attempt left on its item cannot fail the retry that found the transaction.
func TestProcessTxMetaUsingStore_OneByOneReadRetriesWithAFreshRequest(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.LaneMigration = settings.LaneMigrationShadow
	tSettings.BlockValidation.ProcessTxMetaUsingStoreRetries = 2

	calls := 0

	store := &utxo.MockUtxostore{}
	store.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		item := args.Get(1).([]*utxo.UnresolvedMetaData)[0]

		calls++
		if calls == 1 {
			item.Err = errors.NewServiceUnavailableError("busy")
			return
		}

		item.Data = &meta.Data{Fee: 1, SizeInBytes: 1, LanePosition: lane.Pack(4, 9)}
	}).Return(nil)

	server := &Server{logger: ulogger.TestLogger{}, settings: tSettings, utxoStore: store}

	txMetaSlice := make([]metaSliceItem, 1)
	_, err := server.processTxMetaUsingStore(context.Background(), []chainhash.Hash{{0x01}}, txMetaSlice, nil, false, false, true)
	require.NoError(t, err)
	require.Equal(t, 2, calls, "the transient failure was retried")
	require.Equal(t, lane.Pack(4, 9), txMetaSlice[0].lanePosition)
}

// Through the txmeta cache, the shadow one-by-one read fills the cache as GetMeta did, now with the
// own lane position, so the transaction is read from the cache next time. The shared field set it
// builds its fields from is left as it was.
func TestProcessTxMetaUsingStore_OneByOneReadFillsTheCacheAtShadow(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.LaneMigration = settings.LaneMigrationShadow

	metaFields := append([]fields.FieldName(nil), utxo.MetaFields...)

	store := &utxo.MockUtxostore{}
	store.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		args.Get(1).([]*utxo.UnresolvedMetaData)[0].Data = &meta.Data{
			Fee:          1,
			SizeInBytes:  1,
			TxInpoints:   subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{{0x09}}, []uint32{1, 0}),
			LanePosition: lane.Pack(4, 1),
		}
	}).Return(nil)

	cache, err := txmetacache.NewTxMetaCache(context.Background(), tSettings, ulogger.TestLogger{}, store, txmetacache.Unallocated)
	require.NoError(t, err)

	server := &Server{logger: ulogger.TestLogger{}, settings: tSettings, utxoStore: cache}

	hashes := []chainhash.Hash{{0x01}}
	txMetaSlice := make([]metaSliceItem, len(hashes))

	_, err = server.processTxMetaUsingStore(context.Background(), hashes, txMetaSlice, nil, false, false, false)
	require.NoError(t, err)
	require.Equal(t, lane.Pack(4, 1), txMetaSlice[0].lanePosition)

	out, found := make([]txmetacache.CachedMeta, 1), make([]bool, 1)
	_, err = cache.(*txmetacache.TxMetaCache).GetMetaCachedInto(context.Background(), hashes, nil, out, found)
	require.NoError(t, err)
	require.True(t, found[0], "the read filled the cache")
	require.Equal(t, lane.Pack(4, 1), out[0].LanePosition, "with the own position")

	require.Equal(t, metaFields, utxo.MetaFields, "utxo.MetaFields is shared and left as it was")
}

// A transaction in neither the cache nor the store is validated here, and the position its create
// returns goes into the subtree's metadata, so the subtree's lane bitmaps hold it.
func TestBlessMissingTransactions_KeepsThePositionTheCreateReturns(t *testing.T) {
	server, _, txs, _ := newUnlockFailureServer(t, true, 0)
	server.validatorClient = &positionValidator{Interface: server.validatorClient, position: lane.Pack(8, 14)}

	missing := []missingTx{{tx: txs[1], idx: 0}}
	txMetaSlice := make([]metaSliceItem, len(missing))

	require.NoError(t, server.blessMissingTransactions(context.Background(), chainhash.Hash{}, missing, "", "", txMetaSlice, 2, map[uint32]bool{}, true))
	require.True(t, txMetaSlice[0].isSet)
	require.Equal(t, lane.Pack(8, 14), txMetaSlice[0].lanePosition)
}

// At shadow subtree validation fills the lane position of metadata that came back without one,
// GetMeta's, from the store; a record that has its position, or lanes off, needs no read, and a
// failure to read it leaves it at zero without an error, because the position is only measured.
func TestFillLanePosition(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mode    settings.LaneMigration
		has     uint64
		getErr  error
		getNone bool
		want    uint64
		reads   bool
	}{
		{name: "shadow", mode: settings.LaneMigrationShadow, want: lane.Pack(8, 14), reads: true},
		{name: "shadow, the position already there", mode: settings.LaneMigrationShadow, has: lane.Pack(3, 7), want: lane.Pack(3, 7)},
		{name: "shadow, the position not read", mode: settings.LaneMigrationShadow, getErr: errors.NewStorageError("down"), reads: true},
		{name: "shadow, the store returns no record and no error", mode: settings.LaneMigrationShadow, getNone: true, reads: true},
		{name: "off", mode: settings.LaneMigrationOff},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.UtxoStore.LaneMigration = tc.mode

			store := &utxo.MockUtxostore{}

			switch {
			case tc.getErr != nil, tc.getNone:
				store.On("Get", mock.Anything, mock.Anything, []fields.FieldName{fields.LanePosition}).Return(nil, tc.getErr)
			default:
				store.On("Get", mock.Anything, mock.Anything, []fields.FieldName{fields.LanePosition}).Return(&meta.Data{LanePosition: lane.Pack(8, 14)}, nil)
			}

			server := &Server{logger: ulogger.TestLogger{}, settings: tSettings, utxoStore: store}

			md := &meta.Data{Fee: 1, LanePosition: tc.has}
			server.fillLanePosition(context.Background(), &chainhash.Hash{0x01}, md)
			require.Equal(t, tc.want, md.LanePosition)

			if !tc.reads {
				store.AssertNotCalled(t, "Get", mock.Anything, mock.Anything, mock.Anything)
			}
		})
	}
}

// A transaction subtree validation validates itself keeps the position its record holds even when
// the validator's answer has none, as an older remote validator's or the SQL store's create gives:
// at shadow it is read from the store before it goes into the subtree's metadata.
func TestBlessMissingTransactions_FillsThePositionTheAnswerLacks(t *testing.T) {
	server, _, txs, _ := newUnlockFailureServer(t, true, 0)
	server.settings.UtxoStore.LaneMigration = settings.LaneMigrationShadow
	server.validatorClient = &positionValidator{Interface: server.validatorClient, position: lane.None}
	server.utxoStore = &lanePositionStore{Store: server.utxoStore, position: lane.Pack(8, 21)}

	missing := []missingTx{{tx: txs[1], idx: 0}}
	txMetaSlice := make([]metaSliceItem, len(missing))

	require.NoError(t, server.blessMissingTransactions(context.Background(), chainhash.Hash{}, missing, "", "", txMetaSlice, 2, map[uint32]bool{}, true))
	require.Equal(t, lane.Pack(8, 21), txMetaSlice[0].lanePosition)
}

// lanePositionStore answers a read that names only fields.LanePosition with position, as a store
// holding the record's position does; every other call goes through.
type lanePositionStore struct {
	utxo.Store
	position uint64
}

func (s *lanePositionStore) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	if len(f) == 1 && f[0] == fields.LanePosition {
		return &meta.Data{LanePosition: s.position}, nil
	}

	return s.Store.Get(ctx, hash, f...)
}

// positionValidator validates as its validator does and gives the created record position, as a
// validator at shadow on utxoset or Aerospike does, in process or over gRPC (the validate response
// carries the position beside the metadata; services/validator's client tests pin that).
type positionValidator struct {
	validator.Interface
	position uint64
}

func (p *positionValidator) ValidateWithOptions(ctx context.Context, tx *bt.Tx, height uint32, opts *validator.Options) (*meta.Data, error) {
	md, err := p.Interface.ValidateWithOptions(ctx, tx, height, opts)
	if md != nil {
		md.LanePosition = p.position
	}

	return md, err
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

	writeFailed := laneMissingBitmaps(t, lane.MissingBitmapsWriteFailed)

	require.NoError(t, server.storeSubtreeFiles(ctx, gocore.NewStat("test"), root, st, m, bitmaps))

	exists, err := store.Exists(ctx, root[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, writeFailed+1, laneMissingBitmaps(t, lane.MissingBitmapsWriteFailed), "the failed write is counted as one")
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

			// the tip a node validating at the block's height has, so the records' unmined_since reads
			// as the height they were validated at
			require.NoError(t, utxoStore.SetBlockHeight(chaincfg.GenesisActivationHeight-1))

			for _, tx := range []*bt.Tx{tx1, tx2, tx3, tx4} {
				// validated at the height of the block below, so the block takes their stored verdicts
				_, _, err := utxoStore.SpendAndCreate(context.Background(), tx, chaincfg.GenesisActivationHeight, utxo.WithCreateOnly())
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

			// A block's first subtree: the coinbase placeholder, which has no position yet and is
			// neither in the bitmaps nor counted, then the four transactions.
			st, err := subtreepkg.NewTreeByLeafCount(8)
			require.NoError(t, err)
			require.NoError(t, st.AddCoinbaseNode())

			for _, h := range []*chainhash.Hash{hash1, hash2, hash3, hash4} {
				require.NoError(t, st.AddNode(*h, 1, 0))
			}

			unpositioned := laneTxsWithoutPosition(t)

			_, err = server.ValidateSubtreeInternal(context.Background(), ValidateSubtree{
				SubtreeHash: *st.RootHash(),
				BaseURL:     "http://localhost:8000",
				TxHashes:    []chainhash.Hash{*subtreepkg.CoinbasePlaceholderHash, *hash1, *hash2, *hash3, *hash4},
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
			require.Equal(t, *st.RootHash(), got.SubtreeHash, "the file names its own subtree")
			require.Len(t, got.Lanes, 2)
			require.Equal(t, []uint64{10, 12}, got.Lanes[7].ToArray())
			require.Equal(t, []uint64{1}, got.Lanes[3].ToArray())
			require.Equal(t, unpositioned+1, laneTxsWithoutPosition(t), "tx4 has no position")
		})
	}
}

// A transaction whose own position is known but one of whose parents has none, as on mainnet for a
// spend of an output created before lane positions were on, reaches the cache through an
// own-position txmeta entry and lands in the subtree's lane bitmaps. Through a plain ADD entry,
// which the validator sent for it before WireActionADDOwn, the cache has no position, so the
// subtree's bitmaps leave it out and count it, and its block's rows come up short.
func TestValidateSubtreeInternal_AnOwnPositionEntryReachesTheLaneBitmaps(t *testing.T) {
	for _, tc := range []struct {
		name   string
		action byte
		want   []uint64
	}{
		{name: "own-position entry", action: txmetacache.WireActionADDOwn, want: []uint64{10, 14}},
		{name: "plain entry", action: txmetacache.WireActionADD, want: []uint64{10}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			InitPrometheusMetrics()

			utxoStore, validatorClient, txStore, subtreeStore, blockchainClient, deferFunc := setup(t)
			defer deferFunc()

			// the tip a node validating at the block's height has, so the records' unmined_since reads
			// as the height they were validated at
			require.NoError(t, utxoStore.SetBlockHeight(chaincfg.GenesisActivationHeight-1))

			for _, tx := range []*bt.Tx{tx1, tx2} {
				// validated at the height of the block below, so the block takes their stored verdicts
				_, _, err := utxoStore.SpendAndCreate(context.Background(), tx, chaincfg.GenesisActivationHeight, utxo.WithCreateOnly())
				require.NoError(t, err)
			}

			tSettings := test.CreateBaseTestSettings(t)
			tSettings.SubtreeValidation.TxMetaCacheEnabled = true
			tSettings.UtxoStore.LaneMigration = settings.LaneMigrationShadow

			nilConsumer := &kafka.KafkaConsumerGroup{}

			server, err := New(context.Background(), ulogger.TestLogger{}, tSettings, subtreeStore, txStore, utxoStore, validatorClient, blockchainClient, nilConsumer, nilConsumer, nil, nil)
			require.NoError(t, err)

			// tx1 arrives in the lane encoding, tx2 in the entry under test, both as the validator
			// sends them.
			first, err := subtreepkg.NewTxInpointsFromTx(tx1)
			require.NoError(t, err)

			laneContent, err := (&meta.Data{Fee: 1, SizeInBytes: 1, TxInpoints: first, LanePosition: lane.Pack(8, 10)}).LaneMetaBytesInto(nil, []uint64{lane.Pack(8, 1)})
			require.NoError(t, err)

			second, err := subtreepkg.NewTxInpointsFromTx(tx2)
			require.NoError(t, err)

			d := &meta.Data{Fee: 1, SizeInBytes: 1, TxInpoints: second, LanePosition: lane.Pack(8, 14)}

			content, err := d.CacheBytes()
			require.NoError(t, err)

			if tc.action == txmetacache.WireActionADD {
				content, err = d.MetaBytes()
				require.NoError(t, err)
			}

			require.NoError(t, server.txmetaHandler(context.Background(), txmetaMessage(false,
				txmetaEntry{hash: *hash1, action: txmetacache.WireActionADDLane, content: laneContent},
				txmetaEntry{hash: *hash2, action: tc.action, content: content},
			)))

			st, err := subtreepkg.NewTreeByLeafCount(4)
			require.NoError(t, err)

			for _, h := range []*chainhash.Hash{hash1, hash2} {
				require.NoError(t, st.AddNode(*h, 1, 0))
			}

			unpositioned := laneTxsWithoutPosition(t)

			_, err = server.ValidateSubtreeInternal(context.Background(), ValidateSubtree{
				SubtreeHash: *st.RootHash(),
				BaseURL:     "http://localhost:8000",
				TxHashes:    []chainhash.Hash{*hash1, *hash2},
				FromBlock:   true,
			}, chaincfg.GenesisActivationHeight, nil)
			require.NoError(t, err)

			data, err := subtreeStore.Get(context.Background(), st.RootHash()[:], fileformat.FileTypeSubtreeLaneBitmaps)
			require.NoError(t, err)

			got, err := lane.DecodeSubtreeBitmaps(data)
			require.NoError(t, err)
			require.Equal(t, tc.want, got.Lanes[8].ToArray())
			require.Equal(t, unpositioned+float64(2-len(tc.want)), laneTxsWithoutPosition(t))
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
