package seeder

import (
	"bytes"
	"context"
	"encoding/binary"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/settings"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxofactory "github.com/bsv-blockchain/teranode/stores/utxo/factory"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/utxoset"
	testAerospike "github.com/bsv-blockchain/teranode/test/utils/aerospike"
	tpostgres "github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/lanereader"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// The layout pinned by hand: A and C at height 7, B at 9, coinbase D at 7, E at 8, from 100.
func TestSeedLayoutHandsOutPositionsByHeight(t *testing.T) {
	w := func(name string, height uint32, coinbase bool) *utxopersister.UTXOWrapper {
		return &utxopersister.UTXOWrapper{TxID: chainhash.HashH([]byte(name)), Height: height, Coinbase: coinbase,
			UTXOs: []*utxopersister.UTXO{{Index: 0, Value: 1, Script: []byte{0x51}}}}
	}

	path := writeCompleteSnapshotFile(t, []*utxopersister.UTXOWrapper{w("A", 7, false), w("B", 9, false), w("C", 7, false), w("D", 7, true), w("E", 8, false)})

	layout, err := countSeedLayout(context.Background(), path)
	require.NoError(t, err)
	require.Equal(t, uint64(5), layout.total())

	lanes := &seededLanes{layout: layout, grant: lane.Grant{Lane: 3, First: 100, Last: 104}}
	got := positionsInFileOrder(t, path, lanes)
	require.Equal(t, []uint64{100, 103, 101, 104, 102}, got)

	// A second pass gives each record the same position.
	require.Equal(t, got, positionsInFileOrder(t, path, lanes))
}

// positionsInFileOrder returns the in-lane positions one pass gives the file's records, in order.
func positionsInFileOrder(t *testing.T, path string, lanes *seededLanes) []uint64 {
	t.Helper()

	f, reader, _, _, err := openUTXOSetFile(path)
	require.NoError(t, err)

	defer func() { _ = f.Close() }()

	p := lanes.positioner()

	var out []uint64

	for {
		frame, _, readErr := utxopersister.ReadUTXOWrapperFrame(reader, nil)
		if readErr != nil {
			break
		}

		packed, posErr := p.next(frame)
		require.NoError(t, posErr)

		l, pos := lane.Unpack(packed)
		require.Equal(t, lanes.grant.Lane, l)

		out = append(out, pos)
	}

	return out
}

// The count refuses what it cannot place, and a pass refuses a record the count did not see.
func TestSeedLayoutRefusesWhatItCannotPlace(t *testing.T) {
	one := func(height uint32) []*utxopersister.UTXOWrapper {
		return []*utxopersister.UTXOWrapper{{TxID: chainhash.HashH([]byte{byte(height)}), Height: height,
			UTXOs: []*utxopersister.UTXO{{Index: 0, Value: 1, Script: []byte{0x51}}}}}
	}

	// writeCompleteSnapshotFile's tip is height 200.
	_, err := countSeedLayout(context.Background(), writeCompleteSnapshotFile(t, one(201)))
	require.ErrorContains(t, err, "above the file's tip")

	path := writeCompleteSnapshotFile(t, one(5))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	binary.LittleEndian.PutUint64(data[len(data)-16:], 2) // the footer claims two records
	require.NoError(t, os.WriteFile(path, data, 0o600))
	_, err = countSeedLayout(context.Background(), path)
	require.ErrorContains(t, err, "the footer says 2 transactions")

	layout, err := countSeedLayout(context.Background(), writeCompleteSnapshotFile(t, one(5)))
	require.NoError(t, err)

	p := (&seededLanes{layout: layout, grant: lane.Grant{Lane: 1, First: 1, Last: 1}}).positioner()
	frame := one(5)[0].Bytes()
	_, err = p.next(frame)
	require.NoError(t, err)
	_, err = p.next(frame)
	require.ErrorContains(t, err, "more records at height 5")

	cb := one(6)[0]
	cb.Coinbase = true
	_, err = p.next(cb.Bytes())
	require.ErrorContains(t, err, "more coinbases")
}

// seedFixture is a chain of headers and a UTXO set for whole-seeder tests, with stores that
// outlive one seeder run.
type seedFixture struct {
	t        *testing.T
	settings *settings.Settings
	dir      string
	name     string
	tip      chainhash.Hash
	tipH     uint32
	records  []*utxopersister.UTXOWrapper
}

// newSeedFixture writes blocks 1 to tipHeight on genesis and a UTXO set of records at the given
// heights, sorted by txid as a real snapshot is; every coinbaseEvery-th record is a coinbase.
func newSeedFixture(t *testing.T, migration settings.LaneMigration, tipHeight uint32, heights []uint32) *seedFixture {
	t.Helper()

	s := test.CreateBaseTestSettings(t)
	s.ProfilerAddr = ""
	s.UtxoStore.LaneMigration = migration

	dbs := t.TempDir()
	s.BlockChain.StoreURL = mustURL(t, "sqlite:///"+filepath.Join(dbs, "blockchain"))
	s.UtxoStore.UtxoStore = mustURL(t, "sqlite:///"+filepath.Join(dbs, "utxo"))
	s.Block.BlockStore = mustURL(t, "file://"+t.TempDir())

	genesisStore := newTestBlockchainStore(t)
	genesis, err := genesisStore.GetBlockByID(context.Background(), 0)
	require.NoError(t, err)

	genesisRecord, err := blockToIndexBytes(t, genesis, genesis.Header.Hash())
	require.NoError(t, err)

	headerRecords := [][]byte{genesisRecord}
	prev := genesis.Header.Hash()

	for h := uint32(1); h <= tipHeight; h++ {
		cb := makeCoinbaseTx(t, byte(h), byte(h>>8))
		block := &model.Block{
			Header: &model.BlockHeader{Version: 1, Timestamp: 1700000000 + h, Nonce: h, HashPrevBlock: prev,
				HashMerkleRoot: cb.TxIDChainHash(), Bits: genesis.Header.Bits},
			CoinbaseTx:       cb,
			TransactionCount: 3,
			Height:           h,
		}

		rec, recErr := blockToIndexBytes(t, block, block.Header.Hash())
		require.NoError(t, recErr)

		headerRecords = append(headerRecords, rec)
		prev = block.Header.Hash()
	}

	f := &seedFixture{t: t, settings: s, dir: t.TempDir(), name: prev.String(), tip: *prev, tipH: tipHeight}

	for i, h := range heights {
		f.records = append(f.records, &utxopersister.UTXOWrapper{
			TxID: chainhash.HashH([]byte{byte(i), byte(i >> 8), byte(h), byte(h >> 8), 0x5e}), Height: h, Coinbase: i%7 == 6,
			UTXOs: []*utxopersister.UTXO{{Index: 0, Value: uint64(1000 + i), Script: []byte{0x51}}},
		})
	}

	// One transaction whose outputs span several store records, so the import's second pass,
	// which creates only those, must give it the position the first pass would.
	wide := &utxopersister.UTXOWrapper{TxID: chainhash.HashH([]byte("wide")), Height: min(4, tipHeight)}
	for i := uint32(0); i < 3; i++ {
		wide.UTXOs = append(wide.UTXOs, &utxopersister.UTXO{Index: i * uint32(max(s.UtxoStore.UtxoBatchSize, 1)), Value: 7, Script: []byte{0x51}})
	}

	f.records = append(f.records, wide)

	sort.Slice(f.records, func(i, j int) bool { return bytes.Compare(f.records[i].TxID[:], f.records[j].TxID[:]) < 0 })

	f.writeHeaders(headerRecords)
	f.writeSet()

	return f
}

func mustURL(t *testing.T, s string) *url.URL {
	u, err := url.Parse(s)
	require.NoError(t, err)

	return u
}

func (f *seedFixture) writeHeaders(records [][]byte) {
	data := fileformat.NewHeader(fileformat.FileTypeUtxoHeaders).Bytes()
	data = append(data, f.tip[:]...)
	data = binary.LittleEndian.AppendUint32(data, f.tipH)

	for _, r := range records {
		data = append(data, r...)
	}

	require.NoError(f.t, os.WriteFile(filepath.Join(f.dir, f.name+".utxo-headers"), data, 0o600))
}

func (f *seedFixture) writeSet() {
	data := fileformat.NewHeader(fileformat.FileTypeUtxoSet).Bytes()
	data = append(data, f.tip[:]...)
	data = binary.LittleEndian.AppendUint32(data, f.tipH)
	data = append(data, make([]byte, 32)...)

	var utxos uint64

	for _, w := range f.records {
		data = append(data, w.Bytes()...)
		utxos += uint64(len(w.UTXOs))
	}

	data = binary.LittleEndian.AppendUint64(data, uint64(len(f.records)))
	data = binary.LittleEndian.AppendUint64(data, utxos)

	require.NoError(f.t, os.WriteFile(f.utxoFile(), data, 0o600))
}

func (f *seedFixture) utxoFile() string { return filepath.Join(f.dir, f.name+".utxo-set") }

// seed runs the whole seeder over the fixture.
func (f *seedFixture) seed(force bool) error {
	return Seeder(ulogger.TestLogger{}, f.settings, f.dir, f.name, false, false, force, false, true)
}

func (f *seedFixture) blockchainStore() blockchain_store.Store {
	store, err := blockchain_store.NewStore(ulogger.TestLogger{}, f.settings.BlockChain.StoreURL, f.settings)
	require.NoError(f.t, err)
	f.t.Cleanup(func() { _ = store.Close(context.Background()) })

	return store
}

func (f *seedFixture) utxoStore() utxo.Store {
	store, err := utxofactory.NewStore(context.Background(), ulogger.TestLogger{}, f.settings, "seeder-test", false)
	require.NoError(f.t, err)

	return store
}

// wantPositions is the in-lane offset each record takes from the grant's first position: by
// height, in file order within a height, coinbases after every height.
func (f *seedFixture) wantPositions() map[chainhash.Hash]uint64 {
	byHeight := map[uint32][]chainhash.Hash{}

	var coinbases []chainhash.Hash

	for _, w := range f.records {
		if w.Coinbase {
			coinbases = append(coinbases, w.TxID)
		} else {
			byHeight[w.Height] = append(byHeight[w.Height], w.TxID)
		}
	}

	out := map[chainhash.Hash]uint64{}

	var next uint64

	for h := uint32(0); h <= f.tipH; h++ {
		for _, id := range byHeight[h] {
			out[id] = next
			next++
		}
	}

	for _, id := range coinbases {
		out[id] = next
		next++
	}

	return out
}

// requireStamped checks every record carries its position from grant and every block its row.
func (f *seedFixture) requireStamped(grant lane.Grant) {
	ctx := context.Background()
	store := f.utxoStore()
	want := f.wantPositions()

	for _, w := range f.records {
		md, err := store.Get(ctx, &w.TxID, fields.LanePosition)
		require.NoError(f.t, err)
		require.Equal(f.t, lane.Pack(grant.Lane, grant.First+want[w.TxID]), md.LanePosition, "record at height %d, coinbase %t", w.Height, w.Coinbase)
	}

	bc := f.blockchainStore()
	_, metas, err := bc.GetBlockHeadersByHeight(ctx, 0, f.tipH)
	require.NoError(f.t, err)
	require.Len(f.t, metas, int(f.tipH)+1)

	for _, m := range metas {
		var first, last uint64

		n := 0

		for _, w := range f.records {
			if w.Height == m.Height && !w.Coinbase {
				if n == 0 || want[w.TxID] < first {
					first = want[w.TxID]
				}

				last = max(last, want[w.TxID])
				n++
			}
		}

		rows, err := bc.LaneMapRows(ctx, m.ID)
		require.NoError(f.t, err)

		if n == 0 {
			require.Empty(f.t, rows, "height %d holds no record but a coinbase", m.Height)

			continue
		}

		require.Equal(f.t, []lane.MapRow{{Lane: grant.Lane, First: grant.First + first, Last: grant.First + last, Tag: lane.RowTagSeededRange}}, rows, "height %d", m.Height)
		require.Equal(f.t, uint64(n), last-first+1, "height %d's stretch holds exactly its records", m.Height)
	}
}

func (f *seedFixture) recordedGrant() lane.Grant {
	r, ok, err := readSeederGrant(context.Background(), f.blockchainStore(), f.tip)
	require.NoError(f.t, err)
	require.True(f.t, ok)
	require.Equal(f.t, f.tip, r.tip)
	require.Equal(f.t, uint64(len(f.records)), r.records)

	return r.grant
}

// fixtureHeights spreads records over a short chain: several at some heights, none at others.
var fixtureHeights = []uint32{3, 1, 3, 5, 2, 5, 5, 1, 4, 3, 6, 2, 6, 1, 3, 5, 2, 4}

// A seed at shadow gives every record a position by height from one grant recorded at height 0,
// writes one seeded row per block, and a second run over the same stores takes no new grant.
func TestSeederStampsLanePositionsAndRows(t *testing.T) {
	// The second snapshot holds fewer records than the check for stale records reads.
	for _, heights := range [][]uint32{fixtureHeights, fixtureHeights[:3]} {
		t.Run(strconv.Itoa(len(heights))+" records", func(t *testing.T) {
			seedStampsPositionsAndRows(t, heights)
		})
	}
}

func seedStampsPositionsAndRows(t *testing.T, heights []uint32) {
	f := newSeedFixture(t, settings.LaneMigrationShadow, 8, heights)
	require.NoError(t, f.seed(false))

	grant := f.recordedGrant()
	require.Equal(t, uint64(len(f.records)), grant.Len())
	f.requireStamped(grant)

	ctx := context.Background()
	bc := f.blockchainStore()

	// The grant is the only one, and block assembly's restart, which reads grants at or above a
	// height near the tip, never finds it.
	grants, err := bc.LaneCapGrants(ctx, 0)
	require.NoError(t, err)
	require.Len(t, grants, 1)
	require.Equal(t, grant.First, grants[0].First)

	grants, err = bc.LaneCapGrants(ctx, 1)
	require.NoError(t, err)
	require.Empty(t, grants)

	exists, err := blockAssemblerStateExists(ctx, bc)
	require.NoError(t, err)
	require.True(t, exists)

	require.NoError(t, f.seed(true), "a forced re-run over the same stores")
	require.Equal(t, grant, f.recordedGrant())
	f.requireStamped(grant)

	grants, err = f.blockchainStore().LaneCapGrants(ctx, 0)
	require.NoError(t, err)
	require.Len(t, grants, 1, "the re-run took no new grant")

	// A grant taken for another snapshot leaves this seed's grant recorded.
	other, err := blockchain_store.NewStore(ulogger.TestLogger{}, f.settings.BlockChain.StoreURL, f.settings)
	require.NoError(t, err)
	_, err = takeSeederGrant(ctx, ulogger.TestLogger{}, other, f.settings, chainhash.HashH([]byte("another snapshot")), 3)
	require.NoError(t, err)

	// A tip that shares the state key's hash prefix is another seed, not this one.
	colliding := f.tip
	colliding[31] ^= 0xff
	_, ok, err := readSeederGrant(ctx, other, colliding)
	require.NoError(t, err)
	require.False(t, ok)

	// A plain re-run after an import that cleared the rows' state, as a forced run does before it
	// imports, writes the rows again and leaves the checkpoint.
	require.NoError(t, setSeededRowsWritten(ctx, other, f.tip, false))
	require.NoError(t, other.Close(ctx))
	require.Equal(t, grant, f.recordedGrant())

	require.NoError(t, f.seed(false))

	written, err := seededRowsWritten(ctx, f.blockchainStore(), f.tip)
	require.NoError(t, err)
	require.True(t, written)
	f.requireStamped(grant)
}

// The stores lanes runs on take every record's position from the seed, the transactions that
// span several records and the external-blob path included; utxoset first needs its lane columns.
func TestSeederStampsPositionsOnTheLaneStores(t *testing.T) {
	backends := map[string]func(t *testing.T, s *settings.Settings){
		"aerospike": func(t *testing.T, s *settings.Settings) {
			aerospikeURL, teardown, err := testAerospike.InitAerospikeContainer()
			test.SkipIfContainerUnavailable(t, err)
			t.Cleanup(func() { _ = teardown() })

			u := mustURL(t, aerospikeURL)
			q := u.Query()
			q.Set("externalStore", "file://"+t.TempDir())
			u.RawQuery = q.Encode()
			s.UtxoStore.UtxoStore = u
		},
		"utxoset": func(t *testing.T, s *settings.Settings) {
			dsn, teardown, err := tpostgres.SetupTestPostgresContainer()
			test.SkipIfContainerUnavailable(t, err)
			require.NoError(t, err)
			t.Cleanup(func() { _ = teardown() })

			u := mustURL(t, dsn)
			u.Scheme = "utxoset"
			s.UtxoStore.UtxoStore = u
		},
	}

	for name, use := range backends {
		t.Run(name, func(t *testing.T) {
			f := newSeedFixture(t, settings.LaneMigrationShadow, 8, fixtureHeights)
			use(t, f.settings)

			done := make(chan error, 1)
			go func() { done <- f.seed(false) }()

			select {
			case err := <-done:
				if name == "utxoset" {
					require.ErrorContains(t, err, "prepare-lane-schema")

					store, err := utxoset.New(context.Background(), ulogger.TestLogger{}, f.settings, f.settings.UtxoStore.UtxoStore)
					require.NoError(t, err)
					require.NoError(t, store.PrepareLaneSchema(context.Background()))
					require.NoError(t, store.Close(context.Background()))
					require.NoError(t, f.seed(false))
				} else {
					require.NoError(t, err)
				}
			case <-time.After(3 * time.Minute):
				t.Fatal("the seed did not finish")
			}

			f.requireStamped(f.recordedGrant())
		})
	}
}

// A seed whose import finished but whose run stopped before its rows and checkpoint is finished
// by -recover-checkpoint, which writes the rows from the recorded grant without importing.
func TestSeederRecoverCheckpointWritesTheSeededRows(t *testing.T) {
	f := newSeedFixture(t, settings.LaneMigrationShadow, 8, fixtureHeights)
	ctx := context.Background()

	bc, err := blockchain_store.NewStore(ulogger.TestLogger{}, f.settings.BlockChain.StoreURL, f.settings)
	require.NoError(t, err)

	tip, err := processUTXOs(ctx, ulogger.TestLogger{}, f.settings, bc, f.utxoFile(), "", false, false)
	require.NoError(t, err)
	require.NotNil(t, tip.lanes)

	// The recorded grant is this snapshot's; one recorded for another tip is not used.
	_, err = recordedSeedLanes(ctx, bc, f.utxoFile(), chainhash.HashH([]byte("another tip")))
	require.ErrorContains(t, err, "records no lane grant for the seed")
	require.NoError(t, bc.Close(ctx))

	// Recovery over a UTXO store that lost the import is refused.
	imported := f.settings.UtxoStore.UtxoStore
	f.settings.UtxoStore.UtxoStore = mustURL(t, "sqlite:///"+filepath.Join(t.TempDir(), "empty"))
	require.ErrorContains(t, Seeder(ulogger.TestLogger{}, f.settings, f.dir, f.name, false, false, false, true, true), "does not hold")

	f.settings.UtxoStore.UtxoStore = imported

	require.NoError(t, Seeder(ulogger.TestLogger{}, f.settings, f.dir, f.name, false, false, false, true, true))
	f.requireStamped(tip.lanes.grant)
}

// A seed whose rows cannot be written leaves no BlockAssembler checkpoint, so a plain re-run does
// not take the seed as done. Here block 2 already holds a different row.
func TestSeederWritesNoCheckpointWhenItsRowsFail(t *testing.T) {
	f := newSeedFixture(t, settings.LaneMigrationShadow, 8, fixtureHeights)
	ctx := context.Background()

	bc, err := blockchain_store.NewStore(ulogger.TestLogger{}, f.settings.BlockChain.StoreURL, f.settings)
	require.NoError(t, err)
	require.NoError(t, processHeaders(ctx, ulogger.TestLogger{}, bc, filepath.Join(f.dir, f.name+".utxo-headers")))

	_, metas, err := bc.GetBlockHeadersByHeight(ctx, 2, 2)
	require.NoError(t, err)
	require.NoError(t, bc.InsertLaneMapRows(ctx, metas[0].ID, []lane.MapRow{{Lane: 200, First: 1, Last: 1, Tag: lane.RowTagRange}}))
	require.NoError(t, bc.Close(ctx))

	require.ErrorContains(t, f.seed(false), "seeded lane rows")

	exists, err := blockAssemblerStateExists(ctx, f.blockchainStore())
	require.NoError(t, err)
	require.False(t, exists, "no checkpoint without the rows")
}

// At off the seeder takes no lease, gives no position and writes no row.
func TestSeederAtOffStampsNothing(t *testing.T) {
	f := newSeedFixture(t, settings.LaneMigrationOff, 8, fixtureHeights)
	require.NoError(t, f.seed(false))

	ctx := context.Background()
	store := f.utxoStore()

	for _, w := range f.records {
		md, err := store.Get(ctx, &w.TxID, fields.LanePosition)
		require.NoError(t, err)
		require.Equal(t, lane.None, md.LanePosition)
	}

	bc := f.blockchainStore()

	_, ok, err := readSeederGrant(ctx, bc, f.tip)
	require.NoError(t, err)
	require.False(t, ok)

	grants, err := bc.LaneCapGrants(ctx, 0)
	require.NoError(t, err)
	require.Empty(t, grants)

	_, metas, err := bc.GetBlockHeadersByHeight(ctx, 0, f.tipH)
	require.NoError(t, err)

	for _, m := range metas {
		rows, err := bc.LaneMapRows(ctx, m.ID)
		require.NoError(t, err)
		require.Empty(t, rows)
	}
}

// failAfterStore creates through the real store until limit creates, then fails every create.
type failAfterStore struct {
	utxo.Store
	creates atomic.Int64
	limit   int64
}

func (s *failAfterStore) SpendAndCreate(ctx context.Context, tx *bt.Tx, height uint32, opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	if s.creates.Add(1) > s.limit {
		return nil, nil, errors.NewStorageError("stopped after %d creates", s.limit)
	}

	return s.Store.SpendAndCreate(ctx, tx, height, opts...)
}

// A seed stopped part way and run again gives every record the position the first run gave or
// would have given it, from the first run's grant: a position is never taken twice for a record.
func TestSeederResumesAnInterruptedSeedWithTheSamePositions(t *testing.T) {
	f := newSeedFixture(t, settings.LaneMigrationShadow, 8, fixtureHeights)
	ctx := context.Background()

	// Opened apart from blockchainStore, whose cleanup would close it a second time.
	bc, err := blockchain_store.NewStore(ulogger.TestLogger{}, f.settings.BlockChain.StoreURL, f.settings)
	require.NoError(t, err)

	store := f.utxoStore()

	lanes, err := prepareSeedLanes(ctx, ulogger.TestLogger{}, f.settings, bc, store, f.utxoFile(), f.tip)
	require.NoError(t, err)

	opts := testImportOptions()
	opts.workerCount, opts.multiRecordWorkerCount, opts.channelSize = 1, 1, 1
	opts.lanes = lanes

	err = importUTXOSet(ctx, ulogger.TestLogger{}, &failAfterStore{Store: store, limit: 7}, f.utxoFile(), opts)
	require.ErrorContains(t, err, "stopped after 7 creates")
	require.NoError(t, bc.Close(ctx))

	require.NoError(t, f.seed(false))
	require.Equal(t, lanes.grant, f.recordedGrant(), "the re-run reuses the first run's grant")
	f.requireStamped(lanes.grant)
}

// A seed at shadow refuses records it cannot stamp: a store already holding this file's records
// from an import at off, and a completed seed made at off.
func TestSeederAtShadowRefusesAnImportMadeAtOff(t *testing.T) {
	t.Run("records already in the UTXO store", func(t *testing.T) {
		f := newSeedFixture(t, settings.LaneMigrationOff, 8, fixtureHeights)
		require.NoError(t, f.seed(false))

		f.settings.UtxoStore.LaneMigration = settings.LaneMigrationShadow
		require.ErrorContains(t, f.seed(true), "import made at off or from another grant")
	})

	t.Run("a completed seed", func(t *testing.T) {
		f := newSeedFixture(t, settings.LaneMigrationOff, 8, fixtureHeights)
		require.NoError(t, f.seed(false))

		f.settings.UtxoStore.LaneMigration = settings.LaneMigrationShadow
		require.ErrorContains(t, f.seed(false), "records no lane grant for the seed")
	})

	t.Run("a completed seed after a refused forced one", func(t *testing.T) {
		f := newSeedFixture(t, settings.LaneMigrationOff, 8, fixtureHeights)
		require.NoError(t, f.seed(false))

		// The refused run took and recorded a grant before it found the old records.
		f.settings.UtxoStore.LaneMigration = settings.LaneMigrationShadow
		require.ErrorContains(t, f.seed(true), "import made at off or from another grant")
		require.ErrorContains(t, f.seed(false), "import made at off or from another grant")
	})
}

// The lane reader, loading the rows a seed wrote the way a process does at start, answers a
// seeded parent mined once its block is deep enough, and the pruner's lane rule settles it then;
// a parent older than the blocks the reader loads, and a coinbase, cannot be judged.
func TestSeededParentsThroughTheLaneReaderAndPruner(t *testing.T) {
	const tipHeight = 400

	// Index 6, the second record at 50, is the coinbase (newSeedFixture).
	f := newSeedFixture(t, settings.LaneMigrationShadow, tipHeight, []uint32{50, 200, 250, 390, 395, 300, 50})
	require.NoError(t, f.seed(false))

	// Mainnet's settle depth: blocks 100 or more deep fold into the floor.
	params := *f.settings.ChainCfgParams
	params.CoinbaseMaturity = 100
	f.settings.ChainCfgParams = &params

	grant := f.recordedGrant()
	want := f.wantPositions()
	pos := func(i int) uint64 { return lane.Pack(grant.Lane, grant.First+want[f.records[i].TxID]) }

	of := func(height uint32, coinbase bool) int {
		for i, w := range f.records {
			if w.Height == height && w.Coinbase == coinbase {
				return i
			}
		}

		t.Fatalf("no record at height %d, coinbase %t", height, coinbase)

		return -1
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, f.settings, f.blockchainStore(), nil, nil)
	require.NoError(t, err)

	r := lanereader.Start(ctx, ulogger.TestLogger{}, f.settings, client)
	require.NotNil(t, r)
	require.Eventually(t, func() bool { return r.Snapshot() != nil && r.Snapshot().TipHeight() == tipHeight }, 30*time.Second, 50*time.Millisecond)

	v := r.MainView()

	deep := v.Lookup(pos(of(200, false)))
	require.True(t, deep.Mined)
	require.Equal(t, lane.ViaFloor, deep.Via, "a block more than the settle depth deep is folded into the floor")

	shallow := v.Lookup(pos(of(395, false)))
	require.Equal(t, lane.Answer{Mined: true, Via: lane.ViaRow, BlockID: shallow.BlockID, Height: 395, Seeded: true}, shallow)

	judge := r.PruneJudge(lanereader.PruneStoreUtxoset, 288)
	defer judge.Done()

	verdict := func(c lanereader.PruneCandidate) lanereader.PruneVerdict {
		return judge.Judge(lanereader.TodayDelete, &c)
	}

	require.Equal(t, lanereader.PruneVerdict{Lanes: lanereader.PruneDelete, Reason: lanereader.ReasonSettled},
		verdict(lanereader.PruneCandidate{ParentPos: pos(of(200, false)), Spenders: []lanereader.PruneSpender{{Pos: pos(of(250, false))}}}),
		"a seeded parent and spender in folded blocks are settled")

	require.Equal(t, lanereader.PruneVerdict{Lanes: lanereader.PruneKeep, Reason: lanereader.ReasonParentShallow},
		verdict(lanereader.PruneCandidate{ParentPos: pos(of(395, false))}))

	require.Equal(t, lanereader.PruneVerdict{Lanes: lanereader.PruneKeep, Reason: lanereader.ReasonSpenderShallow},
		verdict(lanereader.PruneCandidate{ParentPos: pos(of(200, false)), Spenders: []lanereader.PruneSpender{{Pos: pos(of(390, false))}}}),
		"a spender ten blocks deep is not settled yet")

	require.Equal(t, lanereader.PruneVerdict{Lanes: lanereader.PruneCannotJudge, Reason: lanereader.ReasonNotLoaded},
		verdict(lanereader.PruneCandidate{ParentPos: pos(of(50, false))}),
		"block 50 is below the 288 blocks the reader loads")

	require.Equal(t, lanereader.PruneVerdict{Lanes: lanereader.PruneCannotJudge, Reason: lanereader.ReasonCoinbase},
		verdict(lanereader.PruneCandidate{ParentPos: pos(of(50, true)), Coinbase: true}),
		"no row lists a seeded coinbase")
}
