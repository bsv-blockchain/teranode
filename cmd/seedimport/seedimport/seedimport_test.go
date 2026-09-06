package seedimport

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/pkg/muhash"
	"github.com/bsv-blockchain/teranode/pkg/seedcheckpoint"
	"github.com/bsv-blockchain/teranode/pkg/seedpack"
	"github.com/bsv-blockchain/teranode/pkg/utxoseed"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// testNetMagic is an arbitrary network magic; producer signing and consumer
// verification in these tests must agree on it.
const testNetMagic uint32 = 0xe8f3e1e3

func newTestUTXOStore(t *testing.T) utxo.Store {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	u, err := url.Parse("sqlitememory:///seedimport-" + t.Name())
	require.NoError(t, err)

	store, err := utxosql.New(t.Context(), ulogger.TestLogger{}, tSettings, u)
	require.NoError(t, err)

	return store
}

func TestLoadWrapperMakesOutputsSpendable(t *testing.T) {
	ctx := context.Background()
	store := newTestUTXOStore(t)

	txid := chainhash.HashH([]byte("wrapper-tx"))

	w := &utxopersister.UTXOWrapper{
		TxID:     txid,
		Height:   100,
		Coinbase: false,
		UTXOs: []*utxopersister.UTXO{
			{Index: 0, Value: 1000, Script: []byte{0x76, 0xa9, 0x51}},
			{Index: 2, Value: 2000, Script: []byte{0x6a}},
		},
	}

	require.NoError(t, loadWrapper(ctx, store, w, 42, false))

	for _, vout := range []uint32{0, 2} {
		resp, err := store.GetSpend(ctx, &utxo.Spend{TxID: &txid, Vout: vout})
		require.NoError(t, err)
		require.Equal(t, int(utxo.Status_OK), resp.Status, "vout %d should be spendable", vout)
		require.Nil(t, resp.SpendingData)
	}
}

func TestWrapperToTxUsesRealTxID(t *testing.T) {
	txid := chainhash.HashH([]byte("real-txid"))

	w := &utxopersister.UTXOWrapper{
		TxID:   txid,
		Height: 5,
		UTXOs:  []*utxopersister.UTXO{{Index: 0, Value: 1, Script: []byte{0x51}}},
	}

	tx, err := wrapperToTx(w)
	require.NoError(t, err)
	require.Equal(t, txid, *tx.TxIDChainHash(), "synthesized tx must report the real txid via SetTxHash")
	require.Empty(t, tx.Inputs)
	require.Len(t, tx.Outputs, 1)
	require.Equal(t, uint64(1), tx.Outputs[0].Satoshis)
}

func TestWrapperToTxRejectsHugeVout(t *testing.T) {
	w := &utxopersister.UTXOWrapper{
		TxID:  chainhash.HashH([]byte("huge")),
		UTXOs: []*utxopersister.UTXO{{Index: 0xFFFFFFFF, Value: 1, Script: []byte{0x51}}},
	}

	_, err := wrapperToTx(w)
	require.Error(t, err, "an absurd vout from an unverified wrapper must be rejected before allocation")
}

type stubLookup struct {
	id     uint32
	height uint32
	onMain bool
	err    error
}

func (s stubLookup) BlockIDAndHeight(ctx context.Context, h *chainhash.Hash) (uint32, uint32, bool, error) {
	return s.id, s.height, s.onMain, s.err
}

type stubBlockchainStore struct {
	meta   *model.BlockHeaderMeta
	onMain bool
}

func (s stubBlockchainStore) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return nil, s.meta, nil
}

func (s stubBlockchainStore) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	return s.onMain, nil
}

func TestBlockchainLookupReturnsIDHeightAndOnMain(t *testing.T) {
	ctx := context.Background()

	stub := stubBlockchainStore{meta: &model.BlockHeaderMeta{ID: 5, Height: 101}, onMain: true}

	h := chainhash.HashH([]byte("block"))

	id, height, onMain, err := NewBlockchainLookup(stub).BlockIDAndHeight(ctx, &h)
	require.NoError(t, err)
	require.Equal(t, uint32(5), id)
	require.Equal(t, uint32(101), height)
	require.True(t, onMain)
}

func TestLoadTrustedKeysParsesValidKey(t *testing.T) {
	priv, err := bec.NewPrivateKey()
	require.NoError(t, err)

	keyHex := hex.EncodeToString(priv.PubKey().Compressed())

	keys, err := LoadTrustedKeys(nil, keyHex)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, priv.PubKey().Compressed(), keys[0])
}

func TestLoadTrustedKeysNormalizesUncompressedKey(t *testing.T) {
	priv, err := bec.NewPrivateKey()
	require.NoError(t, err)

	// An operator pastes the uncompressed (65-byte) form. It must be normalized
	// to the 33-byte compressed key the checkpoint embeds, or it would never
	// match and every import would fail.
	uncompressedHex := hex.EncodeToString(priv.PubKey().Uncompressed())

	keys, err := LoadTrustedKeys(nil, uncompressedHex)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, priv.PubKey().Compressed(), keys[0])
}

func TestLoadTrustedKeysAcceptsCompiledIn(t *testing.T) {
	priv, err := bec.NewPrivateKey()
	require.NoError(t, err)

	keyHex := hex.EncodeToString(priv.PubKey().Compressed())

	keys, err := LoadTrustedKeys([]string{keyHex}, "")
	require.NoError(t, err)
	require.Len(t, keys, 1)
}

func TestLoadTrustedKeysErrorsWhenEmpty(t *testing.T) {
	_, err := LoadTrustedKeys(nil, "")
	require.Error(t, err)
}

func TestLoadTrustedKeysErrorsOnGarbageHex(t *testing.T) {
	_, err := LoadTrustedKeys(nil, "not-hex")
	require.Error(t, err)
}

func TestLoadTrustedKeysErrorsOnInvalidPubKey(t *testing.T) {
	_, err := LoadTrustedKeys(nil, "deadbeef")
	require.Error(t, err)
}

// frameSeedBody frames a utxo-set body (blockHash|height|prevHash header,
// wrappers, then a txCount|utxoCount footer). footerTxs/footerUTXOs are written
// verbatim so a test can inject a footer that disagrees with the wrappers; it
// also returns the MuHash over the actual wrappers.
func frameSeedBody(blockHash, prevHash chainhash.Hash, height uint32, wrappers []*utxopersister.UTXOWrapper, footerTxs, footerUTXOs uint64) ([]byte, [32]byte) {
	var body []byte
	body = append(body, blockHash[:]...)

	var hb [4]byte
	binary.LittleEndian.PutUint32(hb[:], height)
	body = append(body, hb[:]...)
	body = append(body, prevHash[:]...)

	acc := muhash.New()

	for _, w := range wrappers {
		body = append(body, w.Bytes()...)

		for _, u := range w.UTXOs {
			acc.Add(utxoseed.Element(w.TxID, u.Index, w.Height, w.Coinbase, u.Value, u.Script))
		}
	}

	var footer [16]byte
	binary.LittleEndian.PutUint64(footer[0:8], footerTxs)
	binary.LittleEndian.PutUint64(footer[8:16], footerUTXOs)
	body = append(body, footer[:]...)

	return body, acc.Digest()
}

// packageAndSignSeed writes body into a memory blob store as a seed package plus
// a checkpoint signed over setHash, and returns the store and trusted pubkey.
func packageAndSignSeed(t *testing.T, blockHash chainhash.Hash, height uint32, body []byte, setHash [32]byte) (*memory.Memory, []byte) {
	t.Helper()

	ctx := context.Background()
	store := memory.New()

	cfg := seedpack.Config{Min: 16, Max: 256, Mask: (1 << 6) - 1}
	require.NoError(t, utxopersister.BuildSeedPackage(ctx, store, bytes.NewReader(body), height, blockHash, setHash, cfg))

	priv, err := bec.NewPrivateKey()
	require.NoError(t, err)

	sc, err := seedcheckpoint.Sign(priv, seedcheckpoint.Checkpoint{CommitmentVersion: utxoseed.CommitmentVersion, Height: height, BlockHash: blockHash, SetHash: setHash}, testNetMagic)
	require.NoError(t, err)

	require.NoError(t, store.Set(ctx, blockHash[:], fileformat.FileTypeSeedCheckpoint, sc.Serialize(), options.WithAllowOverwrite(true)))

	return store, priv.PubKey().Compressed()
}

// buildSeed writes a utxo-set body (header|wrappers|footer) into a memory blob
// store as a seed package + signed checkpoint. Returns the store, block hash,
// and the trusted (compressed) pubkey that signed the checkpoint.
func buildSeed(t *testing.T, wrappers []*utxopersister.UTXOWrapper, height uint32) (*memory.Memory, chainhash.Hash, []byte) {
	t.Helper()

	blockHash := chainhash.HashH([]byte("seed-h"))
	prevHash := chainhash.HashH([]byte("seed-prev"))

	var utxoCount uint64
	for _, w := range wrappers {
		utxoCount += uint64(len(w.UTXOs))
	}

	body, setHash := frameSeedBody(blockHash, prevHash, height, wrappers, uint64(len(wrappers)), utxoCount)

	store, key := packageAndSignSeed(t, blockHash, height, body, setHash)

	return store, blockHash, key
}

func sampleWrappers() []*utxopersister.UTXOWrapper {
	txA := chainhash.HashH([]byte("tx-a"))
	txB := chainhash.HashH([]byte("tx-b"))

	return []*utxopersister.UTXOWrapper{
		{TxID: txA, Height: 100, Coinbase: true, UTXOs: []*utxopersister.UTXO{{Index: 0, Value: 5000000000, Script: []byte{0x51}}}},
		{TxID: txB, Height: 101, UTXOs: []*utxopersister.UTXO{{Index: 0, Value: 1000, Script: []byte{0x76, 0xa9}}}},
	}
}

func TestRunLoadsAndVerifies(t *testing.T) {
	ctx := context.Background()

	wrappers := sampleWrappers()
	seedStore, blockHash, trustedKey := buildSeed(t, wrappers, 101)
	utxoStore := newTestUTXOStore(t)

	cfg := Config{
		SeedStore:    seedStore,
		UTXOStore:    utxoStore,
		Lookup:       stubLookup{id: 7, height: 101, onMain: true},
		TrustedKeys:  [][]byte{trustedKey},
		BlockHash:    blockHash,
		NetworkMagic: testNetMagic,
	}

	require.NoError(t, Run(ctx, ulogger.TestLogger{}, cfg))

	// wrappers[1] is a non-coinbase tx, so its output is immediately spendable.
	txB := wrappers[1].TxID
	resp, err := utxoStore.GetSpend(ctx, &utxo.Spend{TxID: &txB, Vout: 0})
	require.NoError(t, err)
	require.Equal(t, int(utxo.Status_OK), resp.Status)

	// wrappers[0] is a coinbase mined at height 100; at the seed height it is
	// still within the maturity window and therefore loaded as IMMATURE.
	txA := wrappers[0].TxID
	respA, err := utxoStore.GetSpend(ctx, &utxo.Spend{TxID: &txA, Vout: 0})
	require.NoError(t, err)
	require.Equal(t, int(utxo.Status_IMMATURE), respA.Status)
}

func TestRunRejectsUntrustedKey(t *testing.T) {
	ctx := context.Background()

	seedStore, blockHash, _ := buildSeed(t, sampleWrappers(), 101)

	other, err := bec.NewPrivateKey()
	require.NoError(t, err)

	cfg := Config{SeedStore: seedStore, UTXOStore: newTestUTXOStore(t), Lookup: stubLookup{id: 1, height: 101, onMain: true}, TrustedKeys: [][]byte{other.PubKey().Compressed()}, BlockHash: blockHash, NetworkMagic: testNetMagic}
	require.Error(t, Run(ctx, ulogger.TestLogger{}, cfg))
}

func TestRunRejectsNotOnMainChain(t *testing.T) {
	ctx := context.Background()

	seedStore, blockHash, trustedKey := buildSeed(t, sampleWrappers(), 101)

	cfg := Config{SeedStore: seedStore, UTXOStore: newTestUTXOStore(t), Lookup: stubLookup{id: 1, height: 101, onMain: false}, TrustedKeys: [][]byte{trustedKey}, BlockHash: blockHash, NetworkMagic: testNetMagic}
	require.Error(t, Run(ctx, ulogger.TestLogger{}, cfg))
}

func TestRunRollsBackOnSetHashMismatch(t *testing.T) {
	ctx := context.Background()

	wrappers := sampleWrappers()
	seedStore, blockHash, _ := buildSeed(t, wrappers, 101)
	utxoStore := newTestUTXOStore(t)

	// Re-sign the checkpoint over a WRONG setHash with a key we control. The
	// chunks remain valid, so the set streams and loads; the recomputed digest
	// then disagrees with the signed setHash, forcing a rollback.
	priv, err := bec.NewPrivateKey()
	require.NoError(t, err)

	var wrongSetHash [32]byte
	for i := range wrongSetHash {
		wrongSetHash[i] = 0xee
	}

	sc, err := seedcheckpoint.Sign(priv, seedcheckpoint.Checkpoint{CommitmentVersion: utxoseed.CommitmentVersion, Height: 101, BlockHash: blockHash, SetHash: wrongSetHash}, testNetMagic)
	require.NoError(t, err)

	require.NoError(t, seedStore.Set(ctx, blockHash[:], fileformat.FileTypeSeedCheckpoint, sc.Serialize(), options.WithAllowOverwrite(true)))

	cfg := Config{
		SeedStore:    seedStore,
		UTXOStore:    utxoStore,
		Lookup:       stubLookup{id: 7, height: 101, onMain: true},
		TrustedKeys:  [][]byte{priv.PubKey().Compressed()},
		BlockHash:    blockHash,
		NetworkMagic: testNetMagic,
	}

	require.Error(t, Run(ctx, ulogger.TestLogger{}, cfg), "a set hash mismatch must fail")

	// Rollback must have deleted every record it created.
	for _, w := range wrappers {
		txid := w.TxID
		_, err := utxoStore.Get(ctx, &txid)
		require.Error(t, err, "record for %s should have been rolled back", txid.String())
	}
}

func TestRunRejectsTamperedSet(t *testing.T) {
	ctx := context.Background()

	seedStore, blockHash, trustedKey := buildSeed(t, sampleWrappers(), 101)

	// Corrupt the first chunk so the streamed body no longer hashes correctly.
	mb, err := seedStore.Get(ctx, blockHash[:], fileformat.FileTypeSeedManifest)
	require.NoError(t, err)

	m, err := seedpack.ParseManifest(mb)
	require.NoError(t, err)

	require.NoError(t, seedStore.Set(ctx, m.Chunks[0].Hash[:], fileformat.FileTypeSeedChunk, make([]byte, int(m.Chunks[0].Size)), options.WithAllowOverwrite(true)))

	cfg := Config{SeedStore: seedStore, UTXOStore: newTestUTXOStore(t), Lookup: stubLookup{id: 1, height: 101, onMain: true}, TrustedKeys: [][]byte{trustedKey}, BlockHash: blockHash, NetworkMagic: testNetMagic}
	require.Error(t, Run(ctx, ulogger.TestLogger{}, cfg))
}

func TestRunRejectsFooterMismatchAndRollsBack(t *testing.T) {
	ctx := context.Background()

	wrappers := sampleWrappers()
	blockHash := chainhash.HashH([]byte("seed-h"))
	prevHash := chainhash.HashH([]byte("seed-prev"))

	// Frame a body whose footer declares one more tx than was actually written.
	// The wrappers (and therefore the set hash) are valid, so streamLoad loads
	// them all and only then fails footer validation — exercising both the
	// footer check and rollback on a streamLoad error (before the set-hash gate).
	body, setHash := frameSeedBody(blockHash, prevHash, 101, wrappers, uint64(len(wrappers)+1), 999)
	seedStore, trustedKey := packageAndSignSeed(t, blockHash, 101, body, setHash)

	utxoStore := newTestUTXOStore(t)

	cfg := Config{SeedStore: seedStore, UTXOStore: utxoStore, Lookup: stubLookup{id: 7, height: 101, onMain: true}, TrustedKeys: [][]byte{trustedKey}, BlockHash: blockHash, NetworkMagic: testNetMagic}

	err := Run(ctx, ulogger.TestLogger{}, cfg)
	require.Error(t, err, "a footer disagreeing with the loaded set must fail")
	require.Contains(t, err.Error(), "footer")

	for _, w := range wrappers {
		txid := w.TxID
		_, err := utxoStore.Get(ctx, &txid)
		require.Error(t, err, "record for %s should have been rolled back", txid.String())
	}
}

// failingCreateStore wraps a utxo.Store and fails SpendAndCreate after failAfter
// successful calls, to exercise rollback on a mid-stream load error.
type failingCreateStore struct {
	utxo.Store

	failAfter int
	calls     int
}

func (s *failingCreateStore) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	s.calls++
	if s.calls > s.failAfter {
		return nil, nil, errors.NewStorageError("injected store failure on create #%d", s.calls)
	}

	return s.Store.SpendAndCreate(ctx, tx, blockHeight, opts...)
}

func TestRunRollsBackOnMidStreamLoadError(t *testing.T) {
	ctx := context.Background()

	wrappers := sampleWrappers() // two wrappers, loaded in order
	seedStore, blockHash, trustedKey := buildSeed(t, wrappers, 101)

	base := newTestUTXOStore(t)
	utxoStore := &failingCreateStore{Store: base, failAfter: 1} // first Create ok, second fails

	cfg := Config{SeedStore: seedStore, UTXOStore: utxoStore, Lookup: stubLookup{id: 7, height: 101, onMain: true}, TrustedKeys: [][]byte{trustedKey}, BlockHash: blockHash, NetworkMagic: testNetMagic}

	require.Error(t, Run(ctx, ulogger.TestLogger{}, cfg), "a mid-stream store failure must fail the import")

	// The first wrapper was created before the failure; rollback must delete it,
	// leaving no orphaned records for a retry to collide with.
	for _, w := range wrappers {
		txid := w.TxID
		_, err := base.Get(ctx, &txid)
		require.Error(t, err, "record for %s must have been rolled back", txid.String())
	}
}

func TestRunRejectsUnsupportedCommitmentVersion(t *testing.T) {
	ctx := context.Background()

	seedStore, blockHash, _ := buildSeed(t, sampleWrappers(), 101)

	// A validly signed checkpoint by a trusted key, but at a commitment version
	// this build does not implement. It must be refused before any loading: the
	// consumer cannot recompute the set hash under an unknown construction.
	priv, err := bec.NewPrivateKey()
	require.NoError(t, err)

	sc, err := seedcheckpoint.Sign(priv, seedcheckpoint.Checkpoint{CommitmentVersion: utxoseed.CommitmentVersion + 1, Height: 101, BlockHash: blockHash, SetHash: [32]byte{}}, testNetMagic)
	require.NoError(t, err)

	require.NoError(t, seedStore.Set(ctx, blockHash[:], fileformat.FileTypeSeedCheckpoint, sc.Serialize(), options.WithAllowOverwrite(true)))

	cfg := Config{SeedStore: seedStore, UTXOStore: newTestUTXOStore(t), Lookup: stubLookup{id: 1, height: 101, onMain: true}, TrustedKeys: [][]byte{priv.PubKey().Compressed()}, BlockHash: blockHash, NetworkMagic: testNetMagic}
	require.Error(t, Run(ctx, ulogger.TestLogger{}, cfg))
}

// TestRunConsumesProductionBuiltSeed is the consumer half of the full
// production-to-production round trip. It builds a seed with the *real*
// producer functions — utxopersister.BuildSeedPackage (content-defined
// chunking + manifest) and utxopersister.BuildSignedCheckpoint (reads the
// persisted set hash, signs it) — and then ingests it with the real
// seedimport.Run, asserting the verified set lands in the UTXO store.
//
// The producer half (real consolidator + CreateUTXOSet framing) is proved in
// utxopersister.TestProductionSeedIsConsumerReadable; package utxopersister
// cannot import seedimport (cycle), so the round trip is expressed as two tests
// meeting at the on-disk seed package + signed checkpoint.
func TestRunConsumesProductionBuiltSeed(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	wrappers := sampleWrappers()
	const seedHeight = uint32(101)

	blockHash := chainhash.HashH([]byte("p2p-seed-block"))
	prevHash := chainhash.HashH([]byte("p2p-seed-prev"))

	// Body framing matches what CreateUTXOSet writes (and what streamLoad
	// reads): blockHash(32) | height(4 LE) | prevHash(32) | wrappers | footer.
	var body []byte
	body = append(body, blockHash[:]...)

	var hb [4]byte
	binary.LittleEndian.PutUint32(hb[:], seedHeight)
	body = append(body, hb[:]...)
	body = append(body, prevHash[:]...)

	acc := muhash.New()

	var utxoCount uint64
	for _, w := range wrappers {
		body = append(body, w.Bytes()...)
		utxoCount += uint64(len(w.UTXOs))

		for _, u := range w.UTXOs {
			acc.Add(utxoseed.Element(w.TxID, u.Index, w.Height, w.Coinbase, u.Value, u.Script))
		}
	}

	var footer [16]byte
	binary.LittleEndian.PutUint64(footer[0:8], uint64(len(wrappers)))
	binary.LittleEndian.PutUint64(footer[8:16], utxoCount)
	body = append(body, footer[:]...)

	setHash := acc.Digest()

	// Real producer packaging.
	cfg := seedpack.Config{Min: 16, Max: 256, Mask: (1 << 6) - 1}
	require.NoError(t, utxopersister.BuildSeedPackage(ctx, store, bytes.NewReader(body), seedHeight, blockHash, setHash, cfg))

	// Persist the set hash so the production BuildSignedCheckpoint can read it.
	// This single store.Set mirrors the unexported persistSetHash, which the
	// producer's Server.processNextBlock calls before building the checkpoint.
	require.NoError(t, store.Set(ctx, blockHash[:], fileformat.FileTypeUtxoSetHash, setHash[:], options.WithAllowOverwrite(true)))

	priv, err := bec.NewPrivateKey()
	require.NoError(t, err)

	// Real producer signing.
	sc, err := utxopersister.BuildSignedCheckpoint(ctx, store, blockHash, seedHeight, priv, testNetMagic)
	require.NoError(t, err)
	require.Equal(t, setHash, sc.Checkpoint.SetHash)

	// Real consumer ingest.
	icfg := Config{
		SeedStore:    store,
		UTXOStore:    newTestUTXOStore(t),
		Lookup:       stubLookup{id: 9, height: seedHeight, onMain: true},
		TrustedKeys:  [][]byte{priv.PubKey().Compressed()},
		BlockHash:    blockHash,
		NetworkMagic: testNetMagic,
	}
	require.NoError(t, Run(ctx, ulogger.TestLogger{}, icfg))

	// Non-coinbase output is immediately spendable.
	txB := wrappers[1].TxID
	resp, err := icfg.UTXOStore.GetSpend(ctx, &utxo.Spend{TxID: &txB, Vout: 0})
	require.NoError(t, err)
	require.Equal(t, int(utxo.Status_OK), resp.Status)

	// Coinbase at height 100 is still within the maturity window at height 101.
	txA := wrappers[0].TxID
	respA, err := icfg.UTXOStore.GetSpend(ctx, &utxo.Spend{TxID: &txA, Vout: 0})
	require.NoError(t, err)
	require.Equal(t, int(utxo.Status_IMMATURE), respA.Status)
}
