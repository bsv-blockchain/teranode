package utxopersister

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// p2pkhTx builds a distinct non-coinbase tx with a single spendable P2PKH
// output. Distinct `seed` bytes yield distinct txids. A P2PKH script is
// stored by ShouldStoreOutputAsUTXO in every era.
func p2pkhTx(t *testing.T, seed byte, satoshis uint64) *bt.Tx {
	t.Helper()

	b := make([]byte, 25)
	b[0], b[1], b[2] = 0x76, 0xa9, 0x14 // OP_DUP OP_HASH160 PUSH20
	for i := 3; i < 23; i++ {
		b[i] = seed
	}
	b[23], b[24] = 0x88, 0xac // OP_EQUALVERIFY OP_CHECKSIG

	ls := bscript.Script(b)

	tx := bt.NewTx()
	tx.AddOutput(&bt.Output{Satoshis: satoshis, LockingScript: &ls})

	return tx
}

// stageBlockDeltas writes real utxo-additions/utxo-deletions files for a
// synthetic block using the production writer (NewUTXOSet + ProcessTx + Close),
// keyed by blockHash.
func stageBlockDeltas(t *testing.T, ctx context.Context, tSettings *settings.Settings, store blob.Store, blockHash *chainhash.Hash, height uint32, txs ...*bt.Tx) {
	t.Helper()

	us, err := NewUTXOSet(ctx, ulogger.TestLogger{}, tSettings, store, blockHash, height)
	require.NoError(t, err)

	for _, tx := range txs {
		require.NoError(t, us.ProcessTx(tx))
	}

	require.NoError(t, us.Close())
}

// buildChainHeaders builds n contiguous headers for heights 1..n, each linking
// to the previous (block 1 links to genesis). Returns the header/meta slices
// (index 0 = height 1) and a height->hash map (indices 1..n).
func buildChainHeaders(t *testing.T, genesis *chainhash.Hash, n uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, map[uint32]*chainhash.Hash) {
	t.Helper()

	nBits, err := model.NewNBitFromString("1d00ffff")
	require.NoError(t, err)

	merkle := chainhash.HashH([]byte("merkle-root-fixture"))

	var (
		headers = make([]*model.BlockHeader, 0, n)
		metas   = make([]*model.BlockHeaderMeta, 0, n)
		byH     = make(map[uint32]*chainhash.Hash, n)
		prev    = genesis
	)

	for h := uint32(1); h <= n; h++ {
		hdr := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prev,
			HashMerkleRoot: &merkle,
			Timestamp:      1231006505 + h,
			Bits:           *nBits,
			Nonce:          h,
		}

		hash := hdr.Hash()
		byH[h] = hash
		prev = hash

		headers = append(headers, hdr)
		metas = append(metas, &model.BlockHeaderMeta{Height: h})
	}

	return headers, metas, byH
}

// readSetUTXOs parses a utxo-set file into a {txid:index -> value} map.
// GetUTXOSetReader strips the 8-byte fileformat magic; the per-file metadata
// CreateUTXOSet writes (32 block hash + 4 height + 32 previous hash = 68 bytes)
// is skipped before the wrapper records. End-of-records is the 16-byte
// footer, reported via the ErrRecordBoundary sentinel (mirrors CreateUTXOSet's
// own reader). This helper reads back a file the test just wrote in the same
// process, so - unlike the production readers - it does not need to validate
// the footer's counts against what it processed.
func readSetUTXOs(t *testing.T, ctx context.Context, tSettings *settings.Settings, store blob.Store, hash *chainhash.Hash) map[string]uint64 {
	t.Helper()

	us, err := GetUTXOSet(ctx, ulogger.TestLogger{}, tSettings, store, hash)
	require.NoError(t, err)

	r, err := us.GetUTXOSetReader(hash)
	require.NoError(t, err)
	defer r.Close()

	_, err = io.CopyN(io.Discard, r, 68)
	require.NoError(t, err)

	out := map[string]uint64{}

	for {
		w, err := NewUTXOWrapperFromReader(ctx, r)
		if err != nil {
			if _, ok := err.(*ErrRecordBoundary); ok {
				break
			}

			require.NoError(t, err)
		}

		for _, u := range w.UTXOs {
			out[fmt.Sprintf("%s:%d", w.TxID.String(), u.Index)] = u.Value
		}
	}

	return out
}

// newBuilderServer wires a Server whose header source is a mock blockchain
// client returning the given headers for the (from, endHeight) call. Direct
// blockchainStore is left nil, so the utxo-headers write is skipped — the
// consolidation, set write, and marker logic are all still exercised.
func newBuilderServer(t *testing.T, store blob.Store, headers []*model.BlockHeader, metas []*model.BlockHeaderMeta, from, end uint32) (*Server, *settings.Settings) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	mockClient := &blockchain.Mock{}
	mockClient.On("GetBlockHeadersByHeight", mock.Anything, from, end).Return(headers, metas, nil)

	s := New(context.Background(), ulogger.TestLogger{}, tSettings, store, mockClient)

	return s, tSettings
}

func TestBuildUTXOSetToHeight_GenesisHappyPath(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	tSettings := test.CreateBaseTestSettings(t)
	genesis := tSettings.ChainCfgParams.GenesisHash

	headers, metas, byH := buildChainHeaders(t, genesis, 3)

	tx1 := p2pkhTx(t, 0x11, 1000)
	tx2 := p2pkhTx(t, 0x22, 2000)
	tx3 := p2pkhTx(t, 0x33, 3000)

	stageBlockDeltas(t, ctx, tSettings, store, byH[1], 1, tx1)
	stageBlockDeltas(t, ctx, tSettings, store, byH[2], 2, tx2)
	stageBlockDeltas(t, ctx, tSettings, store, byH[3], 3, tx3)

	s, _ := newBuilderServer(t, store, headers, metas, 1, 3)

	err := s.BuildUTXOSetToHeight(ctx, 0, 3, false)
	require.NoError(t, err)

	got := readSetUTXOs(t, ctx, tSettings, store, byH[3])
	require.Equal(t, map[string]uint64{
		fmt.Sprintf("%s:0", tx1.TxIDChainHash().String()): 1000,
		fmt.Sprintf("%s:0", tx2.TxIDChainHash().String()): 2000,
		fmt.Sprintf("%s:0", tx3.TxIDChainHash().String()): 3000,
	}, got)

	// updateLastProcessed=false must leave the marker untouched.
	h, err := s.readLastHeight(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(0), h)
}

func TestBuildUTXOSetToHeight_UpdatesLastProcessedWhenAsked(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	tSettings := test.CreateBaseTestSettings(t)
	genesis := tSettings.ChainCfgParams.GenesisHash

	headers, metas, byH := buildChainHeaders(t, genesis, 2)
	stageBlockDeltas(t, ctx, tSettings, store, byH[1], 1, p2pkhTx(t, 0x11, 1000))
	stageBlockDeltas(t, ctx, tSettings, store, byH[2], 2, p2pkhTx(t, 0x22, 2000))

	s, _ := newBuilderServer(t, store, headers, metas, 1, 2)

	err := s.BuildUTXOSetToHeight(ctx, 0, 2, true)
	require.NoError(t, err)

	h, err := s.readLastHeight(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(2), h)
}

func TestBuildUTXOSetToHeight_RefusesToRewindLastProcessed(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	tSettings := test.CreateBaseTestSettings(t)
	genesis := tSettings.ChainCfgParams.GenesisHash

	headers, metas, byH := buildChainHeaders(t, genesis, 2)
	stageBlockDeltas(t, ctx, tSettings, store, byH[1], 1, p2pkhTx(t, 0x11, 1000))
	stageBlockDeltas(t, ctx, tSettings, store, byH[2], 2, p2pkhTx(t, 0x22, 2000))

	s, _ := newBuilderServer(t, store, headers, metas, 1, 2)

	// The service has already advanced its resume marker to height 5.
	require.NoError(t, s.writeLastHeight(ctx, 5))

	// A one-shot build to a lower height that also asks to advance the marker
	// must be refused up front, and must leave the marker untouched.
	err := s.BuildUTXOSetToHeight(ctx, 0, 2, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "refusing to rewind")

	h, err := s.readLastHeight(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(5), h)
}

func TestBuildUTXOSetToHeight_RejectsBadArguments(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	tSettings := test.CreateBaseTestSettings(t)
	genesis := tSettings.ChainCfgParams.GenesisHash

	headers, metas, _ := buildChainHeaders(t, genesis, 3)

	// endHeight < 1
	s0 := New(ctx, ulogger.TestLogger{}, tSettings, store, &blockchain.Mock{})
	err := s0.BuildUTXOSetToHeight(ctx, 0, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "end height")

	// startHeight >= endHeight
	s1 := New(ctx, ulogger.TestLogger{}, tSettings, store, &blockchain.Mock{})
	err = s1.BuildUTXOSetToHeight(ctx, 3, 3, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "start height")

	// no header source at all
	s2 := &Server{logger: ulogger.TestLogger{}, settings: tSettings, blockStore: store, stats: gocore.NewStat("t")}
	err = s2.BuildUTXOSetToHeight(ctx, 0, 3, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no blockchain source")

	// nil block store, with otherwise-valid arguments and a header source
	s3 := &Server{logger: ulogger.TestLogger{}, settings: tSettings, blockchainClient: &blockchain.Mock{}, stats: gocore.NewStat("t")}
	err = s3.BuildUTXOSetToHeight(ctx, 0, 3, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "block store is not initialized")

	_ = headers
	_ = metas
}

func TestBuildUTXOSetToHeight_ChainTooShort(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	tSettings := test.CreateBaseTestSettings(t)
	genesis := tSettings.ChainCfgParams.GenesisHash

	// Chain only reaches height 2, but caller requests 3.
	headers, metas, byH := buildChainHeaders(t, genesis, 2)
	stageBlockDeltas(t, ctx, tSettings, store, byH[1], 1, p2pkhTx(t, 0x11, 1000))
	stageBlockDeltas(t, ctx, tSettings, store, byH[2], 2, p2pkhTx(t, 0x22, 2000))

	s, _ := newBuilderServer(t, store, headers, metas, 1, 3)

	err := s.BuildUTXOSetToHeight(ctx, 0, 3, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "only reaches height 2")
}

func TestBuildUTXOSetToHeight_MissingDeltasFailsBeforeWriting(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	genesis := tSettings.ChainCfgParams.GenesisHash
	headers, metas, byH := buildChainHeaders(t, genesis, 3)

	// Missing utxo-additions (block 3 not staged at all).
	t.Run("missing additions", func(t *testing.T) {
		store := memory.New()
		stageBlockDeltas(t, ctx, tSettings, store, byH[1], 1, p2pkhTx(t, 0x11, 1000))
		stageBlockDeltas(t, ctx, tSettings, store, byH[2], 2, p2pkhTx(t, 0x22, 2000))

		s, _ := newBuilderServer(t, store, headers, metas, 1, 3)
		err := s.BuildUTXOSetToHeight(ctx, 0, 3, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "utxo-additions")

		exists, err := store.Exists(ctx, byH[3][:], fileformat.FileTypeUtxoSet)
		require.NoError(t, err)
		require.False(t, exists, "no set should be written when the range is incomplete")
	})

	// Missing utxo-deletions specifically (delete just that file after staging).
	t.Run("missing deletions", func(t *testing.T) {
		store := memory.New()
		stageBlockDeltas(t, ctx, tSettings, store, byH[1], 1, p2pkhTx(t, 0x11, 1000))
		stageBlockDeltas(t, ctx, tSettings, store, byH[2], 2, p2pkhTx(t, 0x22, 2000))
		stageBlockDeltas(t, ctx, tSettings, store, byH[3], 3, p2pkhTx(t, 0x33, 3000))
		require.NoError(t, store.Del(ctx, byH[2][:], fileformat.FileTypeUtxoDeletions))

		s, _ := newBuilderServer(t, store, headers, metas, 1, 3)
		err := s.BuildUTXOSetToHeight(ctx, 0, 3, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "utxo-deletions")

		exists, err := store.Exists(ctx, byH[3][:], fileformat.FileTypeUtxoSet)
		require.NoError(t, err)
		require.False(t, exists)
	})
}

func TestBuildUTXOSetToHeight_MissingBaseSet(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	tSettings := test.CreateBaseTestSettings(t)
	genesis := tSettings.ChainCfgParams.GenesisHash

	headers, metas, byH := buildChainHeaders(t, genesis, 3)
	stageBlockDeltas(t, ctx, tSettings, store, byH[3], 3, p2pkhTx(t, 0x33, 3000))

	// Request start-height 2 with no set@2 present.
	s, _ := newBuilderServer(t, store, headers, metas, 2, 3)
	err := s.BuildUTXOSetToHeight(ctx, 2, 3, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no utxo-set found at start-height 2")
	_ = byH
}

func TestBuildUTXOSetToHeight_StartFromExistingSet(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	genesis := tSettings.ChainCfgParams.GenesisHash
	headers, metas, byH := buildChainHeaders(t, genesis, 3)

	tx1 := p2pkhTx(t, 0x11, 1000)
	tx2 := p2pkhTx(t, 0x22, 2000)
	tx3 := p2pkhTx(t, 0x33, 3000)

	// Reference: single-pass genesis -> 3.
	refStore := memory.New()
	stageBlockDeltas(t, ctx, tSettings, refStore, byH[1], 1, tx1)
	stageBlockDeltas(t, ctx, tSettings, refStore, byH[2], 2, tx2)
	stageBlockDeltas(t, ctx, tSettings, refStore, byH[3], 3, tx3)
	refS, _ := newBuilderServer(t, refStore, headers, metas, 1, 3)
	require.NoError(t, refS.BuildUTXOSetToHeight(ctx, 0, 3, false))
	want := readSetUTXOs(t, ctx, tSettings, refStore, byH[3])

	// Incremental: genesis -> 2, then 2 -> 3.
	store := memory.New()
	stageBlockDeltas(t, ctx, tSettings, store, byH[1], 1, tx1)
	stageBlockDeltas(t, ctx, tSettings, store, byH[2], 2, tx2)
	stageBlockDeltas(t, ctx, tSettings, store, byH[3], 3, tx3)

	h012, m012, _ := buildChainHeaders(t, genesis, 2)
	s1, _ := newBuilderServer(t, store, h012, m012, 1, 2)
	require.NoError(t, s1.BuildUTXOSetToHeight(ctx, 0, 2, false))

	// s2 resolves headers twice: BuildUTXOSetToHeight fetches [2..3] to learn
	// the base hash and run the pre-flight, and ConsolidateBlockRange
	// independently fetches [3..3]. Stub both ranges with correct slices.
	// buildChainHeaders returns index 0 = height 1, so headers[1:3] = heights
	// 2,3 and headers[2:3] = height 3.
	mock2 := &blockchain.Mock{}
	mock2.On("GetBlockHeadersByHeight", mock.Anything, uint32(2), uint32(3)).Return(headers[1:3], metas[1:3], nil)
	mock2.On("GetBlockHeadersByHeight", mock.Anything, uint32(3), uint32(3)).Return(headers[2:3], metas[2:3], nil)
	s2 := New(ctx, ulogger.TestLogger{}, tSettings, store, mock2)
	require.NoError(t, s2.BuildUTXOSetToHeight(ctx, 2, 3, false))

	got := readSetUTXOs(t, ctx, tSettings, store, byH[3])
	require.Equal(t, want, got, "incremental 0->2->3 must equal single-pass 0->3")

	// Base set at height 2 must be preserved (not deleted).
	exists, err := store.Exists(ctx, byH[2][:], fileformat.FileTypeUtxoSet)
	require.NoError(t, err)
	require.True(t, exists, "base set at start-height must not be deleted")
}

// storeChainBlock builds and stores a single valid successor block on the
// given sqlitememory blockchain store, linking HashPrevBlock to prevHash.
// Mirrors services/blockchain/median_time_past_test.go's createTestBlockAtHeight:
// StoreBlock does not verify proof-of-work, so fixed Bits/Nonce values are fine
// as long as HashPrevBlock correctly chains to the real previous block hash.
func storeChainBlock(t *testing.T, ctx context.Context, blockchainStore blockchain_store.Store, prevHash *chainhash.Hash, height uint32) *chainhash.Hash {
	t.Helper()

	coinbaseTx := bt.NewTx()
	require.NoError(t, coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))

	heightBytes := []byte{0x03, byte(height & 0xff), byte((height >> 8) & 0xff), byte((height >> 16) & 0xff)}
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(heightBytes)
	coinbaseTx.Inputs[0].SequenceNumber = 0xffffffff
	require.NoError(t, coinbaseTx.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000))

	merkleRoot := chainhash.HashH(coinbaseTx.Bytes())
	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevHash,
		HashMerkleRoot: &merkleRoot,
		Timestamp:      1231006505 + height,
		Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits, PoW is not checked by StoreBlock
		Nonce:          height,
	}

	subtreeHash := chainhash.HashH([]byte(fmt.Sprintf("subtree-%d", height)))

	block := &model.Block{
		Header:           header,
		CoinbaseTx:       coinbaseTx,
		Subtrees:         []*chainhash.Hash{&subtreeHash},
		TransactionCount: 1,
		SizeInBytes:      1000,
	}

	_, _, err := blockchainStore.StoreBlock(ctx, block, "")
	require.NoError(t, err)

	return header.Hash()
}

// TestBuildUTXOSetToHeight_DirectModeWritesHeaders exercises the real
// direct-mode production path (blockchainStore != nil), which always runs
// WriteHeadersToStore, and asserts it writes both the utxo-set and its
// utxo-headers file. The blob store is a real FILE store so the store's own
// checksum sidecar handling is exercised on the same path production uses.
func TestBuildUTXOSetToHeight_DirectModeWritesHeaders(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	genesis := tSettings.ChainCfgParams.GenesisHash

	fileURL, err := url.Parse("file://" + t.TempDir() + "?disableDAH=true")
	require.NoError(t, err)

	blockStore, err := blob.NewStore(ulogger.TestLogger{}, fileURL)
	require.NoError(t, err)

	bcURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	// A real sqlitememory blockchain store; genesis is auto-inserted by
	// NewStore, matching tSettings.ChainCfgParams.
	blockchainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, bcURL, tSettings)
	require.NoError(t, err)

	const numBlocks = uint32(3)

	prevHash := genesis
	for h := uint32(1); h <= numBlocks; h++ {
		prevHash = storeChainBlock(t, ctx, blockchainStore, prevHash, h)
	}

	headers, _, err := blockchainStore.GetBlockHeadersByHeight(ctx, 1, numBlocks)
	require.NoError(t, err)
	require.Len(t, headers, int(numBlocks))

	for i, hdr := range headers {
		height := uint32(i + 1) //nolint:gosec // i bounded by numBlocks (3), no overflow risk
		stageBlockDeltas(t, ctx, tSettings, blockStore, hdr.Hash(), height, p2pkhTx(t, byte(0x10+height), uint64(height)*1000))
	}

	s, err := NewDirect(ctx, ulogger.TestLogger{}, tSettings, blockStore, blockchainStore)
	require.NoError(t, err)

	require.NoError(t, s.BuildUTXOSetToHeight(ctx, 0, numBlocks, false))

	tipHash := headers[numBlocks-1].Hash()

	exists, err := blockStore.Exists(ctx, tipHash[:], fileformat.FileTypeUtxoSet)
	require.NoError(t, err)
	require.True(t, exists, "utxo-set file must be written")

	exists, err = blockStore.Exists(ctx, tipHash[:], fileformat.FileTypeUtxoHeaders)
	require.NoError(t, err)
	require.True(t, exists, "utxo-headers file must be written")
}

// TestBuildUTXOSetToHeight_HeaderWriteFailureIsFatal pins that a failure writing
// the utxo-headers file is fatal for the one-shot builder (returns an error),
// rather than the warn-and-continue that processNextBlock uses. The one-shot
// has no retry loop and the headers file is a required half of the seed, so
// swallowing the error would exit 0 with an incomplete, unseedable snapshot.
// The failure is induced by pre-staging the utxo-headers file so
// WriteHeadersToStore's FileStorer collides with an "already exists" error.
func TestBuildUTXOSetToHeight_HeaderWriteFailureIsFatal(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	genesis := tSettings.ChainCfgParams.GenesisHash

	fileURL, err := url.Parse("file://" + t.TempDir() + "?disableDAH=true")
	require.NoError(t, err)

	blockStore, err := blob.NewStore(ulogger.TestLogger{}, fileURL)
	require.NoError(t, err)

	bcURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	blockchainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, bcURL, tSettings)
	require.NoError(t, err)

	const numBlocks = uint32(3)

	prevHash := genesis
	for h := uint32(1); h <= numBlocks; h++ {
		prevHash = storeChainBlock(t, ctx, blockchainStore, prevHash, h)
	}

	headers, _, err := blockchainStore.GetBlockHeadersByHeight(ctx, 1, numBlocks)
	require.NoError(t, err)
	require.Len(t, headers, int(numBlocks))

	for i, hdr := range headers {
		height := uint32(i + 1) //nolint:gosec // i bounded by numBlocks (3), no overflow risk
		stageBlockDeltas(t, ctx, tSettings, blockStore, hdr.Hash(), height, p2pkhTx(t, byte(0x10+height), uint64(height)*1000))
	}

	tipHash := headers[numBlocks-1].Hash()

	// Pre-stage the utxo-headers file so WriteHeadersToStore's FileStorer fails
	// with "already exists" (FileStorer.NewFileStorer refuses to overwrite).
	require.NoError(t, blockStore.Set(ctx, tipHash[:], fileformat.FileTypeUtxoHeaders, []byte("preexisting")))

	s, err := NewDirect(ctx, ulogger.TestLogger{}, tSettings, blockStore, blockchainStore)
	require.NoError(t, err)

	err = s.BuildUTXOSetToHeight(ctx, 0, numBlocks, true)
	require.Error(t, err, "a utxo-headers write failure must be fatal for the one-shot builder")
	require.Contains(t, err.Error(), "utxo-headers")

	// The utxo-set was written before the header write, so it exists — but the
	// seed is incomplete, which is exactly why the error is surfaced.
	exists, err := blockStore.Exists(ctx, tipHash[:], fileformat.FileTypeUtxoSet)
	require.NoError(t, err)
	require.True(t, exists, "utxo-set is written before the header step")

	// The lastProcessed marker must NOT have advanced past a failed build, even
	// though updateLastProcessed was requested.
	h, err := s.readLastHeight(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(0), h, "lastProcessed must not advance when the seed is incomplete")
}
