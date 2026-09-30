package seeder

import (
	"context"
	"encoding/hex"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/stores/blob"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// newTestBlobStore builds a fresh in-memory blob store for lastProcessed.dat checks.
func newTestBlobStore(t *testing.T) blob.Store {
	t.Helper()

	storeURL, err := url.Parse("memory://")
	require.NoError(t, err)

	store, err := blob.NewStore(ulogger.TestLogger{}, storeURL)
	require.NoError(t, err)

	return store
}

func markLastProcessed(t *testing.T, ctx context.Context, blockStore blob.Store) {
	t.Helper()

	err := blockStore.Set(ctx, nil, fileformat.FileTypeDat, []byte("1\n"),
		bloboptions.WithFilename("lastProcessed"), bloboptions.WithNoHashPrefix())
	require.NoError(t, err)
}

// newTestBlockchainStore builds a fresh in-memory blockchain store. The store
// auto-initialises with the genesis block.
func newTestBlockchainStore(t *testing.T) blockchain_store.Store {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	store, err := blockchain_store.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	return store
}

// makeCoinbaseTx builds a unique coinbase-style tx by patching a handful of
// bytes in a base hex payload. Copied from cmd/rewindblockchain test helpers.
func makeCoinbaseTx(t *testing.T, heightByte, nonceSalt byte) *bt.Tx {
	t.Helper()

	base := "01000000010000000000000000000000000000000000000000000000000000000000000000" +
		"ffffffff17030100002f6d312d65752f29c267ffea1adb87f33b398fffffffff" +
		"03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac" +
		"aa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588ac" +
		"aa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac" +
		"00000000"

	b, err := hex.DecodeString(base)
	require.NoError(t, err)

	const scriptStart = 42
	b[scriptStart+1] = heightByte
	b[scriptStart+11] = nonceSalt
	b[scriptStart+22] = nonceSalt ^ 0xff

	tx, err := bt.NewTxFromBytes(b)
	require.NoError(t, err)

	return tx
}

// storeBlockAboveGenesis stores a single minimal block on top of genesis and
// returns it, so tests have a real non-genesis tip with a persisted header.
func storeBlockAboveGenesis(t *testing.T, ctx context.Context, store blockchain_store.Store) *model.Block {
	t.Helper()

	genesis, err := store.GetBlockByID(ctx, 0)
	require.NoError(t, err)

	coinbase := makeCoinbaseTx(t, 1, 1)
	root := coinbase.TxIDChainHash()

	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1700000001,
			Nonce:          1,
			HashPrevBlock:  genesis.Header.Hash(),
			HashMerkleRoot: root,
			Bits:           genesis.Header.Bits,
		},
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
		Height:           1,
		Subtrees:         []*chainhash.Hash{root},
	}

	_, _, err = store.StoreBlock(ctx, block, "seeder-test", blockchainoptions.WithMinedSet(true))
	require.NoError(t, err)

	return block
}

func TestBlockAssemblerStateExists(t *testing.T) {
	ctx := context.Background()
	store := newTestBlockchainStore(t)

	exists, err := blockAssemblerStateExists(ctx, store)
	require.NoError(t, err)
	require.False(t, exists, "fresh store must not have BlockAssembler state")

	block := storeBlockAboveGenesis(t, ctx, store)
	require.NoError(t, writeBlockAssemblerState(ctx, ulogger.TestLogger{}, store, &utxoSetTip{hash: *block.Hash(), height: 1}))

	exists, err = blockAssemblerStateExists(ctx, store)
	require.NoError(t, err)
	require.True(t, exists, "state must be detected after it has been written")
}

func TestWriteBlockAssemblerState(t *testing.T) {
	ctx := context.Background()
	store := newTestBlockchainStore(t)

	block := storeBlockAboveGenesis(t, ctx, store)

	tip := &utxoSetTip{hash: *block.Hash(), height: 1}
	require.NoError(t, writeBlockAssemblerState(ctx, ulogger.TestLogger{}, store, tip))

	// The persisted state must decode, via the shared codec, to exactly the
	// utxo-set tip header and height — this is what BlockAssembler.initState
	// reads back on startup.
	data, err := store.GetState(ctx, blockassembly.StateKey)
	require.NoError(t, err)

	gotHeader, gotHeight, err := blockassembly.DecodeState(data)
	require.NoError(t, err)
	require.Equal(t, uint32(1), gotHeight)
	require.Equal(t, block.Header.Hash().String(), gotHeader.Hash().String())
}

func TestWriteBlockAssemblerState_MissingHeader(t *testing.T) {
	ctx := context.Background()
	store := newTestBlockchainStore(t)

	// A tip hash that was never stored in the blockchain store must not silently
	// adopt — writing state would let BlockAssembly resume on a header it has no
	// record of.
	var missing chainhash.Hash
	missing[0] = 0xde
	missing[1] = 0xad

	err := writeBlockAssemblerState(ctx, ulogger.TestLogger{}, store, &utxoSetTip{hash: missing, height: 5})
	require.Error(t, err)

	// And nothing should have been persisted.
	exists, err := blockAssemblerStateExists(ctx, store)
	require.NoError(t, err)
	require.False(t, exists)
}

// TestCheckSkipUTXOImport_NoMarker_ProceedsWithImport covers the common case:
// no lastProcessed.dat means this is a fresh seed, so the UTXO pass must run.
func TestCheckSkipUTXOImport_NoMarker_ProceedsWithImport(t *testing.T) {
	ctx := context.Background()
	blockchainStore := newTestBlockchainStore(t)
	blockStore := newTestBlobStore(t)

	skip, err := checkSkipUTXOImport(ctx, blockStore, blockchainStore)
	require.NoError(t, err)
	require.False(t, skip)
}

// TestCheckSkipUTXOImport_MarkerWithCheckpoint_SkipsIdempotently covers a
// cleanly completed prior seed: lastProcessed.dat and the BlockAssembler
// checkpoint are both present, so re-running must skip idempotently.
func TestCheckSkipUTXOImport_MarkerWithCheckpoint_SkipsIdempotently(t *testing.T) {
	ctx := context.Background()
	blockchainStore := newTestBlockchainStore(t)
	blockStore := newTestBlobStore(t)

	markLastProcessed(t, ctx, blockStore)

	block := storeBlockAboveGenesis(t, ctx, blockchainStore)
	require.NoError(t, writeBlockAssemblerState(ctx, ulogger.TestLogger{}, blockchainStore, &utxoSetTip{hash: *block.Hash(), height: 1}))

	skip, err := checkSkipUTXOImport(ctx, blockStore, blockchainStore)
	require.NoError(t, err)
	require.True(t, skip)
}

// TestCheckSkipUTXOImport_MarkerWithoutCheckpoint_RefusesWithForceHint is the
// ChiR1 regression: lastProcessed.dat present but no BlockAssembler checkpoint
// means an earlier run imported the UTXO set but failed before the checkpoint
// was written (e.g. the header pass rejected a truncated headers file after
// the UTXO pass had already finished). Silently skipping here would report
// success on a half-seeded store with no checkpoint; the seeder must refuse
// instead and name -force as the recovery.
func TestCheckSkipUTXOImport_MarkerWithoutCheckpoint_RefusesWithForceHint(t *testing.T) {
	ctx := context.Background()
	blockchainStore := newTestBlockchainStore(t)
	blockStore := newTestBlobStore(t)

	markLastProcessed(t, ctx, blockStore)

	skip, err := checkSkipUTXOImport(ctx, blockStore, blockchainStore)
	require.Error(t, err)
	require.ErrorContains(t, err, "-force")
	require.False(t, skip)

	// And it must not have silently written a checkpoint either.
	exists, err := blockAssemblerStateExists(ctx, blockchainStore)
	require.NoError(t, err)
	require.False(t, exists)
}
