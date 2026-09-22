package utxopersister

import (
	"context"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// stageForeignDelta writes a delta file under storeKey whose 36-byte header names a different
// block than storeKey, or the same block at a different height. The file is produced by the
// production writer into a scratch store and copied across verbatim, so its magic and record
// framing stay genuine - only the key it is stored under changes.
func stageForeignDelta(t *testing.T, ctx context.Context, tSettings *settings.Settings, store blob.Store,
	storeKey *chainhash.Hash, headerHash *chainhash.Hash, headerHeight uint32, fileType fileformat.FileType, txs ...*bt.Tx) {
	t.Helper()

	scratch := memory.New()
	stageBlockDeltas(t, ctx, tSettings, scratch, headerHash, headerHeight, txs...)

	data, err := scratch.Get(ctx, headerHash[:], fileType)
	require.NoError(t, err)

	require.NoError(t, store.Set(ctx, storeKey[:], fileType, data))
}

// TestGetUTXOAdditionsReader_AcceptsMatchingHeader is the positive control: a genuine delta
// opened under its own block hash and height still reads back.
func TestGetUTXOAdditionsReader_AcceptsMatchingHeader(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	store := memory.New()

	blockA := chainhash.HashH([]byte("delta-header-block-a"))
	tx := p2pkhTx(t, 0x11, 1000)

	stageBlockDeltas(t, ctx, tSettings, store, &blockA, 7, tx)

	us, err := GetUTXOSet(ctx, logger, tSettings, store, &blockA, 7)
	require.NoError(t, err)

	r, err := us.GetUTXOAdditionsReader(ctx)
	require.NoError(t, err)

	defer r.Close()

	wrapper, err := NewUTXOWrapperFromReader(ctx, r)
	require.NoError(t, err)
	require.Equal(t, tx.TxIDChainHash().String(), wrapper.TxID.String())
	require.Equal(t, uint32(7), wrapper.Height)
}

// TestGetUTXOAdditionsReader_RefusesWhenHeightUnknown pins that the height-optional
// constructor cannot be used to read a delta: zero is a real block height, so it cannot stand
// in for "unknown", and a set opened without one has nothing to check the header against.
func TestGetUTXOAdditionsReader_RefusesWhenHeightUnknown(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	store := memory.New()

	blockA := chainhash.HashH([]byte("delta-header-height-unknown"))

	stageBlockDeltas(t, ctx, tSettings, store, &blockA, 7, p2pkhTx(t, 0x12, 1000))

	us, err := GetUTXOSet(ctx, logger, tSettings, store, &blockA)
	require.NoError(t, err)

	_, err = us.GetUTXOAdditionsReader(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires the block height")
}

// TestGetUTXOAdditionsReader_RejectsForeignBlockHash is the regression test issue 4841 asks
// for: a genuine delta for one block, stored under another block's key, must not be folded in.
func TestGetUTXOAdditionsReader_RejectsForeignBlockHash(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	store := memory.New()

	blockA := chainhash.HashH([]byte("delta-header-key-block"))
	blockB := chainhash.HashH([]byte("delta-header-foreign-block"))

	stageForeignDelta(t, ctx, tSettings, store, &blockA, &blockB, 7, fileformat.FileTypeUtxoAdditions, p2pkhTx(t, 0x13, 1000))

	us, err := GetUTXOSet(ctx, logger, tSettings, store, &blockA, 7)
	require.NoError(t, err)

	_, err = us.GetUTXOAdditionsReader(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), blockB.String())
	require.Contains(t, err.Error(), blockA.String())
}

// TestGetUTXOAdditionsReader_RejectsHeightMismatch covers the same block hash at the wrong
// height - a delta replayed at another point in the chain.
func TestGetUTXOAdditionsReader_RejectsHeightMismatch(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	store := memory.New()

	blockA := chainhash.HashH([]byte("delta-header-height-mismatch"))

	stageForeignDelta(t, ctx, tSettings, store, &blockA, &blockA, 100, fileformat.FileTypeUtxoAdditions, p2pkhTx(t, 0x14, 1000))

	us, exists, err := GetUTXOSetWithExistCheck(ctx, logger, tSettings, store, &blockA, 101)
	require.NoError(t, err)
	require.False(t, exists)

	_, err = us.GetUTXOAdditionsReader(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "names height 100")
	require.Contains(t, err.Error(), "opened at height 101")
}

// TestGetUTXOAdditionsReader_RejectsHeightMismatchAtHeightZero proves zero is a known height
// rather than a stand-in for "unknown": this fails against any guard of the form
// blockHeight != 0.
func TestGetUTXOAdditionsReader_RejectsHeightMismatchAtHeightZero(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	store := memory.New()

	blockA := chainhash.HashH([]byte("delta-header-height-zero"))

	stageForeignDelta(t, ctx, tSettings, store, &blockA, &blockA, 5, fileformat.FileTypeUtxoAdditions, p2pkhTx(t, 0x15, 1000))

	us, _, err := GetUTXOSetWithExistCheck(ctx, logger, tSettings, store, &blockA, 0)
	require.NoError(t, err)

	_, err = us.GetUTXOAdditionsReader(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "names height 5")
}

// TestGetUTXODeletionsReader_RejectsForeignBlockHash mirrors the additions case: both deltas
// carry the same header and both must be checked.
func TestGetUTXODeletionsReader_RejectsForeignBlockHash(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	store := memory.New()

	blockA := chainhash.HashH([]byte("delta-header-deletions-key"))
	blockB := chainhash.HashH([]byte("delta-header-deletions-foreign"))

	stageForeignDelta(t, ctx, tSettings, store, &blockA, &blockB, 7, fileformat.FileTypeUtxoDeletions, p2pkhTx(t, 0x16, 1000))

	us, err := GetUTXOSet(ctx, logger, tSettings, store, &blockA, 7)
	require.NoError(t, err)

	_, err = us.GetUTXODeletionsReader(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), blockB.String())
	require.Contains(t, err.Error(), blockA.String())
}

// TestGetUTXOAdditionsReader_ClosesReaderOnHeaderMismatch pins that the new rejection path
// releases the file-store read permit, the same contract the read-error paths already have:
// leaking permits here would exhaust the read semaphore.
func TestGetUTXOAdditionsReader_ClosesReaderOnHeaderMismatch(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	someHash := chainhash.HashH([]byte("delta-header-close-on-mismatch"))

	// 36 readable bytes: a complete but all-zero header, which names a block that is not
	// someHash. The header check, not a read error, is what rejects it.
	errReader := &readErrCloser{allowedBytes: 36, err: io.EOF}
	store := &fakeStoreReturningErrCloser{
		Memory:     memory.New(),
		targetType: fileformat.FileTypeUtxoAdditions,
		reader:     errReader,
	}

	us, err := GetUTXOSet(ctx, logger, tSettings, store, &someHash, 7)
	require.NoError(t, err)

	_, err = us.GetUTXOAdditionsReader(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), someHash.String())
	require.True(t, errReader.closed, "reader must be Closed when the header is rejected - otherwise the file-store read permit leaks")
}
