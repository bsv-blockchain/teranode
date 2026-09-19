package seeder

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestProcessHeaders_ReRun_FailsOnAlreadyStoredBlock is the reproduction for
// the first half of the ordishs P1 on PR #1604: the prescribed "-force"
// recovery re-runs processHeaders, which re-stores every block from the
// headers file - including ones a prior run already stored. Before the fix,
// StoreBlock's BlockExistsError is fatal, so the recovery run fails before it
// ever reaches the BlockAssembler checkpoint write.
func TestProcessHeaders_ReRun_FailsOnAlreadyStoredBlock(t *testing.T) {
	ctx := context.Background()
	store := newTestBlockchainStore(t)

	genesis, err := store.GetBlockByID(ctx, 0)
	require.NoError(t, err)

	genesisRecord, err := blockToIndexBytes(t, genesis, genesis.Header.Hash())
	require.NoError(t, err)

	coinbase := makeCoinbaseTx(t, 1, 1)
	block1 := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1700000001,
			Nonce:          1,
			HashPrevBlock:  genesis.Header.Hash(),
			HashMerkleRoot: coinbase.TxIDChainHash(),
			Bits:           genesis.Header.Bits,
		},
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
		Height:           1,
	}
	block1Record, err := blockToIndexBytes(t, block1, block1.Header.Hash())
	require.NoError(t, err)

	path := writeUtxoHeadersFile(t, 1, genesisRecord, block1Record)

	// First pass: a complete, successful header import.
	require.NoError(t, processHeaders(ctx, ulogger.TestLogger{}, store, path))

	// Recovery re-run against the identical, complete file: this is exactly
	// what re-running with -force does. It must not fail on blocks the first
	// pass already stored.
	err = processHeaders(ctx, ulogger.TestLogger{}, store, path)
	require.NoError(t, err, "a re-run over already-stored blocks must be idempotent, not fatal")
}

// writeUtxoSetPreamble writes a minimal but complete .utxo-set file
// containing only the preamble readUTXOSetTip depends on (file header, tip
// hash, tip height, previous-block-hash placeholder) and no UTXO records.
// Sufficient for exercising the cheap checkpoint-recovery path, which never
// reads past the preamble.
func writeUtxoSetPreamble(t *testing.T, tipHash chainhash.Hash, tipHeight uint32) string {
	t.Helper()

	header := fileformat.NewHeader(fileformat.FileTypeUtxoSet)

	heightBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(heightBytes, tipHeight)

	var previousBlockHash [32]byte

	data := make([]byte, 0, len(header.Bytes())+len(tipHash)+len(heightBytes)+len(previousBlockHash))
	data = append(data, header.Bytes()...)
	data = append(data, tipHash[:]...)
	data = append(data, heightBytes...)
	data = append(data, previousBlockHash[:]...)

	path := filepath.Join(t.TempDir(), "test.utxo-set")
	require.NoError(t, os.WriteFile(path, data, 0o600))

	return path
}

// TestReadUTXOSetTip_ReadsPreambleWithoutFullImport is the positive-path
// coverage for the cheapest of the three recovery options ordishs offered:
// the utxo-set file's own preamble already carries the tip hash and height,
// so the missing checkpoint can be recovered by reading a handful of bytes -
// no re-import of the (already complete) UTXO set required.
func TestReadUTXOSetTip_ReadsPreambleWithoutFullImport(t *testing.T) {
	var tipHash chainhash.Hash
	tipHash[0] = 0xab
	tipHash[31] = 0xcd

	path := writeUtxoSetPreamble(t, tipHash, 42)

	tip, err := readUTXOSetTip(path)
	require.NoError(t, err)
	require.Equal(t, uint32(42), tip.height)
	require.Equal(t, tipHash.String(), tip.hash.String())
}

// TestCheckpointRecoverable_MarkerWithoutCheckpoint_ReportsRecoverable covers
// the decision the -force path uses to avoid re-importing an already-complete
// UTXO set: lastProcessed.dat present with no BlockAssembler checkpoint is
// exactly the state a cheap, preamble-only recovery applies to.
func TestCheckpointRecoverable_MarkerWithoutCheckpoint_ReportsRecoverable(t *testing.T) {
	ctx := context.Background()
	blockchainStore := newTestBlockchainStore(t)
	blockStore := newTestBlobStore(t)

	markLastProcessed(t, ctx, blockStore)

	recoverable, err := checkpointRecoverable(ctx, blockStore, blockchainStore)
	require.NoError(t, err)
	require.True(t, recoverable)
}

// TestCheckpointRecoverable_NoMarker_NotRecoverable covers the fresh-seed
// case: no lastProcessed.dat means there is nothing to recover - the normal
// full import must run instead.
func TestCheckpointRecoverable_NoMarker_NotRecoverable(t *testing.T) {
	ctx := context.Background()
	blockchainStore := newTestBlockchainStore(t)
	blockStore := newTestBlobStore(t)

	recoverable, err := checkpointRecoverable(ctx, blockStore, blockchainStore)
	require.NoError(t, err)
	require.False(t, recoverable)
}

// TestCheckpointRecoverable_MarkerWithCheckpoint_NotRecoverable covers the
// cleanly-completed case: both the marker and the checkpoint are present, so
// there is nothing left to recover.
func TestCheckpointRecoverable_MarkerWithCheckpoint_NotRecoverable(t *testing.T) {
	ctx := context.Background()
	blockchainStore := newTestBlockchainStore(t)
	blockStore := newTestBlobStore(t)

	markLastProcessed(t, ctx, blockStore)

	block := storeBlockAboveGenesis(t, ctx, blockchainStore)
	require.NoError(t, writeBlockAssemblerState(ctx, ulogger.TestLogger{}, blockchainStore, &utxoSetTip{hash: *block.Hash(), height: 1}))

	recoverable, err := checkpointRecoverable(ctx, blockStore, blockchainStore)
	require.NoError(t, err)
	require.False(t, recoverable)
}

// TestLastProcessedMarker_RewriteWithoutAllowOverwrite_Fails is the
// reproduction for the second half of the ordishs P1: without
// WithAllowOverwrite, rewriting lastProcessed.dat over a store that already
// holds one - exactly the marker write processUTXOs performs on completion -
// fails with BlobAlreadyExists.
func TestLastProcessedMarker_RewriteWithoutAllowOverwrite_Fails(t *testing.T) {
	ctx := context.Background()
	blockStore := newTestBlobStore(t)

	markLastProcessed(t, ctx, blockStore)

	err := blockStore.Set(ctx, nil, fileformat.FileTypeDat, []byte("2\n"),
		bloboptions.WithFilename("lastProcessed"), bloboptions.WithNoHashPrefix())
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlobAlreadyExists))
}

// TestLastProcessedMarker_RewriteWithAllowOverwrite_Succeeds is the positive
// counterpart: with WithAllowOverwrite(true) - the fix applied to the marker
// write at the end of processUTXOs - forcing over an existing
// lastProcessed.dat must succeed, so the marker write can never itself be the
// reason a forced recovery fails.
func TestLastProcessedMarker_RewriteWithAllowOverwrite_Succeeds(t *testing.T) {
	ctx := context.Background()
	blockStore := newTestBlobStore(t)

	markLastProcessed(t, ctx, blockStore)

	err := blockStore.Set(ctx, nil, fileformat.FileTypeDat, []byte("2\n"),
		bloboptions.WithFilename("lastProcessed"), bloboptions.WithNoHashPrefix(), bloboptions.WithAllowOverwrite(true))
	require.NoError(t, err)
}

// TestCheckSkipUTXOImport_MarkerWithoutCheckpoint_NamesWorkingRecovery
// guards the error message operators actually see: it must not promise a
// recovery ("-force to re-import the UTXO set") that cannot complete. The
// real recovery -force now performs is a cheap checkpoint recovery, not a
// re-import.
func TestCheckSkipUTXOImport_MarkerWithoutCheckpoint_NamesWorkingRecovery(t *testing.T) {
	ctx := context.Background()
	blockchainStore := newTestBlockchainStore(t)
	blockStore := newTestBlobStore(t)

	markLastProcessed(t, ctx, blockStore)

	_, err := checkSkipUTXOImport(ctx, blockStore, blockchainStore)
	require.Error(t, err)
	require.ErrorContains(t, err, "-force")
	require.NotContains(t, err.Error(), "re-import the UTXO set",
		"the message must not promise a re-import as the recovery - the actual recovery never re-imports the UTXO set")
}
