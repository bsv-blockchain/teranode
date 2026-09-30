package seeder

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// dummyBlockIndexBytes serialises a structurally-valid, but otherwise
// meaningless, BlockIndex record at the given height. Used to build
// synthetic .utxo-headers files without needing a real chain-linked block:
// the truncation tests below only need the *earlier* record(s) to parse
// successfully and the *last* record to be cut short - the content of a
// fully-written record is otherwise irrelevant to what's under test.
func dummyBlockIndexBytes(t *testing.T, height uint32) []byte {
	t.Helper()

	bi := &utxopersister.BlockIndex{
		Hash:    &chainhash.Hash{},
		Height:  height,
		TxCount: 1,
		BlockHeader: &model.BlockHeader{
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
		},
	}

	var buf bytes.Buffer
	require.NoError(t, bi.Serialise(&buf))

	return buf.Bytes()
}

// writeUtxoHeadersFile assembles a .utxo-headers file byte for byte in the
// layout WriteHeadersToStore writes (services/utxopersister/UTXOSet.go): file
// header, tip hash (32 bytes), tip height (4 bytes), then the given raw
// per-block record bytes concatenated verbatim - a caller can pass a
// deliberately truncated final record to simulate a cut-off snapshot.
func writeUtxoHeadersFile(t *testing.T, tipHeight uint32, records ...[]byte) string {
	t.Helper()

	header := fileformat.NewHeader(fileformat.FileTypeUtxoHeaders)

	data := make([]byte, 0)
	data = append(data, header.Bytes()...)

	var tipHash chainhash.Hash
	tipHash[0] = 0xee
	data = append(data, tipHash[:]...)

	heightBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(heightBytes, tipHeight)
	data = append(data, heightBytes...)

	for _, r := range records {
		data = append(data, r...)
	}

	path := filepath.Join(t.TempDir(), "truncated.utxo-headers")
	require.NoError(t, os.WriteFile(path, data, 0o600))

	return path
}

// TestProcessHeaders_TruncatedFile_ReturnsError is the regression test for the
// silent-truncation bug in processHeaders's utxo-headers read loop: cutting a
// file off mid-record - here, 10 bytes into the second block's 32-byte hash
// field - must fail the import outright rather than being misread as a clean
// end-of-records boundary. NewUTXOHeaderFromReader wraps the resulting short
// read (io.ErrUnexpectedEOF) into a *errors.Error whose rendered message
// contains "unexpected EOF"; the old `errors.Is(err, io.EOF)` check matched it
// via this package's substring-matching Is() fallback (io.EOF's message "EOF"
// is a substring of "unexpected EOF"), silently treating the truncated
// snapshot as a complete, successful import of just the genesis header.
func TestProcessHeaders_TruncatedFile_ReturnsError(t *testing.T) {
	genesisRecord := dummyBlockIndexBytes(t, 0)
	secondRecord := dummyBlockIndexBytes(t, 1)

	// The header file claims a tip height of 1 (2 records: heights 0 and 1),
	// but only the genesis record is written in full - the second record is
	// cut off 10 bytes into its 32-byte hash field.
	path := writeUtxoHeadersFile(t, 1, genesisRecord, secondRecord[:10])

	store := newTestBlockchainStore(t)

	err := processHeaders(context.Background(), ulogger.TestLogger{}, store, path)
	require.ErrorContains(t, err, "truncated",
		"the record-count validation must be what rejects the file")
}

// TestProcessHeaders_BoundaryAlignedCut_ReturnsError covers the case a
// mid-record cut cannot: a file cut exactly on a record boundary produces a
// genuinely clean io.EOF, indistinguishable from a complete file by any
// error-inspection fix. Only the record-count check catches it.
func TestProcessHeaders_BoundaryAlignedCut_ReturnsError(t *testing.T) {
	genesisRecord := dummyBlockIndexBytes(t, 0)

	// The header claims a tip height of 1 (2 records: heights 0 and 1), but
	// only the genesis record is present - cut cleanly on a record boundary
	// rather than mid-record.
	path := writeUtxoHeadersFile(t, 1, genesisRecord)

	store := newTestBlockchainStore(t)

	err := processHeaders(context.Background(), ulogger.TestLogger{}, store, path)
	require.ErrorContains(t, err, "truncated",
		"a boundary-aligned cut yields a clean io.EOF and must be caught by the record-count check")
}

// TestProcessHeaders_V1File_FinishesCleanly guards the V1 (no-coinbase) path
// against the new record-count validation: processHeaders still supports V1
// utxo-headers files, and the invariant applies identically to both formats.
func TestProcessHeaders_V1File_FinishesCleanly(t *testing.T) {
	ctx := context.Background()
	store := newTestBlockchainStore(t)

	genesis, err := store.GetBlockByID(ctx, 0)
	require.NoError(t, err)

	genesisRecord, err := blockToIndexBytesV1(t, genesis, genesis.Header.Hash())
	require.NoError(t, err)

	coinbase := makeCoinbaseTx(t, 1, 1)
	root := coinbase.TxIDChainHash()

	block1 := &model.Block{
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
	}
	block1Hash := block1.Header.Hash()

	block1Record, err := blockToIndexBytesV1(t, block1, block1Hash)
	require.NoError(t, err)

	path := writeV1UtxoHeadersFile(t, 1, genesisRecord, block1Record)

	require.NoError(t, processHeaders(ctx, ulogger.TestLogger{}, store, path))

	stored, err := store.GetBlockByID(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, block1Hash.String(), stored.Header.Hash().String())
}

// TestLoadCoinbaseTxs_TruncatedFile_ReturnsError is the same regression, for
// loadCoinbaseTxs's independent utxo-headers read loop.
func TestLoadCoinbaseTxs_TruncatedFile_ReturnsError(t *testing.T) {
	firstRecord := dummyBlockIndexBytes(t, 0)
	secondRecord := dummyBlockIndexBytes(t, 1)

	path := writeUtxoHeadersFile(t, 1, firstRecord, secondRecord[:10])

	_, err := loadCoinbaseTxs(ulogger.TestLogger{}, path)
	require.ErrorContains(t, err, "truncated",
		"the record-count validation must be what rejects the file")
}

// TestLoadCoinbaseTxs_TruncatedFile_ReturnsPartialMap is the regression test
// for the caller-visible fallout of the truncation fix: loadCoinbaseTxs must
// still hand back whatever coinbases it read before hitting the truncation,
// not discard them. Before this fix, the function returned (nil, err) on
// truncation, and the sole caller (processUTXOs) replaced that with a
// completely empty map on any error - so a headers file truncated after the
// first several blocks lost every coinbase input, not just the ones past the
// cut. Returning the partial map alongside the error preserves the
// pre-truncation-fix best-effort behaviour (use whatever was read) while
// still surfacing the truncation via a non-nil error.
func TestLoadCoinbaseTxs_TruncatedFile_ReturnsPartialMap(t *testing.T) {
	store := newTestBlockchainStore(t)
	ctx := context.Background()

	genesis, err := store.GetBlockByID(ctx, 0)
	require.NoError(t, err)

	genesisRecord, err := blockToIndexBytes(t, genesis, genesis.Header.Hash())
	require.NoError(t, err)

	coinbase1 := makeCoinbaseTx(t, 1, 1)
	block1 := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1700000001,
			Nonce:          1,
			HashPrevBlock:  genesis.Header.Hash(),
			HashMerkleRoot: coinbase1.TxIDChainHash(),
			Bits:           genesis.Header.Bits,
		},
		CoinbaseTx:       coinbase1,
		TransactionCount: 1,
		Height:           1,
	}
	block1Record, err := blockToIndexBytes(t, block1, block1.Header.Hash())
	require.NoError(t, err)

	coinbase2 := makeCoinbaseTx(t, 2, 2)
	block2 := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1700000002,
			Nonce:          2,
			HashPrevBlock:  block1.Header.Hash(),
			HashMerkleRoot: coinbase2.TxIDChainHash(),
			Bits:           genesis.Header.Bits,
		},
		CoinbaseTx:       coinbase2,
		TransactionCount: 1,
		Height:           2,
	}
	block2Record, err := blockToIndexBytes(t, block2, block2.Header.Hash())
	require.NoError(t, err)

	// The file claims a tip height of 2 (3 records: heights 0, 1, 2), but
	// block2's record is cut off 10 bytes into its hash field - block1's
	// coinbase was fully read before the truncation hits.
	path := writeUtxoHeadersFile(t, 2, genesisRecord, block1Record, block2Record[:10])

	coinbaseTxs, err := loadCoinbaseTxs(ulogger.TestLogger{}, path)
	require.Error(t, err, "a truncated utxo-headers file must still be reported as an error")
	require.NotNil(t, coinbaseTxs, "the partial map read before truncation must not be discarded")

	got, ok := coinbaseTxs[*coinbase1.TxIDChainHash()]
	require.True(t, ok, "block1's coinbase was fully read before the truncation and must survive in the partial map")
	require.Equal(t, coinbase1.TxIDChainHash().String(), got.TxIDChainHash().String())

	_, ok = coinbaseTxs[*coinbase2.TxIDChainHash()]
	require.False(t, ok, "block2's coinbase was never fully read and must not appear")
}

// TestLoadCoinbaseTxs_UndecodableRecord_ReturnsPartialMap is the ChiR2
// regression test for the decode/IO-error exit distinct from truncation: a
// final record that is read in full (unlike the mid-record cuts above, which
// hit io.ErrUnexpectedEOF and are caught by the record-count check) but whose
// coinbase bytes fail to decode as a transaction. Before the fix this path
// discarded every coinbase read so far; it must now hand back the partial map,
// just like the count-mismatch path does.
func TestLoadCoinbaseTxs_UndecodableRecord_ReturnsPartialMap(t *testing.T) {
	store := newTestBlockchainStore(t)
	ctx := context.Background()

	genesis, err := store.GetBlockByID(ctx, 0)
	require.NoError(t, err)

	genesisRecord, err := blockToIndexBytes(t, genesis, genesis.Header.Hash())
	require.NoError(t, err)

	coinbase1 := makeCoinbaseTx(t, 1, 1)
	block1 := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1700000001,
			Nonce:          1,
			HashPrevBlock:  genesis.Header.Hash(),
			HashMerkleRoot: coinbase1.TxIDChainHash(),
			Bits:           genesis.Header.Bits,
		},
		CoinbaseTx:       coinbase1,
		TransactionCount: 1,
		Height:           1,
	}
	block1Record, err := blockToIndexBytes(t, block1, block1.Header.Hash())
	require.NoError(t, err)

	// A fully-present final record: 32 (hash) + 4 (height) + 8 (txCount) + 80
	// (block header) bytes, followed by a coinbase length of 4 and four bytes
	// that are not a decodable transaction. This reaches the non-EOF error
	// exit in the read loop, unlike the mid-record cuts used by the truncation
	// tests above.
	badRecord := append([]byte{}, block1Record[:124]...)
	badRecord = append(badRecord, 0x04, 0x00, 0x00, 0x00, 0xde, 0xad, 0xbe, 0xef)

	path := writeUtxoHeadersFile(t, 2, genesisRecord, block1Record, badRecord)

	coinbaseTxs, err := loadCoinbaseTxs(ulogger.TestLogger{}, path)
	require.Error(t, err, "an undecodable record must still be reported as an error")
	require.NotNil(t, coinbaseTxs, "the partial map read before the undecodable record must not be discarded")

	got, ok := coinbaseTxs[*coinbase1.TxIDChainHash()]
	require.True(t, ok, "block1's coinbase was fully read before the undecodable record and must survive in the partial map")
	require.Equal(t, coinbase1.TxIDChainHash().String(), got.TxIDChainHash().String())
}

// TestProcessHeaders_CompleteFile_FinishesCleanly is the positive-path
// counterpart to the truncation regression test above: a well-formed,
// complete utxo-headers file (tip height matches the number of records
// actually written) must import cleanly, with every non-genesis header
// stored - guarding against the new record-count validation rejecting a
// legitimate, complete snapshot.
func TestProcessHeaders_CompleteFile_FinishesCleanly(t *testing.T) {
	ctx := context.Background()
	store := newTestBlockchainStore(t)

	genesis, err := store.GetBlockByID(ctx, 0)
	require.NoError(t, err)

	genesisRecord, err := blockToIndexBytes(t, genesis, genesis.Header.Hash())
	require.NoError(t, err)

	coinbase := makeCoinbaseTx(t, 1, 1)
	root := coinbase.TxIDChainHash()

	block1 := &model.Block{
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
	}
	block1Hash := block1.Header.Hash()

	block1Record, err := blockToIndexBytes(t, block1, block1Hash)
	require.NoError(t, err)

	path := writeUtxoHeadersFile(t, 1, genesisRecord, block1Record)

	require.NoError(t, processHeaders(ctx, ulogger.TestLogger{}, store, path))

	stored, err := store.GetBlockByID(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, block1Hash.String(), stored.Header.Hash().String())
}

// blockToIndexBytes serialises a model.Block into the BlockIndex wire format
// processHeaders/loadCoinbaseTxs read back, mirroring what WriteHeadersToStore
// produces for a real block.
func blockToIndexBytes(t *testing.T, block *model.Block, hash *chainhash.Hash) ([]byte, error) {
	t.Helper()

	bi := &utxopersister.BlockIndex{
		Hash:        hash,
		Height:      block.Height,
		TxCount:     block.TransactionCount,
		BlockHeader: block.Header,
		CoinbaseTx:  block.CoinbaseTx,
	}

	var buf bytes.Buffer
	if err := bi.Serialise(&buf); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// blockToIndexBytesV1 serialises a model.Block into the legacy V1 BlockIndex
// wire format: hash, height, tx count and block header only - no coinbase
// length/data fields at all. NewUTXOHeaderFromReader(reader, isV1=true) never
// reads a coinbase field, so a V1 record carries none, unlike
// BlockIndex.Serialise (which always writes a V2-shaped record).
func blockToIndexBytesV1(t *testing.T, block *model.Block, hash *chainhash.Hash) ([]byte, error) {
	t.Helper()

	var buf bytes.Buffer

	if _, err := buf.Write(hash[:]); err != nil {
		return nil, err
	}

	var heightBytes [4]byte
	binary.LittleEndian.PutUint32(heightBytes[:], block.Height)

	if _, err := buf.Write(heightBytes[:]); err != nil {
		return nil, err
	}

	var txCountBytes [8]byte
	binary.LittleEndian.PutUint64(txCountBytes[:], block.TransactionCount)

	if _, err := buf.Write(txCountBytes[:]); err != nil {
		return nil, err
	}

	if err := block.Header.ToWireBlockHeader().Serialize(&buf); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// writeV1UtxoHeadersFile is writeUtxoHeadersFile's V1 counterpart: it emits
// the legacy "U-H-1.0 " magic instead of the V2 default NewHeader produces, so
// the read loop takes the isV1 branch.
func writeV1UtxoHeadersFile(t *testing.T, tipHeight uint32, records ...[]byte) string {
	t.Helper()

	data := make([]byte, 0)
	data = append(data, []byte("U-H-1.0 ")...)

	var tipHash chainhash.Hash
	tipHash[0] = 0xee
	data = append(data, tipHash[:]...)

	heightBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(heightBytes, tipHeight)
	data = append(data, heightBytes...)

	for _, r := range records {
		data = append(data, r...)
	}

	path := filepath.Join(t.TempDir(), "v1.utxo-headers")
	require.NoError(t, os.WriteFile(path, data, 0o600))

	return path
}

// TestLoadCoinbaseTxs_CompleteFile_FinishesCleanly is the positive-path
// counterpart to TestLoadCoinbaseTxs_TruncatedFile_ReturnsError: a complete
// V2 utxo-headers file must still yield its coinbase transactions without
// the new record-count validation rejecting it.
func TestLoadCoinbaseTxs_CompleteFile_FinishesCleanly(t *testing.T) {
	store := newTestBlockchainStore(t)
	ctx := context.Background()

	genesis, err := store.GetBlockByID(ctx, 0)
	require.NoError(t, err)

	genesisRecord, err := blockToIndexBytes(t, genesis, genesis.Header.Hash())
	require.NoError(t, err)

	coinbase := makeCoinbaseTx(t, 1, 1)
	root := coinbase.TxIDChainHash()

	block1 := &model.Block{
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
	}
	block1Hash := block1.Header.Hash()

	block1Record, err := blockToIndexBytes(t, block1, block1Hash)
	require.NoError(t, err)

	path := writeUtxoHeadersFile(t, 1, genesisRecord, block1Record)

	coinbaseTxs, err := loadCoinbaseTxs(ulogger.TestLogger{}, path)
	require.NoError(t, err)

	got, ok := coinbaseTxs[*coinbase.TxIDChainHash()]
	require.True(t, ok, "block1's coinbase must be recovered")
	require.Equal(t, coinbase.TxIDChainHash().String(), got.TxIDChainHash().String())
}
