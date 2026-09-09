// Package utxopersister provides functionality for managing UTXO (Unspent Transaction Output) persistence.
package utxopersister

import (
	"context"
	"encoding/binary"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateUTXOSet_NilLastBlockHash pins the defensive check at the entry
// of CreateUTXOSet: if the consolidator never set lastBlockHash (its loop
// body bailed early on per-block ErrNotFound from a UTXO file
// BlockPersister hasn't written yet, or the range contained only the
// genesis block which the loop skips with `continue`), the previous
// implementation crashed at `c.lastBlockHash[:]` with SIGSEGV. The
// function must surface a clear error instead.
func TestCreateUTXOSet_NilLastBlockHash(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()

	// blockHash here is only used to initialise the UTXOSet handle;
	// the bug is in dereferencing c.lastBlockHash, not us.blockHash.
	someHash := chainhash.HashH([]byte("test-utxoset-blockhash"))
	us, err := GetUTXOSet(ctx, logger, tSettings, blockStore, &someHash)
	require.NoError(t, err)

	// Construct a consolidator with lastBlockHash == nil — exactly the
	// state ConsolidateBlockRange leaves it in when no non-genesis
	// block was successfully processed.
	c := NewConsolidator(logger, tSettings, nil, nil, blockStore, nil)
	require.Nil(t, c.lastBlockHash, "test precondition: consolidator must have nil lastBlockHash")

	err = us.CreateUTXOSet(ctx, c)
	require.Error(t, err, "CreateUTXOSet must reject a consolidator with nil lastBlockHash instead of dereferencing it")
	assert.Contains(t, err.Error(), "lastBlockHash", "error message should name the offending field")
}

// TestCreateUTXOSet_PreviousSetReadDoesNotDoubleReadMagic pins that
// CreateUTXOSet, when reading the previous block's UTXO set, does NOT
// call fileformat.ReadHeader on a reader the store layer has already
// advanced past — and consumes the per-file metadata (current block
// hash + height + previous block hash) before the wrapper loop. Without
// the fix, this path either crashed with "unknown magic: [...]" (when
// the store strips the header, which is the production case) or
// silently misaligned the wrapper reader by 8 bytes and consolidated
// the wrong UTXOs.
//
// Test scenario: a "previous" UTXO set file for hash P is staged in a
// memory store, containing the 68-byte header records (current block
// hash = P, height, parent hash), zero wrappers, and the 16-byte
// zero-count footer every real writer appends — so the OUTER loop hits
// the footer boundary immediately after the metadata and the counts
// agree (0 read, 0 expected). A consolidator pointing at P as the
// firstPreviousBlockHash should consolidate successfully and produce a
// new UTXO set for the current block.
func TestCreateUTXOSet_PreviousSetReadDoesNotDoubleReadMagic(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()

	previousBlockHash := chainhash.HashH([]byte("previous-block-hash-for-double-read-test"))
	currentBlockHash := chainhash.HashH([]byte("current-block-hash-for-double-read-test"))
	grandparentHash := chainhash.HashH([]byte("grandparent-block-hash-for-double-read-test"))

	// Stage the previous UTXO set file with its 68-byte metadata
	// (matching the layout CreateUTXOSet writes: current block hash +
	// 4-byte height + previous block hash) followed by the 16-byte
	// zero-count footer (no wrappers were written). memory.Set prepends
	// the fileformat magic, so we only provide post-header bytes.
	var heightBuf [4]byte
	binary.LittleEndian.PutUint32(heightBuf[:], 42)
	body := make([]byte, 0, len(previousBlockHash)+len(heightBuf)+len(grandparentHash)+FooterSize)
	body = append(body, previousBlockHash[:]...)
	body = append(body, heightBuf[:]...)
	body = append(body, grandparentHash[:]...)
	body = append(body, make([]byte, FooterSize)...) // txCount=0, utxoCount=0
	require.NoError(t, blockStore.Set(ctx, previousBlockHash[:], fileformat.FileTypeUtxoSet, body))

	// Consolidator: firstPreviousBlockHash = P drives the read of the
	// staged file; lastBlockHash/height/previousBlockHash drive the
	// write of the new file CreateUTXOSet produces.
	c := NewConsolidator(logger, tSettings, nil, nil, blockStore, &previousBlockHash)
	c.lastBlockHash = &currentBlockHash
	c.lastBlockHeight = 43
	c.previousBlockHash = &previousBlockHash

	us, err := GetUTXOSet(ctx, logger, tSettings, blockStore, &currentBlockHash)
	require.NoError(t, err)

	err = us.CreateUTXOSet(ctx, c)
	require.NoError(t, err, "CreateUTXOSet must succeed against a valid previous UTXO set; double-read of the fileformat magic would surface here as \"unknown magic: [...]\" or as misaligned wrapper reads")
	if err != nil {
		require.NotContains(t, err.Error(), "unknown magic")
	}
}

// TestCreateUTXOSet_PreviousSetWrongBlockHash pins that the post-fix
// metadata validation rejects a previous UTXO set file whose stored
// current-block-hash doesn't match what the consolidator expected to
// open. Catches file/key confusion loudly rather than silently
// consolidating UTXOs from the wrong ancestor.
func TestCreateUTXOSet_PreviousSetWrongBlockHash(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()

	previousBlockHash := chainhash.HashH([]byte("previous-block-mismatch-key"))
	wrongStoredHash := chainhash.HashH([]byte("wrong-stored-current-hash"))
	currentBlockHash := chainhash.HashH([]byte("current-block-mismatch-key"))
	grandparentHash := chainhash.HashH([]byte("grandparent-block-mismatch-key"))

	// File stored under key=previousBlockHash but whose stored
	// "current block hash" metadata is something else — simulates
	// corruption or a mis-keyed file.
	var heightBuf [4]byte
	binary.LittleEndian.PutUint32(heightBuf[:], 42)
	body := make([]byte, 0, len(wrongStoredHash)+len(heightBuf)+len(grandparentHash))
	body = append(body, wrongStoredHash[:]...)
	body = append(body, heightBuf[:]...)
	body = append(body, grandparentHash[:]...)
	require.NoError(t, blockStore.Set(ctx, previousBlockHash[:], fileformat.FileTypeUtxoSet, body))

	c := NewConsolidator(logger, tSettings, nil, nil, blockStore, &previousBlockHash)
	c.lastBlockHash = &currentBlockHash
	c.lastBlockHeight = 43
	c.previousBlockHash = &previousBlockHash

	us, err := GetUTXOSet(ctx, logger, tSettings, blockStore, &currentBlockHash)
	require.NoError(t, err)

	err = us.CreateUTXOSet(ctx, c)
	require.Error(t, err, "CreateUTXOSet must reject a previous UTXO set whose stored block hash doesn't match the expected ancestor")
	assert.Contains(t, err.Error(), "block hash mismatch")
}

// TestCreateUTXOSet_PreviousSetWithFooterTerminatesCleanly pins that the
// previous-set consolidation read stops at the 16-byte footer (txCount +
// utxoCount) that CreateUTXOSet writes after the final UTXOWrapper, instead
// of reading those 16 bytes as the start of another 32-byte txid and
// crashing the UTXOPersister service with "failed to read txid, expected
// 32 bytes got 16 -> unexpected EOF".
//
// The pre-fix OUTER loop only broke on a clean io.EOF, so it survived
// TestCreateUTXOSet_PreviousSetReadDoesNotDoubleReadMagic (which stages
// zero wrappers and no footer) but crashed on any real previous set. Every
// file CreateUTXOSet writes carries the footer, so this is the realistic
// shape: 68-byte header + one wrapper + 16-byte footer.
func TestCreateUTXOSet_PreviousSetWithFooterTerminatesCleanly(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()

	previousBlockHash := chainhash.HashH([]byte("previous-block-hash-for-footer-test"))
	currentBlockHash := chainhash.HashH([]byte("current-block-hash-for-footer-test"))
	grandparentHash := chainhash.HashH([]byte("grandparent-block-hash-for-footer-test"))
	wrapperTxID := chainhash.HashH([]byte("wrapper-txid-for-footer-test"))

	// One real UTXOWrapper, serialized exactly as CreateUTXOSet writes it.
	wrapper := &UTXOWrapper{
		TxID:   wrapperTxID,
		Height: 42,
		UTXOs:  []*UTXO{{Index: 0, Value: 1000, Script: []byte{0x76, 0xa9, 0x88, 0xac}}},
	}
	wrapperBytes := wrapper.Bytes()

	// The 16-byte footer CreateUTXOSet appends after the last wrapper:
	// txCount then utxoCount, both little-endian uint64.
	var footer [16]byte
	binary.LittleEndian.PutUint64(footer[0:8], 1)
	binary.LittleEndian.PutUint64(footer[8:16], 1)

	var heightBuf [4]byte
	binary.LittleEndian.PutUint32(heightBuf[:], 42)

	// Layout matches CreateUTXOSet's writer (post-magic): current block hash
	// (== the key we open under) + height + previous hash, then the
	// wrappers, then the footer. memory.Set prepends the fileformat magic.
	body := make([]byte, 0, len(previousBlockHash)+len(heightBuf)+len(grandparentHash)+len(wrapperBytes)+len(footer))
	body = append(body, previousBlockHash[:]...)
	body = append(body, heightBuf[:]...)
	body = append(body, grandparentHash[:]...)
	body = append(body, wrapperBytes...)
	body = append(body, footer[:]...)
	require.NoError(t, blockStore.Set(ctx, previousBlockHash[:], fileformat.FileTypeUtxoSet, body))

	c := NewConsolidator(logger, tSettings, nil, nil, blockStore, &previousBlockHash)
	c.lastBlockHash = &currentBlockHash
	c.lastBlockHeight = 43
	c.previousBlockHash = &previousBlockHash

	us, err := GetUTXOSet(ctx, logger, tSettings, blockStore, &currentBlockHash)
	require.NoError(t, err)

	err = us.CreateUTXOSet(ctx, c)
	require.NoError(t, err, "CreateUTXOSet must terminate the previous-set read at the 16-byte footer; the pre-fix loop crashed here with \"failed to read txid, expected 32 bytes got 16\"")
}

// TestCreateUTXOSet_PreviousSetTruncatedMidTxID_ReturnsError is the
// regression test for the silent-truncation-laundering bug: a previous UTXO
// set cut off partway through a second record's 32-byte TxID - with no
// footer at all - must fail the consolidation loudly instead of being
// treated as a clean end of the record stream and used to write a new,
// self-consistent but truncated snapshot.
func TestCreateUTXOSet_PreviousSetTruncatedMidTxID_ReturnsError(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()

	previousBlockHash := chainhash.HashH([]byte("previous-block-hash-for-midtxid-test"))
	currentBlockHash := chainhash.HashH([]byte("current-block-hash-for-midtxid-test"))
	grandparentHash := chainhash.HashH([]byte("grandparent-block-hash-for-midtxid-test"))

	wrapper := &UTXOWrapper{
		TxID:   chainhash.HashH([]byte("wrapper-a-for-midtxid-test")),
		Height: 42,
		UTXOs:  []*UTXO{{Index: 0, Value: 1000, Script: []byte{0x76, 0xa9, 0x88, 0xac}}},
	}

	secondTxID := chainhash.HashH([]byte("wrapper-b-for-midtxid-test"))

	var heightBuf [4]byte
	binary.LittleEndian.PutUint32(heightBuf[:], 42)

	body := make([]byte, 0, len(previousBlockHash)+len(heightBuf)+len(grandparentHash)+len(wrapper.Bytes())+10)
	body = append(body, previousBlockHash[:]...)
	body = append(body, heightBuf[:]...)
	body = append(body, grandparentHash[:]...)
	body = append(body, wrapper.Bytes()...)
	// Cut 10 bytes into the second record's 32-byte TxID - a genuine
	// mid-record truncation, with no footer appended at all.
	body = append(body, secondTxID[:10]...)
	require.NoError(t, blockStore.Set(ctx, previousBlockHash[:], fileformat.FileTypeUtxoSet, body))

	c := NewConsolidator(logger, tSettings, nil, nil, blockStore, &previousBlockHash)
	c.lastBlockHash = &currentBlockHash
	c.lastBlockHeight = 43
	c.previousBlockHash = &previousBlockHash

	us, err := GetUTXOSet(ctx, logger, tSettings, blockStore, &currentBlockHash)
	require.NoError(t, err)

	err = us.CreateUTXOSet(ctx, c)
	require.Error(t, err, "a previous UTXO set truncated mid-TxID must not be silently accepted as a complete record stream")
}

// TestCreateUTXOSet_PreviousSetMissingFooter_ReturnsError covers the second
// traced laundering path: a previous UTXO set cut exactly after a complete
// UTXOWrapper record, with the trailing 16-byte footer missing entirely. The
// read of the next record's TxID then sees a clean io.EOF (zero bytes
// available), not a short read. Every writer in this package appends the
// footer unconditionally, so this shape can only happen from truncation and
// must always be rejected - it must not be confused with a legitimate,
// footer-terminated end of the stream.
func TestCreateUTXOSet_PreviousSetMissingFooter_ReturnsError(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()

	previousBlockHash := chainhash.HashH([]byte("previous-block-hash-for-missing-footer-test"))
	currentBlockHash := chainhash.HashH([]byte("current-block-hash-for-missing-footer-test"))
	grandparentHash := chainhash.HashH([]byte("grandparent-block-hash-for-missing-footer-test"))

	wrapper := &UTXOWrapper{
		TxID:   chainhash.HashH([]byte("wrapper-a-for-missing-footer-test")),
		Height: 42,
		UTXOs:  []*UTXO{{Index: 0, Value: 1000, Script: []byte{0x76, 0xa9, 0x88, 0xac}}},
	}

	var heightBuf [4]byte
	binary.LittleEndian.PutUint32(heightBuf[:], 42)

	body := make([]byte, 0, len(previousBlockHash)+len(heightBuf)+len(grandparentHash)+len(wrapper.Bytes()))
	body = append(body, previousBlockHash[:]...)
	body = append(body, heightBuf[:]...)
	body = append(body, grandparentHash[:]...)
	body = append(body, wrapper.Bytes()...)
	// No footer at all: the file ends exactly on a wrapper boundary.
	require.NoError(t, blockStore.Set(ctx, previousBlockHash[:], fileformat.FileTypeUtxoSet, body))

	c := NewConsolidator(logger, tSettings, nil, nil, blockStore, &previousBlockHash)
	c.lastBlockHash = &currentBlockHash
	c.lastBlockHeight = 43
	c.previousBlockHash = &previousBlockHash

	us, err := GetUTXOSet(ctx, logger, tSettings, blockStore, &currentBlockHash)
	require.NoError(t, err)

	err = us.CreateUTXOSet(ctx, c)
	require.Error(t, err, "a previous UTXO set with a missing footer must not be silently accepted as complete")
}

// TestCreateUTXOSet_PreviousSetFooterMismatch_ReturnsError pins the counts
// check itself: a footer that decodes cleanly but whose counts don't match
// what the loop actually read must still be rejected, not just a footer that
// is structurally malformed or absent.
func TestCreateUTXOSet_PreviousSetFooterMismatch_ReturnsError(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()

	previousBlockHash := chainhash.HashH([]byte("previous-block-hash-for-footer-mismatch-test"))
	currentBlockHash := chainhash.HashH([]byte("current-block-hash-for-footer-mismatch-test"))
	grandparentHash := chainhash.HashH([]byte("grandparent-block-hash-for-footer-mismatch-test"))

	wrapper := &UTXOWrapper{
		TxID:   chainhash.HashH([]byte("wrapper-a-for-footer-mismatch-test")),
		Height: 42,
		UTXOs:  []*UTXO{{Index: 0, Value: 1000, Script: []byte{0x76, 0xa9, 0x88, 0xac}}},
	}

	var heightBuf [4]byte
	binary.LittleEndian.PutUint32(heightBuf[:], 42)

	// Footer claims 2 transactions/2 utxos, but only 1 wrapper (1 utxo) is
	// actually present in the body.
	var footer [16]byte
	binary.LittleEndian.PutUint64(footer[0:8], 2)
	binary.LittleEndian.PutUint64(footer[8:16], 2)

	body := make([]byte, 0, len(previousBlockHash)+len(heightBuf)+len(grandparentHash)+len(wrapper.Bytes())+len(footer))
	body = append(body, previousBlockHash[:]...)
	body = append(body, heightBuf[:]...)
	body = append(body, grandparentHash[:]...)
	body = append(body, wrapper.Bytes()...)
	body = append(body, footer[:]...)
	require.NoError(t, blockStore.Set(ctx, previousBlockHash[:], fileformat.FileTypeUtxoSet, body))

	c := NewConsolidator(logger, tSettings, nil, nil, blockStore, &previousBlockHash)
	c.lastBlockHash = &currentBlockHash
	c.lastBlockHeight = 43
	c.previousBlockHash = &previousBlockHash

	us, err := GetUTXOSet(ctx, logger, tSettings, blockStore, &currentBlockHash)
	require.NoError(t, err)

	err = us.CreateUTXOSet(ctx, c)
	require.Error(t, err, "a footer whose counts disagree with what was actually read must be rejected")
}

// TestCreateUTXOSet_PreviousSetWithDeletions_NotReportedAsTruncated is the
// regression test for the survivor/read miscount bug: txCount/utxoCount only
// increment for wrappers that survive deletion filtering (they exist to
// write the *new* file's own footer), but the previous-set footer check
// compared them directly against the previous file's footer, which records
// records actually written there. Any real consolidation that spends a
// pre-existing UTXO makes survivors < previous total, and the pre-fix check
// fired a spurious "previous utxo-set ... is truncated" on an intact file.
// The previous set here has two wrappers/two UTXOs; one output is spent
// (present in c.deletions), so only one wrapper survives filtering, but the
// file is not truncated and CreateUTXOSet must succeed.
func TestCreateUTXOSet_PreviousSetWithDeletions_NotReportedAsTruncated(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()

	previousBlockHash := chainhash.HashH([]byte("previous-block-hash-for-deletions-test"))
	currentBlockHash := chainhash.HashH([]byte("current-block-hash-for-deletions-test"))
	grandparentHash := chainhash.HashH([]byte("grandparent-block-hash-for-deletions-test"))

	spentTxID := chainhash.HashH([]byte("wrapper-spent-for-deletions-test"))
	keptTxID := chainhash.HashH([]byte("wrapper-kept-for-deletions-test"))

	spentWrapper := &UTXOWrapper{
		TxID:   spentTxID,
		Height: 42,
		UTXOs:  []*UTXO{{Index: 0, Value: 1000, Script: []byte{0x76, 0xa9, 0x88, 0xac}}},
	}
	keptWrapper := &UTXOWrapper{
		TxID:   keptTxID,
		Height: 42,
		UTXOs:  []*UTXO{{Index: 0, Value: 2000, Script: []byte{0x76, 0xa9, 0x88, 0xac}}},
	}

	// Footer records the previous file's total: 2 transactions/2 utxos.
	var footer [16]byte
	binary.LittleEndian.PutUint64(footer[0:8], 2)
	binary.LittleEndian.PutUint64(footer[8:16], 2)

	var heightBuf [4]byte
	binary.LittleEndian.PutUint32(heightBuf[:], 42)

	body := make([]byte, 0, len(previousBlockHash)+len(heightBuf)+len(grandparentHash)+
		len(spentWrapper.Bytes())+len(keptWrapper.Bytes())+len(footer))
	body = append(body, previousBlockHash[:]...)
	body = append(body, heightBuf[:]...)
	body = append(body, grandparentHash[:]...)
	body = append(body, spentWrapper.Bytes()...)
	body = append(body, keptWrapper.Bytes()...)
	body = append(body, footer[:]...)
	require.NoError(t, blockStore.Set(ctx, previousBlockHash[:], fileformat.FileTypeUtxoSet, body))

	c := NewConsolidator(logger, tSettings, nil, nil, blockStore, &previousBlockHash)
	c.lastBlockHash = &currentBlockHash
	c.lastBlockHeight = 43
	c.previousBlockHash = &previousBlockHash
	// spentWrapper's single output was spent within the consolidated range.
	c.deletions[UTXODeletion{TxID: spentTxID, Index: 0}] = struct{}{}

	us, err := GetUTXOSet(ctx, logger, tSettings, blockStore, &currentBlockHash)
	require.NoError(t, err)

	err = us.CreateUTXOSet(ctx, c)
	require.NoError(t, err, "a previous set with spent outputs must not be reported as truncated: survivor counts are expected to be lower than the footer's total record counts")
}

var (
	hash1 = chainhash.HashH([]byte{0x00, 0x01, 0x02, 0x03, 0x04})
	// hash2 = chainhash.HashH([]byte{0x05, 0x06, 0x07, 0x08, 0x09})
	// ctx   = context.Background()

	// TX has 1 input and 10 outputs
	txid  = "9797ceee1543d53db03f5cedc877f638119cddb6f2f469af70504d1e1ccecebd"
	tx, _ = bt.NewTxFromString("010000000000000000ef01c0f6beed3f280acac9e3268b3a4b6cecac6160f84f750fdd2f8eac06284d960a000000006a47304402206b2782cc5b4a1d68d34f36df0241964bbc23eca0d2d8d698407429541993b063022016954b628894df8f6295097403148c3d7ae84097b538ab3c46cba2727f6deafd4121030ca32438b798eda7d8a818f108340a85bf77fefe24850979ac5dd7e15000ee1affffffff80746802000000001976a914f13bf914962276da063784e9e8b7ecbd59b20bf888ac0a002d3101000000001976a914954dede73fba730977b8630e3f7c93024b33795f88ac404b4c00000000001976a914e429e73ad33123c1a7248f660a162f0098fb819988ac80841e00000000001976a914df7974fdbb7890e0a608f923ef59112c475c078688ac80841e00000000001976a91422f9476db77bcad3998a9d4f96dbcaa2c9ef507288aca0860100000000001976a9143729fa58808bf6db6bf69e15adc96e0f20c26e6a88ac50c30000000000001976a91417accfc5f92836427c14299c51abbdbaedb791ce88ac204e0000000000001976a91462a4e3fab0ef92f1c130681aa657f8c858b59def88ac10270000000000001976a9149928c96c401b326f93043ce1434680ac502f487b88aca00a0000000000001976a9146ed6d5942deab79b654c1b31b86c3e62a7b5e61c88ac1528ab00000000001976a914239bae4bd2abf49a0a493b962cc0c027936b1b4788ac00000000")
)

func TestPadUTXOs(t *testing.T) {
	utxos := make([]*UTXO, 3)

	utxos[0] = &UTXO{
		Index: uint32(0),
		Value: uint64(0),
	}

	utxos[1] = &UTXO{
		Index: uint32(5),
		Value: uint64(5),
	}

	utxos[2] = &UTXO{
		Index: uint32(11),
		Value: uint64(11),
	}

	padded := PadUTXOsWithNil(utxos)

	assert.Equal(t, 12, len(padded))

	assert.Equal(t, utxos[0], padded[0])
	assert.Nil(t, padded[1])
	assert.Nil(t, padded[2])
	assert.Nil(t, padded[3])
	assert.Nil(t, padded[4])
	assert.Equal(t, utxos[1], padded[5])
	assert.Nil(t, padded[6])
	assert.Nil(t, padded[7])
	assert.Nil(t, padded[8])
	assert.Nil(t, padded[8])
	assert.Nil(t, padded[10])
	assert.Equal(t, utxos[2], padded[11])

	for i, u := range padded {
		if u == nil {
			t.Logf("%d: nil", i)
		} else {
			t.Logf("%d: %d", i, u.Index)
		}
	}
}

func TestNewUTXOSet(t *testing.T) {
	store := memory.New()

	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings(t)

	ud1, err := NewUTXOSet(ctx, ulogger.TestLogger{}, tSettings, store, &hash1, 0)
	require.NoError(t, err)

	ud1.blockHeight = 10

	err = ud1.ProcessTx(tx)
	require.NoError(t, err)

	for i := uint32(0); i < 5; i++ {
		err = ud1.delete(&UTXODeletion{*tx.TxIDChainHash(), i})
		require.NoError(t, err)
	}

	err = ud1.Close()
	require.NoError(t, err)

	checkAdditions(t, ud1)
	checkDeletions(t, ud1)
}

func checkAdditions(t *testing.T, ud *UTXOSet) {
	ctx := context.Background()

	r, err := ud.GetUTXOAdditionsReader(ctx)
	require.NoError(t, err)

	defer r.Close()

	for {
		utxoWrapper, err := NewUTXOWrapperFromReader(context.Background(), r)
		if err != nil {
			assert.ErrorIs(t, err, io.EOF)
			break
		}

		require.NoError(t, err)
		assert.Equal(t, tx.TxIDChainHash().String(), utxoWrapper.TxID.String())
		assert.Equal(t, uint32(10), utxoWrapper.Height)
		assert.False(t, utxoWrapper.Coinbase)

		for i, utxo := range utxoWrapper.UTXOs {
			// nolint:gosec
			assert.Equal(t, uint32(i), utxo.Index)
			assert.Equal(t, tx.Outputs[i].Satoshis, utxo.Value)
			assert.True(t, tx.Outputs[i].LockingScript.EqualsBytes(utxo.Script))
		}
	}
}

func checkDeletions(t *testing.T, ud *UTXOSet) {
	r, err := ud.GetUTXODeletionsReader(context.Background())
	require.NoError(t, err)

	defer r.Close()

	// _, err = fileformat.ReadHeader(r)
	// // require.NoError(t, err)

	// Read the deletion caused by the processTX of tx
	_, err = NewUTXODeletionFromReader(r)
	require.NoError(t, err)

	// assert.Equal(t, tx.Inputs[0].PreviousTxID(), utxoDeletion.TxID)

	for i := 0; i < 5; i++ {
		utxoDeletion, err := NewUTXODeletionFromReader(r)
		if err != nil {
			assert.ErrorIs(t, err, io.EOF)
			break
		}

		require.NoError(t, err)
		assert.Equal(t, tx.TxIDChainHash().String(), utxoDeletion.TxID.String())
		// nolint:gosec
		assert.Equal(t, uint32(i), utxoDeletion.Index)
	}
}

// readErrCloser is a test double that errors a configurable number of bytes
// into the Read, then surfaces a sticky error on subsequent Reads. It records
// whether Close was called so a test can assert the contract that consumers
// release the file-store read permit on every error path.
type readErrCloser struct {
	allowedBytes int
	readBytes    int
	err          error
	closed       bool
}

func (r *readErrCloser) Read(p []byte) (int, error) {
	if r.readBytes >= r.allowedBytes {
		return 0, r.err
	}
	remaining := r.allowedBytes - r.readBytes
	n := len(p)
	if n > remaining {
		n = remaining
	}
	r.readBytes += n
	return n, nil
}

func (r *readErrCloser) Close() error {
	r.closed = true
	return nil
}

// fakeStoreReturningErrCloser is a blob.Store implementation that returns a
// caller-supplied io.ReadCloser from GetIoReader for one fileType (used for
// the test's specific failing read), and delegates everything else to an
// embedded memory store. Embedding memory.Memory means we only have to
// implement what we need to control - the rest is the in-memory default.
type fakeStoreReturningErrCloser struct {
	*memory.Memory
	targetType fileformat.FileType
	reader     *readErrCloser
}

func (s *fakeStoreReturningErrCloser) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	if fileType == s.targetType {
		return s.reader, nil
	}
	return s.Memory.GetIoReader(ctx, key, fileType, opts...)
}

// TestGetUTXOAdditionsReader_ClosesOnReadError pins that GetUTXOAdditionsReader
// Closes the underlying reader when one of its two seek-past-header Reads
// fails. Without Close, the file-store's per-reader semaphore permit is held
// for the lifetime of the process; under sustained load this exhausts the
// pool (default 768) and acquireReadPermit times out at 25s, after which
// every Exists/GetIoReader returns SERVICE_UNAVAILABLE.
func TestGetUTXOAdditionsReader_ClosesOnReadError(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	someHash := chainhash.HashH([]byte("test-additions-reader-close"))
	errReader := &readErrCloser{allowedBytes: 0, err: io.ErrUnexpectedEOF}
	store := &fakeStoreReturningErrCloser{
		Memory:     memory.New(),
		targetType: fileformat.FileTypeUtxoAdditions,
		reader:     errReader,
	}

	us, err := GetUTXOSet(ctx, logger, tSettings, store, &someHash)
	require.NoError(t, err)

	_, err = us.GetUTXOAdditionsReader(ctx)
	require.Error(t, err, "GetUTXOAdditionsReader must surface the read error")
	require.True(t, errReader.closed, "reader must be Closed when GetUTXOAdditionsReader returns an error - otherwise the file-store read permit leaks")
}

// TestGetUTXODeletionsReader_ClosesOnReadError mirrors the above for the
// deletions reader.
func TestGetUTXODeletionsReader_ClosesOnReadError(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	someHash := chainhash.HashH([]byte("test-deletions-reader-close"))
	errReader := &readErrCloser{allowedBytes: 0, err: io.ErrUnexpectedEOF}
	store := &fakeStoreReturningErrCloser{
		Memory:     memory.New(),
		targetType: fileformat.FileTypeUtxoDeletions,
		reader:     errReader,
	}

	us, err := GetUTXOSet(ctx, logger, tSettings, store, &someHash)
	require.NoError(t, err)

	_, err = us.GetUTXODeletionsReader(ctx)
	require.Error(t, err, "GetUTXODeletionsReader must surface the read error")
	require.True(t, errReader.closed, "reader must be Closed when GetUTXODeletionsReader returns an error - otherwise the file-store read permit leaks")
}
