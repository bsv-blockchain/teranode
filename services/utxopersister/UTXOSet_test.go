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
// memory store, containing just the 68-byte header records (current
// block hash = P, height, parent hash) and zero wrappers — so the
// OUTER loop hits a clean io.EOF after the metadata. A consolidator
// pointing at P as the firstPreviousBlockHash should consolidate
// successfully and produce a new UTXO set for the current block.
func TestCreateUTXOSet_PreviousSetReadDoesNotDoubleReadMagic(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()

	previousBlockHash := chainhash.HashH([]byte("previous-block-hash-for-double-read-test"))
	currentBlockHash := chainhash.HashH([]byte("current-block-hash-for-double-read-test"))
	grandparentHash := chainhash.HashH([]byte("grandparent-block-hash-for-double-read-test"))

	// Stage the previous UTXO set file with just its 68-byte metadata
	// (matching the layout CreateUTXOSet writes: current block hash +
	// 4-byte height + previous block hash). memory.Set prepends the
	// fileformat magic, so we only provide post-header bytes.
	var heightBuf [4]byte
	binary.LittleEndian.PutUint32(heightBuf[:], 42)
	body := make([]byte, 0, len(previousBlockHash)+len(heightBuf)+len(grandparentHash))
	body = append(body, previousBlockHash[:]...)
	body = append(body, heightBuf[:]...)
	body = append(body, grandparentHash[:]...)
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
