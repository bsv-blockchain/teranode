package netsync

/*
This file used to prove the streaming pipeline is a byte-for-byte replacement
for prepareSubtrees, running the same wire transactions through both and
comparing every stored file. prepareSubtrees itself is gone now — the
below-checkpoint inline route it belonged to was removed along with the
decode-then-convert path — so that comparison test went with it
(TestPipeline_ProducesTheSameFilesAsPrepareSubtrees and its compareSubtreeData
helper). What is left is the pipeline's own internal check: that the
structure bytes it writes deserialise back to the root hash it reported
(TestPipeline_DeserialisesToTheSameSubtree), plus the fixtures other pipeline
tests in this package still rely on (wireBlockWithTxs, btTxFromWireTx,
coinbaseFromBlock, newManagerWithSubtreeStore).
*/

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestPipeline_DeserialisesToTheSameSubtree is a second, weaker check that
// survives a deliberate format change: whatever the pipeline's own structure
// bytes are, reading them back must reproduce the root hash the builder itself
// reported for that subtree.
//
// The original version of this test never touched the pipeline at all: it read
// a file PRODUCTION wrote and checked a root hash PRODUCTION had already
// reported, so neither the builder nor the writer was constructed. Fixed here
// to actually drive the pipeline path; that flaw was in the task brief's
// skeleton, not introduced by the first round of this test.
func TestPipeline_DeserialisesToTheSameSubtree(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()

	const txCount = 20
	const maxItems = 8

	// The manager here is only a source of a real logger and settings for the
	// writer; sm.subtreeStore (an unused throwaway store) is never written to
	// because prepareSubtrees is never called in this test.
	sm := newManagerWithSubtreeStore(t, blobmemory.New(), maxItems)
	block := wireBlockWithTxs(t, txCount, false)

	pipeStore := blobmemory.New()
	writer := newSubtreeWriter(sm.logger, sm.settings, pipeStore, uint32(block.Height()), true) //nolint:gosec // test height is small and non-negative

	seen := txmap.NewSplitSwissMapUint64(uint32(txCount)) //nolint:gosec // test tx count is small

	b, err := newBlockStreamBuilder(txCount, maxItems, coinbaseFromBlock(t, block), writer.Emit(ctx), seen)
	require.NoError(t, err)

	for i, wireTx := range block.Transactions() {
		if i == 0 {
			continue // the coinbase occupies slot zero as a placeholder
		}

		tx, hash := btTxFromWireTx(t, wireTx)
		require.NoError(t, b.AddTx(tx, hash))
	}

	_, pipeHashes, err := b.Finish()
	require.NoError(t, err)
	require.Greater(t, len(pipeHashes), 0, "test case must produce at least one subtree")

	raw, err := pipeStore.Get(ctx, pipeHashes[0][:], fileformat.FileTypeSubtree)
	require.NoError(t, err)

	st, err := subtreepkg.NewSubtreeFromBytes(raw)
	require.NoError(t, err)

	require.Equal(t, pipeHashes[0].String(), st.RootHash().String())
}

// pipelineEquivalenceStoreCounter gives each subtest its own sqlitememory
// instance name, so an accidental store write in one case cannot leak into
// another. Nothing here is expected to touch the store at all: see
// newManagerWithSubtreeStore.
var pipelineEquivalenceStoreCounter atomic.Int64

// newManagerWithSubtreeStore builds a SyncManager the same way
// runInlinePipeline (unified_parity_test.go) does — real settings, a real
// sqlitememory UTXO store, the caller's real in-memory blob store — except
// LegacyUnifiedBelowCheckpoint is forced true, not false.
//
// That flip is the entire point of this file: on the unified route, below the
// hard-coded checkpoint, prepareSubtrees never calls AssignBlockID and never
// creates or spends a UTXO (see legacyUnified's doc comment on
// handle_block.go). So this constructor wires no blockchain mock and no
// validator mock — nothing here is a stub standing in for a real dependency;
// the real dependencies are simply never called on this route, and the
// production code path itself is what skips them, not a narrowed test double.
func newManagerWithSubtreeStore(t *testing.T, subtreeStore blob.Store, maxItems int) *SyncManager {
	t.Helper()

	const checkpointHeight = int32(1000) // block height 500 below sits well under this

	tSettings, params := newOutpointOnlySettings(t, true, true, checkpointHeight)
	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	tSettings.BlockAssembly.MaximumMerkleItemsPerSubtree = maxItems

	dbName := fmt.Sprintf("pipeline_equivalence_%d", pipelineEquivalenceStoreCounter.Add(1))
	u, err := url.Parse("sqlitememory:///" + dbName)
	require.NoError(t, err)
	tSettings.UtxoStore.UtxoStore = u

	ctx := context.Background()
	logger := ulogger.TestLogger{}

	store, err := utxosql.New(ctx, logger, tSettings, u)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	return &SyncManager{
		settings:     tSettings,
		chainParams:  params,
		logger:       logger,
		utxoStore:    store,
		subtreeStore: subtreeStore,
		ctx:          ctx,
	}
}

// wireBlockWithTxs builds a *bsvutil.Block with a coinbase plus txCount-1
// non-coinbase transactions, at a fixed height below the checkpoint
// newManagerWithSubtreeStore configures.
//
// When chained is false, every transaction spends a distinct EXTERNAL
// (never-created) outpoint: extendTransactions's phase 2 decorate is skipped
// entirely below the checkpoint's outpoint-only fast path (handle_block.go:1826),
// so a parent that resolves to nothing is exactly what production itself
// tolerates on this route, not a simplification this test introduces. But it
// also means no transaction's parent is ever IN this block, which silently
// switches off extendFromTxMap's same-block fill-in (phase 1,
// handle_block.go:1846) — a real block routinely has in-block spends and this
// shape never exercises that.
//
// When chained is true, tx[i] (i >= 2) spends tx[i-1]'s output instead, so
// phase 1 actually has same-block parents to fill in. tx[1] still spends an
// external outpoint, since there is no earlier in-block transaction for it.
func wireBlockWithTxs(t *testing.T, txCount int, chained bool) *bsvutil.Block {
	t.Helper()

	const height = int32(500)

	cb := wire.NewMsgTx(1)
	cb.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: chainhash.Hash{}, Index: 0xffffffff},
		SignatureScript:  []byte{byte(height & 0xff), 0x01}, //nolint:gosec // test coinbase script
		Sequence:         0xffffffff,
	})
	cb.AddTxOut(&wire.TxOut{Value: 5000000000, PkScript: []byte{0x76, 0xa9, 0x14}})

	msgBlock := &wire.MsgBlock{
		Header:       wire.BlockHeader{Version: 1, Timestamp: time.Now(), Bits: 0x1d00ffff},
		Transactions: make([]*wire.MsgTx, 0, txCount),
	}
	msgBlock.Transactions = append(msgBlock.Transactions, cb)

	var prevTxHash chainhash.Hash // only used when chained

	for i := 1; i < txCount; i++ {
		tag := []byte{byte(i), byte(i >> 8)} //nolint:gosec // test loop index

		var prevHash chainhash.Hash
		if chained && i > 1 {
			prevHash = prevTxHash // spend the previous non-coinbase tx in THIS block
		} else {
			binary.LittleEndian.PutUint32(prevHash[:4], uint32(i)) //nolint:gosec // test loop index
		}

		tx := wire.NewMsgTx(1)
		tx.AddTxIn(&wire.TxIn{
			PreviousOutPoint: wire.OutPoint{Hash: prevHash, Index: 0},
			SignatureScript:  append([]byte{0x00}, tag...),
			Sequence:         0xffffffff,
		})
		tx.AddTxOut(&wire.TxOut{Value: int64(1000 + i), PkScript: append([]byte{0x76, 0xa9, 0x14}, tag...)})

		msgBlock.Transactions = append(msgBlock.Transactions, tx)
		prevTxHash = tx.TxHash()
	}

	// prepareSubtrees now binds every prepared subtree to the header's declared
	// merkle root (commitment.CheckMerkleRoot, handle_block.go) before it will
	// hand any of them back — the fix this branch's own body-commitment work
	// added, merged in verbatim from upstream. An all-zero MerkleRoot fails that
	// bind unconditionally, so the header needs the real root over the final
	// transaction set for prepareSubtrees to get far enough to be compared
	// against the pipeline at all.
	setBodyMerkleRoot(msgBlock)

	block := bsvutil.NewBlock(msgBlock)
	block.SetHeight(height)

	return block
}

// btTxFromWireTx converts one wire transaction the same way createTxMap does
// (handle_block.go:2074): via WireTxToGoBtTx, then SetTxHash for every
// non-coinbase transaction. Reusing exactly that conversion function on both
// paths is deliberate — the divergence under test is what each path does with
// the converted transaction (how it is packed into subtree files), not how the
// conversion itself works.
func btTxFromWireTx(t *testing.T, wireTx *bsvutil.Tx) (*bt.Tx, *chainhash.Hash) {
	t.Helper()

	hash := wireTx.Hash()

	var buf bytes.Buffer
	require.NoError(t, wireTx.MsgTx().Serialize(&buf))

	tx, err := bt.NewTxFromBytes(buf.Bytes())
	require.NoError(t, err)

	if !tx.IsCoinbase() {
		tx.SetTxHash(hash)
	}

	return tx, hash
}

// coinbaseFromBlock returns the block's coinbase as a *bt.Tx, converted the
// same way createTxMap converts it (hash not memoised, matching production's
// "don't add the coinbase to the txMap" branch).
func coinbaseFromBlock(t *testing.T, block *bsvutil.Block) *bt.Tx {
	t.Helper()

	tx, _ := btTxFromWireTx(t, block.Transactions()[0])

	return tx
}
