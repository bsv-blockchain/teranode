package teranodecli

import (
	"bytes"
	"context"
	"encoding/hex"
	"flag"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	gosubtree "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	blockchainsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// minedSetAuditHarness is a real sqlitememory blockchain store behind the ordinary
// LocalClient, a sqlitememory UTXO store and an in-memory subtree store.
type minedSetAuditHarness struct {
	t        *testing.T
	ctx      context.Context
	settings *settings.Settings
	chain    *blockchainsql.SQL
	utxo     *utxosql.Store
	subtrees *memory.Memory
	auditor  *minedSetAuditor
	prevHash *chainhash.Hash
	height   byte
	txSalt   byte
}

func newMinedSetAuditHarness(t *testing.T) *minedSetAuditHarness {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	chainStore, err := blockchainsql.New(logger, &url.URL{Scheme: "sqlitememory", Path: "/audit-chain"}, tSettings)
	require.NoError(t, err)

	client, err := blockchain.NewLocalClient(logger, tSettings, chainStore, nil, nil)
	require.NoError(t, err)

	utxoStore, err := utxosql.New(ctx, logger, tSettings, &url.URL{Scheme: "sqlitememory", Path: "/audit-utxo"})
	require.NoError(t, err)

	subtrees := memory.New()

	return &minedSetAuditHarness{
		t:        t,
		ctx:      ctx,
		settings: tSettings,
		chain:    chainStore,
		utxo:     utxoStore,
		subtrees: subtrees,
		auditor:  &minedSetAuditor{chain: client, utxo: utxoStore, subtrees: subtrees},
		prevHash: tSettings.ChainCfgParams.GenesisHash,
	}
}

// auditCoinbase returns a coinbase whose script pushes height, the layout
// stores/blockchain/sql/sql_test.go uses, so StoreBlock's coinbase-height check passes.
func auditCoinbase(t *testing.T, height byte) *bt.Tx {
	t.Helper()

	b, err := hex.DecodeString("01000000010000000000000000000000000000000000000000000000000000000000000000" +
		"ffffffff17030100002f6d312d65752f29c267ffea1adb87f33b398fffffffff" +
		"01ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac" +
		"00000000")
	require.NoError(t, err)

	const scriptStart = 42

	b[scriptStart+1] = height
	b[scriptStart+11] = height ^ 0x5a

	tx, err := bt.NewTxFromBytes(b)
	require.NoError(t, err)

	return tx
}

// auditTx returns a distinct non-coinbase transaction with one extended input.
func (h *minedSetAuditHarness) auditTx() *bt.Tx {
	h.t.Helper()

	h.txSalt++

	tx := bt.NewTx()

	prev := chainhash.Hash{h.txSalt, 0xaa}
	require.NoError(h.t, tx.From(prev.String(), 0, "76a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac", 10_000))

	script, err := bscript.NewFromHexString("76a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac")
	require.NoError(h.t, err)

	tx.AddOutput(&bt.Output{Satoshis: 9_000, LockingScript: script})

	unlocking, err := bscript.NewFromHexString("00")
	require.NoError(h.t, err)

	tx.Inputs[0].UnlockingScript = unlocking

	return tx
}

// storeBlock stores the next block on the main chain with one subtree per entry of
// subtrees, each holding those transactions (the first also the coinbase placeholder),
// and writes the subtree files unless skipFiles. It returns the block and its id.
func (h *minedSetAuditHarness) storeBlock(minedSet, skipFiles bool, subtrees ...[]*bt.Tx) (*model.Block, uint32) {
	h.t.Helper()

	h.height++

	roots := make([]*chainhash.Hash, 0, len(subtrees))
	txCount := uint64(1) // the coinbase

	for i, txs := range subtrees {
		txCount += uint64(len(txs))

		st, err := gosubtree.NewTreeByLeafCount(4)
		require.NoError(h.t, err)

		if i == 0 {
			require.NoError(h.t, st.AddCoinbaseNode())
		}

		for _, tx := range txs {
			require.NoError(h.t, st.AddNode(*tx.TxIDChainHash(), 1, uint64(tx.Size()))) //nolint:gosec // a test tx is small
		}

		if !skipFiles {
			b, err := st.Serialize()
			require.NoError(h.t, err)
			require.NoError(h.t, h.subtrees.Set(h.ctx, st.RootHash()[:], fileformat.FileTypeSubtree, b))
		}

		roots = append(roots, st.RootHash())
	}

	coinbase := auditCoinbase(h.t, h.height)

	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      uint32(1_700_000_000) + uint32(h.height),
			Nonce:          uint32(h.height),
			HashPrevBlock:  h.prevHash,
			HashMerkleRoot: coinbase.TxIDChainHash(),
			Bits:           mustNBit(h.t),
		},
		CoinbaseTx:       coinbase,
		TransactionCount: txCount,
		Subtrees:         roots,
	}

	var opts []blockchainoptions.StoreBlockOption
	if minedSet {
		opts = append(opts, blockchainoptions.WithMinedSet(true))
	}

	id, _, err := h.chain.StoreBlock(h.ctx, block, "peer", opts...)
	require.NoError(h.t, err)

	h.prevHash = block.Hash()

	return block, uint32(id) //nolint:gosec // a test store's ids are small
}

func mustNBit(t *testing.T) model.NBit {
	t.Helper()

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	return *bits
}

// createTagged records tx in the UTXO store as mined in blockID.
func (h *minedSetAuditHarness) createTagged(tx *bt.Tx, blockID uint32) {
	h.t.Helper()

	_, err := h.utxo.Create(h.ctx, tx, uint32(h.height), utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: blockID, BlockHeight: uint32(h.height)}))
	require.NoError(h.t, err)
}

// createUntagged records tx in the UTXO store with no block and its outputs locked, as
// full validation left it when the block was stored with mined_set true.
func (h *minedSetAuditHarness) createUntagged(tx *bt.Tx) {
	h.t.Helper()

	_, err := h.utxo.Create(h.ctx, tx, uint32(h.height), utxo.WithLocked(true))
	require.NoError(h.t, err)
}

// TestMinedSetAudit_ClassifiesBlocks drives one block of each kind through the audit.
//
// Mutation targets: comparing the sample against any block id instead of this block's (the
// height-8 block carrying another block's id then passes), and sampling only the first
// subtree (the partly tagged block then passes).
func TestMinedSetAudit_ClassifiesBlocks(t *testing.T) {
	h := newMinedSetAuditHarness(t)

	// Height 1: fully tagged, two subtrees.
	a1, a2 := h.auditTx(), h.auditTx()
	_, taggedID := h.storeBlock(true, false, []*bt.Tx{a1}, []*bt.Tx{a2})
	h.createTagged(a1, taggedID)
	h.createTagged(a2, taggedID)

	// Height 2: stored with mined_set true and never tagged, as the ProcessBlock route left it.
	b1 := h.auditTx()
	untagged, untaggedID := h.storeBlock(true, false, []*bt.Tx{b1})
	h.createUntagged(b1)

	// Height 3: quick validation tagged the first subtree, then fell through; the last
	// subtree is untagged.
	c1, c2 := h.auditTx(), h.auditTx()
	partial, partialID := h.storeBlock(true, false, []*bt.Tx{c1}, []*bt.Tx{c2})
	h.createTagged(c1, partialID)
	h.createUntagged(c2)

	// Height 4: mined_set false, so the setMined worker still owns it.
	d1 := h.auditTx()
	h.storeBlock(false, false, []*bt.Tx{d1})
	h.createUntagged(d1)

	// Height 5: the subtree file is gone.
	e1 := h.auditTx()
	missing, _ := h.storeBlock(true, true, []*bt.Tx{e1})

	// Height 6: the sampled transaction is not in the UTXO store, as when it was spent and pruned.
	pruned, _ := h.storeBlock(true, false, []*bt.Tx{h.auditTx()})

	// Height 7: only the coinbase.
	h.storeBlock(true, false, []*bt.Tx{})

	// Height 8: the transaction carries another block's id, as one shared with a stored
	// fork does, but not this block's.
	f1 := h.auditTx()
	otherBlock, _ := h.storeBlock(true, false, []*bt.Tx{f1})
	h.createTagged(f1, taggedID)

	// Height 9: quick validation stamped the transaction, locked, then fell through; the
	// stored block never unlocked it.
	g1 := h.auditTx()
	stillLocked, stillLockedID := h.storeBlock(true, false, []*bt.Tx{g1})
	_, err := h.utxo.Create(h.ctx, g1, uint32(h.height), utxo.WithLocked(true),
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: stillLockedID, BlockHeight: uint32(h.height)}))
	require.NoError(t, err)

	result, err := h.auditor.run(h.ctx, 1, 9, 10)
	require.NoError(t, err)

	require.Equal(t, 9, result.Scanned)
	require.Equal(t, 1, result.NotMinedSet)
	require.Equal(t, 1, result.Tagged)
	require.Equal(t, 1, result.CoinbaseOnly)

	require.Len(t, result.Affected, 4)
	require.Equal(t, *untagged.Hash(), result.Affected[0].Hash)
	require.Equal(t, untaggedID, result.Affected[0].BlockID)
	require.Equal(t, *b1.TxIDChainHash(), result.Affected[0].TxID)
	require.True(t, result.Affected[0].Locked)
	require.Equal(t, *partial.Hash(), result.Affected[1].Hash)
	require.Equal(t, *c2.TxIDChainHash(), result.Affected[1].TxID, "the untagged last subtree must be the one reported")
	require.Equal(t, *otherBlock.Hash(), result.Affected[2].Hash, "another block's id must not count as this block's")
	require.Equal(t, *stillLocked.Hash(), result.Affected[3].Hash, "a tagged but still locked transaction must be reported")
	require.Contains(t, result.Affected[3].Reason, "still locked")

	require.Len(t, result.Unchecked, 2)
	require.Equal(t, *missing.Hash(), result.Unchecked[0].Hash)
	require.Contains(t, result.Unchecked[0].Reason, "not in the subtree store")
	require.Equal(t, *pruned.Hash(), result.Unchecked[1].Hash)
	require.Contains(t, result.Unchecked[1].Reason, "no sampled transaction is in the UTXO store")

	var out bytes.Buffer
	require.Error(t, result.report(&out, 1, 9), "an affected block must make the command exit non-zero")
	require.Contains(t, out.String(), "resync the node")
}

// TestMinedSetAudit_SkipsNettedRoute: a block the netting store can only quick validate
// is not audited, because full validation never stores one and the store keeps no record
// for every transaction.
func TestMinedSetAudit_SkipsNettedRoute(t *testing.T) {
	h := newMinedSetAuditHarness(t)

	b1 := h.auditTx()
	h.storeBlock(true, false, []*bt.Tx{b1})
	h.createUntagged(b1)

	h.auditor.netted = func(uint32) bool { return true }

	result, err := h.auditor.run(h.ctx, 1, 1, 10)
	require.NoError(t, err)
	require.Equal(t, 1, result.Netted)
	require.Empty(t, result.Affected)

	var out bytes.Buffer
	require.NoError(t, result.report(&out, 1, 1))
}

// TestMinedSetAudit_NothingCheckedIsNotAPass: a run whose every audited block is UNCHECKED
// is flagged, so the command exits 2 rather than 0 and a script does not read a missing
// subtree store as clean. One checked block, or nothing to audit at all, is not flagged.
//
// Mutation targets: nothingChecked returning false (the all-unchecked subcase then passes
// silently), and nothingChecked ignoring coinbase-only blocks (the coinbase-only subcase then
// exits 2 although one of its two blocks was classified).
func TestMinedSetAudit_NothingCheckedIsNotAPass(t *testing.T) {
	t.Run("every audited block is unchecked", func(t *testing.T) {
		h := newMinedSetAuditHarness(t)

		h.storeBlock(true, true, []*bt.Tx{h.auditTx()})
		h.storeBlock(true, true, []*bt.Tx{h.auditTx()})

		result, err := h.auditor.run(h.ctx, 1, 2, 10)
		require.NoError(t, err)
		require.Len(t, result.Unchecked, 2)
		require.True(t, result.nothingChecked())

		var out bytes.Buffer
		require.NoError(t, result.report(&out, 1, 2))
		require.Contains(t, out.String(), "No block in this range could be checked")
	})

	t.Run("one block is checked", func(t *testing.T) {
		h := newMinedSetAuditHarness(t)

		h.storeBlock(true, true, []*bt.Tx{h.auditTx()})

		tagged := h.auditTx()
		_, taggedID := h.storeBlock(true, false, []*bt.Tx{tagged})
		h.createTagged(tagged, taggedID)

		result, err := h.auditor.run(h.ctx, 1, 2, 10)
		require.NoError(t, err)
		require.Len(t, result.Unchecked, 1)
		require.Equal(t, 1, result.Tagged)
		require.False(t, result.nothingChecked())

		var out bytes.Buffer
		require.NoError(t, result.report(&out, 1, 2))
		require.NotContains(t, out.String(), "No block in this range could be checked")
	})

	t.Run("one block is coinbase only", func(t *testing.T) {
		h := newMinedSetAuditHarness(t)

		h.storeBlock(true, false, []*bt.Tx{})
		h.storeBlock(true, true, []*bt.Tx{h.auditTx()})

		result, err := h.auditor.run(h.ctx, 1, 2, 10)
		require.NoError(t, err)
		require.Equal(t, 1, result.CoinbaseOnly)
		require.Len(t, result.Unchecked, 1)
		require.False(t, result.nothingChecked())
	})

	t.Run("nothing to audit", func(t *testing.T) {
		h := newMinedSetAuditHarness(t)

		h.storeBlock(false, true, []*bt.Tx{h.auditTx()})

		result, err := h.auditor.run(h.ctx, 1, 1, 10)
		require.NoError(t, err)
		require.Equal(t, 1, result.NotMinedSet)
		require.False(t, result.nothingChecked())
	})
}

// TestMinedSetAudit_RefusesUnboundedRange: a range above --max-blocks is refused before
// anything is read.
func TestMinedSetAudit_RefusesUnboundedRange(t *testing.T) {
	h := newMinedSetAuditHarness(t)

	_, err := h.auditor.run(h.ctx, 1, 20_001, minedSetAuditDefaultMaxBlocks)
	require.Error(t, err)
	require.Contains(t, err.Error(), "--max-blocks")

	_, err = h.auditor.run(h.ctx, 5, 4, minedSetAuditDefaultMaxBlocks)
	require.Error(t, err)
}

// TestMinedSetAudit_MalformedSubtreeIsUnchecked: a subtree file cut short is reported as
// unchecked, not as a failure of the whole run.
func TestMinedSetAudit_MalformedSubtreeIsUnchecked(t *testing.T) {
	for _, tc := range []struct {
		name   string
		bytes  []byte
		reason string
	}{
		{name: "shorter than its header", bytes: make([]byte, subtreeHeaderBytes-1), reason: "shorter than its header"},
		{name: "shorter than its node count", bytes: func() []byte {
			b := make([]byte, subtreeHeaderBytes+subtreeNodeBytes+8)
			b[subtreeNodeCountOffset] = 2 // two nodes claimed, under one and a bit present

			return b
		}(), reason: "shorter than its node count"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newMinedSetAuditHarness(t)

			block, _ := h.storeBlock(true, true, []*bt.Tx{h.auditTx()})

			// The root must match, so the file reaches the length check it is cut short for.
			if len(tc.bytes) >= chainhash.HashSize {
				copy(tc.bytes, block.Subtrees[0][:])
			}

			require.NoError(t, h.subtrees.Set(h.ctx, block.Subtrees[0][:], fileformat.FileTypeSubtree, tc.bytes))

			result, err := h.auditor.run(h.ctx, 1, 1, 10)
			require.NoError(t, err)
			require.Len(t, result.Unchecked, 1)
			require.Contains(t, result.Unchecked[0].Reason, tc.reason)
		})
	}
}

// TestMinedSetAudit_SubtreeRootMismatchIsUnchecked: a file stored under one subtree's hash
// that holds another subtree is unchecked. Its nodes belong to some other block, so a
// sample read from it proves nothing about this one.
//
// Mutation target: dropping the root check in subtreeNode. The block's file then holds a
// transaction tagged with the block's id, and the block counts as tagged although its own
// transaction is untagged and locked.
func TestMinedSetAudit_SubtreeRootMismatchIsUnchecked(t *testing.T) {
	h := newMinedSetAuditHarness(t)

	own := h.auditTx()
	block, blockID := h.storeBlock(true, false, []*bt.Tx{own})
	h.createUntagged(own)

	// Another subtree, whose one transaction does carry this block's id.
	stranger := h.auditTx()
	h.createTagged(stranger, blockID)

	other, err := gosubtree.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, other.AddCoinbaseNode())
	require.NoError(t, other.AddNode(*stranger.TxIDChainHash(), 1, uint64(stranger.Size()))) //nolint:gosec // a test tx is small
	require.NotEqual(t, *other.RootHash(), *block.Subtrees[0], "precondition: the two subtrees must differ")

	b, err := other.Serialize()
	require.NoError(t, err)
	require.NoError(t, h.subtrees.Set(h.ctx, block.Subtrees[0][:], fileformat.FileTypeSubtree, b, bloboptions.WithAllowOverwrite(true)))

	result, err := h.auditor.run(h.ctx, 1, 1, 10)
	require.NoError(t, err)
	require.Zero(t, result.Tagged)
	require.Empty(t, result.Affected)
	require.Len(t, result.Unchecked, 1)
	require.Equal(t, *block.Hash(), result.Unchecked[0].Hash)
	require.Contains(t, result.Unchecked[0].Reason, "holds subtree "+other.RootHash().String())
}

// TestMinedSetAudit_NodeCountTooSmallIsUnchecked: a subtree file whose root matches but whose
// node count is too small for the block is unchecked, not coinbase only.
//
// Mutation targets: treating a first subtree with no node 1 as coinbase only whatever the
// block says (the first subcase then counts as coinbase only), and skipping an empty last
// subtree (the second subcase then counts as tagged on the first sample alone).
func TestMinedSetAudit_NodeCountTooSmallIsUnchecked(t *testing.T) {
	headerOnly := minedSetAuditSubtreeFile

	t.Run("first subtree holds only the coinbase", func(t *testing.T) {
		h := newMinedSetAuditHarness(t)

		own := h.auditTx()
		block, _ := h.storeBlock(true, false, []*bt.Tx{own})
		h.createUntagged(own)
		require.Equal(t, uint64(2), block.TransactionCount, "precondition: the block must hold a transaction besides the coinbase")

		require.NoError(t, h.subtrees.Set(h.ctx, block.Subtrees[0][:], fileformat.FileTypeSubtree, headerOnly(block.Subtrees[0], 1), bloboptions.WithAllowOverwrite(true)))

		result, err := h.auditor.run(h.ctx, 1, 1, 10)
		require.NoError(t, err)
		require.Zero(t, result.CoinbaseOnly)
		require.Len(t, result.Unchecked, 1)
		require.Contains(t, result.Unchecked[0].Reason, "too few for a block of 2 transactions")
	})

	t.Run("last subtree holds no nodes", func(t *testing.T) {
		h := newMinedSetAuditHarness(t)

		first, last := h.auditTx(), h.auditTx()
		block, blockID := h.storeBlock(true, false, []*bt.Tx{first}, []*bt.Tx{last})
		h.createTagged(first, blockID)
		h.createUntagged(last)

		require.NoError(t, h.subtrees.Set(h.ctx, block.Subtrees[1][:], fileformat.FileTypeSubtree, headerOnly(block.Subtrees[1], 0), bloboptions.WithAllowOverwrite(true)))

		result, err := h.auditor.run(h.ctx, 1, 1, 10)
		require.NoError(t, err)
		require.Zero(t, result.Tagged)
		require.Len(t, result.Unchecked, 1)
		require.Contains(t, result.Unchecked[0].Reason, "holds no nodes")
	})
}

// minedSetAuditSubtreeFile returns a subtree file for root claiming count nodes and holding
// count nodes, the first of them the coinbase placeholder and the rest zeros.
func minedSetAuditSubtreeFile(root *chainhash.Hash, count byte) []byte {
	b := make([]byte, subtreeHeaderBytes+int(count)*subtreeNodeBytes)
	copy(b, root[:])
	b[subtreeNodeCountOffset] = count

	if count > 0 {
		copy(b[subtreeHeaderBytes:], gosubtree.CoinbasePlaceholder[:])
	}

	return b
}

// TestMinedSetAudit_MalformedCoinbaseOnlyIsUnchecked: a coinbase-only block whose subtree
// file is malformed is unchecked, not counted as coinbase only. The audit reads the first
// subtree's node 0 and requires the coinbase placeholder there.
//
// Mutation targets: dropping the zero-count check (the first subcase then counts as coinbase
// only), reading no node when the count is 1 (the second subcase then counts as coinbase
// only), and dropping the placeholder check (the third subcase then counts as coinbase only).
func TestMinedSetAudit_MalformedCoinbaseOnlyIsUnchecked(t *testing.T) {
	for _, tc := range []struct {
		name   string
		file   func(root *chainhash.Hash) []byte
		reason string
	}{
		{name: "zero nodes", file: func(root *chainhash.Hash) []byte {
			return minedSetAuditSubtreeFile(root, 0)
		}, reason: "not even the coinbase placeholder"},
		{name: "one node claimed, its bytes missing", file: func(root *chainhash.Hash) []byte {
			return minedSetAuditSubtreeFile(root, 1)[:subtreeHeaderBytes]
		}, reason: "shorter than its node count"},
		{name: "node 0 is not the coinbase placeholder", file: func(root *chainhash.Hash) []byte {
			b := minedSetAuditSubtreeFile(root, 1)
			clear(b[subtreeHeaderBytes:])

			return b
		}, reason: "does not start with the coinbase placeholder"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newMinedSetAuditHarness(t)

			block, _ := h.storeBlock(true, true, []*bt.Tx{})
			require.Equal(t, uint64(1), block.TransactionCount, "precondition: the block must hold only its coinbase")
			require.Len(t, block.Subtrees, 1)
			require.NoError(t, h.subtrees.Set(h.ctx, block.Subtrees[0][:], fileformat.FileTypeSubtree, tc.file(block.Subtrees[0])))

			result, err := h.auditor.run(h.ctx, 1, 1, 10)
			require.NoError(t, err)
			require.Zero(t, result.CoinbaseOnly)
			require.Len(t, result.Unchecked, 1)
			require.Contains(t, result.Unchecked[0].Reason, tc.reason)
			require.True(t, result.nothingChecked())
		})
	}

	t.Run("a well-formed coinbase-only file still counts as coinbase only", func(t *testing.T) {
		h := newMinedSetAuditHarness(t)

		block, _ := h.storeBlock(true, true, []*bt.Tx{})
		require.NoError(t, h.subtrees.Set(h.ctx, block.Subtrees[0][:], fileformat.FileTypeSubtree, minedSetAuditSubtreeFile(block.Subtrees[0], 1)))

		result, err := h.auditor.run(h.ctx, 1, 1, 10)
		require.NoError(t, err)
		require.Equal(t, 1, result.CoinbaseOnly)
		require.Empty(t, result.Unchecked)
	})
}

// minedSetAuditExitHelperEnv makes TestMinedSetAudit_ExitStatus run one audit in a child
// process and end it the way teranode-cli does.
const minedSetAuditExitHelperEnv = "TERANODE_MINED_SET_AUDIT_EXIT_HELPER"

// TestMinedSetAudit_ExitStatus runs runMinedSetAudit, the body of the command after the
// stores are opened, in a child process, and reads the process's real exit status. A range
// where every audited block is UNCHECKED exits 2, and a range with a checked block exits 0.
//
// Mutation target: removing the os.Exit call in runMinedSetAudit. The all-unchecked child
// then exits 0.
func TestMinedSetAudit_ExitStatus(t *testing.T) {
	if mode := os.Getenv(minedSetAuditExitHelperEnv); mode != "" {
		h := newMinedSetAuditHarness(t)

		switch mode {
		case "unchecked":
			h.storeBlock(true, true, []*bt.Tx{h.auditTx()})
		case "tagged":
			tx := h.auditTx()
			_, id := h.storeBlock(true, false, []*bt.Tx{tx})
			h.createTagged(tx, id)
		}

		// cli.go ends the process with 1 when a command returns an error.
		if err := runMinedSetAudit(h.ctx, h.auditor, 1, 1, 10, io.Discard); err != nil {
			os.Exit(1)
		}

		os.Exit(0)
	}

	for _, tc := range []struct {
		mode string
		want int
	}{
		{mode: "unchecked", want: minedSetAuditExitNothingChecked},
		{mode: "tagged", want: 0},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestMinedSetAudit_ExitStatus$") //nolint:gosec // re-runs this test binary
			cmd.Env = append(os.Environ(), minedSetAuditExitHelperEnv+"="+tc.mode)

			out, err := cmd.CombinedOutput()

			code := 0
			if err != nil {
				exitErr, ok := err.(*exec.ExitError) //nolint:errorlint // CombinedOutput returns *ExitError unwrapped
				require.True(t, ok, "the child must run: %v\n%s", err, out)
				code = exitErr.ExitCode()
			}

			require.Equal(t, tc.want, code, "child output:\n%s", out)
		})
	}
}

// TestMinedSetAudit_BadFileHeaderIsUnchecked: the file blob store checks a file's own
// header inside GetIoReader and returns a storage error for a bad one. That is one block
// the audit could not check, not a failure of the whole run.
//
// Mutation target: returning GetIoReader's storage error as a run failure in subtreeNode.
// The run then stops with an error.
func TestMinedSetAudit_BadFileHeaderIsUnchecked(t *testing.T) {
	h := newMinedSetAuditHarness(t)

	dir := t.TempDir()
	fileStore, err := blob.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "file", Path: dir})
	require.NoError(t, err)

	h.auditor.subtrees = fileStore

	block, _ := h.storeBlock(true, true, []*bt.Tx{h.auditTx()})
	require.NoError(t, fileStore.Set(h.ctx, block.Subtrees[0][:], fileformat.FileTypeSubtree, make([]byte, subtreeHeaderBytes+subtreeNodeBytes*2)))

	// Overwrite the stored file, header and all, so the store's own header check fails.
	var written []string

	require.NoError(t, filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && filepath.Ext(path) == ".subtree" {
			written = append(written, path)
		}

		return err
	}))
	require.Len(t, written, 1, "precondition: the file store must hold exactly the one subtree file")
	require.NoError(t, os.WriteFile(written[0], []byte("not a teranode file"), 0o600))

	result, err := h.auditor.run(h.ctx, 1, 1, 10)
	require.NoError(t, err, "a bad subtree file must not stop the run")
	require.Len(t, result.Unchecked, 1)
	require.Equal(t, *block.Hash(), result.Unchecked[0].Hash)
	require.Contains(t, result.Unchecked[0].Reason, "could not be read")
}

// TestMinedSetAudit_NoSubtreesIsCoinbaseOnly: a block stored with no subtrees at all is
// counted as coinbase only.
//
// Mutation target: dropping the len(block.Subtrees) == 0 guard in sampleTxIDs. The audit
// then indexes Subtrees[0] and panics.
func TestMinedSetAudit_NoSubtreesIsCoinbaseOnly(t *testing.T) {
	h := newMinedSetAuditHarness(t)

	block, _ := h.storeBlock(true, false)
	require.Empty(t, block.Subtrees, "precondition: the block must carry no subtrees")

	result, err := h.auditor.run(h.ctx, 1, 1, 10)
	require.NoError(t, err)
	require.Equal(t, 1, result.Scanned)
	require.Equal(t, 1, result.CoinbaseOnly)
	require.Empty(t, result.Unchecked)
	require.Empty(t, result.Affected)
}

// TestMinedSetAudit_FlagValidation: the command refuses a missing end height and values
// above uint32 before it connects to anything.
func TestMinedSetAudit_FlagValidation(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "end height missing", args: nil, want: "--end-height is required"},
		{name: "end height above uint32", args: []string{"--end-height=4294967296"}, want: "--end-height"},
		{name: "max blocks above uint32", args: []string{"--end-height=5", "--max-blocks=4294967296"}, want: "--max-blocks"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := flag.NewFlagSet("audit-mined-set", flag.ContinueOnError)
			execute := minedSetAuditExecute(ulogger.TestLogger{}, tSettings, registerMinedSetAuditFlags(fs))
			require.NoError(t, fs.Parse(tc.args))

			err := execute(nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}
