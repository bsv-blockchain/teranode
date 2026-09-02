package rewindblockchain

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	gosubtree "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	blockchainsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxofields "github.com/bsv-blockchain/teranode/stores/utxo/fields"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

// makeCoinbaseTx builds a unique coinbase-style tx by taking a base hex and
// replacing a handful of bytes in the scriptsig's arbitrary-data region. All
// tests run with ChainCfgParams.ChainCfgParams = RegressionNetParams and
// block.Header.Version = 1, so BIP34 height validation is skipped entirely.
// Nevertheless we encode the height for tidiness and keep each coinbase's
// txid distinct by varying the 12-byte "nonce" region.
func makeCoinbaseTx(t *testing.T, heightByte byte, nonceSalt byte) *bt.Tx {
	t.Helper()
	// Layout of the coinbase scriptsig taken from stores/blockchain/sql/sql_test.go:
	//   17 030X00 00 2f6d312d65752f <12 bytes extraNonce>
	// We flip heightByte in position 6 and a byte of the extraNonce to vary txid.
	base := "01000000010000000000000000000000000000000000000000000000000000000000000000" +
		"ffffffff17030100002f6d312d65752f29c267ffea1adb87f33b398fffffffff" +
		"03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac" +
		"aa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588ac" +
		"aa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac" +
		"00000000"

	b, err := hex.DecodeString(base)
	require.NoError(t, err)

	// The scriptsig starts at offset 42 (after version 4 + tx_in_count 1 +
	// prev hash 32 + prev idx 4 + script len 1 = 42). Script opcodes at
	// script[0..3] = "03 XX 00 00" push the 3-byte height.
	//
	// The 2f6d312d65752f string starts at script offset 4, length 7 (= /m1-eu/).
	// The 12-byte extraNonce follows from script offset 11 to script offset 22.
	//
	// In the byte stream above, script starts at byte 42.
	const scriptStart = 42
	b[scriptStart+1] = heightByte // height byte (low byte of 3-byte push)
	b[scriptStart+11] = nonceSalt // first byte of extraNonce
	b[scriptStart+22] = nonceSalt ^ 0xff

	tx, err := bt.NewTxFromBytes(b)
	require.NoError(t, err)

	return tx
}

// makeRegularTx builds a unique "regular" (non-coinbase) extended-format tx by
// mutating a byte in the canonical extended-format hex used elsewhere in the
// repo. The extended format encodes previous outputs inline, which is what
// PreviousOutputsDecorate relies on for Unspend during rewind.
func makeRegularTx(t *testing.T, salt byte) *bt.Tx {
	t.Helper()
	// Canonical extended-format tx hex from stores/utxo/sql/sql_test.go:77-82.
	// Byte offset math: Version(4) + EF marker (6 "0000000000ef") = 10 bytes of
	// preamble, then input_count (1) at 10, then the first input's previous
	// tx hash (32 bytes) starts at 11. Replacing a byte in this prev-hash
	// region changes which UTXO the tx claims to spend, producing a brand-new
	// txid without breaking parsing.
	base := "010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000"

	b, err := hex.DecodeString(base)
	require.NoError(t, err)

	// Flip a byte in the prev-tx-hash region (bytes 11..43). Using offset 20
	// lands us cleanly inside the 32-byte hash so only the parent reference
	// changes — no structural re-parsing needed.
	b[20] ^= salt

	tx, err := bt.NewTxFromBytes(b)
	require.NoError(t, err)

	return tx
}

// makeSubtreeAndStore builds a single subtree with a coinbase placeholder at
// index 0 and the supplied tx hashes appended, serializes it, writes the
// serialized bytes to the blob store under the subtree root hash, and returns
// the root hash. Callers use the returned hash as the block's Subtrees entry.
func makeSubtreeAndStore(t *testing.T, ctx context.Context, store interface {
	Set(ctx context.Context, key []byte, ft fileformat.FileType, value []byte, opts ...blobOption) error
}, nodes []subtreeNode) *chainhash.Hash {
	t.Helper()

	// height 2 = 4-leaf tree (coinbase placeholder + up to 3 tx nodes).
	st, err := gosubtree.NewTree(2)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	for _, n := range nodes {
		require.NoError(t, st.AddNode(n.hash, n.fee, n.size))
	}

	serialized, err := st.Serialize()
	require.NoError(t, err)

	root := st.RootHash()
	require.NotNil(t, root)

	require.NoError(t, store.Set(ctx, root[:], fileformat.FileTypeSubtree, serialized))

	return root
}

// subtreeNode bundles the three values AddNode takes so we can pass slices.
type subtreeNode struct {
	hash chainhash.Hash
	fee  uint64
	size uint64
}

// blobOption is a minimal interface alias so we don't pull the options
// package into the helper signature just for the (unused) opts variadic.
type blobOption interface{}

// setBlockAssemblerState encodes a target height + header into the
// LE-uint32|header-bytes format that Rewind's preflight reads.
func setBlockAssemblerState(ctx context.Context, store blockchain_store.Store, height uint32, header *model.BlockHeader) error {
	headerBytes := header.Bytes()
	payload := make([]byte, 4, 4+len(headerBytes))
	binary.LittleEndian.PutUint32(payload, height)
	payload = append(payload, headerBytes...)
	return store.SetState(ctx, "BlockAssembler", payload)
}

// -----------------------------------------------------------------------------
// Integration test
// -----------------------------------------------------------------------------

// TestRewindBlockchain_Integration exercises the full Rewind pipeline against
// sqlitememory blockchain + UTXO stores and an in-memory blob store. The
// fixture builds a 4-block main chain with a losing fork at height 3, seeds
// unmined and conflicting txs, seeds a multi-block tx referenced by both a
// surviving block and a deleted fork block, then runs Rewind(target=2) and
// asserts.
func TestRewindBlockchain_Integration(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	// Make sure UTXO batcher fires synchronously in-test (mirrors the setup in
	// stores/utxo/sql/sql_test.go).
	tSettings.BatcherDrainMode = true

	// -------------------------------------------------------------------
	// 1. Build stores
	// -------------------------------------------------------------------
	bcURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	bcStore, err := blockchainsql.New(logger, bcURL, tSettings)
	require.NoError(t, err)

	utxoURL, err := url.Parse("sqlitememory:///rewind-utxo")
	require.NoError(t, err)
	utxoStore, err := utxosql.New(ctx, logger, tSettings, utxoURL)
	require.NoError(t, err)

	subtreeStore := memory.New()

	// -------------------------------------------------------------------
	// 2. Build coinbase + regular txs
	// -------------------------------------------------------------------
	coinbase1 := makeCoinbaseTx(t, 0x01, 0x01)    // block height 1
	coinbase2 := makeCoinbaseTx(t, 0x02, 0x02)    // block height 2
	coinbase3 := makeCoinbaseTx(t, 0x03, 0x03)    // block height 3
	coinbase4 := makeCoinbaseTx(t, 0x04, 0x04)    // block height 4
	coinbase3alt := makeCoinbaseTx(t, 0x03, 0x13) // fork block at height 3

	// Ensure distinct txids.
	seen := map[string]struct{}{}
	for _, tx := range []*bt.Tx{coinbase1, coinbase2, coinbase3, coinbase4, coinbase3alt} {
		seen[tx.TxID()] = struct{}{}
	}
	require.Len(t, seen, 5, "coinbases should have 5 distinct txids")

	// utxoParent is a single synthetic "root" transaction that we put into the
	// UTXO store before any other regular tx. Every regular tx in this test
	// has its input[0].PreviousTxID rewritten to point at utxoParent.
	//
	// Rewind's Unspend path now tolerates both ErrTxNotFound and ErrNotFound
	// (tx_delete.go isNotFound), so the Rewind pipeline itself no longer
	// requires a real parent row. We still need utxoParent because the SQL
	// UTXO store's WithConflicting Create path inserts into conflicting_children
	// using a subquery on the parent's transactions.id — without a real parent
	// row, the insert fails the transaction_id NOT NULL constraint during
	// seeding (long before Rewind runs). See stores/utxo/sql/sql.go:1231.
	utxoParent := makeRegularTx(t, 0xA0)

	// One "regular" (non-coinbase) tx per block's subtree. Each must have a
	// unique txid so UTXO store Create doesn't collide.
	regTx1 := makeRegularTx(t, 0x01)
	regTx2 := makeRegularTx(t, 0x02)
	regTx3 := makeRegularTx(t, 0x03)
	regTx4 := makeRegularTx(t, 0x04)

	// The "multi-blockID" tx: will be mined into both block2 (surviving) and
	// block3alt (deleted on rewind target=2). After rewind, BlockIDs should
	// contain only the surviving block2 id.
	multiBlockTx := makeRegularTx(t, 0x05)

	// Unmined + conflicting txs.
	unmined1 := makeRegularTx(t, 0x10)
	unmined2 := makeRegularTx(t, 0x11)
	conflictingTx := makeRegularTx(t, 0x20)

	// Retarget every regular tx's first input at utxoParent, and bump each
	// SequenceNumber so that after the shared PreviousTxID the resulting
	// txids remain distinct. Do this BEFORE building subtrees — subtrees
	// embed the tx hashes, which change as a result.
	retargets := []*bt.Tx{regTx1, regTx2, regTx3, regTx4, multiBlockTx, unmined1, unmined2, conflictingTx}
	for i, tx := range retargets {
		require.NotEmpty(t, tx.Inputs, "tx has no inputs")
		require.NoError(t, tx.Inputs[0].PreviousTxIDAdd(utxoParent.TxIDChainHash()))
		tx.Inputs[0].SequenceNumber = 0xFFFF0000 + uint32(i)
	}

	// Sanity: all retargeted txs have distinct ids + different from utxoParent.
	txidSet := map[string]struct{}{utxoParent.TxID(): {}}
	for _, tx := range retargets {
		txidSet[tx.TxID()] = struct{}{}
	}
	require.Len(t, txidSet, 1+len(retargets), "retargeted txs must have distinct txids")

	// -------------------------------------------------------------------
	// 3. Build subtrees and write them to the blob store
	// -------------------------------------------------------------------
	subtreeStoreAdapter := blobStoreSet{store: subtreeStore}

	subtree1Root := makeSubtreeAndStore(t, ctx, subtreeStoreAdapter, []subtreeNode{
		{hash: *regTx1.TxIDChainHash(), fee: 1, size: uint64(regTx1.Size())},
	})

	// block2's subtree contains the multiBlockTx so that when block2 survives
	// the rewind and we trim BlockIDs on multiBlockTx, we can later verify the
	// surviving blockID matches block2's.
	subtree2Root := makeSubtreeAndStore(t, ctx, subtreeStoreAdapter, []subtreeNode{
		{hash: *regTx2.TxIDChainHash(), fee: 1, size: uint64(regTx2.Size())},
		{hash: *multiBlockTx.TxIDChainHash(), fee: 1, size: uint64(multiBlockTx.Size())},
	})

	subtree3Root := makeSubtreeAndStore(t, ctx, subtreeStoreAdapter, []subtreeNode{
		{hash: *regTx3.TxIDChainHash(), fee: 1, size: uint64(regTx3.Size())},
	})

	subtree4Root := makeSubtreeAndStore(t, ctx, subtreeStoreAdapter, []subtreeNode{
		{hash: *regTx4.TxIDChainHash(), fee: 1, size: uint64(regTx4.Size())},
	})

	// block3alt references the multiBlockTx — this is the trigger for the
	// "trim BlockIDs, don't delete" branch in phase 2.
	subtree3altRoot := makeSubtreeAndStore(t, ctx, subtreeStoreAdapter, []subtreeNode{
		{hash: *multiBlockTx.TxIDChainHash(), fee: 1, size: uint64(multiBlockTx.Size())},
	})

	// -------------------------------------------------------------------
	// 4. Build blocks and store them
	// -------------------------------------------------------------------
	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	// Previous block hash for block1 = regtest genesis, computed from
	// RegressionNetParams.GenesisHash.
	genesisHash := tSettings.ChainCfgParams.GenesisHash

	block1 := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1700000001,
			Nonce:          1,
			HashPrevBlock:  genesisHash,
			HashMerkleRoot: subtree1Root,
			Bits:           *bits,
		},
		CoinbaseTx:       coinbase1,
		TransactionCount: 2,
		Subtrees:         []*chainhash.Hash{subtree1Root},
	}

	block2 := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1700000002,
			Nonce:          2,
			HashPrevBlock:  block1.Hash(),
			HashMerkleRoot: subtree2Root,
			Bits:           *bits,
		},
		CoinbaseTx:       coinbase2,
		TransactionCount: 3,
		Subtrees:         []*chainhash.Hash{subtree2Root},
	}

	block3 := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1700000003,
			Nonce:          3,
			HashPrevBlock:  block2.Hash(),
			HashMerkleRoot: subtree3Root,
			Bits:           *bits,
		},
		CoinbaseTx:       coinbase3,
		TransactionCount: 2,
		Subtrees:         []*chainhash.Hash{subtree3Root},
	}

	block4 := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1700000004,
			Nonce:          4,
			HashPrevBlock:  block3.Hash(),
			HashMerkleRoot: subtree4Root,
			Bits:           *bits,
		},
		CoinbaseTx:       coinbase4,
		TransactionCount: 2,
		Subtrees:         []*chainhash.Hash{subtree4Root},
	}

	// Losing fork: branches off block2.
	block3alt := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1700000013,
			Nonce:          13,
			HashPrevBlock:  block2.Hash(),
			HashMerkleRoot: subtree3altRoot,
			Bits:           *bits,
		},
		CoinbaseTx:       coinbase3alt,
		TransactionCount: 2,
		Subtrees:         []*chainhash.Hash{subtree3altRoot},
	}

	// Sanity: block3 and block3alt must have different hashes.
	require.NotEqual(t, block3.Hash().String(), block3alt.Hash().String())

	// Store blocks. Main chain has highest cumulative work so block4 is tip.
	block1ID, _, err := bcStore.StoreBlock(ctx, block1, "peer", blockchainoptions.WithMinedSet(true))
	require.NoError(t, err)

	block2ID, _, err := bcStore.StoreBlock(ctx, block2, "peer", blockchainoptions.WithMinedSet(true))
	require.NoError(t, err)

	block3ID, _, err := bcStore.StoreBlock(ctx, block3, "peer", blockchainoptions.WithMinedSet(true))
	require.NoError(t, err)

	block4ID, _, err := bcStore.StoreBlock(ctx, block4, "peer", blockchainoptions.WithMinedSet(true))
	require.NoError(t, err)

	// Fork stored *after* the main chain; its cumulative work ties with block3
	// on the main chain but since block3 was first, it remains the winning
	// side. Storing after is fine: we don't care which side is "main" per-se,
	// we just need block3alt to be enumerated by ListBlockRefsAboveHeight(2).
	block3altID, _, err := bcStore.StoreBlock(ctx, block3alt, "peer", blockchainoptions.WithMinedSet(true))
	require.NoError(t, err)

	// Best block should be block4 (height 4).
	_, bestMetaPre, err := bcStore.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(4), bestMetaPre.Height)

	// -------------------------------------------------------------------
	// 5. Seed the UTXO store
	// -------------------------------------------------------------------
	// utxoParent — the synthetic parent every regular tx points at. Mined
	// into block1 so it isn't scooped up by the unmined iterator during
	// Phase 1 of Rewind. Its presence satisfies the SQL store's
	// conflicting_children FK constraint when we later create conflictingTx.
	_, err = utxoStore.Create(ctx, utxoParent, 0, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: uint32(block1ID), BlockHeight: 1, SubtreeIdx: 0}))
	require.NoError(t, err)

	// Coinbases (mined into their respective blocks).
	_, err = utxoStore.Create(ctx, coinbase1, 0, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: uint32(block1ID), BlockHeight: 1, SubtreeIdx: 0}))
	require.NoError(t, err)
	_, err = utxoStore.Create(ctx, coinbase2, 0, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: uint32(block2ID), BlockHeight: 2, SubtreeIdx: 0}))
	require.NoError(t, err)
	_, err = utxoStore.Create(ctx, coinbase3, 0, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: uint32(block3ID), BlockHeight: 3, SubtreeIdx: 0}))
	require.NoError(t, err)
	_, err = utxoStore.Create(ctx, coinbase4, 0, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: uint32(block4ID), BlockHeight: 4, SubtreeIdx: 0}))
	require.NoError(t, err)
	_, err = utxoStore.Create(ctx, coinbase3alt, 0, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: uint32(block3altID), BlockHeight: 3, SubtreeIdx: 0}))
	require.NoError(t, err)

	// Regular block txs.
	_, err = utxoStore.Create(ctx, regTx1, 0, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: uint32(block1ID), BlockHeight: 1, SubtreeIdx: 0}))
	require.NoError(t, err)
	_, err = utxoStore.Create(ctx, regTx2, 0, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: uint32(block2ID), BlockHeight: 2, SubtreeIdx: 0}))
	require.NoError(t, err)
	_, err = utxoStore.Create(ctx, regTx3, 0, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: uint32(block3ID), BlockHeight: 3, SubtreeIdx: 0}))
	require.NoError(t, err)
	_, err = utxoStore.Create(ctx, regTx4, 0, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: uint32(block4ID), BlockHeight: 4, SubtreeIdx: 0}))
	require.NoError(t, err)

	// Multi-blockID tx: mined into both block2 (survives) and block3alt (deleted).
	_, err = utxoStore.Create(ctx, multiBlockTx, 0,
		utxo.WithMinedBlockInfo(
			utxo.MinedBlockInfo{BlockID: uint32(block2ID), BlockHeight: 2, SubtreeIdx: 0},
			utxo.MinedBlockInfo{BlockID: uint32(block3altID), BlockHeight: 3, SubtreeIdx: 0},
		),
	)
	require.NoError(t, err)

	// Sanity: multi-block tx has both IDs.
	mbMetaBefore, err := utxoStore.Get(ctx, multiBlockTx.TxIDChainHash(), utxofields.BlockIDs)
	require.NoError(t, err)
	require.ElementsMatch(t, []uint32{uint32(block2ID), uint32(block3altID)}, mbMetaBefore.BlockIDs)

	// Unmined txs (no MinedBlockInfo -> unmined_since is populated).
	_, err = utxoStore.Create(ctx, unmined1, 0)
	require.NoError(t, err)
	_, err = utxoStore.Create(ctx, unmined2, 0)
	require.NoError(t, err)

	_, err = utxoStore.Create(ctx, conflictingTx, 0, utxo.WithConflicting(true))
	require.NoError(t, err)

	// -------------------------------------------------------------------
	// 6. Seed FSM + state keys
	// -------------------------------------------------------------------
	require.NoError(t, bcStore.SetFSMState(ctx, "IDLE"))

	// BlockAssembler state — even though opts.TargetHeight will override,
	// Rewind still reads it via GetBlockHeader in Phase 3 indirectly via
	// the target header. Setting it to the current tip keeps the preflight
	// happy if TargetHeight were < 0.
	require.NoError(t, setBlockAssemblerState(ctx, bcStore, 4, block4.Header))

	// BlockPersisterHeight: 4 bytes LE of the current tip so Phase 3 rewrites
	// it to target.
	bpPayload := make([]byte, 4)
	binary.LittleEndian.PutUint32(bpPayload, 4)
	require.NoError(t, bcStore.SetState(ctx, "BlockPersisterHeight", bpPayload))

	// -------------------------------------------------------------------
	// 7. Run Rewind(target=2)
	// -------------------------------------------------------------------
	var stdout bytes.Buffer

	// Verify=true exercises Phase 4 end-to-end. DeleteBlock now invalidates
	// the block-timestamp / response / chain-walk caches and triggers the
	// off-chain-set rebuild (mirroring InvalidateBlock), so GetBestBlockHeader
	// inside Phase 4 reflects the new tip without any manual cache flush.
	opts := Options{
		TargetHeight: 2,
		AssumeYes:    true,
		Verify:       true,
		Stdin:        strings.NewReader(""),
		Stdout:       &stdout,
		Stores: &Stores{
			Blockchain: bcStore,
			UTXO:       utxoStore,
			Subtree:    subtreeStore,
		},
	}

	stats, err := Rewind(ctx, logger, tSettings, opts)
	require.NoError(t, err)
	require.NotNil(t, stats)

	// -------------------------------------------------------------------
	// 8. Assertions
	// -------------------------------------------------------------------

	// (1) Best block height is target.
	bestHeader, bestMeta, err := bcStore.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(2), bestMeta.Height, "best block height should match target")
	require.Equal(t, block2.Hash().String(), bestHeader.Hash().String(), "best block hash should be block2")

	// (2) Blocks above target are gone.
	_, err = bcStore.GetBlockByHeight(ctx, 3)
	require.Error(t, err, "block at height 3 should be absent after rewind")
	_, err = bcStore.GetBlockByHeight(ctx, 4)
	require.Error(t, err, "block at height 4 should be absent after rewind")

	// block3alt (fork) should also be gone — look it up by hash directly.
	_, _, err = bcStore.GetBlock(ctx, block3alt.Hash())
	require.Error(t, err, "fork block should be absent after rewind")

	// (3, 4) Coinbases and regular txs from deleted blocks are gone from UTXO store.
	for _, tx := range []*bt.Tx{coinbase3, coinbase4, coinbase3alt, regTx3, regTx4} {
		_, err := utxoStore.Get(ctx, tx.TxIDChainHash())
		require.Error(t, err, "tx %s from deleted block should be gone", tx.TxID())
		require.True(t, errors.Is(err, errors.ErrTxNotFound), "expected ErrTxNotFound for %s, got %v", tx.TxID(), err)
	}

	// Surviving blocks' txs still present.
	_, err = utxoStore.Get(ctx, coinbase1.TxIDChainHash())
	require.NoError(t, err, "block1 coinbase should survive")
	_, err = utxoStore.Get(ctx, coinbase2.TxIDChainHash())
	require.NoError(t, err, "block2 coinbase should survive")
	_, err = utxoStore.Get(ctx, regTx1.TxIDChainHash())
	require.NoError(t, err, "block1 regular tx should survive")
	_, err = utxoStore.Get(ctx, regTx2.TxIDChainHash())
	require.NoError(t, err, "block2 regular tx should survive")

	// (5) Multi-blockID tx is still present; BlockIDs trimmed to just block2.
	mbMetaAfter, err := utxoStore.Get(ctx, multiBlockTx.TxIDChainHash(), utxofields.BlockIDs)
	require.NoError(t, err, "multi-blockID tx must survive the rewind")
	require.ElementsMatch(t, []uint32{uint32(block2ID)}, mbMetaAfter.BlockIDs,
		"multi-blockID tx should keep only block2's ID after rewind")

	// (6) Unmined txs are gone.
	for _, tx := range []*bt.Tx{unmined1, unmined2} {
		_, err := utxoStore.Get(ctx, tx.TxIDChainHash())
		require.Error(t, err, "unmined tx %s should be purged", tx.TxID())
		require.True(t, errors.Is(err, errors.ErrTxNotFound))
	}

	// (7) Conflicting tx is gone.
	_, err = utxoStore.Get(ctx, conflictingTx.TxIDChainHash())
	require.Error(t, err, "conflicting tx should be purged")
	require.True(t, errors.Is(err, errors.ErrTxNotFound))

	// (8) Subtree blobs for deleted blocks are absent.
	for name, h := range map[string]*chainhash.Hash{
		"subtree3":    subtree3Root,
		"subtree4":    subtree4Root,
		"subtree3alt": subtree3altRoot,
	} {
		exists, err := subtreeStore.Exists(ctx, h[:], fileformat.FileTypeSubtree)
		require.NoError(t, err)
		require.False(t, exists, "subtree blob %s should be deleted from blob store", name)
	}

	// Surviving subtree blobs remain.
	for name, h := range map[string]*chainhash.Hash{
		"subtree1": subtree1Root,
		"subtree2": subtree2Root,
	} {
		exists, err := subtreeStore.Exists(ctx, h[:], fileformat.FileTypeSubtree)
		require.NoError(t, err)
		require.True(t, exists, "subtree blob %s should survive the rewind", name)
	}

	// (9) state[BlockAssembler] decodes to target height with target header.
	baBytes, err := bcStore.GetState(ctx, "BlockAssembler")
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(baBytes), 4+80, "BlockAssembler state too short")
	gotHeight := binary.LittleEndian.Uint32(baBytes[:4])
	require.Equal(t, uint32(2), gotHeight, "BlockAssembler height should be target")
	gotHeader, err := model.NewBlockHeaderFromBytes(baBytes[4:])
	require.NoError(t, err)
	require.Equal(t, block2.Hash().String(), gotHeader.Hash().String(), "BlockAssembler header should be block2's")

	// (10) state[BlockPersisterHeight] decodes to LE(target).
	bpBytes, err := bcStore.GetState(ctx, "BlockPersisterHeight")
	require.NoError(t, err)
	require.Len(t, bpBytes, 4)
	require.Equal(t, uint32(2), binary.LittleEndian.Uint32(bpBytes),
		"BlockPersisterHeight should be rewritten to target")

	// (11) Stats counters plausible.
	require.GreaterOrEqual(t, stats.BlocksDeleted, 3, "expected >= 3 blocks deleted (block3, block4, block3alt)")
	require.GreaterOrEqual(t, stats.TxsDeleted, 2, "expected >= 2 txs deleted (deleted coinbases + regular txs)")
	require.GreaterOrEqual(t, stats.UnminedPurged, 2, "expected >= 2 unmined txs purged")
	require.GreaterOrEqual(t, stats.ConflictingPurged, 1, "expected >= 1 conflicting tx purged")
	require.GreaterOrEqual(t, stats.TxsBlockIDsTrimmed, 1, "expected >= 1 tx block-id trim (multiBlockTx)")
	require.GreaterOrEqual(t, stats.SubtreesDeleted, 3, "expected >= 3 subtree blobs deleted")

	// UTXO store internal block height got set to target (Phase 0).
	require.Equal(t, uint32(2), utxoStore.GetBlockHeight(), "utxo store internal height should be target")
}

// -----------------------------------------------------------------------------
// blobStoreSet is a tiny adapter so makeSubtreeAndStore doesn't have to
// import the blob options package. It exposes only the single method the
// helper uses, and its Set method discards the variadic opts (defaults work).
// -----------------------------------------------------------------------------

type blobStoreSet struct {
	store *memory.Memory
}

func (b blobStoreSet) Set(ctx context.Context, key []byte, ft fileformat.FileType, value []byte, _ ...blobOption) error {
	return b.store.Set(ctx, key, ft, value)
}

// -----------------------------------------------------------------------------
// Branch-coverage tests: DryRun / ForceNotIdle / ForceDeep
//
// These exercise the four preflight branches and the rewind.go DryRun early
// return that the main integration test (which sets FSM=IDLE, depth=2,
// DryRun=false) does not reach.
// -----------------------------------------------------------------------------

// buildLinearChain stores n minimal blocks above genesis and returns the
// resulting block list (block[0] is height 1, block[n-1] is height n).
// Blocks share the supplied bits, each gets a unique coinbase, and the
// per-block subtree exists only as a hash — StoreBlock does not validate the
// blob is present in any subtree store, and preflight does not read subtrees.
func buildLinearChain(t *testing.T, ctx context.Context, bcStore blockchain_store.Store, n int, bits *model.NBit, genesisHash *chainhash.Hash) []*model.Block {
	t.Helper()
	require.LessOrEqual(t, n, 255, "buildLinearChain capped at 255 by 1-byte heightByte")

	blocks := make([]*model.Block, 0, n)
	prevHash := genesisHash
	for i := 1; i <= n; i++ {
		heightByte := byte(i)
		coinbase := makeCoinbaseTx(t, heightByte, heightByte)

		// Single-tx merkle root = coinbase txid. Reuse the same hash for the
		// block's single Subtrees entry: preflight only enumerates block refs,
		// it never opens these subtree blobs.
		root := coinbase.TxIDChainHash()

		block := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				Timestamp:      uint32(1700000000 + i),
				Nonce:          uint32(i),
				HashPrevBlock:  prevHash,
				HashMerkleRoot: root,
				Bits:           *bits,
			},
			CoinbaseTx:       coinbase,
			TransactionCount: 1,
			Subtrees:         []*chainhash.Hash{root},
		}
		_, _, err := bcStore.StoreBlock(ctx, block, "peer", blockchainoptions.WithMinedSet(true))
		require.NoError(t, err, "StoreBlock height %d", i)

		blocks = append(blocks, block)
		prevHash = block.Hash()
	}
	return blocks
}

// newTestStores builds a fresh sqlitememory blockchain store + nil-tolerant
// UTXO/subtree stubs suitable for tests whose Rewind call exits inside
// preflight or via DryRun before phase 1.
func newTestStores(t *testing.T, ctx context.Context) (blockchain_store.Store, utxo.Store, *memory.Memory, *settings.Settings) {
	t.Helper()
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BatcherDrainMode = true

	bcURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	bcStore, err := blockchainsql.New(logger(), bcURL, tSettings)
	require.NoError(t, err)

	utxoURL, err := url.Parse("sqlitememory:///rewind-utxo")
	require.NoError(t, err)
	utxoStore, err := utxosql.New(ctx, logger(), tSettings, utxoURL)
	require.NoError(t, err)

	return bcStore, utxoStore, memory.New(), tSettings
}

func logger() ulogger.Logger { return ulogger.TestLogger{} }

// TestRewindBlockchain_DryRun verifies that DryRun=true exits before any
// Phase 0+ mutation: stats stay zero, the tip block is still present, and
// the UTXO store's internal block height is unchanged.
func TestRewindBlockchain_DryRun(t *testing.T) {
	ctx := context.Background()
	bcStore, utxoStore, subtreeStore, tSettings := newTestStores(t, ctx)

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)
	genesisHash := tSettings.ChainCfgParams.GenesisHash

	blocks := buildLinearChain(t, ctx, bcStore, 4, bits, genesisHash)
	require.NoError(t, bcStore.SetFSMState(ctx, "IDLE"))

	preHeight := utxoStore.GetBlockHeight()

	stats, err := Rewind(ctx, logger(), tSettings, Options{
		TargetHeight: 2,
		DryRun:       true,
		AssumeYes:    true,
		Stores: &Stores{
			Blockchain: bcStore,
			UTXO:       utxoStore,
			Subtree:    subtreeStore,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Zero(t, stats.BlocksDeleted, "DryRun must not delete blocks")
	require.Zero(t, stats.TxsDeleted, "DryRun must not delete txs")
	require.Equal(t, preHeight, utxoStore.GetBlockHeight(), "DryRun must not touch UTXO store height")

	// Tip block still present at original height.
	_, bestMeta, err := bcStore.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(4), bestMeta.Height, "DryRun must not delete the tip")
	// Sanity: the 4th block (tip) is still retrievable.
	_, _, err = bcStore.GetBlock(ctx, blocks[3].Hash())
	require.NoError(t, err)
}

// TestRewindBlockchain_ForceNotIdle covers both branches of the FSM gate:
// rejection when FSM!=IDLE without --force-not-idle, and acceptance when
// the flag is supplied. The accepted branch uses DryRun=true to short-circuit
// after preflight so we don't need a UTXO/subtree fixture.
func TestRewindBlockchain_ForceNotIdle(t *testing.T) {
	t.Run("rejected when flag not set", func(t *testing.T) {
		ctx := context.Background()
		bcStore, utxoStore, subtreeStore, tSettings := newTestStores(t, ctx)

		bits, err := model.NewNBitFromString("207fffff")
		require.NoError(t, err)
		buildLinearChain(t, ctx, bcStore, 4, bits, tSettings.ChainCfgParams.GenesisHash)
		require.NoError(t, bcStore.SetFSMState(ctx, "RUNNING"))

		_, err = Rewind(ctx, logger(), tSettings, Options{
			TargetHeight: 2,
			DryRun:       true,
			AssumeYes:    true,
			Stores: &Stores{
				Blockchain: bcStore,
				UTXO:       utxoStore,
				Subtree:    subtreeStore,
			},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "FSM state")
		require.Contains(t, err.Error(), "--force-not-idle")
	})

	t.Run("accepted when flag set", func(t *testing.T) {
		ctx := context.Background()
		bcStore, utxoStore, subtreeStore, tSettings := newTestStores(t, ctx)

		bits, err := model.NewNBitFromString("207fffff")
		require.NoError(t, err)
		buildLinearChain(t, ctx, bcStore, 4, bits, tSettings.ChainCfgParams.GenesisHash)
		require.NoError(t, bcStore.SetFSMState(ctx, "RUNNING"))

		stats, err := Rewind(ctx, logger(), tSettings, Options{
			TargetHeight: 2,
			DryRun:       true,
			AssumeYes:    true,
			ForceNotIdle: true,
			Stores: &Stores{
				Blockchain: bcStore,
				UTXO:       utxoStore,
				Subtree:    subtreeStore,
			},
		})
		require.NoError(t, err, "ForceNotIdle should let preflight pass")
		require.NotNil(t, stats)
		require.Zero(t, stats.BlocksDeleted, "DryRun must not delete blocks")
	})
}

// TestRewindBlockchain_ForceDeep covers both branches of the depth gate:
// rejection when tip-target > coinbaseMaturity (100) without --force-deep,
// and acceptance when the flag is supplied. Uses 102 minimal blocks so
// depth = 102-1 = 101 > 100. Accepted branch short-circuits via DryRun.
func TestRewindBlockchain_ForceDeep(t *testing.T) {
	const chainHeight = 102

	t.Run("rejected when flag not set", func(t *testing.T) {
		ctx := context.Background()
		bcStore, utxoStore, subtreeStore, tSettings := newTestStores(t, ctx)

		bits, err := model.NewNBitFromString("207fffff")
		require.NoError(t, err)
		buildLinearChain(t, ctx, bcStore, chainHeight, bits, tSettings.ChainCfgParams.GenesisHash)
		require.NoError(t, bcStore.SetFSMState(ctx, "IDLE"))

		_, err = Rewind(ctx, logger(), tSettings, Options{
			TargetHeight: 1,
			DryRun:       true,
			AssumeYes:    true,
			Stores: &Stores{
				Blockchain: bcStore,
				UTXO:       utxoStore,
				Subtree:    subtreeStore,
			},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "rewind depth")
		require.Contains(t, err.Error(), "--force-deep")
	})

	t.Run("accepted when flag set", func(t *testing.T) {
		ctx := context.Background()
		bcStore, utxoStore, subtreeStore, tSettings := newTestStores(t, ctx)

		bits, err := model.NewNBitFromString("207fffff")
		require.NoError(t, err)
		buildLinearChain(t, ctx, bcStore, chainHeight, bits, tSettings.ChainCfgParams.GenesisHash)
		require.NoError(t, bcStore.SetFSMState(ctx, "IDLE"))

		stats, err := Rewind(ctx, logger(), tSettings, Options{
			TargetHeight: 1,
			DryRun:       true,
			AssumeYes:    true,
			ForceDeep:    true,
			Stores: &Stores{
				Blockchain: bcStore,
				UTXO:       utxoStore,
				Subtree:    subtreeStore,
			},
		})
		require.NoError(t, err, "ForceDeep should let preflight pass")
		require.NotNil(t, stats)
		require.Zero(t, stats.BlocksDeleted, "DryRun must not delete blocks")
	})
}

// errOnGetStateStore wraps a blockchain.Store and overrides GetState to
// return a configurable error. All other methods delegate to the embedded
// store. Used to simulate genuine storage failures distinct from
// ErrNotFound / sql.ErrNoRows.
type errOnGetStateStore struct {
	blockchain_store.Store
	getStateErr error
	// onlyKey, when non-empty, limits the injected failure to that one state
	// key. Without it a test cannot tell "the persister read aborted preflight"
	// from "the first read of any key aborted preflight".
	onlyKey string
}

func (e *errOnGetStateStore) GetState(ctx context.Context, key string) ([]byte, error) {
	if e.getStateErr != nil && (e.onlyKey == "" || e.onlyKey == key) {
		return nil, e.getStateErr
	}
	return e.Store.GetState(ctx, key)
}

// TestResetBlockPersisterHeight_RealErrorPropagates covers the branch in
// phase3_finalize.go where GetState returns a non-NotFound error. Previously
// any error was silently swallowed as "missing key" — the new code must
// distinguish sql.ErrNoRows / errors.ErrNotFound from real failures and
// surface the latter.
func TestResetBlockPersisterHeight_RealErrorPropagates(t *testing.T) {
	ctx := context.Background()
	bcStore, _, _, _ := newTestStores(t, ctx)

	simulated := errors.NewStorageError("simulated db read failure")
	wrapped := &errOnGetStateStore{Store: bcStore, getStateErr: simulated}

	e := &env{
		logger:          logger(),
		blockchainStore: wrapped,
	}
	pf := &preflightResult{target: 1}

	err := e.resetBlockPersisterHeight(ctx, pf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read state")
	require.True(t, errors.Is(err, simulated), "real error must propagate, not be swallowed as missing")
}

// TestConfirmPrompt exercises the y/N gate in isolation. confirmPrompt only
// reads Options.Stdin and writes Options.Stdout, so no stores or chain fixture
// are required.
func TestConfirmPrompt(t *testing.T) {
	pf := &preflightResult{tip: 1749337, target: 1749330}

	newEnv := func(stdin string, out *bytes.Buffer) *env {
		return &env{
			logger: logger(),
			opts: Options{
				Stdin:  strings.NewReader(stdin),
				Stdout: out,
			},
		}
	}

	t.Run("accepts affirmative answers", func(t *testing.T) {
		// "y" without a trailing newline is the regression guard: ReadString
		// returns ("y", io.EOF) and must still count as a yes.
		for _, in := range []string{"y\n", "yes\n", "Y\n", "YES\n", " y \n", "y"} {
			var out bytes.Buffer

			require.NoError(t, newEnv(in, &out).confirmPrompt(pf), "input %q must be accepted", in)
			require.Contains(t, out.String(), "DESTRUCTIVE", "input %q must still print the warning", in)
			require.Contains(t, out.String(), "1749330", "input %q must print the target height", in)
		}
	})

	t.Run("rejects negative and unrecognised answers", func(t *testing.T) {
		for _, in := range []string{"n\n", "no\n", "\n", "maybe\n"} {
			var out bytes.Buffer

			err := newEnv(in, &out).confirmPrompt(pf)
			require.Error(t, err, "input %q must be rejected", in)
			require.Contains(t, err.Error(), "aborted by user", "input %q", in)
		}
	})

	t.Run("names the remedy when stdin is closed", func(t *testing.T) {
		var out bytes.Buffer

		err := newEnv("", &out).confirmPrompt(pf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "--assume-yes",
			"an operator hitting EOF on a non-TTY must be told how to proceed")
	})

	t.Run("names the remedy when stdin is absent", func(t *testing.T) {
		var out bytes.Buffer

		e := &env{logger: logger(), opts: Options{Stdout: &out}}

		err := e.confirmPrompt(pf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "--assume-yes")
	})
}

// setPersisterHeight seeds state["BlockPersisterHeight"] with an LE uint32,
// the format services/blockpersister/Server.go writes.
func setPersisterHeight(t *testing.T, ctx context.Context, store blockchain_store.Store, height uint32) {
	t.Helper()
	payload := make([]byte, 4)
	binary.LittleEndian.PutUint32(payload, height)
	require.NoError(t, store.SetState(ctx, "BlockPersisterHeight", payload))
}

// countingSetStateStore wraps a blockchain.Store and counts SetState calls per
// key. Needed because a value assertion cannot distinguish "skipped the write"
// from "wrote LE(target)" when the stored height already equals the target —
// both leave the same bytes behind. The call count can.
type countingSetStateStore struct {
	blockchain_store.Store
	setStateCalls map[string]int
}

func newCountingSetStateStore(inner blockchain_store.Store) *countingSetStateStore {
	return &countingSetStateStore{Store: inner, setStateCalls: make(map[string]int)}
}

func (c *countingSetStateStore) SetState(ctx context.Context, key string, data []byte) error {
	c.setStateCalls[key]++
	return c.Store.SetState(ctx, key, data)
}

// TestResetBlockPersisterHeight_NeverMovesForward covers issue #1340: the tool
// used to write LE(target) unconditionally, which raised the recorded persister
// height on nodes whose persister was behind the target (normal after a
// resync). Raising it makes the pruner treat unpersisted blocks as persisted.
func TestResetBlockPersisterHeight_NeverMovesForward(t *testing.T) {
	ctx := context.Background()

	t.Run("below target is left untouched", func(t *testing.T) {
		bcStore, _, _, _ := newTestStores(t, ctx)
		setPersisterHeight(t, ctx, bcStore, 422492)
		spy := newCountingSetStateStore(bcStore)

		e := &env{logger: logger(), blockchainStore: spy}
		require.NoError(t, e.resetBlockPersisterHeight(ctx, &preflightResult{target: 1749334}))

		require.Zero(t, spy.setStateCalls[blockPersisterHeightKey], "no write may be issued at all")

		got, err := bcStore.GetState(ctx, "BlockPersisterHeight")
		require.NoError(t, err)
		require.Len(t, got, 4)
		require.Equal(t, uint32(422492), binary.LittleEndian.Uint32(got),
			"persister height behind the target must not be moved forward")
	})

	t.Run("equal to target is left untouched", func(t *testing.T) {
		bcStore, _, _, _ := newTestStores(t, ctx)
		setPersisterHeight(t, ctx, bcStore, 7)
		spy := newCountingSetStateStore(bcStore)

		e := &env{logger: logger(), blockchainStore: spy}
		require.NoError(t, e.resetBlockPersisterHeight(ctx, &preflightResult{target: 7}))

		// The call count is the only assertion that can fail here: skipping the
		// write and rewriting LE(7) both leave 7 in the store.
		require.Zero(t, spy.setStateCalls[blockPersisterHeightKey], "no write may be issued at the equality boundary")

		got, err := bcStore.GetState(ctx, "BlockPersisterHeight")
		require.NoError(t, err)
		require.Equal(t, uint32(7), binary.LittleEndian.Uint32(got))
	})

	t.Run("above target is rewritten down", func(t *testing.T) {
		bcStore, _, _, _ := newTestStores(t, ctx)
		setPersisterHeight(t, ctx, bcStore, 10)
		spy := newCountingSetStateStore(bcStore)

		e := &env{logger: logger(), blockchainStore: spy}
		require.NoError(t, e.resetBlockPersisterHeight(ctx, &preflightResult{target: 4}))

		require.Equal(t, 1, spy.setStateCalls[blockPersisterHeightKey], "exactly one write down to the target")

		got, err := bcStore.GetState(ctx, "BlockPersisterHeight")
		require.NoError(t, err)
		require.Len(t, got, 4)
		require.Equal(t, uint32(4), binary.LittleEndian.Uint32(got),
			"persister height ahead of the target must be rewritten down to it")
	})

	t.Run("payload shorter than 4 bytes is left untouched", func(t *testing.T) {
		bcStore, _, _, _ := newTestStores(t, ctx)
		require.NoError(t, bcStore.SetState(ctx, "BlockPersisterHeight", []byte{0x01, 0x02}))
		spy := newCountingSetStateStore(bcStore)

		e := &env{logger: logger(), blockchainStore: spy}
		require.NoError(t, e.resetBlockPersisterHeight(ctx, &preflightResult{target: 4}),
			"an undecodable payload must not fail the run")

		require.Zero(t, spy.setStateCalls[blockPersisterHeightKey])

		got, err := bcStore.GetState(ctx, "BlockPersisterHeight")
		require.NoError(t, err)
		require.Equal(t, []byte{0x01, 0x02}, got,
			"cannot prove a write would be downward, so leave the value alone")
	})

	t.Run("empty payload is left untouched", func(t *testing.T) {
		bcStore, _, _, _ := newTestStores(t, ctx)
		require.NoError(t, bcStore.SetState(ctx, "BlockPersisterHeight", []byte{}))
		spy := newCountingSetStateStore(bcStore)

		e := &env{logger: logger(), blockchainStore: spy}
		require.NoError(t, e.resetBlockPersisterHeight(ctx, &preflightResult{target: 4}))

		require.Zero(t, spy.setStateCalls[blockPersisterHeightKey])

		got, err := bcStore.GetState(ctx, "BlockPersisterHeight")
		require.NoError(t, err)
		require.Empty(t, got, "a present-but-empty row is as undecodable as a short one")
	})

	t.Run("absent key stays absent", func(t *testing.T) {
		bcStore, _, _, _ := newTestStores(t, ctx)
		spy := newCountingSetStateStore(bcStore)

		e := &env{logger: logger(), blockchainStore: spy}
		require.NoError(t, e.resetBlockPersisterHeight(ctx, &preflightResult{target: 4}))

		require.Zero(t, spy.setStateCalls[blockPersisterHeightKey])

		_, err := bcStore.GetState(ctx, "BlockPersisterHeight")
		require.Error(t, err, "the tool must not create the key when it was never set")
	})
}

// TestPreflight_CapturesBlockPersisterHeight checks that preflight records the
// pre-run persister height so Phase 4 has a baseline to compare against.
func TestPreflight_CapturesBlockPersisterHeight(t *testing.T) {
	ctx := context.Background()
	bcStore, utxoStore, subtreeStore, tSettings := newTestStores(t, ctx)

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	buildLinearChain(t, ctx, bcStore, 4, bits, tSettings.ChainCfgParams.GenesisHash)
	require.NoError(t, bcStore.SetFSMState(ctx, "IDLE"))
	setPersisterHeight(t, ctx, bcStore, 1)

	e := &env{
		logger:          logger(),
		settings:        tSettings,
		blockchainStore: bcStore,
		utxoStore:       utxoStore,
		subtreeStore:    subtreeStore,
		opts:            Options{TargetHeight: 2, AssumeYes: true},
		stats:           &Stats{},
		concurrency:     1,
	}

	pf, err := e.preflight(ctx)
	require.NoError(t, err)
	require.True(t, pf.persisterHeightKnown, "a decodable key must be reported as known")
	require.Equal(t, uint32(1), pf.persisterHeight)
}

// TestPreflight_MissingBlockPersisterHeightIsNotKnown checks the absent-key
// path: preflight succeeds and reports the value as unknown.
func TestPreflight_MissingBlockPersisterHeightIsNotKnown(t *testing.T) {
	ctx := context.Background()
	bcStore, utxoStore, subtreeStore, tSettings := newTestStores(t, ctx)

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	buildLinearChain(t, ctx, bcStore, 4, bits, tSettings.ChainCfgParams.GenesisHash)
	require.NoError(t, bcStore.SetFSMState(ctx, "IDLE"))

	e := &env{
		logger:          logger(),
		settings:        tSettings,
		blockchainStore: bcStore,
		utxoStore:       utxoStore,
		subtreeStore:    subtreeStore,
		opts:            Options{TargetHeight: 2, AssumeYes: true},
		stats:           &Stats{},
		concurrency:     1,
	}

	pf, err := e.preflight(ctx)
	require.NoError(t, err)
	require.False(t, pf.persisterHeightKnown)
	require.Zero(t, pf.persisterHeight)
}

// TestPreflight_BlockPersisterHeightReadErrorFailsFast checks that a genuine
// storage failure on the state read aborts during preflight, before Phase 0
// touches any store — rather than surfacing in Phase 3 with phases 0-2 already
// applied. The injected failure is scoped to the persister key so the test
// cannot pass by aborting on some other state read.
func TestPreflight_BlockPersisterHeightReadErrorFailsFast(t *testing.T) {
	ctx := context.Background()
	bcStore, utxoStore, subtreeStore, tSettings := newTestStores(t, ctx)

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	buildLinearChain(t, ctx, bcStore, 4, bits, tSettings.ChainCfgParams.GenesisHash)
	require.NoError(t, bcStore.SetFSMState(ctx, "IDLE"))

	simulated := errors.NewStorageError("simulated db read failure")
	wrapped := &errOnGetStateStore{Store: bcStore, getStateErr: simulated, onlyKey: blockPersisterHeightKey}

	e := &env{
		logger:          logger(),
		settings:        tSettings,
		blockchainStore: wrapped,
		utxoStore:       utxoStore,
		subtreeStore:    subtreeStore,
		opts:            Options{TargetHeight: 2, AssumeYes: true},
		stats:           &Stats{},
		concurrency:     1,
	}

	_, err = e.preflight(ctx)
	require.Error(t, err)
	require.True(t, errors.Is(err, simulated), "real read failure must abort preflight, got %v", err)
}

// TestPhase4Verify_BlockPersisterHeightIncrease asserts the --verify guard
// catches a persister height that went up during the run. The fixture points
// the target at the existing tip so the best-block checks pass and only the
// persister comparison decides the outcome.
func TestPhase4Verify_BlockPersisterHeightIncrease(t *testing.T) {
	ctx := context.Background()

	t.Run("increase fails verify", func(t *testing.T) {
		bcStore, _, _, tSettings := newTestStores(t, ctx)

		bits, err := model.NewNBitFromString("207fffff")
		require.NoError(t, err)

		blocks := buildLinearChain(t, ctx, bcStore, 4, bits, tSettings.ChainCfgParams.GenesisHash)
		setPersisterHeight(t, ctx, bcStore, 2)

		e := &env{logger: logger(), blockchainStore: bcStore}
		pf := &preflightResult{
			target:               4,
			targetHash:           blocks[3].Hash(),
			persisterHeight:      1,
			persisterHeightKnown: true,
		}

		err = e.phase4Verify(ctx, pf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "increased from 1 to 2")
	})

	t.Run("unchanged passes verify", func(t *testing.T) {
		bcStore, _, _, tSettings := newTestStores(t, ctx)

		bits, err := model.NewNBitFromString("207fffff")
		require.NoError(t, err)

		blocks := buildLinearChain(t, ctx, bcStore, 4, bits, tSettings.ChainCfgParams.GenesisHash)
		setPersisterHeight(t, ctx, bcStore, 1)

		e := &env{logger: logger(), blockchainStore: bcStore}
		pf := &preflightResult{
			target:               4,
			targetHash:           blocks[3].Hash(),
			persisterHeight:      1,
			persisterHeightKnown: true,
		}

		require.NoError(t, e.phase4Verify(ctx, pf))
	})

	t.Run("unreadable before and set after fails verify", func(t *testing.T) {
		bcStore, _, _, tSettings := newTestStores(t, ctx)

		bits, err := model.NewNBitFromString("207fffff")
		require.NoError(t, err)

		blocks := buildLinearChain(t, ctx, bcStore, 4, bits, tSettings.ChainCfgParams.GenesisHash)
		setPersisterHeight(t, ctx, bcStore, 9999)

		e := &env{logger: logger(), blockchainStore: bcStore}
		pf := &preflightResult{
			target:               4,
			targetHash:           blocks[3].Hash(),
			persisterHeightKnown: false,
		}

		// An unreadable baseline is still a baseline: this tool never creates the
		// key, so a value appearing across the run is an anomaly, not a no-op.
		err = e.phase4Verify(ctx, pf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "was unreadable before the run and is now 9999")
	})

	t.Run("force-not-idle downgrades a concurrent increase to a warning", func(t *testing.T) {
		bcStore, _, _, tSettings := newTestStores(t, ctx)

		bits, err := model.NewNBitFromString("207fffff")
		require.NoError(t, err)

		blocks := buildLinearChain(t, ctx, bcStore, 4, bits, tSettings.ChainCfgParams.GenesisHash)
		setPersisterHeight(t, ctx, bcStore, 2)

		log := &capturingLogger{}
		e := &env{logger: log, blockchainStore: bcStore, opts: Options{ForceNotIdle: true}}
		pf := &preflightResult{
			target:               4,
			targetHash:           blocks[3].Hash(),
			persisterHeight:      1,
			persisterHeightKnown: true,
		}

		// With the node left running, block persister raises this key itself. A
		// hard failure here would condemn a run that actually succeeded, after
		// every mutation is already applied.
		require.NoError(t, e.phase4Verify(ctx, pf))
		require.Contains(t, log.warnText(), "--force-not-idle")
	})

	t.Run("value lost during the run is reported as lost, not as zero", func(t *testing.T) {
		bcStore, _, _, tSettings := newTestStores(t, ctx)

		bits, err := model.NewNBitFromString("207fffff")
		require.NoError(t, err)

		blocks := buildLinearChain(t, ctx, bcStore, 4, bits, tSettings.ChainCfgParams.GenesisHash)
		require.NoError(t, bcStore.SetState(ctx, "BlockPersisterHeight", []byte{0x01, 0x02}))

		log := &capturingLogger{}
		e := &env{logger: log, blockchainStore: bcStore}
		pf := &preflightResult{
			target:               4,
			targetHash:           blocks[3].Hash(),
			persisterHeight:      422492,
			persisterHeightKnown: true,
		}

		require.NoError(t, e.phase4Verify(ctx, pf))
		require.Contains(t, log.warnText(), "is no longer readable")
		require.NotContains(t, log.infoText(), "=0 did not increase",
			"an unreadable value must never be reported as a live 0")
	})
}

// TestRewind_PersisterHeightBelowTargetLeftAlone runs the whole pipeline with
// --verify against a node whose persister sits far behind the target — the
// issue #1340 shape. Target equals the tip, so no blocks are deleted and the
// run exercises Phase 0/1/3/4 wiring end to end.
func TestRewind_PersisterHeightBelowTargetLeftAlone(t *testing.T) {
	ctx := context.Background()
	bcStore, utxoStore, subtreeStore, tSettings := newTestStores(t, ctx)

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	blocks := buildLinearChain(t, ctx, bcStore, 4, bits, tSettings.ChainCfgParams.GenesisHash)
	require.NoError(t, bcStore.SetFSMState(ctx, "IDLE"))
	require.NoError(t, setBlockAssemblerState(ctx, bcStore, 4, blocks[3].Header))
	setPersisterHeight(t, ctx, bcStore, 1)

	stats, err := Rewind(ctx, logger(), tSettings, Options{
		TargetHeight: 4,
		AssumeYes:    true,
		Verify:       true,
		Stores: &Stores{
			Blockchain: bcStore,
			UTXO:       utxoStore,
			Subtree:    subtreeStore,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Zero(t, stats.BlocksDeleted)

	got, err := bcStore.GetState(ctx, "BlockPersisterHeight")
	require.NoError(t, err)
	require.Len(t, got, 4)
	require.Equal(t, uint32(1), binary.LittleEndian.Uint32(got),
		"a lagging persister height must survive a full rewind run untouched")
}

// capturingLogger records Infof/Warnf output so tests can assert on the
// operator-facing diagnostics. On the skip path the log IS the deliverable —
// the tool deliberately changes nothing — so it is worth asserting on.
type capturingLogger struct {
	ulogger.TestLogger
	mu    sync.Mutex
	infos []string
	warns []string
}

func (c *capturingLogger) Infof(format string, args ...interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.infos = append(c.infos, fmt.Sprintf(format, args...))
}

func (c *capturingLogger) Warnf(format string, args ...interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.warns = append(c.warns, fmt.Sprintf(format, args...))
}

func (c *capturingLogger) infoText() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.infos, "\n")
}

func (c *capturingLogger) warnText() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.warns, "\n")
}

// TestBlockPersisterGroundTruth derives the persister's real position from the
// blocks table instead of trusting the state key — the same derivation block
// persister itself uses on startup.
func TestBlockPersisterGroundTruth(t *testing.T) {
	ctx := context.Background()

	t.Run("lowest unpersisted block minus one", func(t *testing.T) {
		bcStore, _, _, tSettings := newTestStores(t, ctx)

		bits, err := model.NewNBitFromString("207fffff")
		require.NoError(t, err)

		blocks := buildLinearChain(t, ctx, bcStore, 4, bits, tSettings.ChainCfgParams.GenesisHash)
		require.NoError(t, bcStore.SetBlockPersistedAt(ctx, blocks[0].Hash()))
		require.NoError(t, bcStore.SetBlockPersistedAt(ctx, blocks[1].Hash()))

		e := &env{logger: logger(), blockchainStore: bcStore}
		height, known := e.blockPersisterGroundTruth(ctx)
		require.True(t, known)
		require.Equal(t, uint32(2), height, "blocks 1-2 persisted, lowest pending is 3, so the real position is 2")
	})

	t.Run("nothing pending is not a baseline", func(t *testing.T) {
		bcStore, _, _, tSettings := newTestStores(t, ctx)

		bits, err := model.NewNBitFromString("207fffff")
		require.NoError(t, err)

		blocks := buildLinearChain(t, ctx, bcStore, 2, bits, tSettings.ChainCfgParams.GenesisHash)
		for _, b := range blocks {
			require.NoError(t, bcStore.SetBlockPersistedAt(ctx, b.Hash()))
		}

		e := &env{logger: logger(), blockchainStore: bcStore}
		_, known := e.blockPersisterGroundTruth(ctx)
		require.False(t, known, "with no pending block there is nothing to compare against")
	})
}

// TestPreflight_WarnsOnInflatedAndLaggingPersisterHeight covers the two
// operator-facing diagnostics: a key already inflated past the real position by
// an earlier buggy run (issue 1340 leaves exactly this state behind, and the
// fixed tool cannot repair it), and a persister lagging the target far enough
// that the rewind will not change where it resumes.
func TestPreflight_WarnsOnInflatedAndLaggingPersisterHeight(t *testing.T) {
	ctx := context.Background()

	newEnv := func(t *testing.T, bcStore blockchain_store.Store, utxoStore utxo.Store, subtreeStore *memory.Memory, tSettings *settings.Settings, log *capturingLogger, target int64) *env {
		return &env{
			logger:          log,
			settings:        tSettings,
			blockchainStore: bcStore,
			utxoStore:       utxoStore,
			subtreeStore:    subtreeStore,
			opts:            Options{TargetHeight: target, AssumeYes: true},
			stats:           &Stats{},
			concurrency:     1,
		}
	}

	t.Run("inflated key is called out", func(t *testing.T) {
		bcStore, utxoStore, subtreeStore, tSettings := newTestStores(t, ctx)

		bits, err := model.NewNBitFromString("207fffff")
		require.NoError(t, err)

		blocks := buildLinearChain(t, ctx, bcStore, 4, bits, tSettings.ChainCfgParams.GenesisHash)
		require.NoError(t, bcStore.SetFSMState(ctx, "IDLE"))
		require.NoError(t, bcStore.SetBlockPersistedAt(ctx, blocks[0].Hash()))
		// Real position is 1, but the key claims 4 — the shape a pre-fix run leaves.
		setPersisterHeight(t, ctx, bcStore, 4)

		log := &capturingLogger{}
		pf, err := newEnv(t, bcStore, utxoStore, subtreeStore, tSettings, log, 3).preflight(ctx)
		require.NoError(t, err)
		require.Equal(t, uint32(4), pf.persisterHeight)

		require.Contains(t, log.warnText(), "is ahead of the real block persister position 1",
			"an inflated key must be reported, not silently accepted")
	})

	t.Run("large lag is called out", func(t *testing.T) {
		bcStore, utxoStore, subtreeStore, tSettings := newTestStores(t, ctx)

		bits, err := model.NewNBitFromString("207fffff")
		require.NoError(t, err)

		buildLinearChain(t, ctx, bcStore, 150, bits, tSettings.ChainCfgParams.GenesisHash)
		require.NoError(t, bcStore.SetFSMState(ctx, "IDLE"))
		// Nothing persisted at all: real position 0, target 149 — a 149-block lag.
		setPersisterHeight(t, ctx, bcStore, 0)

		log := &capturingLogger{}
		_, err = newEnv(t, bcStore, utxoStore, subtreeStore, tSettings, log, 149).preflight(ctx)
		require.NoError(t, err)

		require.Contains(t, log.warnText(), "blocks behind the target",
			"a persister lagging past coinbase maturity must warn")
	})

	t.Run("healthy persister stays quiet and the decision is logged", func(t *testing.T) {
		bcStore, utxoStore, subtreeStore, tSettings := newTestStores(t, ctx)

		bits, err := model.NewNBitFromString("207fffff")
		require.NoError(t, err)

		blocks := buildLinearChain(t, ctx, bcStore, 4, bits, tSettings.ChainCfgParams.GenesisHash)
		require.NoError(t, bcStore.SetFSMState(ctx, "IDLE"))
		for _, b := range blocks[:3] {
			require.NoError(t, bcStore.SetBlockPersistedAt(ctx, b.Hash()))
		}
		// Real position 3, key agrees, target 2 → the write-down case.
		setPersisterHeight(t, ctx, bcStore, 3)

		log := &capturingLogger{}
		_, err = newEnv(t, bcStore, utxoStore, subtreeStore, tSettings, log, 2).preflight(ctx)
		require.NoError(t, err)

		require.Empty(t, log.warnText(), "a healthy, slightly-ahead persister is not an anomaly")
		require.Contains(t, log.infoText(), "it will be rewritten down to the target",
			"the preflight log must state the decision, not just the numbers")
	})
}

// -----------------------------------------------------------------------------
// Production store construction (resolveStores)
// -----------------------------------------------------------------------------

// newFileStoreSettings points all three stores at throwaway backends: real
// file:// subtree storage in a temp dir, in-memory SQL for the other two. The
// subtree URL deliberately carries no query parameters, matching the resolved
// config on a real deployment (subtreestore=file:///data/.../subtreestore),
// where the node's default hashPrefix=2 applies.
func newFileStoreSettings(t *testing.T, subtreeQuery string) (*settings.Settings, *url.URL) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	bcURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	tSettings.BlockChain.StoreURL = bcURL

	utxoURL, err := url.Parse("sqlitememory:///resolve-stores-utxo")
	require.NoError(t, err)
	tSettings.UtxoStore.UtxoStore = utxoURL

	raw := "file://" + t.TempDir()
	if subtreeQuery != "" {
		raw += "?" + subtreeQuery
	}

	subtreeURL, err := url.Parse(raw)
	require.NoError(t, err)
	tSettings.SubtreeValidation.SubtreeStore = subtreeURL

	return tSettings, subtreeURL
}

// TestResolveStores_SubtreeStoreReadsWhatTheNodeWrote is the regression test for
// the bug where the tool opened the subtree store without options.WithHashPrefix.
// blob.NewStore does not parse hashPrefix from the URL query (stores/blob/factory.go
// reads only batch, logger, sizeInBytes, writeKeys), so without the option
// Options.HashPrefix stayed 0, CalculatePrefix returned "", and the tool resolved
// flat filenames while the node wrote hash-sharded ones. Every read missed with
// NOT_FOUND and Phase 2 could not rewind any file:// deployment.
//
// The existing Rewind tests all inject memory.New() via Options.Stores, which
// bypasses resolveStores entirely — so none of them could catch this.
func TestResolveStores_SubtreeStoreReadsWhatTheNodeWrote(t *testing.T) {
	ctx := context.Background()
	tSettings, subtreeURL := newFileStoreSettings(t, "")

	hash := chainhash.HashH([]byte("subtree-hashprefix-regression"))
	payload := []byte("subtree bytes")

	// Write through a store built the way the daemon builds it
	// (daemon/daemon_stores.go:454-479): hashPrefix 2, so the blob lands in a
	// two-character shard directory.
	nodeStore, err := blob.NewStore(logger(), subtreeURL, options.WithHashPrefix(2))
	require.NoError(t, err)
	require.NoError(t, nodeStore.Set(ctx, hash[:], fileformat.FileTypeSubtree, payload))

	// Confirm the fixture really is sharded — otherwise this test would pass
	// against a flat layout too and prove nothing.
	shard := filepath.Join(subtreeURL.Path, hash.String()[:2])
	require.DirExists(t, shard, "fixture must be hash-sharded for this test to mean anything")

	// The tool's own construction path must find it.
	stores, ownedByUs, err := resolveStores(ctx, logger(), tSettings, Options{})
	require.NoError(t, err)
	require.True(t, ownedByUs, "resolveStores opened these, so the caller owns closing them")

	// ownedByUs is the close-me signal; honour it.
	t.Cleanup(func() {
		require.NoError(t, stores.Subtree.Close(ctx))
	})

	exists, err := stores.Subtree.Exists(ctx, hash[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, exists,
		"resolveStores must open the subtree store with the node's hashPrefix; "+
			"without it every subtree read misses and Phase 2 cannot rewind")

	got, err := stores.Subtree.Get(ctx, hash[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.Equal(t, payload, got)

	// And the deletion Phase 2 performs must land on the same path.
	require.NoError(t, stores.Subtree.Del(ctx, hash[:], fileformat.FileTypeSubtree))

	exists, err = stores.Subtree.Exists(ctx, hash[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.False(t, exists, "Del must remove the sharded blob, not a flat path that never existed")
}

// TestNewSubtreeStore_FlatLayoutMissesShardedBlob pins the mechanism itself, so
// the regression above cannot be "fixed" by something that merely happens to
// work. A store opened without the option — the old behaviour — cannot see a
// blob the node wrote.
func TestNewSubtreeStore_FlatLayoutMissesShardedBlob(t *testing.T) {
	ctx := context.Background()
	_, subtreeURL := newFileStoreSettings(t, "")

	hash := chainhash.HashH([]byte("flat-vs-sharded"))

	nodeStore, err := blob.NewStore(logger(), subtreeURL, options.WithHashPrefix(2))
	require.NoError(t, err)
	require.NoError(t, nodeStore.Set(ctx, hash[:], fileformat.FileTypeSubtree, []byte("x")))

	flatStore, err := blob.NewStore(logger(), subtreeURL)
	require.NoError(t, err)

	exists, err := flatStore.Exists(ctx, hash[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.False(t, exists,
		"a store opened without WithHashPrefix looks at a flat path — this is the bug, "+
			"and blob.NewStore never reads hashPrefix from the URL query")
}

// TestSubtreeHashPrefix covers the tool's own prefix resolution directly.
//
// This deliberately does NOT assert on file layout. The file backend parses
// ?hashPrefix for itself and applies it after the store options
// (stores/blob/file/file.go:446-453), so a layout-based test passes whether or
// not this code resolves anything — it cannot discriminate. Asserting on the
// resolved value pins the tool's logic for every backend, including s3, which
// does not read the query at all.
func TestSubtreeHashPrefix(t *testing.T) {
	t.Run("defaults to the daemon's 2 when the URL carries no query", func(t *testing.T) {
		// This is every shipped subtreestore setting: a bare file:// URL.
		u, err := url.Parse("file:///data/teranode/subtreestore")
		require.NoError(t, err)

		prefix, err := subtreeHashPrefix(u)
		require.NoError(t, err)
		require.Equal(t, 2, prefix, "must match daemon.GetSubtreeStore's default")
	})

	t.Run("honours an explicit ?hashPrefix", func(t *testing.T) {
		for _, tc := range []struct {
			query string
			want  int
		}{
			{"hashPrefix=4", 4},
			{"hashPrefix=0", 0},
			{"hashPrefix=-2", -2}, // negative means suffix; parity with the daemon
		} {
			u, err := url.Parse("file:///data/teranode/subtreestore?" + tc.query)
			require.NoError(t, err)

			prefix, err := subtreeHashPrefix(u)
			require.NoError(t, err)
			require.Equal(t, tc.want, prefix, "query %q", tc.query)
		}
	})

	t.Run("rejects a non-numeric ?hashPrefix", func(t *testing.T) {
		u, err := url.Parse("file:///data/teranode/subtreestore?hashPrefix=banana")
		require.NoError(t, err)

		_, err = subtreeHashPrefix(u)
		require.Error(t, err)
		// Assert on this tool's message, not the substring "hashPrefix": the file
		// backend also rejects "banana" with a message containing that word, so a
		// looser assertion would pass even with this parse deleted.
		require.Contains(t, err.Error(), "subtreestore hashPrefix config error")
	})
}

// TestNewSubtreeStore_RejectsBadHashPrefix checks the error surfaces through the
// constructor rather than being swallowed or replaced by the backend's own.
func TestNewSubtreeStore_RejectsBadHashPrefix(t *testing.T) {
	tSettings, _ := newFileStoreSettings(t, "hashPrefix=banana")

	_, err := newSubtreeStore(logger(), tSettings)
	require.Error(t, err)
	require.Contains(t, err.Error(), "subtreestore hashPrefix config error")
}

// TestNewSubtreeStore_RejectsNilURL covers the guard added with this fix:
// blob.NewStore dereferences storeURL.Scheme before any nil check, so an
// unconfigured subtreestore used to panic.
func TestNewSubtreeStore_RejectsNilURL(t *testing.T) {
	tSettings, _ := newFileStoreSettings(t, "")
	tSettings.SubtreeValidation.SubtreeStore = nil

	_, err := newSubtreeStore(logger(), tSettings)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not configured")
}
