package model

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// panicTxMetaStore is a utxo.Store whose data methods panic (nil embedded
// interface), but which reports SupportsOutpointOnlySpend()==true. Passing it
// to Valid proves validOrderAndBlessed never touched the store: any real access
// would nil-pointer panic and fail the test. The capability query is safe to
// answer (Valid calls it for the fee/order-blessed skip gates) and does not
// count as touching UTXO data.
type panicTxMetaStore struct {
	utxo.Store
}

// SupportsOutpointOnlySpend models a store that can run the outpoint-only fast
// path (like the SQL store), so the below-checkpoint skips are eligible.
func (s *panicTxMetaStore) SupportsOutpointOnlySpend() bool { return true }

// minedParentTxMetaStore reports every queried tx as already mined in one fixed
// block (minedParentBlockID). It models the store state a fast-path-synced node
// persists for a below-checkpoint block's parents: order/blessing metadata is
// intact, only fees are zeroed. validOrderAndBlessed resolves parents through
// Get and never reads fees, so it passes against this store even for a fee=0
// (flag-off revalidation) block. Embeds NullStore for the unused methods; only
// Get is exercised on the happy path.
type minedParentTxMetaStore struct {
	*nullstore.NullStore
}

const minedParentBlockID = uint32(7)

func (s *minedParentTxMetaStore) SupportsOutpointOnlySpend() bool { return true }

func (s *minedParentTxMetaStore) Get(_ context.Context, _ *chainhash.Hash, _ ...fields.FieldName) (*meta.Data, error) {
	// Fee left 0 on purpose: the outpoint-only fast path persists fee=0, and this
	// test proves validOrderAndBlessed tolerates that (unlike the fee check).
	return &meta.Data{BlockIDs: []uint32{minedParentBlockID}}, nil
}

// newSkipTestSettings returns settings with the outpoint-only flag set as given,
// a sqlitememory UTXO store URL, and one hardcoded checkpoint at checkpointHeight.
func newSkipTestSettings(t *testing.T, enabled bool, checkpointHeight int32) *settings.Settings {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OutpointOnlyBelowCheckpoint = enabled

	u, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)
	tSettings.UtxoStore.UtxoStore = u

	params := chaincfg.RegressionNetParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: checkpointHeight}}
	tSettings.ChainCfgParams = &params

	return tSettings
}

// TestBlock_SkipOrderAndBlessedBelowCheckpoint_Predicate is the full truth table:
// every conjunct (flag on AND a store that supports the fast path AND the block
// is a confirmed checkpoint ancestor AND a checkpoint exists AND 0 < height <=
// checkpoint) must hold; any one missing keeps the skip OFF (fail-safe). The
// store-support and confirmed-ancestor gates mirror the sibling fee skip in
// checkBlockRewardAndFees so both skips engage on exactly the same blocks.
func TestBlock_SkipOrderAndBlessedBelowCheckpoint_Predicate(t *testing.T) {
	const checkpointHeight = int32(2000)

	tests := []struct {
		name          string
		enabled       bool
		supportsStore bool
		confirmed     bool
		noCheckpts    bool
		nilParams     bool
		height        uint32
		want          bool
	}{
		{name: "flag off, below", enabled: false, supportsStore: true, confirmed: true, height: 1000, want: false},
		{name: "flag on, non-supporting store, below", enabled: true, supportsStore: false, confirmed: true, height: 1000, want: false},
		{name: "flag on, supporting, not confirmed ancestor, below", enabled: true, supportsStore: true, confirmed: false, height: 1000, want: false},
		{name: "flag on, supporting, confirmed, below checkpoint", enabled: true, supportsStore: true, confirmed: true, height: 1000, want: true},
		{name: "flag on, supporting, confirmed, at checkpoint", enabled: true, supportsStore: true, confirmed: true, height: 2000, want: true},
		{name: "flag on, supporting, confirmed, above checkpoint", enabled: true, supportsStore: true, confirmed: true, height: 2001, want: false},
		{name: "flag on, supporting, confirmed, height 0", enabled: true, supportsStore: true, confirmed: true, height: 0, want: false},
		{name: "flag on, supporting, confirmed, no checkpoints", enabled: true, supportsStore: true, confirmed: true, noCheckpts: true, height: 1000, want: false},
		{name: "flag on, supporting, confirmed, nil chain params", enabled: true, supportsStore: true, confirmed: true, nilParams: true, height: 1000, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings := newSkipTestSettings(t, tt.enabled, checkpointHeight)

			if tt.noCheckpts {
				noCp := chaincfg.RegressionNetParams
				noCp.Checkpoints = nil
				tSettings.ChainCfgParams = &noCp
			}

			if tt.nilParams {
				tSettings.ChainCfgParams = nil
			}

			// Store capability replaces the old settings-URL check: a supporting
			// store (panicTxMetaStore reports true) vs a NullStore (reports false).
			var store utxo.Store = &nullstore.NullStore{}
			if tt.supportsStore {
				store = &panicTxMetaStore{}
			}

			b := &Block{Height: tt.height}
			b.SetCheckpointConfirmedAncestor(tt.confirmed)

			require.Equal(t, tt.want, b.skipOrderAndBlessedBelowCheckpoint(tSettings, store),
				"skipOrderAndBlessedBelowCheckpoint height=%d", tt.height)
		})
	}

	t.Run("nil settings", func(t *testing.T) {
		b := &Block{Height: 1000}
		b.SetCheckpointConfirmedAncestor(true)
		require.False(t, b.skipOrderAndBlessedBelowCheckpoint(nil, &panicTxMetaStore{}))
	})

	t.Run("nil store", func(t *testing.T) {
		tSettings := newSkipTestSettings(t, true, checkpointHeight)
		b := &Block{Height: 1000}
		b.SetCheckpointConfirmedAncestor(true)
		require.False(t, b.skipOrderAndBlessedBelowCheckpoint(tSettings, nil))
	})
}

// TestBlock_Valid_SkipRefusedWhenMerkleRootNotChecked is the ChiR3 regression
// guard. The below-checkpoint skip of validOrderAndBlessed is honoured ONLY when
// CheckMerkleRoot (step 8) actually ran and bound the block body to the
// checkpoint-certified header — Block.Valid tracks that as merkleRootChecked.
// When the merkle block did not run (forced here with an empty b.Subtrees, the
// same merkleRootChecked == false state a nil-subtreeStore internal-block caller
// reaches) the skip must be REFUSED even though every policy conjunct holds
// (flag on, supporting store, confirmed ancestor, height below the checkpoint).
//
// The "skip taken once the merkle root has been checked" path is proven by
// TestBlock_MerkleFloor_RejectsTamperingWhileSkipIsActive (Case B). Here we prove
// the opposite: with no merkle check, validOrderAndBlessed genuinely runs. We
// observe that by the side effect only it produces — populating oldBlockIDsMap
// with the tx's parent block reference. A taken skip never runs
// validOrderAndBlessed, so that map would stay empty (exactly what the pre-fix
// code did here: silently skip without ever binding the body to the header).
func TestBlock_Valid_SkipRefusedWhenMerkleRootNotChecked(t *testing.T) {
	const checkpointHeight = int32(2000)
	const blockHeight = uint32(1000) // <= checkpoint

	// Flag ON and all policy conjuncts satisfied: only the missing merkle check
	// should keep the skip from engaging.
	tSettings := newSkipTestSettings(t, true, checkpointHeight)

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	txHash, err := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
	require.NoError(t, err)
	parentHash, err := chainhash.NewHashFromStr("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b")
	require.NoError(t, err)

	// One subtree: coinbase placeholder + one tx whose parent is resolved on chain.
	st, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	require.NoError(t, st.AddNode(*txHash, 0, 100))

	stMeta := subtreepkg.NewSubtreeMeta(st)
	parentInput := &bt.Input{PreviousTxOutIndex: 0}
	require.NoError(t, parentInput.PreviousTxIDAdd(parentHash))
	txInpoints, err := subtreepkg.NewTxInpointsFromInputs([]*bt.Input{parentInput})
	require.NoError(t, err)
	require.NoError(t, stMeta.SetTxInpoints(1, txInpoints))

	hdr := minedHeader(t, st.RootHash())
	block, err := NewBlock(hdr, coinbase, []*chainhash.Hash{st.RootHash()}, 2, 123, blockHeight, 0)
	require.NoError(t, err)
	block.SetCheckpointConfirmedAncestor(true)

	// Force merkleRootChecked == false: with b.Subtrees emptied, Valid's
	// "subtreeStore != nil && len(b.Subtrees) > 0" block (which runs CheckMerkleRoot)
	// is skipped, exactly as it is for a nil-subtreeStore internal-block caller.
	// SubtreeSlices stays populated so checkDuplicateTransactions and
	// validOrderAndBlessed still have a body to work on.
	block.Subtrees = nil
	block.SubtreeSlices = []*subtreepkg.Subtree{st}

	// Real (non-nil) subtree store stocked with the meta so validOrderAndBlessed
	// resolves the tx's parent cleanly rather than dereferencing a nil store.
	blobStore := blobmemory.New()
	storeSubtreeMeta(t, blobStore, st, stMeta)

	oldBlockIDs := txmap.NewSyncedMap[chainhash.Hash, []uint32]()

	// Empty currentBlockHeaderIDs: the parent's reported block (minedParentBlockID)
	// is not among them, so checkParentExistsOnChain returns it as an old-block
	// reference — the observable proof that validOrderAndBlessed actually ran.
	valid, err := block.Valid(
		context.Background(), ulogger.TestLogger{}, blobStore,
		&minedParentTxMetaStore{}, oldBlockIDs, []*BlockHeader{}, []uint32{}, tSettings, nil,
	)
	require.NoError(t, err)
	require.True(t, valid)

	_, hasTransactionsReferencingOldBlocks := txmap.ConvertSyncedMapToUint32Slice(oldBlockIDs)
	require.True(t, hasTransactionsReferencingOldBlocks,
		"validOrderAndBlessed must have run (skip refused) because CheckMerkleRoot did not run")
}

// buildSubtreeAndMerkleRoot constructs a one-subtree block body and returns:
//   - the subtree (with coinbase placeholder at index 0 plus txHash at index 1)
//   - the correct HashMerkleRoot for the given coinbase transaction
//
// The root is computed the same way CheckMerkleRoot does: replace the coinbase
// placeholder with the coinbase tx hash, then take the subtree's root.
func buildSubtreeAndMerkleRoot(t *testing.T, coinbase *bt.Tx, txHash chainhash.Hash) (*subtreepkg.Subtree, *chainhash.Hash) {
	t.Helper()

	st, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	require.NoError(t, st.AddNode(txHash, 1, 100))

	// Compute the merkle root as CheckMerkleRoot does: replace coinbase placeholder
	// with the actual coinbase tx hash and return the resulting root.
	merkleRoot, err := st.RootHashWithReplaceRootNode(
		coinbase.TxIDChainHash(),
		0,
		uint64(coinbase.Size()), // nolint: gosec
	)
	require.NoError(t, err)

	return st, merkleRoot
}

// storeSubtree serializes st and writes it into blobStore under the key
// st.RootHash() with file type FileTypeSubtree, mimicking what the block
// persister writes before Valid() is called with a real subtree store.
func storeSubtree(t *testing.T, blobStore *blobmemory.Memory, st *subtreepkg.Subtree) {
	t.Helper()

	subtreeBytes, err := st.Serialize()
	require.NoError(t, err)

	err = blobStore.Set(
		context.Background(),
		st.RootHash()[:],
		fileformat.FileTypeSubtree,
		subtreeBytes,
	)
	require.NoError(t, err)
}

// storeSubtreeMeta serializes st's metadata and writes it into blobStore under
// the subtree root hash with file type FileTypeSubtreeMeta, mimicking what the
// subtree validator/block persister writes before Valid() runs
// validOrderAndBlessed with a real subtree store.
func storeSubtreeMeta(t *testing.T, blobStore *blobmemory.Memory, st *subtreepkg.Subtree, stMeta *subtreepkg.Meta) {
	t.Helper()

	metaBytes, err := stMeta.Serialize()
	require.NoError(t, err)

	err = blobStore.Set(
		context.Background(),
		st.RootHash()[:],
		fileformat.FileTypeSubtreeMeta,
		metaBytes,
	)
	require.NoError(t, err)
}

// minedHeader builds a BlockHeader with nBits=207fffff and the given merkle
// root, then increments Nonce until HasMetTargetDifficulty passes.
// Version is kept at 1 to bypass BIP-34 coinbase-height extraction.
func minedHeader(t *testing.T, merkleRoot *chainhash.Hash) *BlockHeader {
	t.Helper()

	prevHash := chainhash.Hash{} // all-zero prev for a standalone test block
	nBits, err := NewNBitFromString("207fffff")
	require.NoError(t, err)

	hdr := &BlockHeader{
		Version:        1,
		HashPrevBlock:  &prevHash,
		HashMerkleRoot: merkleRoot,
		Timestamp:      1296688602, // fixed past timestamp used across model tests
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		ok, _, _ := hdr.HasMetTargetDifficulty()
		if ok {
			break
		}
		hdr.Nonce++
	}

	return hdr
}

// TestBlock_MerkleFloor_RejectsTamperingWhileSkipIsActive proves that
// CheckMerkleRoot (the integrity floor in steps 1-11 of Block.Valid) still
// runs and still rejects a tampered block body even when the
// below-checkpoint skip is active.
//
// Case A — tampered header: HashMerkleRoot deliberately wrong → Valid returns
// false with an error whose message contains "merkle".
//
// Case B — correct header + panicTxMetaStore: Valid returns true, proving that
// validOrderAndBlessed was skipped (panicTxMetaStore would panic on any
// access) while the full subtree/merkle section ran against a real blob store.
// This strictly supersedes the nil-subtreeStore guarantee in
// TestBlock_Valid_SkipsValidOrderAndBlessedBelowCheckpoint.
func TestBlock_MerkleFloor_RejectsTamperingWhileSkipIsActive(t *testing.T) {
	const checkpointHeight = int32(2000)
	const blockHeight = uint32(1000) // <= checkpoint

	tSettings := newSkipTestSettings(t, true, checkpointHeight)

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	txHash, err := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
	require.NoError(t, err)

	st, correctMerkleRoot := buildSubtreeAndMerkleRoot(t, coinbase, *txHash)

	t.Run("Case A: tampered merkle root is rejected even with skip active", func(t *testing.T) {
		// Flip the first byte of the correct root to produce a wrong hash.
		tampered := *correctMerkleRoot
		tampered[0] ^= 0xff

		hdr := minedHeader(t, &tampered)

		subtreeRootHash := st.RootHash()
		block, err := NewBlock(hdr, coinbase, []*chainhash.Hash{subtreeRootHash}, 2, 123, blockHeight, 0)
		require.NoError(t, err)
		block.SetCheckpointConfirmedAncestor(true)

		blobStore := blobmemory.New()
		storeSubtree(t, blobStore, st)

		oldBlockIDs := txmap.NewSyncedMap[chainhash.Hash, []uint32]()

		valid, valErr := block.Valid(
			context.Background(), ulogger.TestLogger{}, blobStore,
			&panicTxMetaStore{}, oldBlockIDs, []*BlockHeader{}, []uint32{}, tSettings, nil,
		)
		require.Error(t, valErr, "expected an error for tampered merkle root")
		require.False(t, valid, "expected valid=false for tampered merkle root")
		require.Contains(t, valErr.Error(), "merkle", "error must mention merkle mismatch")
	})

	t.Run("Case B: correct merkle root passes floor check while validOrderAndBlessed is skipped", func(t *testing.T) {
		hdr := minedHeader(t, correctMerkleRoot)

		subtreeRootHash := st.RootHash()
		block, err := NewBlock(hdr, coinbase, []*chainhash.Hash{subtreeRootHash}, 2, 123, blockHeight, 0)
		require.NoError(t, err)
		block.SetCheckpointConfirmedAncestor(true)

		blobStore := blobmemory.New()
		storeSubtree(t, blobStore, st)

		oldBlockIDs := txmap.NewSyncedMap[chainhash.Hash, []uint32]()

		// panicTxMetaStore panics on any method call; if validOrderAndBlessed
		// ran it would touch the store and the test would panic (RED without the
		// gate, GREEN with it).
		valid, valErr := block.Valid(
			context.Background(), ulogger.TestLogger{}, blobStore,
			&panicTxMetaStore{}, oldBlockIDs, []*BlockHeader{}, []uint32{}, tSettings, nil,
		)
		require.NoError(t, valErr)
		require.True(t, valid, "expected valid=true: correct merkle, skip active, store floor exercised")
	})
}

// TestBlock_Valid_OrderAndBlessedPassesOnFlagOffRevalidation is the regression
// guard for the safe asymmetry between the two below-checkpoint skips.
//
// The sibling fee skip (checkBlockRewardAndFees) is deliberately NOT gated on
// the OutpointOnlyBelowCheckpoint flag: a fast-path-synced block persists
// subtree fees as 0, so a flag-gated fee skip would recompute
// coinbaseOutput(subsidy+realFees) > subtreeFees(0)+subsidy on later
// revalidation and wrongly reject a genuinely-valid, checkpoint-pinned block.
//
// The order-blessed skip IS additionally gated on the flag, which is only safe
// because validOrderAndBlessed has no equivalent fee=0 hazard: it reads
// persisted subtree meta (parent tx hashes / inpoints) and parent existence on
// chain, never fees. This test proves that — with the flag OFF so the skip is
// disabled and validOrderAndBlessed actually runs — a fast-path-shaped block
// (subtree Fees == 0, parents already mined) still validates.
func TestBlock_Valid_OrderAndBlessedPassesOnFlagOffRevalidation(t *testing.T) {
	const checkpointHeight = int32(2000)
	const blockHeight = uint32(1000) // <= checkpoint

	// Flag OFF: the skip is disabled, so validOrderAndBlessed runs in full.
	tSettings := newSkipTestSettings(t, false, checkpointHeight)

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	txHash, err := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
	require.NoError(t, err)

	// The block's single non-coinbase tx spends a parent that is not in this
	// block; minedParentTxMetaStore reports that parent as already mined in a
	// recent block, which we advertise via currentBlockHeaderIDs.
	parentHash, err := chainhash.NewHashFromStr("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b")
	require.NoError(t, err)

	// One subtree: coinbase placeholder at index 0, one tx at index 1 with fee 0
	// (exactly what the outpoint-only fast path persists).
	st, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	require.NoError(t, st.AddNode(*txHash, 0, 100))
	require.Equal(t, uint64(0), st.Fees, "fast-path-shaped: subtree fees must be 0")

	// Subtree meta records the tx's parent inpoint so validOrderAndBlessed can
	// resolve the parent on chain.
	stMeta := subtreepkg.NewSubtreeMeta(st)
	parentInput := &bt.Input{PreviousTxOutIndex: 0}
	require.NoError(t, parentInput.PreviousTxIDAdd(parentHash))
	txInpoints, err := subtreepkg.NewTxInpointsFromInputs([]*bt.Input{parentInput})
	require.NoError(t, err)
	require.NoError(t, stMeta.SetTxInpoints(1, txInpoints))

	// Correct merkle root, computed the way CheckMerkleRoot does.
	merkleRoot, err := st.RootHashWithReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size())) // nolint: gosec
	require.NoError(t, err)

	hdr := minedHeader(t, merkleRoot)

	subtreeRootHash := st.RootHash()
	block, err := NewBlock(hdr, coinbase, []*chainhash.Hash{subtreeRootHash}, 2, 123, blockHeight, 0)
	require.NoError(t, err)
	block.SetCheckpointConfirmedAncestor(true)

	// Real subtree store stocked with both the subtree and its meta.
	blobStore := blobmemory.New()
	storeSubtree(t, blobStore, st)
	storeSubtreeMeta(t, blobStore, st, stMeta)

	oldBlockIDs := txmap.NewSyncedMap[chainhash.Hash, []uint32]()

	// currentBlockHeaderIDs advertises the parent's mined block as on our chain,
	// so checkParentExistsOnChain resolves it to exactly one recent block.
	valid, err := block.Valid(
		context.Background(), ulogger.TestLogger{}, blobStore,
		&minedParentTxMetaStore{}, oldBlockIDs, []*BlockHeader{}, []uint32{minedParentBlockID}, tSettings, nil,
	)
	require.NoError(t, err, "validOrderAndBlessed must pass on a fee=0 flag-off revalidation")
	require.True(t, valid)
}
