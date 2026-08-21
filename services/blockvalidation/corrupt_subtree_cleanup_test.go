package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// cleanupTestBlock builds a block referencing the given subtree hashes; only the Subtrees field is
// used by removePeerSuppliedSubtreeToCheck (Header is present so block.Hash() in the log line works).
func cleanupTestBlock(t *testing.T, subtreeHashes ...*chainhash.Hash) *model.Block {
	t.Helper()

	nBits, err := model.NewNBitFromString("2000ffff")
	require.NoError(t, err)

	return &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      1234567890,
			Bits:           *nBits,
			Nonce:          0,
		},
		Subtrees: subtreeHashes,
	}
}

// TestRemovePeerSuppliedSubtreeToCheck pins the RUNNING corrupt-path blob cleanup
// (bitcoin-sv/teranode#4692). After a corrupt-body verdict the branch must drop ONLY the unvalidated
// peer-supplied FileTypeSubtreeToCheck marker so a retry re-fetches instead of re-reading the body
// that just failed the block-level merkle check — while NEVER touching FileTypeSubtreeData,
// FileTypeSubtree or FileTypeSubtreeMeta, which can be promoted-permanent / asset-served data of an
// already-persisted block.
func TestRemovePeerSuppliedSubtreeToCheck(t *testing.T) {
	subtreeHash := chainhash.Hash{0x01}
	block := cleanupTestBlock(t, &subtreeHash)

	seed := func(t *testing.T, store *blobmemory.Memory, types ...fileformat.FileType) {
		t.Helper()
		for _, ft := range types {
			require.NoError(t, store.Set(context.Background(), subtreeHash[:], ft, []byte{0x00}))
		}
	}
	exists := func(t *testing.T, store *blobmemory.Memory, ft fileformat.FileType) bool {
		t.Helper()
		ok, err := store.Exists(context.Background(), subtreeHash[:], ft)
		require.NoError(t, err)
		return ok
	}

	// Selectivity: deletes only SubtreeToCheck, preserves the three promoted/validated types.
	// Mutation (widen the helper to delete SubtreeData/Subtree/SubtreeMeta) reddens the preservation
	// assertions; mutation (helper stops deleting) reddens the absent assertion.
	t.Run("deletes only SubtreeToCheck, preserves the promoted/validated types", func(t *testing.T) {
		store := blobmemory.New()
		seed(t, store, fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtreeData,
			fileformat.FileTypeSubtree, fileformat.FileTypeSubtreeMeta)

		bv := &BlockValidation{logger: ulogger.TestLogger{}, subtreeStore: store}
		require.NoError(t, bv.removePeerSuppliedSubtreeToCheck(context.Background(), block))

		require.False(t, exists(t, store, fileformat.FileTypeSubtreeToCheck),
			"the unvalidated peer-supplied marker must be deleted so a retry re-fetches")
		require.True(t, exists(t, store, fileformat.FileTypeSubtreeData),
			"FileTypeSubtreeData can be promoted-permanent / asset-served — must be preserved")
		require.True(t, exists(t, store, fileformat.FileTypeSubtree),
			"validated FileTypeSubtree must be preserved")
		require.True(t, exists(t, store, fileformat.FileTypeSubtreeMeta),
			"FileTypeSubtreeMeta must be preserved")
	})

	// Fallback case: a validated FileTypeSubtree is present, so after cleanup the local validated
	// blob survives as the retry fallback — no re-fetch needed.
	t.Run("fallback: validated Subtree survives, only SubtreeToCheck removed", func(t *testing.T) {
		store := blobmemory.New()
		seed(t, store, fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtree)

		bv := &BlockValidation{logger: ulogger.TestLogger{}, subtreeStore: store}
		require.NoError(t, bv.removePeerSuppliedSubtreeToCheck(context.Background(), block))

		require.False(t, exists(t, store, fileformat.FileTypeSubtreeToCheck))
		require.True(t, exists(t, store, fileformat.FileTypeSubtree),
			"the validated blob is the local fallback and must remain readable")
	})

	// Refetch case: only the unvalidated marker is present, so after cleanup nothing local remains
	// and the next read must take the fetch path.
	t.Run("refetch: only SubtreeToCheck present -> nothing local survives", func(t *testing.T) {
		store := blobmemory.New()
		seed(t, store, fileformat.FileTypeSubtreeToCheck)

		bv := &BlockValidation{logger: ulogger.TestLogger{}, subtreeStore: store}
		require.NoError(t, bv.removePeerSuppliedSubtreeToCheck(context.Background(), block))

		require.False(t, exists(t, store, fileformat.FileTypeSubtreeToCheck),
			"with no validated blob, the stale marker must be gone so the retry re-fetches")
	})

	// A missing marker is not an error (idempotent on retry).
	t.Run("missing marker is not an error", func(t *testing.T) {
		store := blobmemory.New()
		bv := &BlockValidation{logger: ulogger.TestLogger{}, subtreeStore: store}
		require.NoError(t, bv.removePeerSuppliedSubtreeToCheck(context.Background(), block))
	})
}

// TestValidateBlockWithOptions_RunningCorruptBody_CleansUpOnlySubtreeToCheck drives the
// NON-OPTIMISTIC RUNNING validation path end to end and pins the WIRING of the corrupt-body cleanup
// (bitcoin-sv/teranode#4692): after subtree validation passes but block.Valid returns a corrupt
// verdict (here a block-level merkle-root mismatch), ValidateBlockWithOptions must delete ONLY the
// unvalidated FileTypeSubtreeToCheck blob while preserving FileTypeSubtreeData, FileTypeSubtree and
// FileTypeSubtreeMeta, and must still return the corrupt verdict and strike the serving peer.
//
// Unlike the direct-helper unit test above (which pins selectivity), this test pins that the helper
// is actually CALLED from the corrupt branch. Mutation proof: delete the
// removePeerSuppliedSubtreeToCheck call in the corrupt branch of ValidateBlockWithOptions
// (BlockValidation.go, the `if errors.IsBlockCorrupt(err)` block after block.Valid) — the
// FileTypeSubtreeToCheck blob then survives and the "removed" assertion reddens.
func TestValidateBlockWithOptions_RunningCorruptBody_CleansUpOnlySubtreeToCheck(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	// Reach block.Valid on the non-optimistic path: subtree validation must PASS, so mock
	// CheckBlockSubtrees to return nil. block.Valid then fails the block-level merkle-root check,
	// which is the corrupt source (a body whose subtrees each hash correctly but whose roots do not
	// combine to the header merkle root — exactly the asymmetry the RUNNING cleanup exists to close).
	subtreeVal := &subtreevalidation.MockSubtreeValidation{}
	subtreeVal.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	suite.Server.blockValidation.subtreeValidationClient = subtreeVal

	suite.MockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil).Maybe()
	suite.MockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil).Maybe()
	suite.MockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 99, MinedSet: true}, nil).Maybe()
	suite.MockBlockchain.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil).Maybe()
	suite.MockBlockchain.On("GetBlockHeadersFromHeight", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{}, errors.NewServiceError("not mocked")).Maybe()

	block := buildOneSubtreeBlock(t, suite, 100)
	subtreeHash := block.Subtrees[0]

	// Make the DAA nBits check pass regardless of the fixture's difficulty: return the block's own
	// Bits as the expected work.
	suite.MockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).
		Return(&block.Header.Bits, nil).Maybe()

	// buildOneSubtreeBlock seeds FileTypeSubtreeToCheck + FileTypeSubtreeData. Seed a VALID
	// FileTypeSubtree (a copy of the real serialized subtree, so block.Valid's GetAndValidateSubtrees
	// — which reads FileTypeSubtree first — succeeds and the corrupt verdict comes from the merkle
	// check, not a deserialize error) plus a FileTypeSubtreeMeta marker (not read before the merkle
	// check). Then we can assert all three survive the cleanup.
	realSubtreeBytes, err := suite.Server.subtreeStore.Get(suite.Ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.NoError(t, suite.Server.subtreeStore.Set(suite.Ctx, subtreeHash[:], fileformat.FileTypeSubtree, realSubtreeBytes))
	require.NoError(t, suite.Server.subtreeStore.Set(suite.Ctx, subtreeHash[:], fileformat.FileTypeSubtreeMeta, []byte{0x00}))

	// Zero the header merkle root so block.Valid's CheckMerkleRoot cannot match, then re-mine the
	// nonce so the header still meets its PoW target (buildOneSubtreeBlock leaves PoW stale).
	block.Header.HashMerkleRoot = &chainhash.Hash{}
	for {
		if ok, _, _ := block.Header.HasMetTargetDifficulty(); ok {
			break
		}
		block.Header.Nonce++
	}

	allFour := []fileformat.FileType{
		fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtreeData,
		fileformat.FileTypeSubtree, fileformat.FileTypeSubtreeMeta,
	}
	for _, ft := range allFour {
		present, existsErr := suite.Server.subtreeStore.Exists(suite.Ctx, subtreeHash[:], ft)
		require.NoError(t, existsErr)
		require.True(t, present, "%s must exist before the corrupt verdict", ft)
	}

	rec := &banScoreRecorder{}
	suite.Server.blockValidation.p2pClient = rec

	valErr := suite.Server.blockValidation.ValidateBlockWithOptions(suite.Ctx, block, "http://peer",
		&ValidateBlockOptions{PeerID: "peer-corrupt"})
	require.Error(t, valErr)
	require.True(t, errors.IsBlockCorrupt(valErr), "a block-level merkle mismatch must be corrupt, got: %v", valErr)
	require.False(t, errors.Is(valErr, errors.ErrBlockInvalid), "a corrupt body must never be poisoned invalid")

	require.Equal(t, []string{"peer-corrupt"}, rec.struck(), "the serving peer must be struck for the corrupt body")

	// Only the unvalidated peer-supplied marker is removed by the wiring at the corrupt branch.
	gone, err := suite.Server.subtreeStore.Exists(suite.Ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.False(t, gone, "the RUNNING corrupt branch must delete FileTypeSubtreeToCheck (call at the corrupt branch)")

	for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtreeData, fileformat.FileTypeSubtree, fileformat.FileTypeSubtreeMeta} {
		still, existsErr := suite.Server.subtreeStore.Exists(suite.Ctx, subtreeHash[:], ft)
		require.NoError(t, existsErr)
		require.True(t, still, "%s must be preserved by the RUNNING corrupt cleanup", ft)
	}
}
