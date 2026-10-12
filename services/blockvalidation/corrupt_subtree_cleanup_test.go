package blockvalidation

import (
	"context"
	"io"
	"net/url"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
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
// (bitcoin-sv/teranode#4692). The helper is called from ONE branch — the subtree-validation corrupt
// verdict — and it must drop ONLY the unvalidated peer-supplied FileTypeSubtreeToCheck marker, while
// NEVER touching FileTypeSubtreeData, FileTypeSubtree or FileTypeSubtreeMeta, which can be
// promoted-permanent / asset-served data of an already-persisted block.
//
// The reason that one branch needs it is the DUPLICATE-SCAN ASYMMETRY, not the merkle check: the
// fetch side stores FileTypeSubtreeToCheck after verifying only that the bytes hash to the requested
// root, and a CVE-2012-2459 duplicate-last mutation preserves that root — so the blob can hold a
// mutated node list under an honest hash. ValidateSubtreeInternal's own duplicate scan catches it
// BEFORE storeSubtreeFiles writes the scanned FileTypeSubtree, so on that branch the mutated fallback
// is the only local copy and every retry re-reads it until its delete-at height lapses. The
// block.Valid merkle branch is the opposite case and performs no cleanup at all — see
// TestValidateBlockWithOptions_RunningMerkleCorruptBody_DeletesNothing.
//
// The selectivity sub-tests pass a nil presentBefore, i.e. "nothing was pre-existing", which is the
// unbounded behaviour they were written against and still the correct semantics for a nil map. The
// two pre-existence sub-tests at the end pin the bound itself.
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
		require.NoError(t, bv.removePeerSuppliedSubtreeToCheck(context.Background(), block, nil))

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
		require.NoError(t, bv.removePeerSuppliedSubtreeToCheck(context.Background(), block, nil))

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
		require.NoError(t, bv.removePeerSuppliedSubtreeToCheck(context.Background(), block, nil))

		require.False(t, exists(t, store, fileformat.FileTypeSubtreeToCheck),
			"with no validated blob, the stale marker must be gone so the retry re-fetches")
	})

	// A missing marker is not an error (idempotent on retry).
	t.Run("missing marker is not an error", func(t *testing.T) {
		store := blobmemory.New()
		bv := &BlockValidation{logger: ulogger.TestLogger{}, subtreeStore: store}
		require.NoError(t, bv.removePeerSuppliedSubtreeToCheck(context.Background(), block, nil))
	})

	// THE BOUND (bitcoin-sv/teranode#4692): a hash recorded in presentBefore had a local copy before
	// this attempt started, so CheckBlockSubtrees read it rather than writing it and it is not ours
	// to delete. This is what stops a doctored body naming a concurrently-validating block's subtree
	// hashes and having its blobs deleted.
	// Mutation proof: drop the presentBefore filter in the helper and this sub-test reddens.
	t.Run("a hash recorded in presentBefore survives", func(t *testing.T) {
		store := blobmemory.New()
		seed(t, store, fileformat.FileTypeSubtreeToCheck)

		bv := &BlockValidation{logger: ulogger.TestLogger{}, subtreeStore: store}
		presentBefore := map[chainhash.Hash]struct{}{subtreeHash: {}}
		require.NoError(t, bv.removePeerSuppliedSubtreeToCheck(context.Background(), block, presentBefore))

		require.True(t, exists(t, store, fileformat.FileTypeSubtreeToCheck),
			"a pre-existing blob was not written by this attempt and must not be deleted")
	})

	// The converse, so the filter is not simply disabling the helper: a hash absent from
	// presentBefore can only have been written by this attempt, and is still deleted.
	t.Run("a hash absent from presentBefore is deleted", func(t *testing.T) {
		store := blobmemory.New()
		seed(t, store, fileformat.FileTypeSubtreeToCheck)

		otherHash := chainhash.Hash{0x02}
		bv := &BlockValidation{logger: ulogger.TestLogger{}, subtreeStore: store}
		presentBefore := map[chainhash.Hash]struct{}{otherHash: {}}
		require.NoError(t, bv.removePeerSuppliedSubtreeToCheck(context.Background(), block, presentBefore))

		require.False(t, exists(t, store, fileformat.FileTypeSubtreeToCheck),
			"a blob this attempt wrote must still be deleted so a retry re-fetches")
	})
}

// TestSubtreeToCheckPresentBefore pins the snapshot the bound is built on
// (bitcoin-sv/teranode#4692). It must record a hash as present when EITHER FileTypeSubtreeToCheck or
// FileTypeSubtree exists — that pair is exactly CheckBlockSubtrees' own local-branch gate
// (findLocalSubtreeFile), which is what makes "absent before" equivalent to "written by this
// attempt".
func TestSubtreeToCheckPresentBefore(t *testing.T) {
	ctx := context.Background()

	toCheckOnly := chainhash.Hash{0x11}
	subtreeOnly := chainhash.Hash{0x22}
	dataOnly := chainhash.Hash{0x33}
	absent := chainhash.Hash{0x44}

	store := blobmemory.New()
	require.NoError(t, store.Set(ctx, toCheckOnly[:], fileformat.FileTypeSubtreeToCheck, []byte{0x00}))
	require.NoError(t, store.Set(ctx, subtreeOnly[:], fileformat.FileTypeSubtree, []byte{0x00}))
	// SubtreeData is deliberately NOT part of the gate: CheckBlockSubtrees keys its local branch on
	// SubtreeToCheck/Subtree only, so a hash with just SubtreeData can still be written by this
	// attempt and must stay deletable.
	require.NoError(t, store.Set(ctx, dataOnly[:], fileformat.FileTypeSubtreeData, []byte{0x00}))

	block := cleanupTestBlock(t, &toCheckOnly, &subtreeOnly, &dataOnly, &absent)

	bv := &BlockValidation{logger: ulogger.TestLogger{}, subtreeStore: store}
	presentBefore := bv.subtreeToCheckPresentBefore(ctx, block)

	require.Contains(t, presentBefore, toCheckOnly, "an existing SubtreeToCheck marks the hash pre-existing")
	require.Contains(t, presentBefore, subtreeOnly, "an existing validated Subtree marks the hash pre-existing")
	require.NotContains(t, presentBefore, dataOnly, "SubtreeData alone is not CheckBlockSubtrees' local-branch gate")
	require.NotContains(t, presentBefore, absent, "a hash with no local copy is deletable by this attempt")

	// No subtrees: nil, which nil-map lookups then treat as "nothing pre-existing" — there is
	// nothing to skip, so that is correct rather than a silent no-op.
	require.Nil(t, bv.subtreeToCheckPresentBefore(ctx, cleanupTestBlock(t)))
}

// recordingSubtreeStore wraps a blob store and records the reads and deletes the production code
// issues, so a test can assert on what the code under test TOUCHED rather than only on the end state
// (bitcoin-sv/teranode#4692).
//
// It embeds blob.Store, so every method it does not override forwards unchanged. The five it does
// override forward faithfully and return the wrapped store's error unmodified — deliberately, and it
// matters: subtreeToCheckPresentBefore records a hash as present when Exists returns EITHER true OR
// an error (it fails safe), so a decorator that swallowed or invented an error would make every hash
// pre-existing, suppress the cleanup entirely, and pass a zero-delete assertion for a reason that has
// nothing to do with the code under test.
type recordingSubtreeStore struct {
	blob.Store

	mu   sync.Mutex
	dels []storeOp
	gets []storeOp
}

type storeOp struct {
	key      chainhash.Hash
	fileType fileformat.FileType
}

func (r *recordingSubtreeStore) record(dst *[]storeOp, key []byte, fileType fileformat.FileType) {
	var h chainhash.Hash
	copy(h[:], key)

	r.mu.Lock()
	*dst = append(*dst, storeOp{key: h, fileType: fileType})
	r.mu.Unlock()
}

func (r *recordingSubtreeStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	return r.Store.Exists(ctx, key, fileType, opts...)
}

func (r *recordingSubtreeStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	r.record(&r.gets, key, fileType)
	return r.Store.Get(ctx, key, fileType, opts...)
}

func (r *recordingSubtreeStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	r.record(&r.gets, key, fileType)
	return r.Store.GetIoReader(ctx, key, fileType, opts...)
}

func (r *recordingSubtreeStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	return r.Store.Set(ctx, key, fileType, value, opts...)
}

func (r *recordingSubtreeStore) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	r.record(&r.dels, key, fileType)
	return r.Store.Del(ctx, key, fileType, opts...)
}

// readsOf counts recorded reads of one (hash, fileType) pair.
func (r *recordingSubtreeStore) readsOf(hash chainhash.Hash, fileType fileformat.FileType) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	n := 0

	for _, op := range r.gets {
		if op.key == hash && op.fileType == fileType {
			n++
		}
	}

	return n
}

// delsOf counts recorded deletes of one hash, across every file type.
func (r *recordingSubtreeStore) delsOf(hash chainhash.Hash) []fileformat.FileType {
	r.mu.Lock()
	defer r.mu.Unlock()

	var types []fileformat.FileType

	for _, op := range r.dels {
		if op.key == hash {
			types = append(types, op.fileType)
		}
	}

	return types
}

// TestValidateBlockWithOptions_RunningMerkleCorruptBody_DeletesNothing drives the NON-OPTIMISTIC
// RUNNING validation path end to end and pins that the block.Valid merkle corrupt branch performs NO
// blob cleanup (bitcoin-sv/teranode#4692).
//
// WHY THERE IS NOTHING TO CLEAN UP HERE, and why the reason is presence and lookup order rather than
// blob contents: reaching this branch means validateBlockSubtrees SUCCEEDED, so every subtree of the
// block has a FileTypeSubtree — either the missing-subtree gate observed it, or the hash was missing
// and storeSubtreeFiles wrote it. Every reader on a later attempt then either tests FileTypeSubtree
// alone (CheckBlockSubtrees' gate) or prefers it (model.Block.GetAndValidateSubtrees), so deleting
// the FileTypeSubtreeToCheck fallback changes no byte any reader of this block sees. The fallback
// does have other consumers — block assembly, the block persister, the asset service — so deleting it
// could only cost them.
//
// The two assertions are deliberately different in kind, and item 1 is what makes item 2 meaningful:
//
//  1. READ SIDE. No read of FileTypeSubtreeToCheck is issued for the block's subtree hash during the
//     whole call. That is an assertion about what the PRODUCTION code reads — model.Block's fallback
//     at GetAndValidateSubtrees is the only thing that would open it, and only if FileTypeSubtree
//     returns ErrNotFound — so it pins the inertness argument itself rather than restating the stub.
//  2. DELETE SIDE. No Del of the block's subtree hash, for any file type. Stronger than trailing
//     Exists checks: it also catches a delete-then-rewrite and a delete of a type this test did not
//     think to enumerate.
//
// FIXTURE, and why it is shaped this way. NEITHER subtree blob is on disk when the attempt starts;
// the CheckBlockSubtrees stub writes BOTH during the call, mirroring the real fetch branch
// (FileTypeSubtreeToCheck) plus storeSubtreeFiles (FileTypeSubtree). This is load-bearing for the
// mutation proof: because both are absent at snapshot time, subtreeToCheckPresentBefore returns an
// EMPTY map and the hash is genuinely deletion-eligible, so restoring the removed cleanup call really
// does issue a Del and really does redden item 2. Pre-seeding either blob would put the hash into
// presentBefore — findLocalSubtreeFile reports present if EITHER type exists — and
// removePeerSuppliedSubtreeToCheck would then skip it, so the test would pass with or without the
// mutation.
//
// Mutation proof (verified by running it): restore the removePeerSuppliedSubtreeToCheck call in the
// block.Valid corrupt branch of ValidateBlockWithOptions and item 2 fails with
// "site B must not delete any subtree blob ... got [subtreeToCheck]".
func TestValidateBlockWithOptions_RunningMerkleCorruptBody_DeletesNothing(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	// Reach block.Valid on the non-optimistic path: subtree validation must PASS, so mock
	// CheckBlockSubtrees to return nil. block.Valid then fails the block-level merkle-root check,
	// which is the corrupt source (a body whose subtrees each hash correctly but whose roots do not
	// combine to the header merkle root).
	subtreeVal := &subtreevalidation.MockSubtreeValidation{}
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

	// buildOneSubtreeBlock seeds FileTypeSubtreeToCheck + FileTypeSubtreeData. Keep the real
	// serialized subtree bytes, then REMOVE the marker so NEITHER subtree blob is present when the
	// pre-existence snapshot is taken (see the fixture note above).
	realSubtreeBytes, err := suite.Server.subtreeStore.Get(suite.Ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.NoError(t, suite.Server.subtreeStore.Del(suite.Ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck))
	require.NoError(t, suite.Server.subtreeStore.Set(suite.Ctx, subtreeHash[:], fileformat.FileTypeSubtreeMeta, []byte{0x00}))

	// Install the recorder on BOTH fields. The BlockValidation value holds its OWN subtreeStore —
	// NewCatchupTestSuite constructs it with blobmemory.New() and then copies the pointer into
	// Server.subtreeStore — so they are two independent fields that merely start out sharing one
	// store. ValidateBlockWithOptions and block.Valid read the BlockValidation one, so assigning only
	// Server.subtreeStore would leave production on the raw store and the recorder would observe
	// nothing at all, silently vacating both assertions below.
	recStore := &recordingSubtreeStore{Store: suite.Server.subtreeStore}
	suite.Server.blockValidation.subtreeStore = recStore
	suite.Server.subtreeStore = recStore

	// The stub stands in for the real CheckBlockSubtrees: the fetch branch writes
	// FileTypeSubtreeToCheck, and a successful ValidateSubtreeInternal leaves FileTypeSubtree behind
	// via storeSubtreeFiles. Writing BOTH is what makes this fixture model a block that legitimately
	// reached the merkle branch.
	subtreeVal.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ mock.Arguments) {
			require.NoError(t, recStore.Set(suite.Ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck, realSubtreeBytes))
			require.NoError(t, recStore.Set(suite.Ctx, subtreeHash[:], fileformat.FileTypeSubtree, realSubtreeBytes))
		}).Return(nil)

	// Zero the header merkle root so block.Valid's CheckMerkleRoot cannot match, then re-mine the
	// nonce so the header still meets its PoW target (buildOneSubtreeBlock leaves PoW stale).
	block.Header.HashMerkleRoot = &chainhash.Hash{}
	for {
		if ok, _, _ := block.Header.HasMetTargetDifficulty(); ok {
			break
		}
		block.Header.Nonce++
	}

	// State at snapshot time: the companions exist, NEITHER subtree blob does. The absence is what
	// makes the hash deletion-eligible and the mutation proof real, so assert it rather than assume it.
	for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtreeData, fileformat.FileTypeSubtreeMeta} {
		present, existsErr := recStore.Exists(suite.Ctx, subtreeHash[:], ft)
		require.NoError(t, existsErr)
		require.True(t, present, "%s must exist before the corrupt verdict", ft)
	}

	for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtree} {
		present, existsErr := recStore.Exists(suite.Ctx, subtreeHash[:], ft)
		require.NoError(t, existsErr)
		require.False(t, present,
			"%s must be absent at snapshot time, or presentBefore suppresses the delete and the mutation proof is vacuous", ft)
	}

	rec := &banScoreRecorder{}
	suite.Server.blockValidation.p2pClient = rec

	valErr := suite.Server.blockValidation.ValidateBlockWithOptions(suite.Ctx, block, "http://peer",
		&ValidateBlockOptions{PeerID: "peer-corrupt"})
	require.Error(t, valErr)
	require.True(t, errors.IsBlockCorrupt(valErr), "a block-level merkle mismatch must be corrupt, got: %v", valErr)
	require.False(t, errors.Is(valErr, errors.ErrBlockInvalid), "a corrupt body must never be poisoned invalid")

	require.Equal(t, []string{"peer-corrupt"}, rec.struck(), "the serving peer must be struck for the corrupt body")

	// Sanity check on the recorder itself: a recorder that observed NOTHING is indistinguishable from
	// a passing test, so prove it is on the path before trusting the two assertions below.
	require.Positive(t, recStore.readsOf(*subtreeHash, fileformat.FileTypeSubtree),
		"the recorder must be on the path production reads — block.Valid loads FileTypeSubtree for every subtree")

	// 1. READ SIDE — fact (b): the fallback is never consulted, which is WHY the delete would be inert.
	require.Zero(t, recStore.readsOf(*subtreeHash, fileformat.FileTypeSubtreeToCheck),
		"no reader of this block may consult the SubtreeToCheck fallback once FileTypeSubtree is present")

	// 2. DELETE SIDE — the behaviour change.
	require.Empty(t, recStore.delsOf(*subtreeHash),
		"site B must not delete any subtree blob of this block")

	// End state: all four types survive the corrupt verdict.
	for _, ft := range []fileformat.FileType{
		fileformat.FileTypeSubtreeToCheck,
		fileformat.FileTypeSubtree,
		fileformat.FileTypeSubtreeData,
		fileformat.FileTypeSubtreeMeta,
	} {
		still, existsErr := recStore.Exists(suite.Ctx, subtreeHash[:], ft)
		require.NoError(t, existsErr)
		require.True(t, still, "%s must survive the block.Valid merkle corrupt branch", ft)
	}
}

// TestValidateBlockWithOptions_RunningCorruptBody_KeepsPreexistingSubtreeToCheck is the review's
// attack scenario, inverted into an assertion (bitcoin-sv/teranode#4692). The fixture pre-seeds the
// FileTypeSubtreeToCheck blob, i.e. it is already on disk before the attempt starts.
//
// It is pointed at the SUBTREE-VALIDATION corrupt branch, which is the only branch that still calls
// the cleanup: on the block.Valid merkle branch there is no delete left to bound, so this test would
// pass there trivially and would stop testing the pre-existence bound at all.
//
// A doctored body replaying an honest header can name the subtree hashes of a block being validated
// concurrently. Any hash whose blob was already present is one CheckBlockSubtrees LOADED rather than
// wrote, so it belongs to someone else and must survive the corrupt verdict — otherwise the victim
// loses its blobs, and on the legacy route (synthetic baseURL="legacy", no scheme) cannot re-fetch
// them and fails rather than recovering.
//
// Mutation proof: drop the presentBefore filter in removePeerSuppliedSubtreeToCheck and the
// pre-seeded blob is deleted, reddening the survival assertion.
func TestValidateBlockWithOptions_RunningCorruptBody_KeepsPreexistingSubtreeToCheck(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	// CheckBlockSubtrees returns ERR_BLOCK_CORRUPT, making subtree validation the first detector —
	// the branch that still performs the cleanup. It writes nothing, which is what the real one does
	// when the blob is already local.
	subtreeVal := &subtreevalidation.MockSubtreeValidation{}
	subtreeVal.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.NewBlockCorruptError("[CheckBlockSubtrees] duplicate transaction in subtree"))
	suite.Server.blockValidation.subtreeValidationClient = subtreeVal

	suite.MockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil).Maybe()
	suite.MockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil).Maybe()
	suite.MockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 99, MinedSet: true}, nil).Maybe()
	suite.MockBlockchain.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil).Maybe()
	suite.MockBlockchain.On("GetBlockHeadersFromHeight", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{}, errors.NewServiceError("not mocked")).Maybe()

	// buildOneSubtreeBlock seeds FileTypeSubtreeToCheck + FileTypeSubtreeData, so the marker is
	// pre-existing exactly as it would be for a concurrently-validating sibling block.
	block := buildOneSubtreeBlock(t, suite, 100)
	subtreeHash := block.Subtrees[0]

	suite.MockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).
		Return(&block.Header.Bits, nil).Maybe()

	preexisting, err := suite.Server.subtreeStore.Exists(suite.Ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.True(t, preexisting, "the fixture must pre-seed the marker, or this test proves nothing")

	// Re-mine so the header meets its PoW target (buildOneSubtreeBlock leaves PoW stale) and the
	// header checks pass, letting the attempt reach subtree validation.
	for {
		if ok, _, _ := block.Header.HasMetTargetDifficulty(); ok {
			break
		}
		block.Header.Nonce++
	}

	rec := &banScoreRecorder{}
	suite.Server.blockValidation.p2pClient = rec

	valErr := suite.Server.blockValidation.ValidateBlockWithOptions(suite.Ctx, block, "http://peer",
		&ValidateBlockOptions{PeerID: "peer-corrupt"})
	require.Error(t, valErr)
	require.True(t, errors.IsBlockCorrupt(valErr), "a corrupt subtree body must be corrupt, got: %v", valErr)

	// The verdict and the strike are unchanged — the bound narrows the CLEANUP, nothing else.
	require.Equal(t, []string{"peer-corrupt"}, rec.struck(), "the serving peer must still be struck")

	survived, err := suite.Server.subtreeStore.Exists(suite.Ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.True(t, survived,
		"a blob that pre-dated this attempt is not this attempt's to delete: the corrupt body must not destroy it")
}

// TestValidateBlockWithOptions_SubtreeValidationCorruptBody_CleansUpSubtreeToCheck pins the cleanup on
// the SUBTREE-VALIDATION corrupt branch (bitcoin-sv/teranode#4692) — the sibling of the block.Valid
// merkle branch covered by TestValidateBlockWithOptions_RunningCorruptBody_CleansUpOnlySubtreeToCheck.
// When validateBlockSubtrees (CheckBlockSubtrees) is the first detector of a body-derived corruption —
// e.g. a CVE-2012-2459 duplicate that is root-preserving so the fetch-side root check passed and the
// peer bytes are already on disk under FileTypeSubtreeToCheck — the branch must strike the serving peer
// and drop that unvalidated marker, so a retry (even from an honest re-announcer) re-fetches instead of
// re-reading the poisoned body and re-failing forever.
//
// The blockchain store is a real sqlitememory store; CheckBlockSubtrees is the only mocked collaborator,
// because it is the subtree-validation service (not the blockchain) and returning ERR_BLOCK_CORRUPT from
// it is the only way to make subtree validation — rather than the later block.Valid merkle check — the
// first detector. DisableOptimisticMining keeps validation synchronous.
//
// The block names TWO subtrees so this call site pins BOTH halves of the pre-existence bound
// (bitcoin-sv/teranode#4692): one hash whose marker was already on disk before the attempt started
// (someone else's — it must survive) and one the stub writes during the attempt (ours — it must be
// deleted). Mutation proof: drop the presentBefore filter and the pre-existing marker is deleted too.
func TestValidateBlockWithOptions_SubtreeValidationCorruptBody_CleansUpSubtreeToCheck(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	// A coinbase-only subtree whose peer-supplied bytes are ALREADY on disk under
	// FileTypeSubtreeToCheck when the attempt starts — someone else's blob, which the corrupt branch
	// must leave alone. block.Valid never runs (the corrupt verdict comes from subtree validation), so
	// the body need not be otherwise valid; only the header must meet its own target to clear the
	// difficulty gate before subtree validation.
	preexistingSubtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, preexistingSubtree.AddCoinbaseNode())
	preexistingBytes, err := preexistingSubtree.Serialize()
	require.NoError(t, err)
	preexistingHash := preexistingSubtree.RootHash()
	require.NoError(t, subtreeStore.Set(ctx, preexistingHash[:], fileformat.FileTypeSubtreeToCheck, preexistingBytes))

	// A second subtree with NO local copy at snapshot time. The CheckBlockSubtrees stub writes its
	// marker before returning corrupt, standing in for the real fetch branch — so this one IS this
	// attempt's, and is the artifact the corrupt branch must delete.
	freshSubtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, freshSubtree.AddCoinbaseNode())
	require.NoError(t, freshSubtree.AddNode(chainhash.HashH([]byte("fresh-subtree-node")), 1, 1))
	freshBytes, err := freshSubtree.Serialize()
	require.NoError(t, err)
	freshHash := freshSubtree.RootHash()

	subtreeVal := &subtreevalidation.MockSubtreeValidation{}
	subtreeVal.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ mock.Arguments) {
			require.NoError(t, subtreeStore.Set(ctx, freshHash[:], fileformat.FileTypeSubtreeToCheck, freshBytes))
		}).
		Return(errors.NewBlockCorruptError("corrupt subtree body during subtree validation"))

	coinbaseTx := coinbaseAtHeight(t, 1)
	hdr := minedBIP34Header(t, 4, tSettings.ChainCfgParams.GenesisHash, &chainhash.Hash{})
	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{preexistingHash, freshHash},
		uint64(preexistingSubtree.Length()+freshSubtree.Length()), uint64(coinbaseTx.Size()), 1, 0) //nolint:gosec
	require.NoError(t, err)

	present, err := subtreeStore.Exists(ctx, preexistingHash[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.True(t, present, "the pre-existing FileTypeSubtreeToCheck must exist before the corrupt verdict")

	absent, err := subtreeStore.Exists(ctx, freshHash[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.False(t, absent, "the second subtree must be absent at snapshot time, or it is not this attempt's to delete")

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeVal)
	rec := &banScoreRecorder{}
	bv.p2pClient = rec

	valErr := bv.ValidateBlockWithOptions(ctx, block, "http://peer",
		&ValidateBlockOptions{PeerID: "peer-corrupt", DisableOptimisticMining: true})
	require.Error(t, valErr)
	require.True(t, errors.IsBlockCorrupt(valErr), "a corrupt subtree body must surface as corrupt, got: %v", valErr)
	require.False(t, errors.Is(valErr, errors.ErrBlockInvalid), "a corrupt body must never be poisoned invalid")

	require.Equal(t, []string{"peer-corrupt"}, rec.struck(),
		"the serving peer must be struck once for the corrupt subtree body")

	// The fix: the subtree-validation corrupt branch must delete the unvalidated peer-supplied marker
	// THIS attempt wrote, so a retry re-fetches instead of re-reading the body that just failed
	// subtree validation.
	gone, err := subtreeStore.Exists(ctx, freshHash[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.False(t, gone, "the subtree-validation corrupt branch must delete the marker it wrote so a retry re-fetches")

	// And the bound: the marker that pre-dated the attempt belongs to whoever wrote it and must
	// survive, so an untrusted body naming another block's subtree hashes cannot destroy its blobs.
	survived, err := subtreeStore.Exists(ctx, preexistingHash[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.True(t, survived, "a pre-existing marker is not this attempt's to delete")
}
