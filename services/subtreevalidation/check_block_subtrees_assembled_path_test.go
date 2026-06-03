package subtreevalidation

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxometa "github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// recordingValidatorClient wraps MockValidatorClient and captures the
// Options passed to every Validate call. Used by the assembled-path test
// to assert that the block-scoped accumulator correctly populates per-tx
// ParentMetadata for skip-level grandparents and DOES NOT populate it for
// confirmed external parents.
type recordingValidatorClient struct {
	*validator.MockValidatorClient
	mu        sync.Mutex
	callsByTx map[chainhash.Hash][]*validator.Options
}

func newRecordingValidatorClient(inner *validator.MockValidatorClient) *recordingValidatorClient {
	return &recordingValidatorClient{
		MockValidatorClient: inner,
		callsByTx:           make(map[chainhash.Hash][]*validator.Options),
	}
}

// ValidateWithOptions captures the resolved Options struct and returns
// Data with TxInpoints populated from the tx so the downstream subtreeMeta
// serialisation in validateSubtreeInternalImpl succeeds — the production
// validator does this; MockValidatorClient.ValidateWithOptions does not
// (it just calls UtxoStore.Create which returns empty Data in tests).
//
// blessMissingTransaction in processTransactionsInLevels and
// processMissingTransactions calls THIS method (not Validate), so this is
// the right capture point for inspecting per-tx Options.
func (r *recordingValidatorClient) ValidateWithOptions(_ context.Context, tx *bt.Tx, _ uint32, validationOptions *validator.Options) (*utxometa.Data, error) {
	r.mu.Lock()
	r.callsByTx[*tx.TxIDChainHash()] = append(r.callsByTx[*tx.TxIDChainHash()], validationOptions)
	r.mu.Unlock()

	inpoints, err := subtreepkg.NewTxInpointsFromTx(tx)
	if err != nil {
		return nil, err
	}
	return &utxometa.Data{
		Tx:          tx,
		Fee:         1,
		SizeInBytes: uint64(tx.Size()),
		TxInpoints:  inpoints,
	}, nil
}

// recordedOptions returns the slice of options recorded for tx h (nil if
// not called). Multiple entries are expected: the batch loop
// processTransactionsInLevels calls once, and validateSubtreeInternalImpl's
// processMissingTransactions also calls during the ordered-retry pass.
// Every recorded call MUST have the correct ParentMetadata — anything less
// means one of the two pipelines re-narrowed the accumulator's scope.
func (r *recordingValidatorClient) recordedOptions(h chainhash.Hash) []*validator.Options {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*validator.Options, len(r.callsByTx[h]))
	copy(out, r.callsByTx[h])
	return out
}

// TestCheckBlockSubtrees_AssembledPath_SkipLevelAndMixedParent is the
// end-to-end regression for the consensus-split path: the assembled
// CheckBlockSubtrees pipeline (parse → batch → level-order → per-tx
// filter → validator) must thread the block-scoped accumulator's
// metadata into every per-tx Options struct passed to the validator.
// Without this, a refactor that breaks the accumulator (e.g. one that
// re-narrows per-tx ParentMetadata to the immediately-previous level
// only) silently re-opens the skip-level path: a skip-level grandparent
// falls through to the UTXO-store BlockHeights path, finds it empty
// (the parent's blocks_transactions row isn't written until the block
// is accepted), the validator stamps unconfirmedParentHeight, and BDK
// rejects the legitimate block with bad-txns-unconfirmed-input-in-block.
//
// The DAG inside the candidate block:
//
//	G (grandparent, in-block, level 0)     — 2 outputs
//	│
//	├── (G.vout 0) → P (in-block, level 1) — 1 output
//	│                       │
//	│                       └── (P.vout 0) → C (in-block, level 2)
//	│                                            ▲
//	│                                            │
//	└── (G.vout 1) ──────── skip-level edge ─────┘
//	                                             │
//	                                  (Gext.vout 0) ─┘
//	                                             ▲
//	                                             │
//	                                             └── Gext (confirmed
//	                                                   external — in UTXO
//	                                                   store at prior
//	                                                   height, NOT in
//	                                                   this block; must
//	                                                   NOT appear in
//	                                                   C's ParentMetadata)
//
// Why the skip-level edge requires C to ALSO spend P: the dependency-level
// builder places C above its highest in-block parent. If C only spent G,
// both P and C would land at level 1 (each depends only on G) — and
// a previous-level-only buildParentMetadata for level-1 fed txsPerLevel[level-0]=[G],
// so the bug shape would not be triggered. With C also spending P, C is
// at level 2 and a previous-level-only buildParentMetadata for level-2 fed
// txsPerLevel[level-1]=[P] — so G's metadata for C's skip-level input
// would silently be dropped.
//
// Assertions exercised:
//
//   - P's Options.ParentMetadata contains G at the candidate block height
//     (standard parent → child).
//
//   - C's Options.ParentMetadata contains G at the candidate block height
//     (skip-level grandparent — a previous-level-only buildParentMetadata only fed
//     level-1; this is the bug-shape).
//
//   - C's Options.ParentMetadata does NOT contain Gext (confirmed external
//     parents stay out of the accumulator — the validator resolves them
//     through the UTXO-store BlockHeights path with their prior-block
//     height. If a refactor accidentally seeded all parents into the
//     accumulator at the candidate height, Gext would be wrongly stamped
//     at the candidate height, breaking CSV/locktime checks.).
//
//   - The block is accepted (response.Blessed == true).
//
// This black-boxes everything below CheckBlockSubtrees so a future
// refactor that re-tightens the accumulator's scope (e.g. per-level only,
// or per-batch only) is caught here even when component tests stay green.
func TestCheckBlockSubtrees_AssembledPath_SkipLevelAndMixedParent(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Confirmed external parent — represented just by its hash; never
	// constructed as a real tx in this test. It lives "in the UTXO store
	// with BlockHeights=[99]" via the BatchDecorate mock below.
	gextHash := chainhash.Hash{0xaa, 0xaa, 0xaa, 0xaa}

	// Build the three in-block txs G, P, C.
	// All three are valid extended-tx serialisations with deterministic
	// TXIDs — they round-trip through readTransactionsFromSubtreeDataStream
	// (which expects standard tx wire bytes) and the resulting hashes match
	// the subtree's expected node hashes.
	//
	// G has TWO outputs so P can spend vout 0 and C can spend vout 1 (the
	// skip-level edge). C also spends P's vout 0 — without that edge, the
	// level builder would put C at level 1 (same as P), and the test would
	// not actually exercise the skip-level case that a previous-level-only
	// buildParentMetadata implementation regressed on.
	txG := buildAssembledPathTx(t, []*bt.Input{
		// G's input: synthetic "external coinbase-like" hash. Not used by
		// anything else in this test; just gives G a parseable input.
		newSyntheticInput(t, chainhash.Hash{0x01, 0x02, 0x03, 0x04}, 0),
	}, 2)
	gHash := *txG.TxIDChainHash()

	txP := buildAssembledPathTx(t, []*bt.Input{
		// P spends G's output 0 — standard parent → child edge. P has one
		// output (vout 0) which C spends below.
		newSyntheticInput(t, gHash, 0),
	}, 1)
	pHash := *txP.TxIDChainHash()

	txC := buildAssembledPathTx(t, []*bt.Input{
		// C input 0 → P. This is the edge that pushes C to level 2 in the
		// dependency-level builder (P is at level 1; depending on P
		// promotes C to level 2). Without this edge, C and P share level 1.
		newSyntheticInput(t, pHash, 0),
		// C input 1 → G directly. SKIP-LEVEL: C is at level 2, references
		// the level-0 grandparent. A previous-level-only buildParentMetadata only fed
		// txsPerLevel[level-1] (i.e. txs at C's level minus 1 = level 1 =
		// just P) into C's ParentMetadata, so G's metadata would silently
		// be dropped and C would fall through to the UTXO-store empty
		// BlockHeights path → unconfirmedParentHeight → BDK rejects.
		newSyntheticInput(t, gHash, 1),
		// C input 2 → Gext. Mixed parent: confirmed external parent
		// alongside the in-block skip-level edge. The accumulator must
		// NOT include Gext — the validator resolves it through the
		// UTXO-store BlockHeights path at its actual prior-block height.
		newSyntheticInput(t, gextHash, 0),
	}, 1)
	cHash := *txC.TxIDChainHash()

	// PRE-FLIGHT INVARIANT: confirm the constructed DAG actually produces
	// the level structure the test claims. If C does not land at level 2,
	// the skip-level edge assertion below is vacuous — a previous-level-
	// only buildParentMetadata(txsPerLevel[level-1]) implementation for
	// level 1 would have included G's metadata for both P and C, and the
	// test would silently pass even with the bug present. This guard
	// catches a future refactor of the level-builder OR an accidental
	// edit to the DAG that breaks the skip-level shape.
	maxLevel, txsPerLevel, err := server.selectPrepareTxsPerLevel(context.Background(),
		[]missingTx{
			{tx: txG, idx: 0},
			{tx: txP, idx: 1},
			{tx: txC, idx: 2},
		})
	require.NoError(t, err)
	require.Equal(t, uint32(2), maxLevel,
		"DAG must yield 3 levels (G, P, C). If this fails, the skip-level claim below is vacuous")
	require.Len(t, txsPerLevel[0], 1)
	require.True(t, txsPerLevel[0][0].tx.TxIDChainHash().IsEqual(&gHash), "level 0 must be G")
	require.Len(t, txsPerLevel[1], 1)
	require.True(t, txsPerLevel[1][0].tx.TxIDChainHash().IsEqual(&pHash), "level 1 must be P (only depends on G)")
	require.Len(t, txsPerLevel[2], 1)
	require.True(t, txsPerLevel[2][0].tx.TxIDChainHash().IsEqual(&cHash), "level 2 must be C (depends on P AND G — skip-level edge)")

	// Build the subtree containing G, P, C (in that order — block order is
	// the topological order the validator processes).
	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(3)
	require.NoError(t, err)
	require.NoError(t, subtree.AddNode(gHash, 0, 0))
	require.NoError(t, subtree.AddNode(pHash, 0, 0))
	require.NoError(t, subtree.AddNode(cHash, 0, 0))
	subtreeHash := *subtree.RootHash()

	// Serialise subtree structure (FileTypeSubtreeToCheck) so
	// findLocalSubtreeFile picks it up locally and avoids the HTTP fetch
	// fallback. This also keeps the test hermetic (no httpmock).
	subtreeStructBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, server.subtreeStore.Set(context.Background(),
		subtreeHash[:], fileformat.FileTypeSubtreeToCheck, subtreeStructBytes))

	// SubtreeData is the concatenated standard-wire serialisation of the
	// three txs in the same order as subtree.Nodes — that's the format
	// readTransactionsFromSubtreeDataStream expects.
	var subtreeData bytes.Buffer
	subtreeData.Write(txG.Bytes())
	subtreeData.Write(txP.Bytes())
	subtreeData.Write(txC.Bytes())
	require.NoError(t, server.subtreeStore.Set(context.Background(),
		subtreeHash[:], fileformat.FileTypeSubtreeData, subtreeData.Bytes()))

	// Build the candidate block. Header values are placeholder — the
	// validator/accumulator path doesn't inspect them. BlockHeight gates
	// the CSV branch in CheckBlockSubtrees; we keep it below CSVHeight to
	// skip MTP fetching (pre-CSV path), which keeps the test hermetic
	// without having to mock GetBlockHeaders.
	const candidateHeight = uint32(50)
	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{},
		Nonce:          0,
	}
	coinbaseTx := &bt.Tx{Version: 1}
	block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{&subtreeHash},
		uint64(subtree.Length()+1), // +1 for coinbase
		1000,
		candidateHeight,
		0,
	)
	require.NoError(t, err)
	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	// UTXO store mocks. Reset to swap in a BatchDecorate that "knows"
	// about Gext (confirmed external, BlockHeights=[99]) while reporting
	// G, P, C as unknown — the bug-triggering shape for skip-level.
	mockStore := server.utxoStore.(*utxo.MockUtxostore)
	mockStore.ExpectedCalls = nil
	mockStore.On("Create", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&utxometa.Data{}, nil).Maybe()
	mockStore.On("GetBlockHeight").Return(uint32(100)).Maybe()
	mockStore.On("GetMeta", mock.Anything, mock.Anything).
		Return(&utxometa.Data{}, nil).Maybe()
	mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			slice := args.Get(1).([]*utxo.UnresolvedMetaData)
			for _, item := range slice {
				if item.Hash.Equal(gextHash) {
					item.Data = &utxometa.Data{
						Fee:          1,
						SizeInBytes:  100,
						BlockHeights: []uint32{99}, // confirmed in prior block
					}
				}
				// G, P, C deliberately left without Data — the in-block
				// path validates them via processTransactionsInLevels.
			}
		}).
		Return(nil).Maybe()

	// Recording validator client — captures the resolved Options per tx
	// so the test can inspect ParentMetadata after CheckBlockSubtrees
	// returns. Wraps the existing MockValidatorClient so the underlying
	// UtxoStore.Create plumbing still works.
	rec := newRecordingValidatorClient(server.validatorClient.(*validator.MockValidatorClient))
	rec.UtxoStore = server.utxoStore
	server.validatorClient = rec

	// Blockchain client mocks.
	mockBC := server.blockchainClient.(*blockchain.Mock)
	mockBC.ExpectedCalls = nil
	currentState := blockchain.FSMStateRUNNING
	mockBC.On("GetFSMCurrentState", mock.Anything).Return(&currentState, nil).Maybe()
	mockBC.On("IsFSMCurrentState", mock.Anything, blockchain.FSMStateRUNNING).Return(true, nil).Maybe()
	mockBC.On("GetBestBlockHeader", mock.Anything).
		Return(&model.BlockHeader{}, &model.BlockHeaderMeta{}, nil).Maybe()
	// Block IDs lookup — return a small set; the validator just checks
	// membership of the parents' chain.
	mockBC.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{1, 2, 3}, nil).Maybe()
	mockBC.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil).Maybe()
	mockBC.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(&model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}},
			&model.BlockHeaderMeta{ID: 123}, nil).Maybe()
	mockBC.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).Return(true, nil).Maybe()

	// Drive the full pipeline.
	resp, err := server.CheckBlockSubtrees(context.Background(),
		&subtreevalidation_api.CheckBlockSubtreesRequest{
			Block:   blockBytes,
			BaseUrl: "test://no-fetch",
		})
	require.NoError(t, err, "assembled path must accept a valid skip-level + mixed-parent block")
	require.NotNil(t, resp)
	require.True(t, resp.Blessed, "response must mark the block as blessed once all subtrees validate")

	// --- ASSEMBLED-PATH ASSERTIONS ---------------------------------------
	//
	// Every recorded call (both the batch-loop processTransactionsInLevels
	// pass AND the validateSubtreeInternalImpl pass via processMissingTransactions)
	// MUST satisfy the per-tx ParentMetadata invariant. If only one pipeline
	// populated it correctly, a refactor that breaks the other would leave
	// the test silently passing while production rejected legitimate blocks.

	// 1. G is processed at level 0 in every pass. Its ParentMetadata is
	//    nil/empty in every call — G has no in-block parent.
	gAll := rec.recordedOptions(gHash)
	require.NotEmpty(t, gAll, "G must reach the validator at least once")
	for i, opts := range gAll {
		require.NotNil(t, opts, "G call #%d must carry resolved Options", i)
		require.Empty(t, opts.ParentMetadata,
			"G has no in-block parent — ParentMetadata must be absent/empty in every call (call #%d)", i)
	}

	// 2. P is processed at level 1 in every pass. Its ParentMetadata
	//    contains G at the candidate block height in every call.
	pAll := rec.recordedOptions(pHash)
	require.NotEmpty(t, pAll, "P must reach the validator at least once")
	for i, opts := range pAll {
		require.NotNil(t, opts, "P call #%d must carry resolved Options", i)
		require.NotNil(t, opts.ParentMetadata[gHash],
			"P references G — every per-tx ParentMetadata must contain G (call #%d failed; the accumulator wiring on that pipeline is broken)", i)
		require.Equal(t, candidateHeight, opts.ParentMetadata[gHash].BlockHeight,
			"BlockHeight for an in-block parent must be the candidate block's height (call #%d)", i)
	}

	// 3. CRITICAL skip-level assertion: every recorded call for C
	//    contains G at the candidate block height — even though G is
	//    two levels up. A previous-level-only buildParentMetadata only fed level-1
	//    metadata; this assertion guards against a refactor that
	//    re-narrows the accumulator's scope on EITHER the batch-loop
	//    OR the ordered-retry pipeline.
	cAll := rec.recordedOptions(cHash)
	require.NotEmpty(t, cAll, "C must reach the validator at least once")
	for i, opts := range cAll {
		require.NotNil(t, opts, "C call #%d must carry resolved Options", i)
		require.NotNil(t, opts.ParentMetadata[gHash],
			"C references G across a level skip — every per-tx ParentMetadata must contain G (call #%d failed; regression for Path 2: skip-level grandparents fell through to UTXO-store empty BlockHeights and got stamped with unconfirmedParentHeight)", i)
		require.Equal(t, candidateHeight, opts.ParentMetadata[gHash].BlockHeight,
			"skip-level in-block parent height must match the candidate block (call #%d)", i)

		// 4. Mixed-parent assertion: C also references Gext (confirmed
		//    external). The accumulator MUST NOT include Gext in any
		//    call — only in-block txs feed it. The validator resolves
		//    Gext through the UTXO-store BlockHeights path with its
		//    prior-block height. If a refactor accidentally seeded all
		//    parents at the candidate height, Gext would be wrongly
		//    stamped at candidateHeight, breaking CSV/locktime checks.
		require.Nil(t, opts.ParentMetadata[gextHash],
			"Gext is a confirmed external parent (not in this block) — the accumulator must NOT contain it in any call (call #%d)", i)
	}
}

// buildAssembledPathTx constructs a standard-wire bt.Tx with the supplied
// inputs and outputCount placeholder outputs. Each output's locking script
// is a trivial OP_TRUE and satoshis are fixed at 1000 — neither matters
// for the assembled-path test because validation is mocked.
//
// outputCount must be >= 1. The test needs G to have at least 2 outputs
// (so P can spend vout 0 and C can spend vout 1 via the skip-level edge)
// and P to have at least 1 output (so C can spend it via the level-1
// edge that puts C at level 2).
//
// The resulting bytes round-trip cleanly through
// readTransactionsFromSubtreeDataStream, and tx.TxIDChainHash() is
// deterministic from those bytes — so the subtree's node hashes match.
func buildAssembledPathTx(t *testing.T, inputs []*bt.Input, outputCount int) *bt.Tx {
	t.Helper()
	require.GreaterOrEqual(t, outputCount, 1, "buildAssembledPathTx needs at least one output")
	tx := bt.NewTx()
	tx.Inputs = append(tx.Inputs, inputs...)
	for i := 0; i < outputCount; i++ {
		tx.AddOutput(&bt.Output{
			Satoshis:      1000,
			LockingScript: bscript.NewFromBytes([]byte{0x51}), // OP_TRUE
		})
	}
	return tx
}

// newSyntheticInput builds a bt.Input that points at the supplied
// previous-tx hash + vout. Sequence is finalized (0xFFFFFFFF) so finality
// checks don't fire. UnlockingScript is empty — validation is mocked, so
// this never executes.
func newSyntheticInput(t *testing.T, prevTxHash chainhash.Hash, vout uint32) *bt.Input {
	t.Helper()
	input := &bt.Input{
		PreviousTxOutIndex: vout,
		SequenceNumber:     0xFFFFFFFF,
		UnlockingScript:    bscript.NewFromBytes(nil),
	}
	prev := prevTxHash
	require.NoError(t, input.PreviousTxIDAdd(&prev))
	return input
}
