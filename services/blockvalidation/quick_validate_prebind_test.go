package blockvalidation

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	testutil "github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// These tests pin INVARIANT PM for bitcoin-sv/teranode#4838: for any block on the
// quick-validation route, no UTXO-store mutation and no block-id assignment happens
// until the peer-supplied body has been proved to hash to the header.
//
// Everything they assert against is real: a sqlitememory UTXO store, a sqlitememory
// blockchain store behind the ordinary LocalClient, and an in-memory blob store. Two
// tests wrap the BLOB store in a thin double, because "the blob was replaced between
// two reads" and "deletion silently did not happen" are not otherwise reachable; the
// UTXO store and the blockchain store are never doubled, since they are what the
// no-mutation assertions read.

// preBindHeight is the height every body here is served at. Regression-net has no
// checkpoints, so the below-checkpoint fast paths (skip-lock, outpoint-only) stay off
// and the route takes its ordinary create/spend branch.
const preBindHeight = uint32(1)

// preBindPayToAddress is a throwaway address; nothing here executes a script.
const preBindPayToAddress = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"

// countingBlockchainClient wraps a REAL blockchain client (sqlitememory-backed) and
// counts AssignBlockID calls, delegating every one of them. Nothing about the
// blockchain is mocked: only the one durable reservation the tests assert the ABSENCE
// of is intercepted, since the reservation table is unexported and the interface
// exposes no read-only lookup for it.
type countingBlockchainClient struct {
	blockchain.ClientI

	mu      sync.Mutex
	assigns int
}

func (c *countingBlockchainClient) AssignBlockID(ctx context.Context, hash *chainhash.Hash) (uint64, error) {
	c.mu.Lock()
	c.assigns++
	c.mu.Unlock()

	return c.ClientI.AssignBlockID(ctx, hash)
}

func (c *countingBlockchainClient) assignCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.assigns
}

// preBindHarness is a BlockValidation over real stores.
type preBindHarness struct {
	t            *testing.T
	ctx          context.Context
	bv           *BlockValidation
	utxoStore    *sql.Store
	subtreeStore blob.Store
	chain        *countingBlockchainClient
	genesisHash  *chainhash.Hash
}

// newPreBindHarness builds the harness. subtreeStore may be nil, in which case a
// plain in-memory blob store is used; the two doubles below are passed in by the
// tests that need them, wrapping their own blobmemory instance.
func newPreBindHarness(t *testing.T, subtreeStore blob.Store) *preBindHarness {
	t.Helper()

	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := testutil.CreateBaseTestSettings(t)

	dbName := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())

	utxoURL, err := url.Parse("sqlitememory:///" + dbName)
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoURL)
	require.NoError(t, err)
	require.NoError(t, utxoStore.SetBlockHeight(preBindHeight))

	t.Cleanup(func() { require.NoError(t, utxoStore.Close(ctx)) })

	blockChainStore, err := blockchainstore.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	localClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	if subtreeStore == nil {
		subtreeStore = blobmemory.New()
	}

	chain := &countingBlockchainClient{ClientI: localClient}

	bv := &BlockValidation{
		logger:                        logger,
		settings:                      tSettings,
		blockchainClient:              chain,
		utxoStore:                     utxoStore,
		subtreeStore:                  subtreeStore,
		blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
		blockExistsCache:              expiringmap.New[chainhash.Hash, bool](2 * time.Minute),
		lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
		blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
		subtreeBlockHeightRetention:   10,
	}

	return &preBindHarness{
		t:            t,
		ctx:          ctx,
		bv:           bv,
		utxoStore:    utxoStore,
		subtreeStore: subtreeStore,
		chain:        chain,
		genesisHash:  tSettings.ChainCfgParams.GenesisHash,
	}
}

// storeGenuineParent creates a transaction with one spendable output in the UTXO
// store. This stands in for the honest parent the attack needs to see spent.
func (h *preBindHarness) storeGenuineParent(seed byte) *bt.Tx {
	h.t.Helper()

	payTo, err := bscript.NewP2PKHFromAddress(preBindPayToAddress)
	require.NoError(h.t, err)

	parent := bt.NewTx()

	in := &bt.Input{PreviousTxOutIndex: 0, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
	require.NoError(h.t, in.PreviousTxIDAdd(&chainhash.Hash{seed}))
	parent.Inputs = append(parent.Inputs, in)
	parent.Outputs = append(parent.Outputs, &bt.Output{Satoshis: 10_000, LockingScript: payTo})

	_, err = h.utxoStore.Create(h.ctx, parent, preBindHeight, utxo.WithSkipExtendedInputs(true))
	require.NoError(h.t, err)

	return parent
}

// preBindSpendOf builds an unextended spend of parent:0 whose unlocking script would
// never satisfy the P2PKH output it claims. Unextended on purpose: the route
// re-resolves previous outputs from the local store, as it does in production.
func preBindSpendOf(t *testing.T, parent *bt.Tx, satoshis uint64) *bt.Tx {
	t.Helper()

	payTo, err := bscript.NewP2PKHFromAddress(preBindPayToAddress)
	require.NoError(t, err)

	tx := bt.NewTx()

	in := &bt.Input{PreviousTxOutIndex: 0, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
	require.NoError(t, in.PreviousTxIDAdd(parent.TxIDChainHash()))
	tx.Inputs = append(tx.Inputs, in)
	tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: satoshis, LockingScript: payTo})

	return tx
}

// preBindCoinbase builds a consensus-shaped coinbase whose scriptSig is inside the
// length bound the route enforces. nonce varies the txid so different bodies get
// different merkle roots.
func preBindCoinbase(t *testing.T, nonce byte) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x01, 0x00, 0x00, nonce})
	require.NoError(t, tx.AddP2PKHOutputFromAddress(preBindPayToAddress, 50*100_000_000))

	return tx
}

// buildSubtreeOver builds the node list a served .subtree blob would carry: the
// coinbase placeholder at slot 0 for the first subtree, then one node per
// transaction.
func buildSubtreeOver(t *testing.T, first bool, txs []*bt.Tx) *subtreepkg.Subtree {
	t.Helper()

	leaves := len(txs)
	if first {
		leaves++
	}

	st, err := subtreepkg.NewIncompleteTreeByLeafCount(leaves)
	require.NoError(t, err)

	if first {
		require.NoError(t, st.AddCoinbaseNode())
	}

	for _, tx := range txs {
		require.NoError(t, st.AddNode(*tx.TxIDChainHash(), 1, uint64(tx.Size())))
	}

	return st
}

// serializeSubtreeData serializes the subtree_data body for a subtree, with the
// coinbase occupying slot 0 of the first subtree.
func serializeSubtreeData(t *testing.T, st *subtreepkg.Subtree, first bool, coinbase *bt.Tx, txs []*bt.Tx) []byte {
	t.Helper()

	data := subtreepkg.NewSubtreeData(st)

	idx := 0
	if first {
		require.NoError(t, data.AddTx(coinbase, 0))
		idx = 1
	}

	for _, tx := range txs {
		require.NoError(t, data.AddTx(tx, idx))
		idx++
	}

	b, err := data.Serialize()
	require.NoError(t, err)

	return b
}

// storeBlob writes raw bytes under an exact (key, fileType) pair.
func (h *preBindHarness) storeBlob(key *chainhash.Hash, fileType fileformat.FileType, value []byte) {
	h.t.Helper()

	require.NoError(h.t, h.subtreeStore.Set(h.ctx, key[:], fileType, value, bloboptions.WithAllowOverwrite(true)))
}

// forgeSubtreeHeaderRoot returns nodes' own serialization with the 32-byte root the
// .subtree header claims overwritten by claimedRoot. The blob is well formed and
// deserializes cleanly; only its claim is a lie. This is what the claim-only key
// check cannot see, and what Block.CheckMerkleRoot composes verbatim for every
// subtree after the first.
func forgeSubtreeHeaderRoot(t *testing.T, nodes *subtreepkg.Subtree, claimedRoot *chainhash.Hash) []byte {
	t.Helper()

	b, err := nodes.Serialize()
	require.NoError(t, err)
	require.Greater(t, len(b), chainhash.HashSize)

	copy(b[:chainhash.HashSize], claimedRoot[:])

	// Belt and braces: the forgery is only meaningful if the nodes really do hash
	// somewhere else.
	require.False(t, nodes.RootHash().IsEqual(claimedRoot))

	return b
}

// composeBlockMerkleRoot composes per-subtree roots into the header merkle root the
// same way Block.CheckMerkleRoot does, for the equal-length subtrees these fixtures
// build.
func composeBlockMerkleRoot(t *testing.T, roots []chainhash.Hash) *chainhash.Hash {
	t.Helper()

	if len(roots) == 1 {
		root := roots[0]
		return &root
	}

	st, err := subtreepkg.NewIncompleteTreeByLeafCount(len(roots))
	require.NoError(t, err)

	for _, root := range roots {
		require.NoError(t, st.AddNode(root, 1, 0))
	}

	return st.RootHash()
}

// coinbaseSubstitutedRoot is the first subtree's contribution to the header merkle
// root: its own root with the coinbase txid in the placeholder's slot.
func coinbaseSubstitutedRoot(t *testing.T, st *subtreepkg.Subtree, coinbase *bt.Tx) chainhash.Hash {
	t.Helper()

	root, err := st.RootHashWithReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size()))
	require.NoError(t, err)

	return *root
}

// newPreBindBlock assembles the served block. The header chains from genesis so the
// one test that runs to a successful commit can add it to the blockchain store.
func (h *preBindHarness) newPreBindBlock(coinbase *bt.Tx, roots []*chainhash.Hash, merkleRoot *chainhash.Hash, txCount uint64) *model.Block {
	h.t.Helper()

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(h.t, err)

	header := &model.BlockHeader{
		Version:        4,
		HashPrevBlock:  h.genesisHash,
		HashMerkleRoot: merkleRoot,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
	}

	// Regression-net difficulty converges in a few thousand iterations; a header
	// that never reaches AddBlock does not need it, but grinding unconditionally
	// keeps every fixture identical.
	for {
		if ok, _, _ := header.HasMetTargetDifficulty(); ok {
			break
		}

		header.Nonce++
	}

	return &model.Block{
		Header:           header,
		CoinbaseTx:       coinbase,
		Height:           preBindHeight,
		Subtrees:         roots,
		TransactionCount: txCount,
	}
}

// requireNoUTXOMutation is the assertion set every rejection case shares: no block id
// taken, no record written for the transaction the served body would have created,
// and the genuine parent output still unspent.
func (h *preBindHarness) requireNoUTXOMutation(block *model.Block, parent, child *bt.Tx) {
	h.t.Helper()

	require.Zero(h.t, block.ID, "no block id may be set on a body that never bound")
	require.Zero(h.t, h.chain.assignCount(), "AssignBlockID must not be reached")

	_, err := h.utxoStore.Get(h.ctx, child.TxIDChainHash())
	require.Error(h.t, err, "no record at all may exist for the served body's transaction")
	require.True(h.t, errors.Is(err, errors.ErrTxNotFound), "expected not-found, got %v", err)

	h.requireParentUnspent(parent)
}

func (h *preBindHarness) requireParentUnspent(parent *bt.Tx) {
	h.t.Helper()

	utxoHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[0], 0)
	require.NoError(h.t, err)

	resp, err := h.utxoStore.GetSpend(h.ctx, &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash})
	require.NoError(h.t, err)
	require.Equal(h.t, int(utxo.Status_OK), resp.Status, "the genuine parent output must still be unspent")
}

// oneSubtreeBody stores an honest one-subtree body (coinbase + one child spending
// parent) and returns the subtree, so a caller can decide what the header commits to.
func (h *preBindHarness) oneSubtreeBody(coinbase *bt.Tx, child *bt.Tx) *subtreepkg.Subtree {
	h.t.Helper()

	st := buildSubtreeOver(h.t, true, []*bt.Tx{child})

	structureBytes, err := st.Serialize()
	require.NoError(h.t, err)

	h.storeBlob(st.RootHash(), fileformat.FileTypeSubtreeToCheck, structureBytes)
	h.storeBlob(st.RootHash(), fileformat.FileTypeSubtreeData, serializeSubtreeData(h.t, st, true, coinbase, []*bt.Tx{child}))

	return st
}

// TestQuickValidate_MismatchedBody_NoUTXOMutation is the B-029 regression: a body
// served for a checkpoint-certified header that the header does not commit to must
// reach no UTXO mutation at all.
//
// Mutation target: the bindSubtreeBodyToHeader call in quickValidateBlock. The blob
// here is honest under its own key, so the anchor does not fire and the stash is
// unambiguous.
func TestQuickValidate_MismatchedBody_NoUTXOMutation(t *testing.T) {
	h := newPreBindHarness(t, nil)

	coinbase := preBindCoinbase(t, 0x01)
	parent := h.storeGenuineParent(0x11)
	child := preBindSpendOf(t, parent, 9_000)

	served := h.oneSubtreeBody(coinbase, child)

	// The header commits to a different honest body: same coinbase, a different
	// single transaction.
	otherParent := h.storeGenuineParent(0x12)
	otherChild := preBindSpendOf(t, otherParent, 8_000)
	honest := buildSubtreeOver(t, true, []*bt.Tx{otherChild})

	block := h.newPreBindBlock(coinbase,
		[]*chainhash.Hash{served.RootHash()},
		composeBlockMerkleRoot(t, []chainhash.Hash{coinbaseSubstitutedRoot(t, honest, coinbase)}),
		2)

	err := h.bv.quickValidateBlock(h.ctx, block, "peer", "")
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "an unbound body is a corrupt download, got %v", err)

	h.requireNoUTXOMutation(block, parent, child)
}

// TestQuickValidate_HonestBodyAfterMismatch_Validates guards against over-rejection:
// the honest body for its own header still validates, creates its child and spends
// its parent. This is the "honest retry" of the attack sequence.
func TestQuickValidate_HonestBodyAfterMismatch_Validates(t *testing.T) {
	h := newPreBindHarness(t, nil)

	coinbase := preBindCoinbase(t, 0x02)
	parent := h.storeGenuineParent(0x21)
	child := preBindSpendOf(t, parent, 9_000)

	served := h.oneSubtreeBody(coinbase, child)

	block := h.newPreBindBlock(coinbase,
		[]*chainhash.Hash{served.RootHash()},
		composeBlockMerkleRoot(t, []chainhash.Hash{coinbaseSubstitutedRoot(t, served, coinbase)}),
		2)

	require.NoError(t, h.bv.quickValidateBlock(h.ctx, block, "peer", ""))

	created, err := h.utxoStore.Get(h.ctx, child.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, child.TxIDChainHash().String(), created.Tx.TxID())

	utxoHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[0], 0)
	require.NoError(t, err)

	resp, err := h.utxoStore.GetSpend(h.ctx, &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash})
	require.NoError(t, err)
	require.Equal(t, int(utxo.Status_SPENT), resp.Status)
}

// TestQuickValidate_DuplicateTx_NoUTXOMutation pins that the CVE-2012-2459 scan now
// runs before the mutations. The duplicated trailing transaction produces the SAME
// merkle root, and so the same block hash, as the honest body.
func TestQuickValidate_DuplicateTx_NoUTXOMutation(t *testing.T) {
	h := newPreBindHarness(t, nil)

	coinbase := preBindCoinbase(t, 0x03)
	parent := h.storeGenuineParent(0x31)
	child := preBindSpendOf(t, parent, 9_000)

	// Two subtrees, the second holding the same transaction twice: the
	// duplicate-last-node-when-odd rule makes this hash exactly as the honest
	// [coinbase, child, child] body does.
	st := buildSubtreeOver(t, true, []*bt.Tx{child})
	dup := buildSubtreeOver(t, false, []*bt.Tx{child, child})

	structureBytes, err := st.Serialize()
	require.NoError(t, err)
	h.storeBlob(st.RootHash(), fileformat.FileTypeSubtreeToCheck, structureBytes)
	h.storeBlob(st.RootHash(), fileformat.FileTypeSubtreeData, serializeSubtreeData(t, st, true, coinbase, []*bt.Tx{child}))

	dupBytes, err := dup.Serialize()
	require.NoError(t, err)
	h.storeBlob(dup.RootHash(), fileformat.FileTypeSubtreeToCheck, dupBytes)
	h.storeBlob(dup.RootHash(), fileformat.FileTypeSubtreeData, serializeSubtreeData(t, dup, false, nil, []*bt.Tx{child, child}))

	block := h.newPreBindBlock(coinbase,
		[]*chainhash.Hash{st.RootHash(), dup.RootHash()},
		composeBlockMerkleRoot(t, []chainhash.Hash{coinbaseSubstitutedRoot(t, st, coinbase), *dup.RootHash()}),
		4)

	err = h.bv.quickValidateBlock(h.ctx, block, "peer", "")
	require.Error(t, err)

	h.requireNoUTXOMutation(block, parent, child)
}

// TestQuickValidate_NonPlaceholderFirstNode_NoUTXOMutation covers a first subtree
// whose node 0 is a real txid rather than the coinbase placeholder, on the structure
// AS SERVED. Before this change the check ran on the rebuilt slice, where the
// placeholder had just been written.
func TestQuickValidate_NonPlaceholderFirstNode_NoUTXOMutation(t *testing.T) {
	h := newPreBindHarness(t, nil)

	coinbase := preBindCoinbase(t, 0x04)
	parent := h.storeGenuineParent(0x41)
	child := preBindSpendOf(t, parent, 9_000)
	other := preBindSpendOf(t, parent, 8_500)

	// No coinbase placeholder: node 0 is an ordinary txid.
	st := buildSubtreeOver(t, false, []*bt.Tx{other, child})

	structureBytes, err := st.Serialize()
	require.NoError(t, err)
	h.storeBlob(st.RootHash(), fileformat.FileTypeSubtreeToCheck, structureBytes)
	h.storeBlob(st.RootHash(), fileformat.FileTypeSubtreeData, serializeSubtreeData(t, st, false, nil, []*bt.Tx{other, child}))

	block := h.newPreBindBlock(coinbase,
		[]*chainhash.Hash{st.RootHash()},
		composeBlockMerkleRoot(t, []chainhash.Hash{coinbaseSubstitutedRoot(t, st, coinbase)}),
		3)

	err = h.bv.quickValidateBlock(h.ctx, block, "peer", "")
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "got %v", err)

	h.requireNoUTXOMutation(block, parent, child)
}

// forgedHeaderRootFixture stores a two-subtree body whose SECOND subtree blob claims
// the honest root while its nodes hash elsewhere, and returns the key it is stored
// under plus the transactions the forged node list carries.
//
// Two subtrees is the point, not incidental: Block.CheckMerkleRoot recomputes the
// first subtree's root (it substitutes the coinbase) and composes the CACHED claim
// for every subtree after it, so the forgery only binds from index 1 onwards.
func (h *preBindHarness) forgedHeaderRootFixture(coinbase *bt.Tx, parent *bt.Tx, forgedFileType fileformat.FileType) (roots []*chainhash.Hash, merkleRoot *chainhash.Hash, forgedChild *bt.Tx) {
	h.t.Helper()

	filler := preBindSpendOf(h.t, parent, 1_000)
	first := buildSubtreeOver(h.t, true, []*bt.Tx{filler})

	firstBytes, err := first.Serialize()
	require.NoError(h.t, err)
	h.storeBlob(first.RootHash(), fileformat.FileTypeSubtreeToCheck, firstBytes)
	h.storeBlob(first.RootHash(), fileformat.FileTypeSubtreeData, serializeSubtreeData(h.t, first, true, coinbase, []*bt.Tx{filler}))

	// The honest second subtree, whose root the header commits to. Its blob is never
	// served; only its root is borrowed as the key and the claim.
	honestA := preBindSpendOf(h.t, parent, 2_000)
	honestB := preBindSpendOf(h.t, parent, 2_100)
	honest := buildSubtreeOver(h.t, false, []*bt.Tx{honestA, honestB})

	// The attacker's node list, carrying the transaction they want created and the
	// genuine parent output they want spent.
	forgedChild = preBindSpendOf(h.t, parent, 9_000)
	forgedFiller := preBindSpendOf(h.t, parent, 3_000)
	forged := buildSubtreeOver(h.t, false, []*bt.Tx{forgedFiller, forgedChild})

	key := honest.RootHash()

	h.storeBlob(key, forgedFileType, forgeSubtreeHeaderRoot(h.t, forged, key))
	h.storeBlob(key, fileformat.FileTypeSubtreeData, serializeSubtreeData(h.t, forged, false, nil, []*bt.Tx{forgedFiller, forgedChild}))

	roots = []*chainhash.Hash{first.RootHash(), key}
	merkleRoot = composeBlockMerkleRoot(h.t, []chainhash.Hash{coinbaseSubstitutedRoot(h.t, first, coinbase), *key})

	return roots, merkleRoot, forgedChild
}

// TestQuickValidate_ForgedSubtreeHeaderRoot_NoUTXOMutation is the cache-trust
// regression. The blob's 32-byte header root is the key it is stored under while its
// serialized nodes are an unrelated list, so the claim-only key check passes and the
// body binds — unless the root is recomputed from the nodes.
//
// Mutation target: the recomputed-root comparison in
// model.ValidateSubtreeNodesMatchKey. Reverting it to ValidateSubtreeMatchesKey
// (claim only) must let this body bind and mutate state. An ordinarily-serialized
// subtree under a wrong key does NOT test this; that is the case below.
func TestQuickValidate_ForgedSubtreeHeaderRoot_NoUTXOMutation(t *testing.T) {
	h := newPreBindHarness(t, nil)

	coinbase := preBindCoinbase(t, 0x05)
	parent := h.storeGenuineParent(0x51)

	roots, merkleRoot, forgedChild := h.forgedHeaderRootFixture(coinbase, parent, fileformat.FileTypeSubtreeToCheck)

	block := h.newPreBindBlock(coinbase, roots, merkleRoot, 4)

	err := h.bv.quickValidateBlock(h.ctx, block, "peer", "")
	require.Error(t, err)

	h.requireNoUTXOMutation(block, parent, forgedChild)

	exists, err := h.subtreeStore.Exists(h.ctx, roots[1][:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.False(t, exists, "the forged blob must be quarantined")
}

// TestQuickValidate_SubtreeBlobUnderWrongKey_NoUTXOMutation covers the ordinary
// version of the same class: an honest, ordinarily-serialized subtree stored under a
// key that is not its root. The verdict must be a LOCAL fault, not corrupt — the
// fetch path verifies bytes against the requested hash before storing, so a mismatch
// seen at read time cannot be charged to the peer currently serving.
func TestQuickValidate_SubtreeBlobUnderWrongKey_NoUTXOMutation(t *testing.T) {
	h := newPreBindHarness(t, nil)

	coinbase := preBindCoinbase(t, 0x06)
	parent := h.storeGenuineParent(0x61)
	child := preBindSpendOf(t, parent, 9_000)

	st := buildSubtreeOver(t, true, []*bt.Tx{child})

	// Ordinarily serialized — its own claim is its own root — but stored under a key
	// that belongs to a different body.
	borrowed := buildSubtreeOver(t, true, []*bt.Tx{preBindSpendOf(t, parent, 4_000)})
	key := borrowed.RootHash()

	structureBytes, err := st.Serialize()
	require.NoError(t, err)
	h.storeBlob(key, fileformat.FileTypeSubtreeToCheck, structureBytes)
	h.storeBlob(key, fileformat.FileTypeSubtreeData, serializeSubtreeData(t, st, true, coinbase, []*bt.Tx{child}))

	block := h.newPreBindBlock(coinbase,
		[]*chainhash.Hash{key},
		composeBlockMerkleRoot(t, []chainhash.Hash{coinbaseSubstitutedRoot(t, borrowed, coinbase)}),
		2)

	err = h.bv.quickValidateBlock(h.ctx, block, "peer", "")
	require.Error(t, err)
	require.False(t, errors.IsBlockCorrupt(err), "a local blob fault must not condemn the peer's body, got %v", err)

	h.requireNoUTXOMutation(block, parent, child)

	exists, err := h.subtreeStore.Exists(h.ctx, key[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.False(t, exists, "the mismatching blob must be quarantined")
}

// TestQuickValidateAsync_MismatchedBody_NoUTXOMutation drives the DEFAULT catch-up
// entry point, with a real write-job channel and worker, and pins the two contract
// properties tryQuickValidation depends on: the returned WaitGroup is Wait()-safe and
// freshlyWritten is nil when nothing was queued.
func TestQuickValidateAsync_MismatchedBody_NoUTXOMutation(t *testing.T) {
	h := newPreBindHarness(t, nil)

	coinbase := preBindCoinbase(t, 0x07)
	parent := h.storeGenuineParent(0x71)
	child := preBindSpendOf(t, parent, 9_000)

	served := h.oneSubtreeBody(coinbase, child)

	honest := buildSubtreeOver(t, true, []*bt.Tx{preBindSpendOf(t, parent, 5_000)})

	block := h.newPreBindBlock(coinbase,
		[]*chainhash.Hash{served.RootHash()},
		composeBlockMerkleRoot(t, []chainhash.Hash{coinbaseSubstitutedRoot(t, honest, coinbase)}),
		2)

	writeJobsChan := make(chan *SubtreeWriteJob, 16)

	g, gCtx := errgroup.WithContext(h.ctx)
	g.Go(func() error { return h.bv.subtreeWriteWorker(gCtx, writeJobsChan) })

	wg, freshlyWritten, err := h.bv.quickValidateBlockAsync(h.ctx, block, "peer", "", writeJobsChan)

	close(writeJobsChan)
	require.NoError(t, g.Wait())

	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "got %v", err)
	require.NotNil(t, wg)
	require.Nil(t, freshlyWritten)

	waited := make(chan struct{})
	go func() {
		wg.Wait()
		close(waited)
	}()

	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("returned WaitGroup was not Wait()-safe")
	}

	h.requireNoUTXOMutation(block, parent, child)
}

// TestQuickValidate_Sequential_MismatchedBody_NoUTXOMutation runs the same body
// through the sequential variant, selected with SubtreeBatchPrefetchDepth = 0.
func TestQuickValidate_Sequential_MismatchedBody_NoUTXOMutation(t *testing.T) {
	h := newPreBindHarness(t, nil)
	h.bv.settings.BlockValidation.SubtreeBatchPrefetchDepth = 0

	coinbase := preBindCoinbase(t, 0x08)
	parent := h.storeGenuineParent(0x81)
	child := preBindSpendOf(t, parent, 9_000)

	served := h.oneSubtreeBody(coinbase, child)

	honest := buildSubtreeOver(t, true, []*bt.Tx{preBindSpendOf(t, parent, 5_500)})

	block := h.newPreBindBlock(coinbase,
		[]*chainhash.Hash{served.RootHash()},
		composeBlockMerkleRoot(t, []chainhash.Hash{coinbaseSubstitutedRoot(t, honest, coinbase)}),
		2)

	err := h.bv.quickValidateBlock(h.ctx, block, "peer", "")
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "got %v", err)

	h.requireNoUTXOMutation(block, parent, child)
}

// TestQuickValidate_Sequential_HonestBody_WritesSubtreeFiles is the evidence that the
// sequential write phase used to panic: it indexed a per-batch slice the sequential
// batch builder never allocated, which nothing reached because the pipelined variants
// are the default. Run against the base commit this test panics with
// index-out-of-range; here it must simply pass.
func TestQuickValidate_Sequential_HonestBody_WritesSubtreeFiles(t *testing.T) {
	h := newPreBindHarness(t, nil)
	h.bv.settings.BlockValidation.SubtreeBatchPrefetchDepth = 0

	coinbase := preBindCoinbase(t, 0x09)
	parent := h.storeGenuineParent(0x91)
	child := preBindSpendOf(t, parent, 9_000)

	served := h.oneSubtreeBody(coinbase, child)

	block := h.newPreBindBlock(coinbase,
		[]*chainhash.Hash{served.RootHash()},
		composeBlockMerkleRoot(t, []chainhash.Hash{coinbaseSubstitutedRoot(t, served, coinbase)}),
		2)

	require.NotPanics(t, func() {
		_, err := h.bv.processBlockSubtrees(h.ctx, block, false)
		require.NoError(t, err)
	})

	exists, err := h.subtreeStore.Exists(h.ctx, served.RootHash()[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, exists, "the sequential write phase must have written the full subtree")
}

// TestQuickValidate_ForgedFullSubtreeBlob_NoUTXOMutation is INVARIANT PM for the
// already-present full-subtree path: both blobs exist for the same key, the
// FileTypeSubtreeToCheck one honest and the promoted FileTypeSubtree one forged.
//
// Run through all three variants, because the consumer of that blob runs BESIDE
// createAndSpendUTXOsForBatch in the two pipelined variants and AFTER it in the
// sequential one. Asserting the set in every variant is what proves the abort
// happened before create and spend, not in the arm parallel to them.
//
// Mutation target: the carry. Moving the full-blob anchor back into
// buildSubtreeAndQueueWrite / writeSubtreeFilesFromTxs must make this test observe
// mutations.
func TestQuickValidate_ForgedFullSubtreeBlob_NoUTXOMutation(t *testing.T) {
	for _, variant := range []struct {
		name          string
		prefetchDepth int
		async         bool
	}{
		{name: "sequential", prefetchDepth: 0},
		{name: "pipeline", prefetchDepth: 2},
		{name: "async", prefetchDepth: 2, async: true},
	} {
		t.Run(variant.name, func(t *testing.T) {
			h := newPreBindHarness(t, nil)
			h.bv.settings.BlockValidation.SubtreeBatchPrefetchDepth = variant.prefetchDepth

			coinbase := preBindCoinbase(t, 0x0a)
			parent := h.storeGenuineParent(0xa1)
			child := preBindSpendOf(t, parent, 9_000)

			served := h.oneSubtreeBody(coinbase, child)

			// The promoted blob under the SAME key, forged: its header claims the key
			// while its nodes are an unrelated list.
			unrelated := buildSubtreeOver(t, true, []*bt.Tx{preBindSpendOf(t, parent, 6_000)})
			h.storeBlob(served.RootHash(), fileformat.FileTypeSubtree, forgeSubtreeHeaderRoot(t, unrelated, served.RootHash()))

			block := h.newPreBindBlock(coinbase,
				[]*chainhash.Hash{served.RootHash()},
				composeBlockMerkleRoot(t, []chainhash.Hash{coinbaseSubstitutedRoot(t, served, coinbase)}),
				2)

			var err error

			if variant.async {
				writeJobsChan := make(chan *SubtreeWriteJob, 16)

				g, gCtx := errgroup.WithContext(h.ctx)
				g.Go(func() error { return h.bv.subtreeWriteWorker(gCtx, writeJobsChan) })

				_, _, err = h.bv.quickValidateBlockAsync(h.ctx, block, "peer", "", writeJobsChan)

				close(writeJobsChan)
				require.NoError(t, g.Wait())
			} else {
				err = h.bv.quickValidateBlock(h.ctx, block, "peer", "")
			}

			require.Error(t, err)

			h.requireNoUTXOMutation(block, parent, child)

			exists, existsErr := h.subtreeStore.Exists(h.ctx, served.RootHash()[:], fileformat.FileTypeSubtree)
			require.NoError(t, existsErr)
			require.False(t, exists, "the forged promoted blob must be quarantined")
		})
	}
}

// undeletableSubtreeStore reports every Del as a success without removing anything,
// so the quarantine's own confirmation is the only thing that can notice. This is not
// a store mock in the sense the conventions forbid: the UTXO store and the blockchain
// store are still the real sqlitememory ones.
type undeletableSubtreeStore struct {
	blob.Store
}

func (s *undeletableSubtreeStore) Del(_ context.Context, _ []byte, _ fileformat.FileType, _ ...bloboptions.FileOption) error {
	return nil
}

// newAbortServer wraps the harness in the minimal Server tryQuickValidation needs.
// The block-assembly client is deliberately nil, which that function treats as "not
// available" and skips, exactly as it does in the other catch-up tests.
func (h *preBindHarness) newAbortServer() (*Server, *CatchupContext) {
	h.t.Helper()

	server := &Server{
		logger:           h.bv.logger,
		settings:         h.bv.settings,
		blockValidation:  h.bv,
		blockchainClient: h.chain,
		utxoStore:        h.utxoStore,
		subtreeStore:     h.subtreeStore,
	}

	return server, &CatchupContext{
		useQuickValidation:      true,
		highestCheckpointHeight: preBindHeight,
		peerID:                  "peer",
		baseURL:                 "http://peer",
	}
}

// TestQuickValidate_QuarantineUnconfirmed_AbortsWithoutFallthrough pins that an
// unconfirmable quarantine ABORTS rather than falling through to normal validation,
// whose loader checks only the .subtree header's claimed root and so cannot detect
// the blob this route just rejected.
//
// Mutation target: the isUnquarantinedLocalSubtree branch in tryQuickValidation.
func TestQuickValidate_QuarantineUnconfirmed_AbortsWithoutFallthrough(t *testing.T) {
	h := newPreBindHarness(t, &undeletableSubtreeStore{Store: blobmemory.New()})

	coinbase := preBindCoinbase(t, 0x0b)
	parent := h.storeGenuineParent(0xb1)
	child := preBindSpendOf(t, parent, 9_000)

	st := buildSubtreeOver(t, true, []*bt.Tx{child})
	borrowed := buildSubtreeOver(t, true, []*bt.Tx{preBindSpendOf(t, parent, 4_500)})
	key := borrowed.RootHash()

	structureBytes, err := st.Serialize()
	require.NoError(t, err)
	h.storeBlob(key, fileformat.FileTypeSubtreeToCheck, structureBytes)
	h.storeBlob(key, fileformat.FileTypeSubtreeData, serializeSubtreeData(t, st, true, coinbase, []*bt.Tx{child}))

	block := h.newPreBindBlock(coinbase,
		[]*chainhash.Hash{key},
		composeBlockMerkleRoot(t, []chainhash.Hash{coinbaseSubstitutedRoot(t, borrowed, coinbase)}),
		2)

	server, catchupCtx := h.newAbortServer()
	catchupCtx.blockUpTo = block

	writeJobsChan := make(chan *SubtreeWriteJob, 16)

	g, gCtx := errgroup.WithContext(h.ctx)
	g.Go(func() error { return h.bv.subtreeWriteWorker(gCtx, writeJobsChan) })

	tryNormal, err := server.tryQuickValidation(h.ctx, block, catchupCtx, "peer", "http://peer", writeJobsChan, nil)

	close(writeJobsChan)
	require.NoError(t, g.Wait())

	require.False(t, tryNormal, "normal validation must NOT be attempted on an unquarantined mismatching blob")
	require.Error(t, err)
	require.True(t, isUnquarantinedLocalSubtree(err), "got %v", err)
	require.False(t, errors.IsBlockCorrupt(err), "a local blob fault must not condemn the peer's body")
	require.Empty(t, catchupCtx.corruptBlockHash, "no ban score may be applied for a local storage fault")

	h.requireNoUTXOMutation(block, parent, child)
}

// replacingSubtreeStore serves honest bytes for a key on the FIRST GetIoReader and
// forged bytes on every later one, so the whole-block pass passes and the per-batch
// read fails. blockDelete additionally makes Del a silent no-op.
type replacingSubtreeStore struct {
	blob.Store

	mu          sync.Mutex
	forged      map[string][]byte
	honest      map[string]int
	served      map[string]int
	blockDelete bool
}

func newReplacingSubtreeStore(inner blob.Store) *replacingSubtreeStore {
	return &replacingSubtreeStore{
		Store:  inner,
		forged: make(map[string][]byte),
		honest: make(map[string]int),
		served: make(map[string]int),
	}
}

func (s *replacingSubtreeStore) replaceAfterFirstRead(key *chainhash.Hash, fileType fileformat.FileType, forged []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.forged["reader:"+string(key[:])+string(fileType)] = forged
}

// replaceAfterFirstGet is the same for the whole-blob read path, which uses Get
// rather than GetIoReader. Keyed separately so a subtree read through one method
// cannot consume the other's first-read allowance.
func (s *replacingSubtreeStore) replaceAfterFirstGet(key *chainhash.Hash, fileType fileformat.FileType, forged []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.forged["get:"+string(key[:])+string(fileType)] = forged
}

// replaceAfterNGets is replaceAfterFirstGet with an explicit number of honest reads,
// for the case that has to distinguish "two reads, both anchored" from "a third read
// nothing anchors".
func (s *replacingSubtreeStore) replaceAfterNGets(key *chainhash.Hash, fileType fileformat.FileType, honest int, forged []byte) {
	mapKey := "get:" + string(key[:]) + string(fileType)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.forged[mapKey] = forged
	s.honest[mapKey] = honest
}

// replacementFor reports the forged bytes to serve for this call, once a tracked
// pair's honest-read allowance is used up.
func (s *replacingSubtreeStore) replacementFor(method string, key []byte, fileType fileformat.FileType) ([]byte, bool) {
	mapKey := method + ":" + string(key) + string(fileType)

	s.mu.Lock()
	defer s.mu.Unlock()

	forged, tracked := s.forged[mapKey]
	if !tracked {
		return nil, false
	}

	honest, ok := s.honest[mapKey]
	if !ok {
		honest = 1
	}

	served := s.served[mapKey]
	s.served[mapKey] = served + 1

	if served < honest {
		return nil, false
	}

	return forged, true
}

// servedCount reports how many times a tracked pair has been read through method.
func (s *replacingSubtreeStore) servedCount(method string, key *chainhash.Hash, fileType fileformat.FileType) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.served[method+":"+string(key[:])+string(fileType)]
}

func (s *replacingSubtreeStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...bloboptions.FileOption) (io.ReadCloser, error) {
	if forged, replace := s.replacementFor("reader", key, fileType); replace {
		return io.NopCloser(bytes.NewReader(forged)), nil
	}

	return s.Store.GetIoReader(ctx, key, fileType, opts...)
}

func (s *replacingSubtreeStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...bloboptions.FileOption) ([]byte, error) {
	if forged, replace := s.replacementFor("get", key, fileType); replace {
		return forged, nil
	}

	return s.Store.Get(ctx, key, fileType, opts...)
}

func (s *replacingSubtreeStore) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...bloboptions.FileOption) error {
	s.mu.Lock()
	blocked := s.blockDelete
	s.mu.Unlock()

	if blocked {
		return nil
	}

	return s.Store.Del(ctx, key, fileType, opts...)
}

// TestQuickValidate_BlobReplacedAfterPreBind_QuarantinedAndAborts covers a mismatch
// produced OUTSIDE the whole-block pass: the blob is honest when that pass reads it
// and forged when the batch reader reads it again. The error therefore comes from a
// per-batch reader, and the quarantine has to be reached from there too.
//
// Mutation target: wiring the quarantine only into the whole-block pass instead of
// the deferred boundary on both entry points must make this test observe a
// fall-through to normal validation.
func TestQuickValidate_BlobReplacedAfterPreBind_QuarantinedAndAborts(t *testing.T) {
	for _, variant := range []struct {
		name          string
		prefetchDepth int
		async         bool
	}{
		{name: "sequential", prefetchDepth: 0},
		{name: "pipeline", prefetchDepth: 2},
		{name: "async", prefetchDepth: 2, async: true},
	} {
		t.Run(variant.name, func(t *testing.T) {
			store := newReplacingSubtreeStore(blobmemory.New())

			h := newPreBindHarness(t, store)
			h.bv.settings.BlockValidation.SubtreeBatchPrefetchDepth = variant.prefetchDepth

			coinbase := preBindCoinbase(t, 0x0c)
			parent := h.storeGenuineParent(0xc1)
			child := preBindSpendOf(t, parent, 9_000)

			served := h.oneSubtreeBody(coinbase, child)

			unrelated := buildSubtreeOver(t, true, []*bt.Tx{preBindSpendOf(t, parent, 7_000)})
			store.replaceAfterFirstRead(served.RootHash(), fileformat.FileTypeSubtreeToCheck,
				forgeSubtreeHeaderRoot(t, unrelated, served.RootHash()))

			block := h.newPreBindBlock(coinbase,
				[]*chainhash.Hash{served.RootHash()},
				composeBlockMerkleRoot(t, []chainhash.Hash{coinbaseSubstitutedRoot(t, served, coinbase)}),
				2)

			var err error

			if variant.async {
				writeJobsChan := make(chan *SubtreeWriteJob, 16)

				g, gCtx := errgroup.WithContext(h.ctx)
				g.Go(func() error { return h.bv.subtreeWriteWorker(gCtx, writeJobsChan) })

				_, _, err = h.bv.quickValidateBlockAsync(h.ctx, block, "peer", "", writeJobsChan)

				close(writeJobsChan)
				require.NoError(t, g.Wait())
			} else {
				err = h.bv.quickValidateBlock(h.ctx, block, "peer", "")
			}

			require.Error(t, err)

			h.requireNoUTXOMutation(block, parent, child)

			exists, existsErr := h.subtreeStore.Exists(h.ctx, served.RootHash()[:], fileformat.FileTypeSubtreeToCheck)
			require.NoError(t, existsErr)
			require.False(t, exists, "a mismatch found by the batch reader must be quarantined too")
		})
	}
}

// TestQuickValidate_BlobReplacedAfterPreBind_UndeletableAborts is the same
// replacement with deletion blocked: the attempt must abort rather than hand the
// surviving blob to normal validation's claim-only loader.
func TestQuickValidate_BlobReplacedAfterPreBind_UndeletableAborts(t *testing.T) {
	store := newReplacingSubtreeStore(blobmemory.New())

	h := newPreBindHarness(t, store)

	coinbase := preBindCoinbase(t, 0x0d)
	parent := h.storeGenuineParent(0xd1)
	child := preBindSpendOf(t, parent, 9_000)

	served := h.oneSubtreeBody(coinbase, child)

	unrelated := buildSubtreeOver(t, true, []*bt.Tx{preBindSpendOf(t, parent, 7_500)})
	store.replaceAfterFirstRead(served.RootHash(), fileformat.FileTypeSubtreeToCheck,
		forgeSubtreeHeaderRoot(t, unrelated, served.RootHash()))

	store.mu.Lock()
	store.blockDelete = true
	store.mu.Unlock()

	block := h.newPreBindBlock(coinbase,
		[]*chainhash.Hash{served.RootHash()},
		composeBlockMerkleRoot(t, []chainhash.Hash{coinbaseSubstitutedRoot(t, served, coinbase)}),
		2)

	server, catchupCtx := h.newAbortServer()
	catchupCtx.blockUpTo = block

	writeJobsChan := make(chan *SubtreeWriteJob, 16)

	g, gCtx := errgroup.WithContext(h.ctx)
	g.Go(func() error { return h.bv.subtreeWriteWorker(gCtx, writeJobsChan) })

	tryNormal, err := server.tryQuickValidation(h.ctx, block, catchupCtx, "peer", "http://peer", writeJobsChan, nil)

	close(writeJobsChan)
	require.NoError(t, g.Wait())

	require.False(t, tryNormal, "normal validation must NOT be attempted on an unquarantined mismatching blob")
	require.Error(t, err)
	require.True(t, isUnquarantinedLocalSubtree(err), "got %v", err)

	h.requireNoUTXOMutation(block, parent, child)
}

// zeroNodeSubtreeBlob is the serialization of a zero-node subtree, used to prove the
// recomputing key check fails closed rather than indexing an empty merkle store.
func zeroNodeSubtreeBlob(root chainhash.Hash) []byte {
	blob := make([]byte, 64)
	copy(blob[:32], root[:])
	binary.LittleEndian.PutUint64(blob[48:56], 0)
	binary.LittleEndian.PutUint64(blob[56:64], 0)

	return blob
}

// TestValidateSubtreeNodesMatchKey_FailsClosed pins the guards on the helper itself:
// nil inputs and an empty node list are rejected rather than panicking, and a
// claim that agrees with the key does not rescue a node list that hashes elsewhere.
func TestValidateSubtreeNodesMatchKey_FailsClosed(t *testing.T) {
	key := chainhash.Hash{0x01}

	require.Error(t, model.ValidateSubtreeNodesMatchKey(nil, &key))

	honest := buildSubtreeOver(t, false, []*bt.Tx{preBindSpendOf(t, bt.NewTx(), 1), preBindSpendOf(t, bt.NewTx(), 2)})
	require.Error(t, model.ValidateSubtreeNodesMatchKey(honest, nil))

	require.NoError(t, model.ValidateSubtreeNodesMatchKey(honest, honest.RootHash()))
	require.Error(t, model.ValidateSubtreeNodesMatchKey(honest, &key))

	empty, err := subtreepkg.NewSubtreeFromBytes(zeroNodeSubtreeBlob(key))
	require.NoError(t, err)
	require.NotPanics(t, func() {
		require.Error(t, model.ValidateSubtreeNodesMatchKey(empty, &key))
	})
}

// gatedSubtreeStore delays the existence probe for one exact (key, fileType) pair
// until either the caller's context is cancelled or a short grace period elapses.
//
// It exists to make one specific failure deterministic: if the whole-block pass ran
// its reads under a cancellation context, the first failing read would cancel the
// rest, and a gated sibling would return that cancellation in place of its own anchor
// verdict — so its forged blob would never be named for the quarantine. Under the
// cancelling shape the gate resolves via ctx.Done() immediately; under the
// non-cancelling one it waits out the grace period and reads normally.
type gatedSubtreeStore struct {
	blob.Store

	gateKey   string
	graceTime time.Duration
}

func newGatedSubtreeStore(inner blob.Store, key *chainhash.Hash, fileType fileformat.FileType, grace time.Duration) *gatedSubtreeStore {
	return &gatedSubtreeStore{
		Store:     inner,
		gateKey:   string(key[:]) + string(fileType),
		graceTime: grace,
	}
}

func (s *gatedSubtreeStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...bloboptions.FileOption) (bool, error) {
	if string(key)+string(fileType) == s.gateKey {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(s.graceTime):
		}
	}

	return s.Store.Exists(ctx, key, fileType, opts...)
}

// storeForgedStructure stores a blob whose .subtree header claims claimedRoot while
// its serialized nodes hash elsewhere, under claimedRoot as the key.
func (h *preBindHarness) storeForgedStructure(claimedRoot *chainhash.Hash, nodes *subtreepkg.Subtree, fileType fileformat.FileType) {
	h.t.Helper()

	h.storeBlob(claimedRoot, fileType, forgeSubtreeHeaderRoot(h.t, nodes, claimedRoot))
}

// TestQuickValidate_TwoForgedBlobs_BothQuarantined pins that the whole-block pass
// names EVERY mismatching blob, not just whichever read failed first.
//
// The second subtree's existence probe is gated, so under a cancellation-scoped
// errgroup its read would be cancelled by the first subtree's failure and its blob
// would survive on disk — after which the attempt is classified an ordinary local
// fault and normal validation is handed a forged blob its loader cannot detect. That
// is the fall-through the whole-block pass exists to prevent.
//
// Mutation target: making the whole-block pass use errgroup.WithContext (so the first
// failing read cancels its siblings) must leave the second blob present.
func TestQuickValidate_TwoForgedBlobs_BothQuarantined(t *testing.T) {
	inner := blobmemory.New()

	coinbase := preBindCoinbase(t, 0x0f)

	// Built before the harness so the gate can name the second subtree's key.
	firstKey := chainhash.HashH([]byte("prebind-forged-key-one"))
	secondKey := chainhash.HashH([]byte("prebind-forged-key-two"))

	h := newPreBindHarness(t, newGatedSubtreeStore(inner, &secondKey, fileformat.FileTypeSubtreeToCheck, 400*time.Millisecond))

	parent := h.storeGenuineParent(0xf1)
	child := preBindSpendOf(t, parent, 9_000)

	h.storeForgedStructure(&firstKey, buildSubtreeOver(t, true, []*bt.Tx{child}), fileformat.FileTypeSubtreeToCheck)
	h.storeForgedStructure(&secondKey, buildSubtreeOver(t, false, []*bt.Tx{preBindSpendOf(t, parent, 8_000), child}), fileformat.FileTypeSubtreeToCheck)

	block := h.newPreBindBlock(coinbase,
		[]*chainhash.Hash{&firstKey, &secondKey},
		composeBlockMerkleRoot(t, []chainhash.Hash{firstKey, secondKey}),
		4)

	err := h.bv.quickValidateBlock(h.ctx, block, "peer", "")
	require.Error(t, err)
	require.False(t, errors.IsBlockCorrupt(err), "a local blob fault must not condemn the peer's body, got %v", err)

	h.requireNoUTXOMutation(block, parent, child)

	for _, key := range []*chainhash.Hash{&firstKey, &secondKey} {
		exists, existsErr := h.subtreeStore.Exists(h.ctx, key[:], fileformat.FileTypeSubtreeToCheck)
		require.NoError(t, existsErr)
		require.False(t, exists, "every mismatching blob the body named must be quarantined, including %s", key)
	}
}

// TestQuickValidate_FullSubtreeBlobReplacedAfterPreBind_NoUTXOMutation is the
// mutation proof for anchoring the already-present full subtree AT THE BATCH READ.
//
// The promoted blob is honest when the whole-block pass reads it and forged when the
// batch reads it again, so only the per-batch anchor can catch it. That is the window
// the carry closes: the blob is read and anchored once, in the read, and handed to the
// write phase — rather than being re-read there, in the arm that runs beside
// createAndSpendUTXOsForBatch in both pipelined variants and after it in the
// sequential one.
//
// The sibling case where the blob is forged from the start cannot prove this: the
// whole-block pass rejects it before any batch runs, so it stays green if the
// per-batch anchor is removed.
//
// Mutation target: moving the full-blob load and anchor out of readSubtreeStructure
// and back into buildSubtreeAndQueueWrite / writeSubtreeFilesFromTxs must make this
// test observe mutations in every variant.
func TestQuickValidate_FullSubtreeBlobReplacedAfterPreBind_NoUTXOMutation(t *testing.T) {
	for _, variant := range []struct {
		name          string
		prefetchDepth int
		async         bool
	}{
		{name: "sequential", prefetchDepth: 0},
		{name: "pipeline", prefetchDepth: 2},
		{name: "async", prefetchDepth: 2, async: true},
	} {
		t.Run(variant.name, func(t *testing.T) {
			store := newReplacingSubtreeStore(blobmemory.New())

			h := newPreBindHarness(t, store)
			h.bv.settings.BlockValidation.SubtreeBatchPrefetchDepth = variant.prefetchDepth

			coinbase := preBindCoinbase(t, 0x10)
			parent := h.storeGenuineParent(0x1a)
			child := preBindSpendOf(t, parent, 9_000)

			served := h.oneSubtreeBody(coinbase, child)

			// The promoted blob, honest, as a completed earlier attempt would have left
			// it — and replaced by a forged one from the second read onwards.
			honestFull, err := served.Serialize()
			require.NoError(t, err)
			h.storeBlob(served.RootHash(), fileformat.FileTypeSubtree, honestFull)

			unrelated := buildSubtreeOver(t, true, []*bt.Tx{preBindSpendOf(t, parent, 6_500)})
			store.replaceAfterFirstGet(served.RootHash(), fileformat.FileTypeSubtree,
				forgeSubtreeHeaderRoot(t, unrelated, served.RootHash()))

			block := h.newPreBindBlock(coinbase,
				[]*chainhash.Hash{served.RootHash()},
				composeBlockMerkleRoot(t, []chainhash.Hash{coinbaseSubstitutedRoot(t, served, coinbase)}),
				2)

			if variant.async {
				writeJobsChan := make(chan *SubtreeWriteJob, 16)

				g, gCtx := errgroup.WithContext(h.ctx)
				g.Go(func() error { return h.bv.subtreeWriteWorker(gCtx, writeJobsChan) })

				_, _, err = h.bv.quickValidateBlockAsync(h.ctx, block, "peer", "", writeJobsChan)

				close(writeJobsChan)
				require.NoError(t, g.Wait())
			} else {
				err = h.bv.quickValidateBlock(h.ctx, block, "peer", "")
			}

			require.Error(t, err, "the replaced promoted blob must be rejected at the batch read")

			// Self-check: the point of this fixture is that the WHOLE-BLOCK pass saw the
			// honest blob and only a later read saw the forged one. Without this the test
			// could silently degrade into its forged-from-the-start sibling, which the
			// per-batch anchor is not needed for.
			require.GreaterOrEqual(t, store.servedCount("get", served.RootHash(), fileformat.FileTypeSubtree), 2,
				"the promoted blob must have been read at least twice: honest for the whole-block pass, forged for the batch")

			h.requireNoUTXOMutation(block, parent, child)

			exists, existsErr := h.subtreeStore.Exists(h.ctx, served.RootHash()[:], fileformat.FileTypeSubtree)
			require.NoError(t, existsErr)
			require.False(t, exists, "the forged promoted blob must be quarantined")
		})
	}
}

// existsFailingSubtreeStore fails the existence probe for one exact (key, fileType)
// pair while the blob really is present.
type existsFailingSubtreeStore struct {
	blob.Store

	failKey string
}

func (s *existsFailingSubtreeStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...bloboptions.FileOption) (bool, error) {
	if string(key)+string(fileType) == s.failKey {
		return false, errors.NewStorageError("simulated existence probe failure")
	}

	return s.Store.Exists(ctx, key, fileType, opts...)
}

// TestQuickValidate_FullSubtreeExistsProbeFails_FailsClosed pins that a failing
// existence probe for the promoted blob fails CLOSED.
//
// Treating "cannot tell" as "not present" would skip the anchor for a full blob that
// does exist, and the invariant this route relies on is that every already-present
// full blob was anchored. The blob here is honest, so nothing but the probe failure
// can reject the block — and nothing may be mutated.
//
// Mutation target: discarding the error from the FileTypeSubtree Exists probe in
// readSubtreeStructure must make this block validate.
func TestQuickValidate_FullSubtreeExistsProbeFails_FailsClosed(t *testing.T) {
	inner := blobmemory.New()

	coinbase := preBindCoinbase(t, 0x11)

	h := newPreBindHarness(t, inner)

	parent := h.storeGenuineParent(0x2a)
	child := preBindSpendOf(t, parent, 9_000)

	served := h.oneSubtreeBody(coinbase, child)

	honestFull, err := served.Serialize()
	require.NoError(t, err)
	h.storeBlob(served.RootHash(), fileformat.FileTypeSubtree, honestFull)

	// Swap the store in only now, so the fixture could be written through the plain one.
	failing := &existsFailingSubtreeStore{Store: inner, failKey: string(served.RootHash()[:]) + string(fileformat.FileTypeSubtree)}
	h.bv.subtreeStore = failing
	h.subtreeStore = failing

	block := h.newPreBindBlock(coinbase,
		[]*chainhash.Hash{served.RootHash()},
		composeBlockMerkleRoot(t, []chainhash.Hash{coinbaseSubstitutedRoot(t, served, coinbase)}),
		2)

	err = h.bv.quickValidateBlock(h.ctx, block, "peer", "")
	require.Error(t, err, "an existence probe that cannot answer must fail closed, not be read as absent")
	require.False(t, errors.IsBlockCorrupt(err), "a local storage fault must not condemn the peer's body, got %v", err)

	h.requireNoUTXOMutation(block, parent, child)
}

// TestQuickValidate_BothFileTypesForged_BothQuarantined closes the same-key sibling
// hole. findLocalSubtreeFile prefers FileTypeSubtreeToCheck, so when both blobs exist
// under one hash and the preferred one is forged, naming only the blob that was read
// would quarantine only that one — and normal validation's own findLocalSubtreeFile
// would then select the SURVIVING full blob, whose forged header its claim-only loader
// accepts.
//
// Mutation target: dropping the sibling audit in rejectKeyMismatchAndAuditSibling (so
// the mismatch names only the file type that was read) must leave the FileTypeSubtree
// blob present.
func TestQuickValidate_BothFileTypesForged_BothQuarantined(t *testing.T) {
	h := newPreBindHarness(t, nil)

	coinbase := preBindCoinbase(t, 0x12)
	parent := h.storeGenuineParent(0x3a)
	child := preBindSpendOf(t, parent, 9_000)

	// The key is an honest subtree's root; neither stored blob answers to it.
	honest := buildSubtreeOver(t, true, []*bt.Tx{child})
	key := honest.RootHash()

	h.storeForgedStructure(key, buildSubtreeOver(t, true, []*bt.Tx{preBindSpendOf(t, parent, 1_100)}), fileformat.FileTypeSubtreeToCheck)
	h.storeForgedStructure(key, buildSubtreeOver(t, true, []*bt.Tx{preBindSpendOf(t, parent, 1_200)}), fileformat.FileTypeSubtree)

	block := h.newPreBindBlock(coinbase,
		[]*chainhash.Hash{key},
		composeBlockMerkleRoot(t, []chainhash.Hash{coinbaseSubstitutedRoot(t, honest, coinbase)}),
		2)

	err := h.bv.quickValidateBlock(h.ctx, block, "peer", "")
	require.Error(t, err)
	require.False(t, errors.IsBlockCorrupt(err), "a local blob fault must not condemn the peer's body, got %v", err)

	h.requireNoUTXOMutation(block, parent, child)

	for _, fileType := range []fileformat.FileType{fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtree} {
		exists, existsErr := h.subtreeStore.Exists(h.ctx, key[:], fileType)
		require.NoError(t, existsErr)
		require.False(t, exists, "both blobs under a mismatching key must be quarantined, %s survived", fileType)
	}
}

// TestQuickValidate_HonestSiblingSurvivesQuarantine is the other half of that rule: a
// sibling that anchors cleanly has been PROVED to belong to the key, so normal
// validation may safely use it and the quarantine must leave it alone. Deleting every
// sibling unconditionally would force a needless re-fetch of a blob nothing has
// accused of being bad.
func TestQuickValidate_HonestSiblingSurvivesQuarantine(t *testing.T) {
	h := newPreBindHarness(t, nil)

	coinbase := preBindCoinbase(t, 0x13)
	parent := h.storeGenuineParent(0x4a)
	child := preBindSpendOf(t, parent, 9_000)

	honest := buildSubtreeOver(t, true, []*bt.Tx{child})
	key := honest.RootHash()

	honestBytes, err := honest.Serialize()
	require.NoError(t, err)

	h.storeForgedStructure(key, buildSubtreeOver(t, true, []*bt.Tx{preBindSpendOf(t, parent, 2_100)}), fileformat.FileTypeSubtreeToCheck)
	h.storeBlob(key, fileformat.FileTypeSubtree, honestBytes)

	block := h.newPreBindBlock(coinbase,
		[]*chainhash.Hash{key},
		composeBlockMerkleRoot(t, []chainhash.Hash{coinbaseSubstitutedRoot(t, honest, coinbase)}),
		2)

	require.Error(t, h.bv.quickValidateBlock(h.ctx, block, "peer", ""))

	h.requireNoUTXOMutation(block, parent, child)

	forgedGone, err := h.subtreeStore.Exists(h.ctx, key[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.False(t, forgedGone, "the forged blob must be quarantined")

	honestKept, err := h.subtreeStore.Exists(h.ctx, key[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, honestKept, "a sibling proved to belong to the key must survive")
}

// TestQuickValidate_SiblingUnauditable_AbortsWithoutFallthrough covers the third
// outcome: the sibling EXISTS but cannot be audited, so nothing can say whether it is
// sound. Falling through would hand a possibly-forged blob to the claim-only loader,
// so the attempt aborts instead.
//
// Mutation target: treating an unreadable sibling as absent (rather than marking the
// attempt unquarantined) must make tryQuickValidation fall through.
func TestQuickValidate_SiblingUnauditable_AbortsWithoutFallthrough(t *testing.T) {
	inner := blobmemory.New()

	h := newPreBindHarness(t, inner)

	coinbase := preBindCoinbase(t, 0x14)
	parent := h.storeGenuineParent(0x5a)
	child := preBindSpendOf(t, parent, 9_000)

	honest := buildSubtreeOver(t, true, []*bt.Tx{child})
	key := honest.RootHash()

	honestBytes, err := honest.Serialize()
	require.NoError(t, err)

	h.storeForgedStructure(key, buildSubtreeOver(t, true, []*bt.Tx{preBindSpendOf(t, parent, 3_100)}), fileformat.FileTypeSubtreeToCheck)
	h.storeBlob(key, fileformat.FileTypeSubtree, honestBytes)

	// The sibling is present but unreadable, so its soundness cannot be established.
	unreadable := &getFailingSubtreeStore{Store: inner, failKey: string(key[:]) + string(fileformat.FileTypeSubtree)}
	h.bv.subtreeStore = unreadable
	h.subtreeStore = unreadable

	block := h.newPreBindBlock(coinbase,
		[]*chainhash.Hash{key},
		composeBlockMerkleRoot(t, []chainhash.Hash{coinbaseSubstitutedRoot(t, honest, coinbase)}),
		2)

	server, catchupCtx := h.newAbortServer()
	catchupCtx.blockUpTo = block

	writeJobsChan := make(chan *SubtreeWriteJob, 16)

	g, gCtx := errgroup.WithContext(h.ctx)
	g.Go(func() error { return h.bv.subtreeWriteWorker(gCtx, writeJobsChan) })

	tryNormal, err := server.tryQuickValidation(h.ctx, block, catchupCtx, "peer", "http://peer", writeJobsChan, nil)

	close(writeJobsChan)
	require.NoError(t, g.Wait())

	require.False(t, tryNormal, "an unauditable sibling must NOT be handed to normal validation's claim-only loader")
	require.Error(t, err)
	require.True(t, isUnquarantinedLocalSubtree(err), "got %v", err)

	h.requireNoUTXOMutation(block, parent, child)
}

// getFailingSubtreeStore fails Get for one exact (key, fileType) pair while Exists
// still reports the blob present.
type getFailingSubtreeStore struct {
	blob.Store

	failKey string
}

func (s *getFailingSubtreeStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...bloboptions.FileOption) ([]byte, error) {
	if string(key)+string(fileType) == s.failKey {
		return nil, errors.NewStorageError("simulated read failure")
	}

	return s.Store.Get(ctx, key, fileType, opts...)
}

// TestQuickValidate_PromotedSubtreeReadExactlyTwice isolates the CARRY from the
// per-batch anchor, which no other test here does.
//
// Everything is honest, so nothing can reject this block. The promoted blob is served
// honest for the first two reads and forged from the THIRD onwards — a read the
// current implementation never performs, because the batch reader anchors the blob
// once and hands the object to the write phase rather than letting it re-read. An
// implementation that reverted the carry and re-read in buildSubtreeAndQueueWrite /
// writeSubtreeFilesFromTxs (while keeping the batch anchor, so the earlier replacement
// tests stay green) would take that third read and get the forged bytes.
//
// So the block validating, AND the read count being exactly two, is the carry's own
// property: no store access from the arm that runs beside the UTXO work, hence no
// window between an anchor and its use.
//
// Mutation target: reinstating a store read in the consumer for an already-present
// full subtree.
func TestQuickValidate_PromotedSubtreeReadExactlyTwice(t *testing.T) {
	store := newReplacingSubtreeStore(blobmemory.New())

	h := newPreBindHarness(t, store)

	coinbase := preBindCoinbase(t, 0x15)
	parent := h.storeGenuineParent(0x6a)
	child := preBindSpendOf(t, parent, 9_000)

	served := h.oneSubtreeBody(coinbase, child)

	honestFull, err := served.Serialize()
	require.NoError(t, err)
	h.storeBlob(served.RootHash(), fileformat.FileTypeSubtree, honestFull)

	unrelated := buildSubtreeOver(t, true, []*bt.Tx{preBindSpendOf(t, parent, 7_100)})
	store.replaceAfterNGets(served.RootHash(), fileformat.FileTypeSubtree, 2,
		forgeSubtreeHeaderRoot(t, unrelated, served.RootHash()))

	block := h.newPreBindBlock(coinbase,
		[]*chainhash.Hash{served.RootHash()},
		composeBlockMerkleRoot(t, []chainhash.Hash{coinbaseSubstitutedRoot(t, served, coinbase)}),
		2)

	require.NoError(t, h.bv.quickValidateBlock(h.ctx, block, "peer", ""),
		"an honest body must validate, and a third read of the promoted blob would poison it")

	require.Equal(t, 2, store.servedCount("get", served.RootHash(), fileformat.FileTypeSubtree),
		"the promoted blob must be read exactly twice — once for the whole-block pass, once for the batch — and never from the write phase")

	created, err := h.utxoStore.Get(h.ctx, child.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, child.TxIDChainHash().String(), created.Tx.TxID())
}

// TestCombineSweepMismatchError_PropagatesUnquarantinedMarker pins the whole-block
// sweep's error aggregation with no goroutines involved, so the ordering the bug
// depends on is expressed directly rather than raced for.
//
// The sweep keeps only ONE of the failing reads as the error it returns. If the kept
// one is an ordinary mismatch while a DIFFERENT hash was the one whose sibling could
// not be audited, carrying only the kept error's own markers drops the fail-closed
// verdict: the named blobs delete cleanly, the boundary reports success, and the
// attempt falls through to normal validation with an unaudited blob still present.
//
// Mutation target: returning markSubtreeKeyMismatch(firstMismatch, refs...) without
// the anyUnquarantined fold.
func TestCombineSweepMismatchError_PropagatesUnquarantinedMarker(t *testing.T) {
	hashA := chainhash.HashH([]byte("sweep-aggregate-a"))
	hashB := chainhash.HashH([]byte("sweep-aggregate-b"))

	refs := []subtreeBlobRef{
		{hash: hashA, fileType: fileformat.FileTypeSubtreeToCheck},
		{hash: hashB, fileType: fileformat.FileTypeSubtreeToCheck},
	}

	// The error the sweep happened to keep is an ORDINARY mismatch: it carries no
	// fail-closed marker of its own.
	ordinary := markSubtreeKeyMismatch(
		errors.NewProcessingError("subtree %s does not match its key", hashA.String()),
		refs[0],
	)
	require.False(t, isUnquarantinedLocalSubtree(ordinary), "precondition: the kept error is an ordinary mismatch")

	t.Run("a later unauditable sibling is still fail-closed", func(t *testing.T) {
		combined := combineSweepMismatchError(ordinary, refs, true)

		require.True(t, isUnquarantinedLocalSubtree(combined),
			"the fail-closed verdict of a DIFFERENT hash must survive the fold")
		require.ElementsMatch(t, refs, subtreeKeyMismatchRefs(combined),
			"every named blob must still be carried for the quarantine")
	})

	t.Run("no unauditable sibling stays an ordinary local fault", func(t *testing.T) {
		// Rebuilt, because marking mutates the error in place.
		plain := markSubtreeKeyMismatch(
			errors.NewProcessingError("subtree %s does not match its key", hashA.String()),
			refs[0],
		)

		combined := combineSweepMismatchError(plain, refs, false)

		require.False(t, isUnquarantinedLocalSubtree(combined),
			"nothing may be marked fail-closed when every mismatch was fully audited")
		require.ElementsMatch(t, refs, subtreeKeyMismatchRefs(combined))
	})
}

// orderedMismatchStore serializes two subtree reads so the ordinary mismatch is the
// one the sweep keeps, and fails the sibling read of the other.
//
// The ordering is a happens-before edge, not a race: the gated read cannot begin until
// the store has observed the ungated hash's LAST call — its sibling probe — after which
// that goroutine only formats an error and takes a mutex. A settle is added on top so
// the gated path cannot overtake it.
type orderedMismatchStore struct {
	blob.Store

	firstSiblingProbeKey string
	gatedExistsKey       string
	failingGetKey        string
	settle               time.Duration

	once     sync.Once
	released chan struct{}
}

func newOrderedMismatchStore(inner blob.Store, first, gated *chainhash.Hash, settle time.Duration) *orderedMismatchStore {
	return &orderedMismatchStore{
		Store:                inner,
		firstSiblingProbeKey: string(first[:]) + string(fileformat.FileTypeSubtree),
		gatedExistsKey:       string(gated[:]) + string(fileformat.FileTypeSubtreeToCheck),
		failingGetKey:        string(gated[:]) + string(fileformat.FileTypeSubtree),
		settle:               settle,
		released:             make(chan struct{}),
	}
}

func (s *orderedMismatchStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...bloboptions.FileOption) (bool, error) {
	mapKey := string(key) + string(fileType)

	if mapKey == s.gatedExistsKey {
		select {
		case <-s.released:
		case <-ctx.Done():
			return false, ctx.Err()
		}

		time.Sleep(s.settle)
	}

	exists, err := s.Store.Exists(ctx, key, fileType, opts...)

	if mapKey == s.firstSiblingProbeKey {
		// The ungated hash has made its last store call; everything it does after this
		// is in-memory.
		s.once.Do(func() { close(s.released) })
	}

	return exists, err
}

func (s *orderedMismatchStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...bloboptions.FileOption) ([]byte, error) {
	if string(key)+string(fileType) == s.failingGetKey {
		return nil, errors.NewStorageError("simulated sibling read failure")
	}

	return s.Store.Get(ctx, key, fileType, opts...)
}

// TestQuickValidate_OrdinaryMismatchThenUnauditableSibling_Aborts is the end-to-end
// case: two mismatching hashes, the first an ordinary mismatch with no sibling, the
// second one whose sibling exists but cannot be read.
//
// Both named blobs delete cleanly, so nothing in the quarantine itself objects. The
// attempt must still ABORT, because the second hash's sibling was never audited and
// normal validation's loader checks only the claimed root.
func TestQuickValidate_OrdinaryMismatchThenUnauditableSibling_Aborts(t *testing.T) {
	inner := blobmemory.New()

	coinbase := preBindCoinbase(t, 0x16)

	ordinaryKey := chainhash.HashH([]byte("ordinary-mismatch-key"))
	unauditableKey := chainhash.HashH([]byte("unauditable-sibling-key"))

	h := newPreBindHarness(t, inner)

	parent := h.storeGenuineParent(0x7a)
	child := preBindSpendOf(t, parent, 9_000)

	// The ordinary mismatch: a forged blob with no sibling to audit.
	h.storeForgedStructure(&ordinaryKey, buildSubtreeOver(t, true, []*bt.Tx{child}), fileformat.FileTypeSubtreeToCheck)

	// The fail-closed one: a forged blob whose sibling is present but unreadable.
	h.storeForgedStructure(&unauditableKey, buildSubtreeOver(t, false, []*bt.Tx{preBindSpendOf(t, parent, 4_100), child}), fileformat.FileTypeSubtreeToCheck)
	h.storeBlob(&unauditableKey, fileformat.FileTypeSubtree, []byte{0x01})

	ordered := newOrderedMismatchStore(inner, &ordinaryKey, &unauditableKey, 200*time.Millisecond)
	h.bv.subtreeStore = ordered
	h.subtreeStore = ordered

	block := h.newPreBindBlock(coinbase,
		[]*chainhash.Hash{&ordinaryKey, &unauditableKey},
		composeBlockMerkleRoot(t, []chainhash.Hash{ordinaryKey, unauditableKey}),
		4)

	server, catchupCtx := h.newAbortServer()
	catchupCtx.blockUpTo = block

	writeJobsChan := make(chan *SubtreeWriteJob, 16)

	g, gCtx := errgroup.WithContext(h.ctx)
	g.Go(func() error { return h.bv.subtreeWriteWorker(gCtx, writeJobsChan) })

	tryNormal, err := server.tryQuickValidation(h.ctx, block, catchupCtx, "peer", "http://peer", writeJobsChan, nil)

	close(writeJobsChan)
	require.NoError(t, g.Wait())

	require.False(t, tryNormal, "normal validation must NEVER be entered while a mismatching blob's sibling is unaudited")
	require.Error(t, err)
	require.True(t, isUnquarantinedLocalSubtree(err),
		"the fail-closed verdict must survive aggregation with an ordinary mismatch, got %v", err)

	h.requireNoUTXOMutation(block, parent, child)

	// Both named blobs were deletable, which is the point: nothing in the quarantine
	// itself would have objected.
	for _, key := range []*chainhash.Hash{&ordinaryKey, &unauditableKey} {
		exists, existsErr := inner.Exists(h.ctx, key[:], fileformat.FileTypeSubtreeToCheck)
		require.NoError(t, existsErr)
		require.False(t, exists, "the named blob for %s must be quarantined", key)
	}

	// The unaudited sibling is still there, which is precisely why the attempt must
	// not have fallen through.
	survived, err := inner.Exists(h.ctx, unauditableKey[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, survived, "the unaudited sibling is still on disk")
}
