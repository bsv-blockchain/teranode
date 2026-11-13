package subtreeprocessor

import (
	"bytes"
	"context"
	"encoding/binary"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func newReverseHelpersStp(t *testing.T, store interface {
	subtreeStoreAccess
}) *SubtreeProcessor {
	t.Helper()

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(t.Context(), ulogger.TestLogger{}, test.CreateBaseTestSettings(t), utxoStoreURL)
	require.NoError(t, err)

	newSubtreeChan := make(chan NewSubtreeRequest, 1)

	stp, err := NewSubtreeProcessor(t.Context(), ulogger.TestLogger{}, test.CreateBaseTestSettings(t), store.blob(), nil, utxoStore, newSubtreeChan)
	require.NoError(t, err)

	return stp
}

// subtreeStoreAccess lets the helper above accept either the real
// blob_memory store or the regressionFailingStore wrapper below without
// importing the whole blob.Store interface into the test surface.
type subtreeStoreAccess interface {
	blob() *blob_memory.Memory
}

type plainBlob struct{ s *blob_memory.Memory }

func (p plainBlob) blob() *blob_memory.Memory { return p.s }

// TestFindReMinedInMoveForward verifies the carry-over filter's lookup helper:
// it returns exactly the candidates that appear in a moveForward subtree,
// skips nil/missing entries without erroring, never reports the coinbase
// placeholder, and does no store reads at all when there is nothing to look up.
func TestFindReMinedInMoveForward(t *testing.T) {
	t.Run("nil block list returns empty set, no errors", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		got, err := stp.findReMinedInMoveForward(t.Context(), nil, []chainhash.Hash{chainhash.HashH([]byte("x"))})
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Empty(t, got)
	})

	t.Run("no candidates does no store reads", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		// The subtree does not exist, so any read would fail. With no
		// candidates the helper must return before touching the store: this
		// is the common case (a moveBack block with no conflicting nodes) and
		// the moveForward blocks can hold billions of txs.
		phantom := chainhash.HashH([]byte("no-candidates-phantom"))

		got, err := stp.findReMinedInMoveForward(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{&phantom}},
		}, nil)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Empty(t, got)
	})

	t.Run("block with no subtrees contributes nothing", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		got, err := stp.findReMinedInMoveForward(t.Context(), []*model.Block{
			{Subtrees: nil},
		}, []chainhash.Hash{chainhash.HashH([]byte("x"))})
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("nil block entry is tolerated", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		got, err := stp.findReMinedInMoveForward(t.Context(), []*model.Block{nil}, []chainhash.Hash{chainhash.HashH([]byte("x"))})
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("returns only the candidates that are re-mined", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		s, txs := buildSubtreeWithTxs(t, "carry-over-subset", 4)
		require.NoError(t, store.Set(t.Context(), s.RootHash()[:], fileformat.FileTypeSubtree, mustSerialize(t, s)))

		notMined := chainhash.HashH([]byte("carry-over-subset-not-mined"))

		got, err := stp.findReMinedInMoveForward(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{s.RootHash()}},
		}, []chainhash.Hash{txs[1], notMined, txs[3]})
		require.NoError(t, err)
		require.Len(t, got, 2)
		require.Contains(t, got, txs[1])
		require.Contains(t, got, txs[3])
		require.NotContains(t, got, notMined)
		require.NotContains(t, got, txs[0], "txs that were not candidates must not be returned")
	})

	t.Run("nil subtree-hash entry inside block.Subtrees is skipped", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		s, txs := buildSubtreeWithTxs(t, "carry-over-nil-subtree-hash", 2)
		require.NoError(t, store.Set(t.Context(), s.RootHash()[:], fileformat.FileTypeSubtree, mustSerialize(t, s)))

		got, err := stp.findReMinedInMoveForward(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{nil, s.RootHash()}},
		}, txs)
		require.NoError(t, err)
		for _, txHash := range txs {
			require.Contains(t, got, txHash)
		}
	})

	t.Run("coinbase placeholder is never reported", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		// buildSubtreeWithTxs prepends a coinbase placeholder; even when it
		// is asked for, it must not be reported as re-mined.
		s, txs := buildSubtreeWithTxs(t, "carry-over-coinbase", 2)
		require.NoError(t, store.Set(t.Context(), s.RootHash()[:], fileformat.FileTypeSubtree, mustSerialize(t, s)))

		candidates := append([]chainhash.Hash{subtreepkg.CoinbasePlaceholderHashValue}, txs...)

		got, err := stp.findReMinedInMoveForward(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{s.RootHash()}},
		}, candidates)
		require.NoError(t, err)
		require.Len(t, got, len(txs), "coinbase placeholder must be filtered out")
		require.NotContains(t, got, subtreepkg.CoinbasePlaceholderHashValue)
	})

	t.Run("matches across multiple blocks and subtrees", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		s1, txs := buildSubtreeWithTxs(t, "carry-over-multi", 3)
		s2, txs2 := buildSubtreeWithTxs(t, "carry-over-multi-2", 2)

		require.NoError(t, store.Set(t.Context(), s1.RootHash()[:], fileformat.FileTypeSubtree, mustSerialize(t, s1)))
		require.NoError(t, store.Set(t.Context(), s2.RootHash()[:], fileformat.FileTypeSubtree, mustSerialize(t, s2)))

		got, err := stp.findReMinedInMoveForward(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{s1.RootHash()}},
			{Subtrees: []*chainhash.Hash{s2.RootHash()}},
		}, []chainhash.Hash{txs[0], txs2[1]})
		require.NoError(t, err)
		require.Len(t, got, 2)
		require.Contains(t, got, txs[0])
		require.Contains(t, got, txs2[1])
	})

	t.Run("subtreeToCheck fallback used when FileTypeSubtree missing", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		s, txs := buildSubtreeWithTxs(t, "carry-over-tocheck", 2)
		// Store ONLY under FileTypeSubtreeToCheck — exercise the fallback
		// path in the helper's GetIoReader chain.
		require.NoError(t, store.Set(t.Context(), s.RootHash()[:], fileformat.FileTypeSubtreeToCheck, mustSerialize(t, s)))

		got, err := stp.findReMinedInMoveForward(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{s.RootHash()}},
		}, txs)
		require.NoError(t, err)
		for _, txHash := range txs {
			require.Contains(t, got, txHash)
		}
	})

	t.Run("missing subtree surfaces an error", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		phantom := chainhash.HashH([]byte("phantom-subtree"))
		_, err := stp.findReMinedInMoveForward(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{&phantom}},
		}, []chainhash.Hash{chainhash.HashH([]byte("x"))})
		require.Error(t, err, "absent subtree must fail loudly so the reorg doesn't silently skip carry-over txs")
	})

	t.Run("malformed subtree blob surfaces an error", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		corrupt := chainhash.HashH([]byte("carry-over-corrupt"))
		// Just the file magic and a few bytes; deserialize will fail.
		require.NoError(t, store.Set(t.Context(), corrupt[:], fileformat.FileTypeSubtree, []byte("S-1.0   \x00\x00")))

		_, err := stp.findReMinedInMoveForward(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{&corrupt}},
		}, []chainhash.Hash{chainhash.HashH([]byte("x"))})
		require.Error(t, err)
	})

	t.Run("stops reading once every candidate is found", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})
		// One subtree at a time makes the order deterministic.
		stp.settings.BlockAssembly.MoveBackBlockConcurrency = 1

		s, txs := buildSubtreeWithTxs(t, "carry-over-early-exit", 4)
		require.NoError(t, store.Set(t.Context(), s.RootHash()[:], fileformat.FileTypeSubtree, mustSerialize(t, s)))

		// The second subtree does not exist: reading it would fail, so a
		// clean result proves the scan stopped after the first one.
		phantom := chainhash.HashH([]byte("carry-over-early-exit-phantom"))

		got, err := stp.findReMinedInMoveForward(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{s.RootHash()}},
			{Subtrees: []*chainhash.Hash{&phantom}},
		}, []chainhash.Hash{txs[0], txs[2]})
		require.NoError(t, err)
		require.Len(t, got, 2)
	})

	t.Run("cancelled context surfaces an error", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		s, txs := buildSubtreeWithTxs(t, "carry-over-cancelled", 2)
		require.NoError(t, store.Set(t.Context(), s.RootHash()[:], fileformat.FileTypeSubtree, mustSerialize(t, s)))

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		_, err := stp.findReMinedInMoveForward(ctx, []*model.Block{
			{Subtrees: []*chainhash.Hash{s.RootHash()}},
		}, txs)
		require.Error(t, err)
	})

	t.Run("truncated subtree surfaces an error", func(t *testing.T) {
		store := blob_memory.New()
		stp := newReverseHelpersStp(t, plainBlob{store})

		s, txs := buildSubtreeWithTxs(t, "carry-over-truncated", 4)
		b := mustSerialize(t, s)
		// Cut the blob inside the node list.
		require.NoError(t, store.Set(t.Context(), s.RootHash()[:], fileformat.FileTypeSubtree, b[:subtreeHeaderLen+subtreeNodeLen+10]))

		_, err := stp.findReMinedInMoveForward(t.Context(), []*model.Block{
			{Subtrees: []*chainhash.Hash{s.RootHash()}},
		}, []chainhash.Hash{txs[3]})
		require.Error(t, err)
	})
}

// moveBackFixture is block assembly sitting at block 2 of the chain genesis -> block 1 -> block 2,
// both applied through moveForwardBlock, with both coinbases in the UTXO store. Block 1 holds only
// its coinbase. With a conflict, block 2 lists winner as a conflicting node, so applying it gave
// winner the output loser had spent. side is a block on genesis for a reorg or reset to move to.
type moveBackFixture struct {
	stp          *SubtreeProcessor
	store        *countPendingScans
	subtreeStore *blob_memory.Memory
	chain        *walTestChain
	loser        *bt.Tx
	winner       *bt.Tx
	side         *model.Block
}

func newMoveBackFixture(t *testing.T, withConflict bool, seed byte) *moveBackFixture {
	t.Helper()

	ctx := t.Context()
	stp, store, subtreeStore, chain := newResetWALTestProcessor(t, false)

	f := &moveBackFixture{stp: stp, store: store, subtreeStore: subtreeStore, chain: chain}

	block1, block2 := chain.moveForward[0], chain.moveForward[1]
	block1.Subtrees = []*chainhash.Hash{storeTestSubtree(t, subtreeStore, false, unstoredTx(t, seed))}

	if withConflict {
		f.loser, f.winner = createResetWALConflict(t, store, seed)
		block2.Subtrees = []*chainhash.Hash{storeConflictSubtree(t, subtreeStore, f.winner)}
	} else {
		block2.Subtrees = []*chainhash.Hash{storeTestSubtree(t, subtreeStore, false)}
	}

	for _, block := range chain.moveForward {
		_, _, err := stp.moveForwardBlock(ctx, block, true, map[chainhash.Hash]struct{}{}, true, false)
		require.NoError(t, err)
		stp.currentBlockHeader.Store(block.Header)
		requireCoinbaseInStore(t, store, block)
	}

	f.side = &model.Block{Height: 1, CoinbaseTx: coinbaseTx2, TransactionCount: 1, Subtrees: []*chainhash.Hash{}, Header: childHeader(chain.genesis, 500+uint32(seed))}
	require.NoError(t, stp.blockchainClient.AddBlock(ctx, f.side, "", blockchainoptions.WithMinedSet(true)))

	return f
}

// unstoredTx returns a tx that is in no store, to give a subtree its own root hash.
func unstoredTx(t *testing.T, seed byte) *bt.Tx {
	t.Helper()

	parent := bt.NewTx()
	parent.Outputs = []*bt.Output{{Satoshis: 1000 + uint64(seed), LockingScript: bscript.NewFromBytes([]byte{0x52})}}

	return resetWALSpendTx(t, parent, 900)
}

// requireCoinbaseInStore creates the block's coinbase in the UTXO store unless it is there already.
func requireCoinbaseInStore(t *testing.T, store utxo.Store, block *model.Block) {
	t.Helper()

	if _, err := store.Get(t.Context(), block.CoinbaseTx.TxIDChainHash()); err == nil {
		return
	}

	_, _, err := store.SpendAndCreate(t.Context(), block.CoinbaseTx, block.Height, utxo.WithCreateOnly())
	require.NoError(t, err)
}

// moveBackBlocks returns the fixture's blocks in move-back order, tip first.
func (f *moveBackFixture) moveBackBlocks() []*model.Block {
	return []*model.Block{f.chain.moveForward[1], f.chain.moveForward[0]}
}

// run moves back both blocks and forward to side, through reorgBlocks or reset.
func (f *moveBackFixture) run(t *testing.T, viaReset bool) error {
	t.Helper()

	if viaReset {
		return f.stp.reset(f.chain.genesis, f.moveBackBlocks(), []*model.Block{f.side}, false, func() error { return nil })
	}

	return f.stp.reorgBlocks(t.Context(), f.moveBackBlocks(), []*model.Block{f.side})
}

// requireNothingChanged checks that neither moved-back block's coinbase was deleted and, with a
// conflict, that winner still holds the output and loser is still conflicting.
func (f *moveBackFixture) requireNothingChanged(t *testing.T) {
	t.Helper()

	ctx := t.Context()

	for _, block := range f.chain.moveForward {
		_, err := f.store.Get(ctx, block.CoinbaseTx.TxIDChainHash())
		require.NoError(t, err, "the coinbase of moved-back block %d must survive a move-back that failed before it started", block.Height)
	}

	if f.winner == nil {
		return
	}

	spends, err := utxo.GetSpends(f.winner)
	require.NoError(t, err)

	owner, err := f.store.GetSpend(ctx, spends[0])
	require.NoError(t, err)
	require.Equal(t, f.winner.TxIDChainHash(), owner.SpendingData.TxID, "nothing may be reversed: winner must keep the output")

	m, err := f.store.Get(ctx, f.loser.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	require.True(t, m.Conflicting, "nothing may be reversed: loser must stay conflicting")
}

// TestMoveForwardSubtreeMissingChangesNothing checks that a reorg or reset whose moveForward block
// names a subtree that is not in the store fails before the move-back changes the UTXO store. With
// no conflicting nodes in the moved-back blocks nothing reads the moveForward subtrees until the
// moveForward step, so this is what checkMoveForwardSubtreesExist is for: without it both
// coinbases are deleted before the failure.
func TestMoveForwardSubtreeMissingChangesNothing(t *testing.T) {
	for _, viaReset := range []bool{false, true} {
		for _, withConflict := range []bool{false, true} {
			name := map[bool]string{false: "reorg", true: "reset"}[viaReset] + map[bool]string{false: ", no conflicting nodes", true: ", conflicting node"}[withConflict]

			t.Run(name, func(t *testing.T) {
				f := newMoveBackFixture(t, withConflict, 60)

				missing := chainhash.HashH([]byte("missing-move-forward-subtree"))
				f.side.Subtrees = []*chainhash.Hash{&missing}

				err := f.run(t, viaReset)
				require.Error(t, err)

				f.requireNothingChanged(t)
			})
		}
	}
}

// TestMoveBackSubtreeMissingChangesNothing is the two-block case: block 2, moved back first, has a
// conflicting node, and block 1's subtree is missing. Both blocks' conflicting nodes are read before
// the move-back loop, so the missing subtree fails the reorg or reset before block 2's coinbase is
// deleted or its conflict reversed. Reading them inside the loop, as each block is moved back,
// would reverse block 2 and then fail on block 1.
func TestMoveBackSubtreeMissingChangesNothing(t *testing.T) {
	for _, viaReset := range []bool{false, true} {
		t.Run(map[bool]string{false: "reorg", true: "reset"}[viaReset], func(t *testing.T) {
			f := newMoveBackFixture(t, true, 70)

			block1 := f.chain.moveForward[0]
			require.NoError(t, f.subtreeStore.Del(t.Context(), block1.Subtrees[0][:], fileformat.FileTypeSubtree))

			err := f.run(t, viaReset)
			require.Error(t, err)

			// End state first: the error text alone cannot tell a failure before the
			// move-back from one after it.
			f.requireNothingChanged(t)
			require.Contains(t, err.Error(), "error getting conflicting nodes of moveBack blocks")
		})
	}
}

// TestResetSkipsReverseOfReMinedWinner checks that a reset leaves alone a moved-back block's winner
// that the new chain mines again: the winner keeps the output and the loser stays conflicting, as
// TestReorgBlocks_PartialRemineAfterAcceptedHeal checks for reorgBlocks.
func TestResetSkipsReverseOfReMinedWinner(t *testing.T) {
	ctx := t.Context()
	stp, store, subtreeStore, chain := newResetWALTestProcessor(t, false)

	loser, winner := createResetWALConflict(t, store, 80)

	block := chain.moveForward[0]
	block.Subtrees = []*chainhash.Hash{storeConflictSubtree(t, subtreeStore, winner)}

	_, _, err := stp.moveForwardBlock(ctx, block, true, map[chainhash.Hash]struct{}{}, true, false)
	require.NoError(t, err)

	side := &model.Block{Height: 1, CoinbaseTx: coinbaseTx2, TransactionCount: 2, Header: childHeader(chain.genesis, 580),
		Subtrees: []*chainhash.Hash{storeTestSubtree(t, subtreeStore, false, winner, unstoredTx(t, 80))}}
	require.NoError(t, stp.blockchainClient.AddBlock(ctx, side, "", blockchainoptions.WithMinedSet(true)))

	require.NoError(t, stp.reset(chain.genesis, []*model.Block{block}, []*model.Block{side}, false, func() error { return nil }))

	spends, err := utxo.GetSpends(winner)
	require.NoError(t, err)
	owner, err := store.GetSpend(ctx, spends[0])
	require.NoError(t, err)
	require.Equal(t, winner.TxIDChainHash(), owner.SpendingData.TxID, "winner is mined again on the new chain, so it keeps the output")

	m, err := store.Get(ctx, winner.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	require.False(t, m.Conflicting, "the reset must not demote a re-mined winner")

	m, err = store.Get(ctx, loser.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	require.True(t, m.Conflicting)
}

// existsFailingStore is a blob store whose Exists always fails.
type existsFailingStore struct {
	*blob_memory.Memory
}

func (s existsFailingStore) Exists(context.Context, []byte, fileformat.FileType, ...options.FileOption) (bool, error) {
	return false, errors.NewStorageError("exists failed")
}

// TestCheckMoveForwardSubtreesExist checks the preflight on its own: a subtree is found under
// either file type, nil blocks and hashes are skipped, and a missing subtree or a failing store
// is an error.
func TestCheckMoveForwardSubtreesExist(t *testing.T) {
	store := blob_memory.New()
	stp := newReverseHelpersStp(t, plainBlob{store})

	asSubtree := chainhash.HashH([]byte("exists-as-subtree"))
	asToCheck := chainhash.HashH([]byte("exists-as-subtree-to-check"))
	missing := chainhash.HashH([]byte("exists-missing"))

	require.NoError(t, store.Set(t.Context(), asSubtree[:], fileformat.FileTypeSubtree, []byte{1}))
	require.NoError(t, store.Set(t.Context(), asToCheck[:], fileformat.FileTypeSubtreeToCheck, []byte{1}))

	t.Run("present under either file type", func(t *testing.T) {
		require.NoError(t, stp.checkMoveForwardSubtreesExist(t.Context(), []*model.Block{
			nil,
			{Header: dummyHeader(), Subtrees: []*chainhash.Hash{&asSubtree, nil, &asToCheck}},
		}))
	})

	t.Run("missing", func(t *testing.T) {
		err := stp.checkMoveForwardSubtreesExist(t.Context(), []*model.Block{
			{Header: dummyHeader(), Subtrees: []*chainhash.Hash{&asSubtree, &missing}},
		})
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNotFound))
	})

	t.Run("store error", func(t *testing.T) {
		failing := newReverseHelpersStp(t, plainBlob{store})
		failing.subtreeStore = existsFailingStore{store}

		err := failing.checkMoveForwardSubtreesExist(t.Context(), []*model.Block{
			{Header: dummyHeader(), Subtrees: []*chainhash.Hash{&asSubtree}},
		})
		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrNotFound))
	})
}

// TestScanSubtreeNodeHashesMatchesDeserialize checks that a scan that reads every node rejects a
// damaged conflicting-node trailer exactly when go-subtree's DeserializeFromReader does, which is
// what moveForwardBlock's own read rejects too.
func TestScanSubtreeNodeHashesMatchesDeserialize(t *testing.T) {
	s, txs := buildSubtreeWithTxs(t, "trailer-parity", 3)
	require.NoError(t, s.AddConflictingNode(txs[0]))
	require.NoError(t, s.AddConflictingNode(txs[1]))

	intact := mustSerialize(t, s)
	trailer := len(intact) - 8 - 2*chainhash.HashSize

	withCount := func(count uint64) []byte {
		b := append([]byte(nil), intact...)
		binary.LittleEndian.PutUint64(b[trailer:], count)

		return b
	}

	cases := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{"intact", intact, false},
		{"trailer count missing", intact[:trailer], true},
		{"trailer count cut short", intact[:trailer+4], true},
		{"count above leaf count", withCount(uint64(len(s.Nodes)) + 1), true},
		{"count above the hashes present", withCount(3), true},
		{"conflicting hashes cut short", intact[:len(intact)-10], true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deserializeErr := (&subtreepkg.Subtree{}).DeserializeFromReader(bytes.NewReader(tc.data))
			scanErr := scanSubtreeNodeHashes(t.Context(), bytes.NewReader(tc.data), func(chainhash.Hash) bool { return true })

			require.Equal(t, tc.wantErr, deserializeErr != nil, "DeserializeFromReader: %v", deserializeErr)
			require.Equal(t, tc.wantErr, scanErr != nil, "scanSubtreeNodeHashes: %v", scanErr)
		})
	}

	t.Run("a scan that stops early does not read the trailer", func(t *testing.T) {
		require.NoError(t, scanSubtreeNodeHashes(t.Context(), bytes.NewReader(intact[:trailer]), func(chainhash.Hash) bool { return false }))
	})
}

// TestCloneReverseCascadedSet_RoundTrips checks the per-reorg state surface
// exposed to processConflictingTransactions: nil when no active reorg, an
// independent copy when populated (mutating the copy must not affect the
// processor state).
func TestCloneReverseCascadedSet_RoundTrips(t *testing.T) {
	store := blob_memory.New()
	stp := newReverseHelpersStp(t, plainBlob{store})

	require.Nil(t, stp.cloneReverseCascadedSet(),
		"outside an active reorg the helper must return nil to signal no cascade")

	h1 := chainhash.HashH([]byte("clone-cascade-h1"))
	h2 := chainhash.HashH([]byte("clone-cascade-h2"))

	stp.reverseCascadedConflictingSet = map[chainhash.Hash]struct{}{
		h1: {},
		h2: {},
	}

	got := stp.cloneReverseCascadedSet()
	require.Len(t, got, 2)
	require.Contains(t, got, h1)
	require.Contains(t, got, h2)

	// Mutating the returned copy must not affect the processor state — the
	// downstream filter mutates its own conflictingSet during cascade
	// expansion in dequeueDuringBlockMovement.
	delete(got, h1)
	require.Contains(t, stp.reverseCascadedConflictingSet, h1,
		"clone must be deep enough that caller deletion doesn't leak into reorg state")

	stp.reverseCascadedConflictingSet = nil
	require.Nil(t, stp.cloneReverseCascadedSet(),
		"after reorg exit (set cleared) helper must return nil again")
}

// buildSubtreeWithTxs creates a subtree with `count` deterministic non-
// coinbase tx hashes derived from `seed`, plus a leading coinbase placeholder.
// Returns the subtree and the slice of tx hashes (coinbase excluded) in
// insertion order.
func buildSubtreeWithTxs(t *testing.T, seed string, count int) (*subtreepkg.Subtree, []chainhash.Hash) {
	t.Helper()

	leafCount := 2
	for leafCount < count+1 {
		leafCount *= 2
	}

	s, err := subtreepkg.NewTreeByLeafCount(leafCount)
	require.NoError(t, err)
	require.NoError(t, s.AddCoinbaseNode())

	hashes := make([]chainhash.Hash, count)

	for i := 0; i < count; i++ {
		h := chainhash.HashH([]byte(seed))
		// Mix the index in so multiple txs per seed are distinct.
		for j := 0; j <= i; j++ {
			h = chainhash.HashH(h[:])
		}

		hashes[i] = h
		require.NoError(t, s.AddNode(h, uint64(i+1), 100))
	}

	return s, hashes
}

func mustSerialize(t *testing.T, s *subtreepkg.Subtree) []byte {
	t.Helper()

	b, err := s.Serialize()
	require.NoError(t, err)

	return b
}
