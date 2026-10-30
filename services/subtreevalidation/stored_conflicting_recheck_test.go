package subtreevalidation

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// storedConflictingAtLaterHeight returns a tx that double-spends the fixture's parent output
// and spends a coinbase mined at 101, which matures at 102. Another candidate block, at 102,
// has created it as conflicting; at 101 the tx is invalid.
func storedConflictingAtLaterHeight(t *testing.T, f *conflictingFixture) *bt.Tx {
	t.Helper()

	coinbase := transactions.Create(t,
		transactions.WithCoinbaseData(101, "/stored conflicting recheck coinbase/"),
		transactions.WithP2PKHOutputs(1, 100_000, f.privateKey.PubKey()),
	)
	_, err := f.store.Create(t.Context(), coinbase, 101, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 101, BlockHeight: 101, SubtreeIdx: 0}))
	require.NoError(t, err)

	tx := transactions.Create(t,
		transactions.WithPrivateKey(f.privateKey),
		transactions.WithInput(f.parent, 0, f.privateKey),
		transactions.WithInput(coinbase, 0, f.privateKey),
		transactions.WithP2PKHOutputs(1, 150_000, f.privateKey.PubKey()),
	)

	_, err = f.recorder.Interface.Validate(t.Context(), tx.Clone(), 102, validator.WithCreateConflicting(true))
	require.True(t, errors.Is(err, errors.ErrTxConflicting), "%v", err)

	stored := &meta.Data{}
	require.NoError(t, f.store.GetMeta(t.Context(), tx.TxIDChainHash(), stored))
	require.True(t, stored.Conflicting, "precondition: the record the block finds")

	return tx
}

// storedConflictingAtThisHeight returns the fixture's loser, created as conflicting at 101, the
// height of the block: a record any block at 101 may use.
func storedConflictingAtThisHeight(t *testing.T, f *conflictingFixture) *bt.Tx {
	t.Helper()

	_, err := f.recorder.Interface.Validate(t.Context(), f.loser.Clone(), 101, validator.WithCreateConflicting(true))
	require.True(t, errors.Is(err, errors.ErrTxConflicting), "%v", err)

	return f.loser
}

// TestBlockPathRechecksStoredConflictingTxs pins that a block never takes a stored conflicting
// record as its own verdict, and checks it without the validator, which would spend inputs a
// conflicting record does not hold. The block path creates a tx as conflicting at its block's height,
// and another candidate block at another height can create the same tx; a tx valid there may be
// invalid here, for instance spending a coinbase that matures in between. Every block entry
// point accepted the stored record, from the UTXO store or the txmeta cache, without asking the
// validator at its own height.
func TestBlockPathRechecksStoredConflictingTxs(t *testing.T) {
	genesis := func(f *conflictingFixture) *chainhash.Hash {
		return f.settings.ChainCfgParams.GenesisHash
	}

	type entryPoint func(t *testing.T, f *conflictingFixture, tx *bt.Tx) error

	viaSubtreeFromBlock := func(baseURL string, cached bool) entryPoint {
		return func(t *testing.T, f *conflictingFixture, tx *bt.Tx) error {
			server, subtreeStore := f.newServer(t)

			if cached {
				cache, ok := server.utxoStore.(*txmetacache.TxMetaCache)
				require.True(t, ok)

				stored := &meta.Data{}
				require.NoError(t, f.store.GetMeta(t.Context(), tx.TxIDChainHash(), stored))
				require.NoError(t, cache.SetCache(tx.TxIDChainHash(), stored))

				_, found := cache.GetMetaCached(t.Context(), *tx.TxIDChainHash())
				require.True(t, found, "precondition: the block path finds the record in the cache")
			}

			root := servePeerSubtree(t, subtreeStore, tx, f.other)

			// the legacy path reads the subtree from the local store, where the legacy block
			// handler wrote it
			if baseURL == "legacy" {
				subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(2)
				require.NoError(t, err)
				require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 0, 0))
				require.NoError(t, subtree.AddNode(*f.other.TxIDChainHash(), 0, 0))

				subtreeBytes, err := subtree.Serialize()
				require.NoError(t, err)
				require.NoError(t, subtreeStore.Set(t.Context(), root[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes))
			}

			blockHash := chainhash.HashH([]byte("stored conflicting recheck block"))

			_, err := server.checkSubtreeFromBlock(t.Context(), &subtreevalidation_api.CheckSubtreeFromBlockRequest{
				Hash:              root[:],
				BaseUrl:           baseURL,
				BlockHeight:       101,
				BlockHash:         blockHash[:],
				PreviousBlockHash: genesis(f)[:],
			})

			return err
		}
	}

	viaCheckBlockSubtrees := func(t *testing.T, f *conflictingFixture, tx *bt.Tx) error {
		f.settings.AdaptiveFetch.BootstrapMode = "pessimistic"
		server, subtreeStore := f.newServer(t)
		server.adaptiveFetch.Arm()

		root := servePeerSubtree(t, subtreeStore, tx, f.other)

		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  genesis(f),
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
			Bits:           model.NBit{},
		}

		block, err := model.NewBlock(header, &bt.Tx{Version: 1}, []*chainhash.Hash{&root}, 3, 1000, 101, 0)
		require.NoError(t, err)

		blockBytes, err := block.Bytes()
		require.NoError(t, err)

		resp, err := server.CheckBlockSubtrees(t.Context(), &subtreevalidation_api.CheckBlockSubtreesRequest{Block: blockBytes, BaseUrl: "http://peer.invalid"})
		if err == nil {
			require.True(t, resp.Blessed)
		}

		return err
	}

	// the catch-up path writes a batch through the store and sends the failures through the
	// per-transaction path
	viaBatched := func(t *testing.T, f *conflictingFixture, tx *bt.Tx) error {
		checker, ok := f.recorder.Interface.(validator.BlockBatchChecker)
		require.True(t, ok)

		return f.server.processTransactionsBatched(t.Context(), checker, []*bt.Tx{tx.Clone(), f.other.Clone()}, chainhash.Hash{}, 101, 0, 0, map[uint32]bool{})
	}

	// the level path runs before CheckBlockSubtrees validates each subtree, and on its own for the
	// batched path's fallbacks
	viaLevels := func(t *testing.T, f *conflictingFixture, tx *bt.Tx) error {
		return f.server.processTransactionsInLevels(t.Context(), []*bt.Tx{tx.Clone(), f.other.Clone()}, chainhash.Hash{}, chainhash.Hash{}, 101, 0, 0, map[uint32]bool{}, false)
	}

	entryPoints := map[string]entryPoint{
		"processTransactionsInLevels":         viaLevels,
		"checkSubtreeFromBlock http":          viaSubtreeFromBlock("http://peer.invalid", false),
		"checkSubtreeFromBlock legacy":        viaSubtreeFromBlock("legacy", false),
		"checkSubtreeFromBlock cached record": viaSubtreeFromBlock("http://peer.invalid", true),
		"CheckBlockSubtrees":                  viaCheckBlockSubtrees,
		"processTransactionsBatched":          viaBatched,
	}

	for name, check := range entryPoints {
		t.Run(name+": a record created at another height is checked at this one", func(t *testing.T) {
			f := newConflictingFixture(t)
			tx := storedConflictingAtLaterHeight(t, f)

			err := check(t, f, tx)
			require.True(t, errors.Is(err, errors.ErrTxInvalid), "the coinbase is immature at the block's height: %v", err)
			require.False(t, f.recorder.validated(*tx.TxIDChainHash()), "the check writes nothing: the validator would spend inputs the record does not hold")
		})

		t.Run(name+": a record valid at this height still passes", func(t *testing.T) {
			f := newConflictingFixture(t)
			tx := storedConflictingAtThisHeight(t, f)

			require.NoError(t, check(t, f, tx))
			require.False(t, f.recorder.validated(*tx.TxIDChainHash()), "the check writes nothing")

			stored := &meta.Data{}
			require.NoError(t, f.store.GetMeta(t.Context(), tx.TxIDChainHash(), stored))
			require.True(t, stored.Conflicting)
		})

		// The ancestry check fails on a pruned parent before the height check runs, except from
		// the txmeta cache, where the height check fails the same way. A retry, not a verdict;
		// a block referencing a subtree this node already validated fails open (see below).
		t.Run(name+": a record whose parent was pruned fails without a verdict", func(t *testing.T) {
			f := newConflictingFixture(t)
			tx := storedConflictingAtThisHeight(t, f)
			require.NoError(t, f.store.Delete(t.Context(), f.parent.TxIDChainHash()))

			err := check(t, f, tx)
			require.Error(t, err)
			require.False(t, errors.Is(err, errors.ErrTxInvalid), "%v", err)
		})
	}
}

// TestPeerPathTakesStoredConflictingRecords pins the other side: a peer subtree is not a block,
// has no height of its own to check a tx at, and takes a stored conflicting record as before,
// without asking the validator.
func TestPeerPathTakesStoredConflictingRecords(t *testing.T) {
	f := newConflictingFixture(t)
	tx := storedConflictingAtThisHeight(t, f)
	server, subtreeStore := f.newServer(t)

	calls := f.recorder.total()

	root := servePeerSubtree(t, subtreeStore, tx)
	require.NoError(t, livePassFor(t, server, root))

	require.Equal(t, calls, f.recorder.total(), "the stored record is taken without a validator call")
}

// TestCachedSubtreeChecksStoredConflictingTxsAtHeight pins the same for a block that references a
// subtree this node already validated: it never reaches the lookups above, and took a stored
// conflicting tx in it with only the ancestry check, so a node holding the subtree accepted a
// block a node without it rejected.
func TestCachedSubtreeChecksStoredConflictingTxsAtHeight(t *testing.T) {
	check := func(t *testing.T, f *conflictingFixture, tx *bt.Tx) error {
		server, subtreeStore := f.newServer(t)

		subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(2)
		require.NoError(t, err)
		require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 0, 0))
		require.NoError(t, subtree.AddNode(*f.other.TxIDChainHash(), 0, 0))

		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)

		root := *subtree.RootHash()
		require.NoError(t, subtreeStore.Set(t.Context(), root[:], fileformat.FileTypeSubtree, subtreeBytes))

		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  f.settings.ChainCfgParams.GenesisHash,
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
			Bits:           model.NBit{},
		}

		block, err := model.NewBlock(header, &bt.Tx{Version: 1}, []*chainhash.Hash{&root}, 3, 1000, 101, 0)
		require.NoError(t, err)

		return server.checkCachedSubtreesAgainstCandidateChain(t.Context(), block, []chainhash.Hash{root})
	}

	t.Run("a record created at another height is checked at this one", func(t *testing.T) {
		f := newConflictingFixture(t)
		tx := storedConflictingAtLaterHeight(t, f)

		err := check(t, f, tx)
		require.True(t, errors.Is(err, errors.ErrTxInvalid), "the coinbase is immature at the block's height: %v", err)
	})

	t.Run("a record valid at this height still passes", func(t *testing.T) {
		f := newConflictingFixture(t)
		tx := storedConflictingAtThisHeight(t, f)

		require.NoError(t, check(t, f, tx))
	})

	// unlike the full path, which fails on the same block (TestBlockPathRechecksStoredConflictingTxs)
	t.Run("a record whose parent was pruned fails open", func(t *testing.T) {
		f := newConflictingFixture(t)
		tx := storedConflictingAtThisHeight(t, f)
		require.NoError(t, f.store.Delete(t.Context(), f.parent.TxIDChainHash()))

		require.NoError(t, check(t, f, tx))
	})
}
