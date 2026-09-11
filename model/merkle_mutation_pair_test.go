package model

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/blob/null"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// computeMultiSubtreeMerkleRoot replicates the >1-subtree branch of
// Block.CheckMerkleRoot using the SAME real go-subtree primitives production uses
// (coinbase replacement in the first subtree, RootHash/RootHashPadded for the rest, and
// the top-level incomplete tree over the resulting subtree roots). It exists only to
// build a test header's HashMerkleRoot; the pass/fail assertions in the tests below all
// call the real Block.CheckMerkleRoot, so a mistake here just makes THAT comparison fail
// rather than producing a false pass.
func computeMultiSubtreeMerkleRoot(t *testing.T, coinbase *bt.Tx, subtrees []*subtreepkg.Subtree) *chainhash.Hash {
	t.Helper()
	require.Greater(t, len(subtrees), 1, "this helper only implements the >1-subtree branch")

	hashes := make([]chainhash.Hash, len(subtrees))

	for i, st := range subtrees {
		if i == 0 {
			root, err := st.RootHashWithReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size())) //nolint:gosec
			require.NoError(t, err)
			hashes[i] = *root
		} else {
			hashes[i] = *st.RootHash()
		}
	}

	targetLength := subtrees[0].Length()
	targetHeight := subtrees[0].Height

	if last := subtrees[len(subtrees)-1]; last.Length() < targetLength {
		lifted, err := last.RootHashPadded(targetHeight)
		require.NoError(t, err)
		hashes[len(hashes)-1] = *lifted
	}

	top, err := subtreepkg.NewIncompleteTreeByLeafCount(len(subtrees))
	require.NoError(t, err)

	for _, h := range hashes {
		require.NoError(t, top.AddNode(h, 1, 0))
	}

	rootArr := top.RootHash()

	root, err := chainhash.NewHash(rootArr[:])
	require.NoError(t, err)

	return root
}

// TestCVE20122459_MerkleMutantPair_SameHashDifferentBody is the linchpin regression for
// issue 1424 (HARDEN-1424). It builds the CVE-2012-2459 mutant pair exactly as worked out
// in the issue: honest 7 txs split [cb,T1,T2,T3]+[T4,T5,T6] against mutant 8 txs split
// [cb,T1,T2,T3]+[T4,T5,T6,T6], using the pinned go-subtree v1.4.2, subtree capacity 4, and
// real bt.Tx transactions (not hand-faked hashes) — so a change to go-subtree's
// duplicate-last-node-when-odd padding rule would break this test rather than silently
// stop being caught.
//
// It proves three things, in this order:
//
//  1. (a) the honest and mutant blocks compute the IDENTICAL merkle root, and therefore
//     the IDENTICAL block hash when placed under the same header fields — this is the
//     whole point of the CVE: an attacker replays the honest proof-of-work over a
//     different, doctored transaction list at zero mining cost.
//  2. (b) Block.CheckMerkleRoot PASSES on BOTH blocks, pinning that the merkle root
//     alone cannot detect the mutation.
//  3. (c) Block.Valid REJECTS the mutant — via the later, independent
//     duplicate-transaction check (the CVE-2012-2459 floor), not the merkle check.
func TestCVE20122459_MerkleMutantPair_SameHashDifferentBody(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	// T1..T6: real, distinct bt.Tx transactions (distinguished by LockTime, per the
	// newTx helper in update-tx-mined_test.go), so their TxIDChainHash() values are
	// genuine double-SHA256 hashes of real serialized transaction bytes.
	t1, t2, t3 := newTx(1), newTx(2), newTx(3)
	t4, t5, t6 := newTx(4), newTx(5), newTx(6)

	buildFirstSubtree := func() *subtreepkg.Subtree {
		st, buildErr := subtreepkg.NewTreeByLeafCount(4)
		require.NoError(t, buildErr)
		require.NoError(t, st.AddCoinbaseNode())
		require.NoError(t, st.AddNode(*t1.TxIDChainHash(), 1, uint64(t1.Size()))) //nolint:gosec
		require.NoError(t, st.AddNode(*t2.TxIDChainHash(), 1, uint64(t2.Size()))) //nolint:gosec
		require.NoError(t, st.AddNode(*t3.TxIDChainHash(), 1, uint64(t3.Size()))) //nolint:gosec

		return st
	}

	// Honest second subtree: 3 real leaves [T4,T5,T6], incomplete (capacity 4).
	honestSecond, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, honestSecond.AddNode(*t4.TxIDChainHash(), 1, uint64(t4.Size()))) //nolint:gosec
	require.NoError(t, honestSecond.AddNode(*t5.TxIDChainHash(), 1, uint64(t5.Size()))) //nolint:gosec
	require.NoError(t, honestSecond.AddNode(*t6.TxIDChainHash(), 1, uint64(t6.Size()))) //nolint:gosec

	// Mutant second subtree: T6 duplicated to make 4 real leaves [T4,T5,T6,T6], complete.
	mutantSecond, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, mutantSecond.AddNode(*t4.TxIDChainHash(), 1, uint64(t4.Size()))) //nolint:gosec
	require.NoError(t, mutantSecond.AddNode(*t5.TxIDChainHash(), 1, uint64(t5.Size()))) //nolint:gosec
	require.NoError(t, mutantSecond.AddNode(*t6.TxIDChainHash(), 1, uint64(t6.Size()))) //nolint:gosec
	require.NoError(t, mutantSecond.AddNode(*t6.TxIDChainHash(), 1, uint64(t6.Size()))) //nolint:gosec // the CVE duplicate

	honestSubtrees := []*subtreepkg.Subtree{buildFirstSubtree(), honestSecond}
	mutantSubtrees := []*subtreepkg.Subtree{buildFirstSubtree(), mutantSecond}

	// --- (a) same merkle root, computed independently for each block -----------------
	honestRoot := computeMultiSubtreeMerkleRoot(t, coinbase, honestSubtrees)
	mutantRoot := computeMultiSubtreeMerkleRoot(t, coinbase, mutantSubtrees)
	require.True(t, honestRoot.IsEqual(mutantRoot),
		"CVE-2012-2459: honest (7-tx) and mutant (8-tx, duplicated T6) subtree layouts must "+
			"produce the SAME merkle root under go-subtree's duplicate-last-node-when-odd rule")

	// ONE header, mined once, shared by both blocks. That sharing IS the crux of the CVE and
	// is why there is no honest-vs-mutant hash comparison here: since the roots are equal
	// (asserted above), a header built from each would be bit-identical, so comparing their
	// hashes could not fail unless that assertion already had. Hanging both bodies off a
	// single header states the property directly — one block hash, one proof-of-work, two
	// different transaction lists — instead of restating it as an assertion that cannot fail.
	// It also avoids grinding the same nonce twice.
	bits, err := NewNBitFromString("207fffff")
	require.NoError(t, err)

	sharedHeader := &BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: honestRoot, // == mutantRoot, asserted above
		Timestamp:      1231006505, // fixed, so the test is deterministic
		Bits:           *bits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := sharedHeader.HasMetTargetDifficulty(); ok {
			break
		}

		require.Less(t, sharedHeader.Nonce, uint32(1_000_000), "could not grind a nonce meeting the easy target")
		sharedHeader.Nonce++
	}

	buildBlock := func(header *BlockHeader, subtrees []*subtreepkg.Subtree) *Block {
		subtreeHashes := make([]*chainhash.Hash, len(subtrees))

		var txCount uint64

		for i, st := range subtrees {
			subtreeHashes[i] = st.RootHash()
			txCount += uint64(st.Length()) //nolint:gosec
		}

		b, blockErr := NewBlock(header, coinbase, subtreeHashes, txCount, 1000, 0, 0)
		require.NoError(t, blockErr)
		b.SubtreeSlices = subtrees

		return b
	}

	// Both bodies hang off the SAME header, so they necessarily share a block hash.
	honestBlock := buildBlock(sharedHeader, honestSubtrees)
	mutantBlock := buildBlock(sharedHeader, mutantSubtrees)
	require.True(t, honestBlock.Hash().IsEqual(mutantBlock.Hash()),
		"the two blocks must be indistinguishable by hash — that is what makes the mutation free")

	// --- (b) CheckMerkleRoot passes on BOTH -------------------------------------------
	require.NoError(t, honestBlock.CheckMerkleRoot(ctx))
	require.NoError(t, mutantBlock.CheckMerkleRoot(ctx),
		"CheckMerkleRoot must not catch the CVE-2012-2459 mutation: the duplicate-last-node rule "+
			"makes the mutant's subtree root identical to the honest one, so the header still matches")

	// --- (c) Block.Valid rejects the mutant -------------------------------------------
	subtreeStore, err := null.New(ulogger.TestLogger{})
	require.NoError(t, err)

	honestOldBlockIDs := txmap.NewSyncedMap[chainhash.Hash, []uint32]()

	validHonest, err := honestBlock.Valid(ctx, ulogger.TestLogger{}, subtreeStore, nil, honestOldBlockIDs, nil, nil, tSettings, nil)
	require.NoError(t, err, "the honest block must validate cleanly")
	require.True(t, validHonest)

	mutantOldBlockIDs := txmap.NewSyncedMap[chainhash.Hash, []uint32]()

	validMutant, err := mutantBlock.Valid(ctx, ulogger.TestLogger{}, subtreeStore, nil, mutantOldBlockIDs, nil, nil, tSettings, nil)
	require.False(t, validMutant, "the mutant block must be rejected")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "mutant rejection must be BlockInvalid (so the peer is still punished)")
	require.Contains(t, err.Error(), "duplicate transaction", "the mutant must be caught by the duplicate-transaction check, not the merkle check")
}
