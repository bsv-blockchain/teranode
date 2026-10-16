package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/stretchr/testify/require"
)

// noopEmit discards completed subtrees. These tests are about what the builder
// accepts, not about what lands on disk.
func noopEmit(int, *subtreepkg.Subtree, *subtreepkg.Data, *subtreepkg.Meta) error {
	return nil
}

// TestBlockStreamBuilder_RejectsADuplicateTransaction is the CVE-2012-2459 floor.
// The merkle root cannot catch this: the duplicate-last-when-odd rule means a
// mutated transaction list can produce the same root, the same header and the same
// block hash as the honest block. This scan is the only thing that catches it, and
// it is required below the checkpoint too, because a checkpoint anchors the block
// hash while the peer supplies the body.
func TestBlockStreamBuilder_RejectsADuplicateTransaction(t *testing.T) {
	seen := txmap.NewSplitSwissMapUint64(16)

	b, err := newBlockStreamBuilder(6, 8, coinbaseTx(t), noopEmit, seen)
	require.NoError(t, err)

	tx1, hash1 := streamTx(t, 1)
	require.NoError(t, b.AddTx(tx1, hash1))

	tx2, hash2 := streamTx(t, 2)
	require.NoError(t, b.AddTx(tx2, hash2))

	err = b.AddTx(tx1, hash1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate transaction",
		"the message must name the fault, or a later change could satisfy this test without enforcing the rule")
}

// TestBlockStreamBuilder_AcceptsDistinctTransactions is the control: the dedup
// must not reject an honest block.
func TestBlockStreamBuilder_AcceptsDistinctTransactions(t *testing.T) {
	seen := txmap.NewSplitSwissMapUint64(16)

	b, err := newBlockStreamBuilder(8, 8, coinbaseTx(t), noopEmit, seen)
	require.NoError(t, err)

	for i := 1; i < 8; i++ {
		tx, hash := streamTx(t, i)
		require.NoError(t, b.AddTx(tx, hash), "transaction %d is distinct and must be accepted", i)
	}

	root, hashes, err := b.Finish()
	require.NoError(t, err)
	require.NotNil(t, root)
	require.Len(t, hashes, 1)
}

// TestBlockStreamBuilder_RefusesANilDedupMap pins that the check cannot be
// silently disabled. A duplicated transaction is a consensus fault the merkle
// root cannot detect (see the CVE-2012-2459 tests above); a constructor that
// accepted a nil map here would be the same shape as the regression fixed by
// commit c753d2e46 (model/check_duplicate_txs.go), just moved to a new
// component.
func TestBlockStreamBuilder_RefusesANilDedupMap(t *testing.T) {
	_, err := newBlockStreamBuilder(6, 8, coinbaseTx(t), noopEmit, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no duplicate-transaction map",
		"the message must name what is missing, or a future change could satisfy this test without enforcing the rule")
}

// TestBlockStreamBuilder_TheCoinbaseIsNotDeduped pins the exemption. The coinbase
// occupies slot zero as a placeholder and is never offered to AddTx, so nothing
// should insert it; this guards against a future change that starts doing so and
// then trips over the placeholder on the next block.
func TestBlockStreamBuilder_TheCoinbaseIsNotDeduped(t *testing.T) {
	seen := txmap.NewSplitSwissMapUint64(16)

	b, err := newBlockStreamBuilder(4, 8, coinbaseTx(t), noopEmit, seen)
	require.NoError(t, err)

	for i := 1; i < 4; i++ {
		tx, hash := streamTx(t, i)
		require.NoError(t, b.AddTx(tx, hash))
	}

	require.Equal(t, 3, seen.Length(), "only the three real transactions are inserted, never the coinbase")

	var placeholder chainhash.Hash
	for i := range placeholder {
		placeholder[i] = 0xff
	}

	require.False(t, seen.Exists(placeholder), "the coinbase placeholder must never be inserted")
}
