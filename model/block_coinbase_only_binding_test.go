package model

import (
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// TestCheckCoinbaseOnlyBodyBound covers the single definition of the coinbase-only binding
// (bitcoin-sv/teranode#4692), which Valid and both quick-validation entry points now share. Each
// clause is exercised directly.
//
// Mutation proof: dropping the merkle comparison reddens the second case; returning an error
// unconditionally reddens the first and the bound cases.
func TestCheckCoinbaseOnlyBodyBound(t *testing.T) {
	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	// newBody builds the minimal shape the rule reads: a header merkle root, a coinbase, a subtree
	// list and a claimed transaction count. Nothing else is touched, because nothing else is read.
	newBody := func(merkleRoot *chainhash.Hash, subtrees []*chainhash.Hash, txCount uint64) *Block {
		return &Block{
			Header:           &BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: merkleRoot},
			CoinbaseTx:       coinbase,
			Subtrees:         subtrees,
			TransactionCount: txCount,
		}
	}

	otherRoot := chainhash.Hash{0xAB}

	t.Run("subtrees present: no-op, whatever the root says", func(t *testing.T) {
		// Those bodies are bound by CheckMerkleRoot instead, so this rule must not judge them.
		require.NoError(t, newBody(&otherRoot, []*chainhash.Hash{{0x01}}, 99).CheckCoinbaseOnlyBodyBound())
	})

	t.Run("no subtrees, root is not the coinbase txid: corrupt", func(t *testing.T) {
		err := newBody(&otherRoot, nil, 1).CheckCoinbaseOnlyBodyBound()
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err), "an emptied subtree list is a corrupt body, got: %v", err)
		require.False(t, errors.Is(err, errors.ErrBlockInvalid), "an unbound body must never condemn the hash")
		require.Contains(t, err.Error(), "the header merkle root is not the coinbase txid")
	})

	t.Run("no subtrees, root matches, a contradictory count: still bound", func(t *testing.T) {
		// The count is the peer's, not the miner's, so it has no say in the binding.
		require.NoError(t, newBody(coinbase.TxIDChainHash(), nil, 2).CheckCoinbaseOnlyBodyBound())
	})

	// A body whose only transaction is tx and whose header commits to tx's txid.
	boundTo := func(tx *bt.Tx) *Block {
		return &Block{
			Header:           &BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: tx.TxIDChainHash()},
			CoinbaseTx:       tx,
			TransactionCount: 1,
		}
	}

	requireNotBound := func(t *testing.T, tx *bt.Tx) {
		t.Helper()

		require.Equal(t, 64, tx.Size(), "fixture precondition: a 64-byte transaction")

		err := boundTo(tx).CheckCoinbaseOnlyBodyBound()
		require.Error(t, err, "64 bytes may be an inner merkle node, so they must not bind")
		require.True(t, errors.IsBlockCorrupt(err), "an ambiguous body is corrupt, got: %v", err)
		require.False(t, errors.Is(err, errors.ErrBlockInvalid), "an ambiguous body must never condemn the hash")
		require.Contains(t, err.Error(), "may be an inner merkle node")
	}

	t.Run("64-byte transaction with no inputs: not bound", func(t *testing.T) {
		requireNotBound(t, sixtyFourByteTx(t, nil, 45))
	})

	t.Run("64-byte transaction with one non-null input: not bound", func(t *testing.T) {
		requireNotBound(t, sixtyFourByteTx(t, &chainhash.Hash{0x01}, 4))
	})

	t.Run("64-byte consensus coinbase: bound", func(t *testing.T) {
		// A null prevout cannot be ground into a child hash, so this shape is unambiguous: it is a
		// single-transaction block, and SV Node accepts it.
		cb := sixtyFourByteTx(t, &chainhash.Hash{}, 4)
		require.True(t, IsConsensusCoinbase(cb), "fixture precondition: consensus-shaped coinbase")
		require.NoError(t, boundTo(cb).CheckCoinbaseOnlyBodyBound())
	})

	t.Run("65-byte transaction with one non-null input: bound", func(t *testing.T) {
		// Only exactly 64 bytes is ambiguous; the shape checks behind the binding judge the rest.
		require.NoError(t, boundTo(sixtyFourByteTxPlusOne(t)).CheckCoinbaseOnlyBodyBound())
	})

	t.Run("committed non-coinbase with a contradictory count: bound", func(t *testing.T) {
		// The header commits to this exact transaction and the count is untrusted wire data, so the
		// count cannot stop the shape checks behind the binding from condemning it.
		noInputs := bt.NewTx()
		noInputs.AddOutput(&bt.Output{Satoshis: 1, LockingScript: bscript.NewFromBytes([]byte{0x51})})

		for _, tx := range []*bt.Tx{noInputs, sixtyFourByteTxPlusOne(t)} {
			body := boundTo(tx)
			body.TransactionCount = 2
			require.NoError(t, body.CheckCoinbaseOnlyBodyBound())
		}
	})

	t.Run("64 bytes with a contradictory count: still not bound", func(t *testing.T) {
		for _, tx := range []*bt.Tx{sixtyFourByteTx(t, nil, 45), sixtyFourByteTx(t, &chainhash.Hash{0x01}, 4)} {
			body := boundTo(tx)
			body.TransactionCount = 2

			err := body.CheckCoinbaseOnlyBodyBound()
			require.Error(t, err)
			require.True(t, errors.IsBlockCorrupt(err), "got: %v", err)
			require.Contains(t, err.Error(), "may be an inner merkle node")
		}
	})

	t.Run("genuine coinbase-only body: bound, no error", func(t *testing.T) {
		require.NoError(t, newBody(coinbase.TxIDChainHash(), nil, 1).CheckCoinbaseOnlyBodyBound())
		// The binding does not read the count, so 0 binds too. CheckCoinbaseOnlyBodyCount judges it.
		require.NoError(t, newBody(coinbase.TxIDChainHash(), nil, 0).CheckCoinbaseOnlyBodyBound())
	})
}

// TestCheckCoinbaseOnlyBodyCount covers the consistency check callers run after every consensus check.
func TestCheckCoinbaseOnlyBodyCount(t *testing.T) {
	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	newBody := func(subtrees []*chainhash.Hash, txCount uint64) *Block {
		return &Block{
			Header:           &BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: coinbase.TxIDChainHash()},
			CoinbaseTx:       coinbase,
			Subtrees:         subtrees,
			TransactionCount: txCount,
		}
	}

	t.Run("no subtrees, any count but one claimed: corrupt", func(t *testing.T) {
		// The body holds the coinbase, so 0 is as false as 2. A stored 0 would make the legacy
		// export write a zero count followed by the coinbase.
		for _, count := range []uint64{0, 2} {
			err := newBody(nil, count).CheckCoinbaseOnlyBodyCount()
			require.Error(t, err, "count %d", count)
			require.True(t, errors.IsBlockCorrupt(err), "a false count is a corrupt body, got: %v", err)
			require.False(t, errors.Is(err, errors.ErrBlockInvalid), "a false count must never condemn the hash")
			require.Contains(t, err.Error(), fmt.Sprintf("claims %d transactions", count))
		}
	})

	t.Run("no subtrees, exactly one transaction claimed: fine", func(t *testing.T) {
		require.NoError(t, newBody(nil, 1).CheckCoinbaseOnlyBodyCount())
	})

	t.Run("subtrees present: no-op", func(t *testing.T) {
		require.NoError(t, newBody([]*chainhash.Hash{{0x01}}, 99).CheckCoinbaseOnlyBodyCount())
	})
}

// sixtyFourByteTxPlusOne is a one-input spend one byte too long to be an inner merkle node.
func sixtyFourByteTxPlusOne(t *testing.T) *bt.Tx {
	t.Helper()

	tx := sixtyFourByteTx(t, &chainhash.Hash{0x01}, 4)
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x01, 0x02, 0x03, 0x04, 0x05})
	require.Equal(t, 65, tx.Size())

	return tx
}

// sixtyFourByteTx builds a transaction with one output whose locking script is empty or, with no
// input, scriptLen bytes long. With prevTxID nil it has no inputs: 4 + 1 + 1 + 8 + 1 + 45 + 4 = 64
// bytes for scriptLen 45. Otherwise it has one input spending prevTxID (index 0xFFFFFFFF when the
// hash is null, so a null hash makes a consensus coinbase) with a scriptLen-byte unlocking script:
// 4 + 1 + 36 + 1 + 4 + 4 + 1 + 8 + 1 + 4 = 64 bytes for scriptLen 4.
func sixtyFourByteTx(t *testing.T, prevTxID *chainhash.Hash, scriptLen int) *bt.Tx {
	t.Helper()

	script := make([]byte, scriptLen)
	for i := range script {
		script[i] = byte(i + 1)
	}

	tx := bt.NewTx()

	if prevTxID == nil {
		tx.AddOutput(&bt.Output{Satoshis: 50 * 100_000_000, LockingScript: bscript.NewFromBytes(script)})

		return tx
	}

	index := uint32(0)
	if prevTxID.IsEqual(&chainhash.Hash{}) {
		index = 0xFFFFFFFF
	}

	in := &bt.Input{PreviousTxOutIndex: index, UnlockingScript: bscript.NewFromBytes(script), SequenceNumber: bt.DefaultSequenceNumber}
	require.NoError(t, in.PreviousTxIDAdd(prevTxID))
	tx.Inputs = append(tx.Inputs, in)
	tx.AddOutput(&bt.Output{Satoshis: 50 * 100_000_000, LockingScript: bscript.NewFromBytes(nil)})

	return tx
}
