package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestParentOutputsForValidationNeverReadsTheBody pins the mainnet out-of-memory fix of
// 2026-10-15. Subtree validation asks for about 131,000 parent outputs at once, and the store
// read and parsed each parent's whole body to keep one output, which held gigabytes live. The
// answer comes from the coin rows alone: the UTXO table while the output is unspent, the spend
// journal once it is spent.
//
// Each parent's body is overwritten with bytes that do not decode, and its coin rows are left
// as they are. A read that touches the body fails the entry; one that does not answers it. Only
// an output the coin rows do not hold is looked for in the body, so the no-such-output case uses
// a parent whose body is intact.
func TestParentOutputsForValidationNeverReadsTheBody(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	// An unmined parent: one output unspent, one spent by a child, so answered from the journal.
	unmined := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, unmined, 1_000)
	require.NoError(t, err)

	spendOneOutput(t, s, ctx, unmined, 1, 1_001)

	// A parent mined in a block above the checkpoint (the store has none), with its body in its
	// partition.
	mined := mkTx(t, 1, 7_000)
	_, err = s.Create(ctx, mined, 1_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 5, BlockHeight: 1_000, OnLongestChain: true}))
	require.NoError(t, err)

	// A parent whose body stays intact, for the output that is in no coin row.
	intact := mkTx(t, 1, 8_000)
	_, err = s.Create(ctx, intact, 1_000)
	require.NoError(t, err)

	for _, p := range [][]byte{unmined.TxIDChainHash()[:], mined.TxIDChainHash()[:]} {
		tag, uerr := s.pool.Exec(ctx, `UPDATE tx_body SET raw_tx = '\xff'::bytea WHERE txid = $1`, p)
		require.NoError(t, uerr)
		require.Equal(t, int64(1), tag.RowsAffected(), "the parent's body is in its partition")
	}

	answers, err := s.ParentOutputsForValidation(ctx, []utxo.Outpoint{
		{TxID: *unmined.TxIDChainHash(), Vout: 0},
		{TxID: *unmined.TxIDChainHash(), Vout: 1},
		{TxID: *mined.TxIDChainHash(), Vout: 0},
		{TxID: *intact.TxIDChainHash(), Vout: 9},
	})
	require.NoError(t, err)

	for k, name := range []string{"unspent output", "spent output, from the journal", "mined parent", "no such output"} {
		require.NoError(t, answers[k].Err, "%s: the read touched the parent's body", name)
	}

	require.Equal(t, utxo.ParentOutputNotMined, answers[0].Status)
	require.Equal(t, unmined.Outputs[0].Satoshis, answers[0].Satoshis)
	require.Equal(t, []byte(*unmined.Outputs[0].LockingScript), []byte(*answers[0].LockingScript))

	require.Equal(t, utxo.ParentOutputNotMined, answers[1].Status)
	require.Equal(t, unmined.Outputs[1].Satoshis, answers[1].Satoshis)
	require.Equal(t, []byte(*unmined.Outputs[1].LockingScript), []byte(*answers[1].LockingScript))

	require.Equal(t, utxo.ParentOutputMined, answers[2].Status)
	require.Equal(t, uint32(1_000), answers[2].Height)
	require.Equal(t, mined.Outputs[0].Satoshis, answers[2].Satoshis)
	require.Equal(t, []byte(*mined.Outputs[0].LockingScript), []byte(*answers[2].LockingScript))

	require.NoError(t, answers[3].Err)
	require.Equal(t, utxo.ParentOutputNoSuchIndex, answers[3].Status, "a known parent with no such output")

	// An unknown parent stays TxNotFound.
	unknown := mkTx(t, 1, 9_000)
	answers, err = s.ParentOutputsForValidation(ctx, []utxo.Outpoint{{TxID: *unknown.TxIDChainHash(), Vout: 0}})
	require.NoError(t, err)
	require.Equal(t, utxo.ParentOutputTxNotFound, answers[0].Status)
}

// TestParentOutputsForValidationBoundsTheBodiesAMissReads is the bot review's point on PR 1663.
// A sender chooses the outpoints, so a transaction can name outputs that are in no coin row on
// purpose, here an out-of-range index on each of 40 parents. Each miss reads its parent's body,
// so the reads go missedBodyChunk parents at a time, and each answer is still NoSuchIndex.
func TestParentOutputsForValidationBoundsTheBodiesAMissReads(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	const parents = 40

	outpoints := make([]utxo.Outpoint, 0, parents)

	for i := 0; i < parents; i++ {
		p := mkTx(t, 1, uint64(10_000+i))
		_, err := s.Create(ctx, p, 1_000)
		require.NoError(t, err)

		outpoints = append(outpoints, utxo.Outpoint{TxID: *p.TxIDChainHash(), Vout: 50})
	}

	var chunks []int

	missedBodyChunkRead = func(n int) { chunks = append(chunks, n) }

	t.Cleanup(func() { missedBodyChunkRead = nil })

	answers, err := s.ParentOutputsForValidation(ctx, outpoints)
	require.NoError(t, err)

	for i, a := range answers {
		require.NoError(t, a.Err, "outpoint %d", i)
		require.Equal(t, utxo.ParentOutputNoSuchIndex, a.Status, "outpoint %d", i)
	}

	require.Equal(t, []int{16, 16, 8}, chunks, "the misses read their bodies in bounded chunks")
}
