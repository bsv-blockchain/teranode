package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// TestGetReportsAFrozenOutputWithTheFrozenSentinel.
//
// The conflict walks recognise a frozen output by the sentinel spender both reference stores
// put in its slot, and conflict resolution refuses to demote a transaction with a frozen
// descendant on that evidence alone. This store answered a frozen output exactly as an unspent
// one, nil, so the walk came back empty and the refusal could not fire.
//
// The frozen output is vout 1, so the sentinel's input index, which both reference stores set
// to the output number, is distinguishable from the zero a careless answer would give.
func TestGetReportsAFrozenOutputWithTheFrozenSentinel(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 3, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	ph := parent.TxIDChainHash()

	require.NoError(t, s.FreezeUTXOs(ctx, []*utxo.Spend{{TxID: ph, Vout: 1}}, nil))

	got, err := s.Get(ctx, ph, fields.Utxos)
	require.NoError(t, err)
	require.Len(t, got.SpendingDatas, 3)
	require.Nil(t, got.SpendingDatas[0], "output 0 is unspent and not frozen")
	require.NotNil(t, got.SpendingDatas[1], "output 1 is frozen and must say so")
	require.Equal(t, subtree.FrozenBytesTxHash.String(), got.SpendingDatas[1].TxID.String())
	require.Equal(t, 1, got.SpendingDatas[1].Vin, "the sentinel carries the output number, as in sql and aerospike")
	require.Nil(t, got.SpendingDatas[2])

	walk, err := utxo.GetConflictingChildren(ctx, s, *ph, 0)
	require.NoError(t, err)
	require.Contains(t, walk, subtree.FrozenBytesTxHash, "the conflict walk must see the frozen output")
}

// TestCounterConflictingRefusesAWinnerWithAFrozenDescendant drives the consumer of the sentinel.
// The winner took the contested UTXO and one of the winner's own outputs is frozen, so demoting
// the winner would demote a frozen coin; the shared walk refuses that with "tx has frozen
// child", and it can only do so if the store reports the freeze.
func TestCounterConflictingRefusesAWinnerWithAFrozenDescendant(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	winner := spendOutput(t, parent, 0, 2)
	_, err = s.Create(ctx, winner, 101)
	require.NoError(t, err)

	spends, err := spendOnly(ctx, s, winner, 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	require.NoError(t, s.FreezeUTXOs(ctx, []*utxo.Spend{{TxID: winner.TxIDChainHash(), Vout: 1}}, nil))

	loser := spendOutput(t, parent, 0, 3)
	_, err = s.Create(ctx, loser, 101, utxo.WithConflicting(true))
	require.NoError(t, err)

	_, err = s.GetCounterConflicting(ctx, *loser.TxIDChainHash())
	require.Error(t, err, "a winner with a frozen output must not be offered for demotion")
	require.Contains(t, err.Error(), "frozen child")
}

// TestGetNamesWhoSpentEachOutputWhenAsked.
//
// The shared conflict walks ask a parent "who took each of your outputs" through the metadata
// read, and act on the answer. This store deletes the UTXO row on spend, so the answer is not
// in the UTXO table at all: it is in the journal, which recorded the spender at the moment of
// the delete. Without this the walks see an empty answer for every parent and fail on every
// input, which is what stopped conflict handling working here at all.
func TestGetNamesWhoSpentEachOutputWhenAsked(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)

	spends, err := spendOnly(ctx, s, child, 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	got, err := s.Get(ctx, parent.TxIDChainHash(), fields.Utxos)
	require.NoError(t, err)

	require.Len(t, got.SpendingDatas, 2, "one entry per output, indexed by output number")

	require.NotNil(t, got.SpendingDatas[0], "output 0 was taken")
	require.Equal(t, child.TxIDChainHash().String(), got.SpendingDatas[0].TxID.String(),
		"and the journal knows by whom")

	require.Nil(t, got.SpendingDatas[1], "output 1 is still unspent")
}

// TestGetLeavesSpendingDataAloneWhenNotAsked. Naming the spender of every output costs a second
// query over two tables, and the validator resolves parents constantly without needing it, so
// it must stay off the read path unless a caller asks.
func TestGetLeavesSpendingDataAloneWhenNotAsked(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)

	spends, err := spendOnly(ctx, s, child, 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	got, err := s.Get(ctx, parent.TxIDChainHash())
	require.NoError(t, err)
	require.Nil(t, got.SpendingDatas, "not asked for, so not paid for")
}

// TestGetDoesNotNameASpenderFromACollidingKeyPrefix.
//
// Outputs are located by a packed key whose first 12 bytes are the transaction id prefix. That
// prefix is 96 bits and NON-UNIQUE by design, so it can locate a row but must never authorise
// using one. Here the consequence of getting it wrong is naming a stranger as the spender of
// this transaction's UTXO, which the conflict walk would then mark conflicting along with
// everything descended from it.
//
// The colliding row is planted directly, since a 12-byte collision will not arise by chance.
func TestGetDoesNotNameASpenderFromACollidingKeyPrefix(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	ph := parent.TxIDChainHash()

	twin := *ph
	twin[31] ^= 0xff

	stranger := *ph
	stranger[30] ^= 0xff

	require.Equal(t, ph[:12], twin[:12], "the twin must share the key prefix")

	require.NoError(t, s.ensureSpendJournalPartition(ctx, 101))

	// A spend of the TWIN's output 0, which packs into the same key as the parent's output 0.
	_, err = s.pool.Exec(ctx, `
        INSERT INTO spend_journal (spent_height, satoshis, created_height, spendable_from,
                                   flags, ukey, txid, spending_txid, script)
        VALUES (101, 1, 100, 0, 0, $1, $2, $3, '\x00')`,
		Pack(twin[:], 0), twin[:], stranger[:])
	require.NoError(t, err)

	got, err := s.Get(ctx, ph, fields.Utxos)
	require.NoError(t, err)

	for i, sd := range got.SpendingDatas {
		if sd == nil {
			continue
		}

		require.NotEqual(t, stranger.String(), sd.TxID.String(),
			"output %d must not be attributed to a spender of a different transaction", i)
	}
}
