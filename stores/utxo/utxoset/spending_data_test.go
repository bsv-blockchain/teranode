package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// TestGetNamesWhoSpentEachOutputWhenAsked.
//
// The shared conflict walks ask a parent "who took each of your outputs" through the metadata
// read, and act on the answer. This store deletes the coin row on spend, so the answer is not
// in the coin table at all: it is in the journal, which recorded the spender at the moment of
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
// this transaction's coin, which the conflict walk would then mark conflicting along with
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
