package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// reassignedParent creates a parent with two outputs of sats each and reassigns output 0 to
// newOwnerScript. It returns the parent, the new output, its new hash, its old hash, and the
// height from which the reassigned output can be spent.
func reassignedParent(t *testing.T, s *Store, ctx context.Context, sats uint64) (*bt.Tx, *bt.Output, *chainhash.Hash, *chainhash.Hash, uint32) {
	t.Helper()

	tSettings := settings.NewSettings()

	require.NoError(t, s.SetBlockHeight(100))

	parent := mkTx(t, 2, sats)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	old := &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0}
	newOutput := &bt.Output{Satoshis: sats, LockingScript: newOwnerScript(t)}

	newHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), newOutput, 0)
	require.NoError(t, err)

	oldHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[0], 0)
	require.NoError(t, err)

	require.NoError(t, s.FreezeUTXOs(ctx, []*utxo.Spend{old}, tSettings))
	require.NoError(t, s.ReAssignUTXO(ctx, old, &utxo.Spend{TxID: old.TxID, Vout: 0, UTXOHash: newHash}, tSettings))

	spendable := 100 + tSettings.UtxoStore.ReAssignedUtxoSpendableAfterBlocks
	require.NoError(t, s.SetBlockHeight(spendable))

	return parent, newOutput, newHash, oldHash, spendable
}

// spendParentTx builds a transaction spending the given outputs of parent, each input
// extended with the output it claims: the reassigned output 0 claims newOutput when one is
// given. fee makes two spenders of the same outputs different transactions.
func spendParentTx(t *testing.T, parent *bt.Tx, newOutput *bt.Output, fee uint64, vouts ...uint32) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	var total uint64

	for _, vout := range vouts {
		out := parent.Outputs[vout]
		if vout == 0 && newOutput != nil {
			out = newOutput
		}

		require.NoError(t, tx.FromUTXOs(&bt.UTXO{
			TxIDHash:      parent.TxIDChainHash(),
			Vout:          vout,
			LockingScript: out.LockingScript,
			Satoshis:      out.Satoshis,
		}))
		tx.Inputs[len(tx.Inputs)-1].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})

		total += out.Satoshis
	}

	tx.AddOutput(&bt.Output{Satoshis: total - fee, LockingScript: parent.Outputs[1].LockingScript})

	return tx
}

func spenderOf(t *testing.T, s *Store, ctx context.Context, parent *bt.Tx, vout uint32) *chainhash.Hash {
	t.Helper()

	got, err := s.GetSpend(ctx, &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: vout})
	require.NoError(t, err)

	if got.SpendingData == nil {
		return nil
	}

	return got.SpendingData.TxID
}

// TestRestoreConflictSpendsChecksAReassignedUTXOByTheRecordsHash pins how a conflict restore
// treats a reassigned UTXO. The record carries no output, so its UTXO hash is the claim: the
// new hash is restored, a record with no hash (or the zero hash a plain-form body yields) is
// restored, and the old owner's hash is refused. The first write and a replay answer the same,
// because a recovery that is accepted once and refused on its replay is stuck half-applied.
func TestRestoreConflictSpendsChecksAReassignedUTXOByTheRecordsHash(t *testing.T) {
	cases := []struct {
		name    string
		claim   func(newHash, oldHash *chainhash.Hash) *chainhash.Hash
		refused bool
	}{
		{"new hash", func(newHash, _ *chainhash.Hash) *chainhash.Hash { return newHash }, false},
		{"no hash", func(_, _ *chainhash.Hash) *chainhash.Hash { return nil }, false},
		{"zero hash", func(_, _ *chainhash.Hash) *chainhash.Hash { return &chainhash.Hash{} }, false},
		{"old hash", func(_, oldHash *chainhash.Hash) *chainhash.Hash { return oldHash }, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, ctx := newTestStore(t)
			parent, _, newHash, oldHash, spendable := reassignedParent(t, s, ctx, 5_000)

			spender := chainhash.HashH([]byte("restore spender"))
			record := []*utxo.Spend{{
				TxID:         parent.TxIDChainHash(),
				Vout:         0,
				UTXOHash:     tc.claim(newHash, oldHash),
				SpendingData: spendpkg.NewSpendingData(&spender, 0),
			}}

			for attempt := range 2 {
				err := s.RestoreConflictSpends(ctx, record, spendable)
				if tc.refused {
					require.Error(t, err, "attempt %d", attempt)
					require.True(t, errors.Is(err, errors.ErrUtxoHashMismatch), "attempt %d: got %v", attempt, err)
					require.Equal(t, 2, utxoCount(t, s, ctx, parent), "attempt %d: the UTXO stays for its new owner", attempt)

					continue
				}

				require.NoError(t, err, "attempt %d", attempt)
				require.Equal(t, &spender, spenderOf(t, s, ctx, parent, 0), "attempt %d", attempt)
			}
		})
	}
}

// TestProcessConflictingPromotesAWinnerThatSpendsAReassignedUTXO is a winner taking a
// reassigned UTXO that no loser took. This store keeps the winner's body in plain form, so its
// spend records carry the zero hash, and every read and restore conflict resolution makes of
// that UTXO must take it as no claim rather than as the wrong one. Refused, the resolution
// fails on every retry and block assembly cannot move forward.
func TestProcessConflictingPromotesAWinnerThatSpendsAReassignedUTXO(t *testing.T) {
	s, ctx := newTestStore(t)
	parent, newOutput, _, _, spendable := reassignedParent(t, s, ctx, 6_000)

	loser := spendParentTx(t, parent, nil, 1_000, 1)
	_, _, err := s.SpendAndCreate(ctx, loser, spendable)
	require.NoError(t, err)

	winner := spendParentTx(t, parent, newOutput, 2_000, 0, 1)
	_, _, err = s.SpendAndCreate(ctx, winner, spendable, utxo.WithCreateOnly(), utxo.WithConflicting(true))
	require.NoError(t, err)

	losers, _, err := utxo.ProcessConflicting(ctx, s, spendable+1, chainhash.Hash{}, []chainhash.Hash{*winner.TxIDChainHash()}, nil, utxo.NoAncestryGuard)
	require.NoError(t, err)
	require.True(t, losers.Exists(*loser.TxIDChainHash()))

	require.Equal(t, winner.TxIDChainHash(), spenderOf(t, s, ctx, parent, 0), "the winner takes the reassigned UTXO")
	require.Equal(t, winner.TxIDChainHash(), spenderOf(t, s, ctx, parent, 1), "the winner takes the contested UTXO")
}

// failAfterUnspend fails the first Unspend after it has written, so conflict resolution rolls
// back from its mutating phase with the losers' UTXOs already given back.
type failAfterUnspend struct {
	utxo.Store
	fired bool
}

func (f *failAfterUnspend) Unspend(ctx context.Context, spends []*utxo.Spend, locked ...bool) error {
	if err := f.Store.Unspend(ctx, spends, locked...); err != nil {
		return err
	}

	if !f.fired {
		f.fired = true
		return errors.NewStorageError("injected failure after unspend")
	}

	return nil
}

// TestProcessConflictingRollsBackALoserThatSpentAReassignedUTXO is a rollback that must put a
// reassigned UTXO back with the loser that spent it. The checklist names that UTXO by its new
// hash, which the restore must accept on the first write: refused, the rollback ends in manual
// intervention and block assembly refuses to start while the intent is unfinished.
func TestProcessConflictingRollsBackALoserThatSpentAReassignedUTXO(t *testing.T) {
	s, ctx := newTestStore(t)
	parent, newOutput, _, _, spendable := reassignedParent(t, s, ctx, 7_000)

	loser := spendParentTx(t, parent, newOutput, 1_000, 0, 1)
	_, _, err := s.SpendAndCreate(ctx, loser, spendable)
	require.NoError(t, err)

	winner := spendParentTx(t, parent, nil, 2_000, 1)
	_, _, err = s.SpendAndCreate(ctx, winner, spendable, utxo.WithCreateOnly(), utxo.WithConflicting(true))
	require.NoError(t, err)

	wrapped := &failAfterUnspend{Store: s}
	winners := []chainhash.Hash{*winner.TxIDChainHash()}

	_, _, err = utxo.ProcessConflicting(ctx, wrapped, spendable+1, chainhash.Hash{}, winners, nil, utxo.NoAncestryGuard)
	require.Error(t, err)
	require.True(t, wrapped.fired)
	require.NotContains(t, err.Error(), "MANUAL INTERVENTION")

	intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: spendable + 1, TxHashes: winners}
	r, err := s.LoadConflictRecovery(ctx, intent.IntentID())
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Equal(t, utxo.ConflictRecoveryRolledBack, r.Phase)

	require.Equal(t, loser.TxIDChainHash(), spenderOf(t, s, ctx, parent, 0), "the reassigned UTXO is back with the loser")
	require.Equal(t, loser.TxIDChainHash(), spenderOf(t, s, ctx, parent, 1), "the contested UTXO is back with the loser")
}
