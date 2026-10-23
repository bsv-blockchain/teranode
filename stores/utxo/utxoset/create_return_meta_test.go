package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/require"
)

// TestCreateReturnsWrittenState pins the record every create path hands back to its caller.
//
// The validator creates a transaction locked, hands it to block assembly, then unlocks it, and
// it decides whether to unlock from the record the create returned, not from the row. This
// store wrote the Locked bit to the row but left it off the returned record, so the unlock was
// skipped, the row stayed locked, and a child spending the transaction was refused with
// ErrTxLocked. Conflicting and the parent inpoints were dropped the same way; subtree
// validation and the txmeta cache read both from the returned record.
//
// Every path that returns creation metadata goes through appendCreate, so the test drives each
// entry point once in each batcher mode: the batched create and spend-and-create (the default
// settings) and the direct path (a batcher size of one turns both batchers off).
func TestCreateReturnsWrittenState(t *testing.T) {
	modes := []struct {
		name string
		tune func(*settings.Settings)
	}{
		{"batched", func(ts *settings.Settings) { withCheckpoints(ts, nil) }},
		{"direct", func(ts *settings.Settings) {
			withCheckpoints(ts, nil)
			ts.UtxoStore.StoreBatcherSize = 1
		}},
	}

	entries := []struct {
		name   string
		create func(t *testing.T, s *Store, tx *bt.Tx, opts ...utxo.CreateOption) *meta.Data
	}{
		{"Create", func(t *testing.T, s *Store, tx *bt.Tx, opts ...utxo.CreateOption) *meta.Data {
			md, err := s.Create(t.Context(), tx, 100, opts...)
			require.NoError(t, err)

			return md
		}},
		{"SpendAndCreate", func(t *testing.T, s *Store, tx *bt.Tx, opts ...utxo.CreateOption) *meta.Data {
			md, _, err := s.SpendAndCreate(t.Context(), tx, 100, append([]utxo.CreateOption{utxo.WithCreateOnly()}, opts...)...)
			require.NoError(t, err)

			return md
		}},
	}

	states := []struct {
		name        string
		opts        []utxo.CreateOption
		locked      bool
		conflicting bool
	}{
		{"plain", nil, false, false},
		{"locked", []utxo.CreateOption{utxo.WithLocked(true)}, true, false},
		{"conflicting", []utxo.CreateOption{utxo.WithConflicting(true)}, false, true},
	}

	for _, mode := range modes {
		for _, entry := range entries {
			for _, state := range states {
				t.Run(mode.name+"/"+entry.name+"/"+state.name, func(t *testing.T) {
					s, _ := newTestStoreWith(t, mode.tune)

					tx := mkTx(t, 1, 4_000)

					md := entry.create(t, s, tx, state.opts...)
					require.NotNil(t, md)

					require.Equal(t, state.locked, md.Locked,
						"the returned record must say Locked when the row is locked, or the validator never unlocks it")
					require.Equal(t, state.conflicting, md.Conflicting,
						"the returned record must say Conflicting when the row is conflicting")
					require.Equal(t, []chainhash.Hash{*tx.Inputs[0].PreviousTxIDChainHash()}, md.TxInpoints.ParentTxHashes,
						"the returned record must carry the parent inpoints the txmeta cache needs")

					want, err := subtree.NewTxInpointsFromTx(tx)
					require.NoError(t, err)

					wantBytes, err := want.Serialize()
					require.NoError(t, err)

					gotBytes, err := md.TxInpoints.Serialize()
					require.NoError(t, err)
					require.Equal(t, wantBytes, gotBytes, "the parent vouts must match the transaction's inputs")
				})
			}
		}
	}
}

// TestSpendAndCreateMultiReturnsWrittenState covers the list entry point. A plain list takes the
// netted write and builds its records in planCreates; a locked list is refused by canNet and
// goes one SpendAndCreate at a time. Both must return what they wrote.
func TestSpendAndCreateMultiReturnsWrittenState(t *testing.T) {
	for _, tc := range []struct {
		name   string
		opts   []utxo.CreateOption
		locked bool
	}{
		{"netted", nil, false},
		{"locked", []utxo.CreateOption{utxo.WithLocked(true)}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, ctx := newUncheckpointedStore(t)

			parent := mkTx(t, 1, 4_000)
			_, err := s.Create(ctx, parent, 100)
			require.NoError(t, err)

			child := spendingChild(t, parent, 3_000)

			results, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, 101, tc.opts...)
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, utxo.MultiTxCreated, results[0].Status)
			require.NotNil(t, results[0].Meta)

			require.Equal(t, tc.locked, results[0].Meta.Locked)
			require.Equal(t, []chainhash.Hash{*parent.TxIDChainHash()}, results[0].Meta.TxInpoints.ParentTxHashes)
		})
	}
}

// spendingChild builds an extended transaction spending output 0 of parent.
func spendingChild(t *testing.T, parent *bt.Tx, sats uint64) *bt.Tx {
	t.Helper()

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          0,
		LockingScript: parent.Outputs[0].LockingScript,
		Satoshis:      parent.Outputs[0].Satoshis,
	}))

	script, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)

	child.AddOutput(&bt.Output{Satoshis: sats, LockingScript: script})

	return child
}
