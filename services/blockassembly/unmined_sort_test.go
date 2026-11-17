// Package blockassembly provides functionality for assembling Bitcoin blocks in Teranode.
package blockassembly

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// txHash returns a deterministic, unique hash for a given id (0-255).
func txHash(id byte) chainhash.Hash {
	var h chainhash.Hash
	for i := range h {
		h[i] = id
	}

	return h
}

// minedParent is a hash that never appears in the unmined set, standing in for a
// parent that is already mined on the best chain.
var minedParent = chainhash.Hash{}

// multiParentInpointsPtr builds a *TxInpoints spending vout 0 of each parent.
func multiParentInpointsPtr(parents ...chainhash.Hash) *subtree.TxInpoints {
	inputs := make([]*bt.Input, 0, len(parents))

	for _, p := range parents {
		in := &bt.Input{PreviousTxOutIndex: 0}
		if err := in.PreviousTxIDAdd(&p); err != nil {
			panic(err)
		}

		inputs = append(inputs, in)
	}

	ti, err := subtree.NewTxInpointsFromInputs(inputs)
	if err != nil {
		panic(err)
	}

	return &ti
}

// mkUnmined builds an unmined transaction with the given id, CreatedAt and parents.
func mkUnmined(id byte, createdAt int, parents ...chainhash.Hash) *utxo.UnminedTransaction {
	if len(parents) == 0 {
		parents = []chainhash.Hash{minedParent}
	}

	return &utxo.UnminedTransaction{
		Node: &subtree.Node{
			Hash:        txHash(id),
			Fee:         1000,
			SizeInBytes: 250,
		},
		TxInpoints:   multiParentInpointsPtr(parents...),
		CreatedAt:    createdAt,
		UnminedSince: 1,
	}
}

// permutations invokes fn with every permutation of the input slice.
func permutations(txs []*utxo.UnminedTransaction, fn func([]*utxo.UnminedTransaction)) {
	var permute func(k int)

	permute = func(k int) {
		if k == len(txs) {
			cp := make([]*utxo.UnminedTransaction, len(txs))
			copy(cp, txs)
			fn(cp)

			return
		}

		for i := k; i < len(txs); i++ {
			txs[k], txs[i] = txs[i], txs[k]
			permute(k + 1)
			txs[k], txs[i] = txs[i], txs[k]
		}
	}

	permute(0)
}

// indexOf returns the position of the transaction with the given id, or -1.
func indexOf(txs []*utxo.UnminedTransaction, id byte) int {
	want := txHash(id)
	for i, tx := range txs {
		if tx.Node.Hash.IsEqual(&want) {
			return i
		}
	}

	return -1
}

func TestStableSortUnminedByCreatedAt(t *testing.T) {
	t.Run("same timestamp parent/child - all permutations keep parent first", func(t *testing.T) {
		// parent (id 1) and child (id 2) share CreatedAt=100; child spends parent.
		base := []*utxo.UnminedTransaction{
			mkUnmined(1, 100),
			mkUnmined(2, 100, txHash(1)),
		}

		count := 0

		permutations(base, func(perm []*utxo.UnminedTransaction) {
			count++
			stableSortUnminedByCreatedAt(perm)

			require.Len(t, perm, 2, "no transaction may be dropped")
			require.Less(t, indexOf(perm, 1), indexOf(perm, 2), "parent must sort before child")
		})

		require.Equal(t, 2, count)
	})

	t.Run("same timestamp 3-deep chain - all permutations topologically ordered", func(t *testing.T) {
		// A(1) <- B(2) <- C(3), all CreatedAt=100.
		base := []*utxo.UnminedTransaction{
			mkUnmined(1, 100),
			mkUnmined(2, 100, txHash(1)),
			mkUnmined(3, 100, txHash(2)),
		}

		permutations(base, func(perm []*utxo.UnminedTransaction) {
			stableSortUnminedByCreatedAt(perm)

			require.Len(t, perm, 3)
			require.Less(t, indexOf(perm, 1), indexOf(perm, 2))
			require.Less(t, indexOf(perm, 2), indexOf(perm, 3))
		})
	})

	t.Run("same timestamp diamond - both parents before shared child", func(t *testing.T) {
		// A(1) is parent of B(2) and C(3); D(4) spends both B and C. All CreatedAt=100.
		base := []*utxo.UnminedTransaction{
			mkUnmined(1, 100),
			mkUnmined(2, 100, txHash(1)),
			mkUnmined(3, 100, txHash(1)),
			mkUnmined(4, 100, txHash(2), txHash(3)),
		}

		permutations(base, func(perm []*utxo.UnminedTransaction) {
			stableSortUnminedByCreatedAt(perm)

			require.Len(t, perm, 4)
			require.Less(t, indexOf(perm, 1), indexOf(perm, 2))
			require.Less(t, indexOf(perm, 1), indexOf(perm, 3))
			require.Less(t, indexOf(perm, 2), indexOf(perm, 4))
			require.Less(t, indexOf(perm, 3), indexOf(perm, 4))
		})
	})

	t.Run("cross-timestamp ordering is by CreatedAt", func(t *testing.T) {
		// Child (id 2) has an EARLIER CreatedAt than parent (id 1). The primary
		// sort key is CreatedAt, so ordering follows CreatedAt across different
		// timestamps; the topological tiebreak only applies within equal values.
		in := []*utxo.UnminedTransaction{
			mkUnmined(1, 200),
			mkUnmined(2, 100, txHash(1)),
		}

		stableSortUnminedByCreatedAt(in)

		require.Equal(t, 100, in[0].CreatedAt)
		require.Equal(t, 200, in[1].CreatedAt)
	})

	t.Run("unrelated same-timestamp transactions keep iterator order", func(t *testing.T) {
		// ids 10,11,12 are mutually unrelated and share CreatedAt=100; a stable
		// sort must preserve their incoming order.
		in := []*utxo.UnminedTransaction{
			mkUnmined(10, 100),
			mkUnmined(11, 100),
			mkUnmined(12, 100),
		}

		stableSortUnminedByCreatedAt(in)

		require.Equal(t, 0, indexOf(in, 10))
		require.Equal(t, 1, indexOf(in, 11))
		require.Equal(t, 2, indexOf(in, 12))
	})

	t.Run("SQLite-style: many identical CreatedAt with embedded parent/child", func(t *testing.T) {
		// The SQLite store stamps CreatedAt at one-second resolution, so a whole
		// second of transactions collides. Bury a child (id 2) BEFORE its parent
		// (id 1) inside a large equal-timestamp run and confirm it is reordered.
		in := make([]*utxo.UnminedTransaction, 0, 100)
		in = append(in, mkUnmined(2, 100, txHash(1))) // child first (worst case)

		for id := byte(50); id < 148; id++ {
			in = append(in, mkUnmined(id, 100)) // filler, unrelated
		}

		in = append(in, mkUnmined(1, 100)) // parent last

		stableSortUnminedByCreatedAt(in)

		require.Len(t, in, 100)
		require.Less(t, indexOf(in, 1), indexOf(in, 2), "parent must be reordered before child")
	})
}

// TestStableSortPreventsValidateParentChainDrop is the end-to-end regression
// test for issue 1157: a same-timestamp parent/child fed child-first must be
// reordered so validateParentChain retains both rather than dropping the child.
func TestStableSortPreventsValidateParentChainDrop(t *testing.T) {
	ctx := context.Background()

	newAssembler := func() (*BlockAssembler, *utxo.MockUtxostore) {
		mockStore := new(utxo.MockUtxostore)

		testSettings := &settings.Settings{}
		testSettings.BlockAssembly.ParentValidationBatchSize = 50
		testSettings.BlockAssembly.OnRestartValidateParentChain = true
		testSettings.BlockAssembly.OnRestartRemoveInvalidParentChainTxs = true

		ba := &BlockAssembler{
			utxoStore: mockStore,
			settings:  testSettings,
			logger:    ulogger.TestLogger{},
		}

		// Parent (id 1) references a mined parent (empty hash, on best chain);
		// child (id 2) references the in-list parent (id 1).
		mockStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				for _, unresolved := range args.Get(1).([]*utxo.UnresolvedMetaData) {
					if unresolved.Hash.IsEqual(&minedParent) {
						unresolved.Data = &meta.Data{BlockIDs: []uint32{1}, UnminedSince: 0}
					} else {
						// in-list unmined parent (id 1)
						unresolved.Data = &meta.Data{BlockIDs: []uint32{}, UnminedSince: 1}
					}
				}
			}).
			Return(nil)

		return ba, mockStore
	}

	bestChain := map[uint32]bool{1: true}

	t.Run("child-first without sort is dropped (demonstrates the bug)", func(t *testing.T) {
		ba, _ := newAssembler()

		// Deliberately unsorted: child before parent, same CreatedAt.
		unsorted := []*utxo.UnminedTransaction{
			mkUnmined(2, 100, txHash(1)),
			mkUnmined(1, 100),
		}

		valid, err := ba.validateParentChain(ctx, unsorted, bestChain)
		require.NoError(t, err)

		// The child is dropped because its parent appears after it.
		require.Equal(t, -1, indexOf(valid, 2), "child is dropped when parent sorts after it")
	})

	t.Run("stable sort then validate retains both", func(t *testing.T) {
		ba, _ := newAssembler()

		txs := []*utxo.UnminedTransaction{
			mkUnmined(2, 100, txHash(1)),
			mkUnmined(1, 100),
		}

		stableSortUnminedByCreatedAt(txs)

		valid, err := ba.validateParentChain(ctx, txs, bestChain)
		require.NoError(t, err)

		require.Len(t, valid, 2, "no transaction dropped after stable sort")
		require.Less(t, indexOf(valid, 1), indexOf(valid, 2))
	})
}
