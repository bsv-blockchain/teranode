package model

import (
	"context"
	"testing"
	"unsafe"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// recordingRun is an inpointRunBuilder that keeps a copy of what it was given.
type recordingRun struct {
	txs      []uint64
	inpoints [][]subtreepkg.Inpoint
}

func (r *recordingRun) addTx(tx uint64, inpoints []subtreepkg.Inpoint) {
	r.txs = append(r.txs, tx)
	r.inpoints = append(r.inpoints, append([]subtreepkg.Inpoint(nil), inpoints...))
}

func (r *recordingRun) finish() {}

// inpointsBufferBlock returns a block whose txMap holds parent at index 0 and
// txs[i] at index i+1, and a meta in which txs[i] spends vouts of parent.
func inpointsBufferBlock(t *testing.T, vouts [][]uint32) (*Block, *subtreepkg.Meta, []chainhash.Hash, chainhash.Hash) {
	t.Helper()

	parent := chainhash.HashH([]byte("inpoints-buffer-parent"))

	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)

	block := &Block{}
	m := txmap.NewSplitSwissMapUint64(16)
	require.NoError(t, m.Put(parent, 0))
	block.txMap = m

	txs := make([]chainhash.Hash, len(vouts))

	for i := range vouts {
		txs[i] = chainhash.HashH([]byte{byte(i)})
		require.NoError(t, subtree.AddNode(txs[i], 1, 1))
		require.NoError(t, m.Put(txs[i], uint64(i)+1))
	}

	meta := subtreepkg.NewSubtreeMeta(subtree)

	for i, vs := range vouts {
		words := append([]uint32{uint32(len(vs))}, vs...) //nolint:gosec // test sizes
		require.NoError(t, meta.SetTxInpoints(i, subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{parent}, words)))
	}

	return block, meta, txs, parent
}

// The inputs handed to the dedup run are collected in a buffer that is reused
// from one transaction to the next, so validating a transaction on the dedup
// path does not allocate for its inputs.
func TestValidateTransaction_ReusesInpointsBuffer(t *testing.T) {
	vouts := [][]uint32{{0}, {1, 2}, {3}}
	block, meta, txs, parent := inpointsBufferBlock(t, vouts)

	validationCtx := &validationContext{subtreeBase: []uint64{1, 1 + uint64(len(txs))}}
	run := &recordingRun{}

	var inpoints []subtreepkg.Inpoint

	for i := range txs {
		params := &transactionValidationParams{
			subtreeMetaSlice: meta,
			subtreeHash:      &parent,
			snIdx:            i,
			subtreeNode:      subtreepkg.Node{Hash: txs[i]},
			run:              run,
			inpoints:         inpoints,
		}

		missing, err := block.validateTransaction(context.Background(), nil, validationCtx, params)
		require.NoError(t, err)
		// The parent is in the block, so it is handed on only for its freeze records,
		// with the outputs this transaction spends of it.
		require.Equal(t, []missingParentTx{{parentTxHash: parent, txHash: txs[i], vouts: vouts[i], sameBlock: true}}, missing)
		require.True(t, params.inputsChecked)

		inpoints = params.inpoints
	}

	require.Equal(t, []uint64{1, 2, 3}, run.txs)
	require.Equal(t, [][]subtreepkg.Inpoint{
		{{Hash: parent, Index: 0}},
		{{Hash: parent, Index: 1}, {Hash: parent, Index: 2}},
		{{Hash: parent, Index: 3}},
	}, run.inpoints)

	nop := nopRun{}
	params := &transactionValidationParams{subtreeMetaSlice: meta, subtreeHash: &parent, run: nop}

	allocs := testing.AllocsPerRun(100, func() {
		// The edge buffers are per subtree, so each run starts them empty.
		params.missing, params.vouts = params.missing[:0], params.vouts[:0]

		for i := range txs {
			params.snIdx, params.subtreeNode = i, subtreepkg.Node{Hash: txs[i]}

			if _, err := block.validateTransaction(context.Background(), nil, validationCtx, params); err != nil {
				t.Fatal(err)
			}
		}
	})
	require.Zero(t, allocs, "a transaction's inputs and parent edges must go into the reused buffers")
}

type nopRun struct{}

func (nopRun) addTx(uint64, []subtreepkg.Inpoint) {}
func (nopRun) finish()                            {}

// sameBufferRun records the backing array of each slice it is given.
type sameBufferRun struct {
	arrays []*subtreepkg.Inpoint
	lens   []int
}

func (r *sameBufferRun) addTx(_ uint64, inpoints []subtreepkg.Inpoint) {
	r.arrays = append(r.arrays, unsafe.SliceData(inpoints))
	r.lens = append(r.lens, len(inpoints))
}

func (r *sameBufferRun) finish() {}

type sameBufferDedup struct{ run *sameBufferRun }

func (d sameBufferDedup) newRun(int, int) inpointRunBuilder { return d.run }

func (d sameBufferDedup) detect(context.Context, int, func() func(uint64) ([]subtreepkg.Inpoint, error)) (uint64, bool, error) {
	return 0, false, nil
}

// validateSubtree hands every transaction's inputs to the dedup run in one
// buffer, kept from one transaction to the next.
func TestValidateSubtree_ReusesInpointsBufferAcrossTxs(t *testing.T) {
	const n = 16

	// every tx has two inputs, so the buffer never needs to grow after the first
	txs := pvValidBlock(n)
	txs[1] = append(txs[1], pvSpend{pvExternal, 100})
	txs[2] = append(txs[2], pvSpend{pvExternal, 101})
	pv := buildPVBlock(t, txs, n)

	run := &sameBufferRun{}
	validationCtx := &validationContext{
		currentBlockHeaderHashesMap: pv.block.buildBlockHeaderHashesMap(pv.deps.currentChain),
		currentBlockHeaderIDsMap:    pv.block.buildBlockHeaderIDsMap(pv.deps.currentBlockHeaderIDs),
		dedup:                       sameBufferDedup{run: run},
		subtreeBase:                 []uint64{0, n},
	}

	require.NoError(t, pv.block.validateSubtree(context.Background(), ulogger.TestLogger{}, pv.deps, validationCtx,
		pv.block.SubtreeSlices[0], 0))

	require.Len(t, run.arrays, n-1, "every tx but the coinbase")

	for i := range run.arrays {
		require.Equal(t, 2, run.lens[i])
		require.Same(t, run.arrays[0], run.arrays[i], "tx %d got a new buffer", i+1)
	}
}
