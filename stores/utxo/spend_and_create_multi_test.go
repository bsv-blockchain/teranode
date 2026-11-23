package utxo

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

// DefaultSpendAndCreateMulti's contract tests run on real stores, from the
// shared suite in stores/utxo/tests (spend_and_create_multi.go and
// spend_and_create_multi_contract.go), wired into the sqlitememory, Postgres and
// Aerospike packages. What stays here is store-free: checkSpendAndCreateMultiList's
// parent bookkeeping, which no store outcome can observe, and the options each wave's
// calls carry.

// multiTx builds a transaction spending the given outpoints, with nOutputs
// outputs. Each call with a distinct seed gives a distinct txid.
func multiTx(t *testing.T, seed uint32, nOutputs int, spends ...Outpoint) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	tx.LockTime = seed

	for _, op := range spends {
		in := &bt.Input{PreviousTxOutIndex: op.Vout, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
		txid := op.TxID
		require.NoError(t, in.PreviousTxIDAdd(&txid))
		tx.Inputs = append(tx.Inputs, in)
	}

	for i := 0; i < nOutputs; i++ {
		tx.AddOutput(&bt.Output{Satoshis: uint64(1000 + i), LockingScript: bscript.NewFromBytes([]byte{0x51})})
	}

	return tx
}

func outsideOutpoint(n byte) Outpoint {
	return Outpoint{TxID: chainhash.Hash{0xee, n}, Vout: 0}
}

func outOf(tx *bt.Tx, vout uint32) Outpoint {
	return Outpoint{TxID: *tx.TxIDChainHash(), Vout: vout}
}

// laneRecordingMultiStore writes nothing: each create returns its transaction's lane position, or
// ErrTxExists for the transactions named in existing, and each spend-only call records the lane
// position its options named.
type laneRecordingMultiStore struct {
	positions map[chainhash.Hash]uint64
	existing  map[chainhash.Hash]bool

	mu        sync.Mutex
	spendLane map[chainhash.Hash]uint64
}

func (s *laneRecordingMultiStore) SpendAndCreate(_ context.Context, tx *bt.Tx, _ uint32, opts ...CreateOption) (*meta.Data, []*Spend, error) {
	o, err := ParseCreateOptions(opts...)
	if err != nil {
		return nil, nil, err
	}

	txid := *tx.TxIDChainHash()

	if o.SpendOnly {
		s.mu.Lock()
		s.spendLane[txid] = o.LanePosition
		s.mu.Unlock()

		return nil, nil, nil
	}

	if s.existing[txid] {
		return nil, nil, errors.NewTxExistsError("exists")
	}

	return &meta.Data{LanePosition: s.positions[txid]}, nil, nil
}

// In a below-checkpoint list, a child of a parent in the list is created in the first wave and
// spends in the second. The second wave's spends name the position the first wave wrote, so a store
// that records the spender's position in each parent's slot has it; a child whose record already
// existed has none to name.
func TestBelowCheckpointListSpendsNameTheCreatedPosition(t *testing.T) {
	parent := multiTx(t, 1, 2, outsideOutpoint(1))
	child := multiTx(t, 2, 1, outOf(parent, 0))
	existed := multiTx(t, 3, 1, outOf(parent, 1))

	store := &laneRecordingMultiStore{
		positions: map[chainhash.Hash]uint64{
			*parent.TxIDChainHash(): lane.Pack(4, 1),
			*child.TxIDChainHash():  lane.Pack(4, 2),
		},
		existing:  map[chainhash.Hash]bool{*existed.TxIDChainHash(): true},
		spendLane: map[chainhash.Hash]uint64{},
	}

	_, err := DefaultSpendAndCreateMulti(context.Background(), store, UniformSpendAndCreateMultiWidths(2),
		[]*bt.Tx{parent, child, existed}, 100, WithBelowCheckpoint())
	require.NoError(t, err)

	require.Equal(t, map[chainhash.Hash]uint64{
		*child.TxIDChainHash():   lane.Pack(4, 2),
		*existed.TxIDChainHash(): lane.None,
	}, store.spendLane, "only the children of the in-list parent spend in the second wave")
}
