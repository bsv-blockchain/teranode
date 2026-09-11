package util

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// buildExtendedTx assembles an extended transaction with the requested shape.
// scriptLen < 0 leaves PreviousTxScript nil (which serializes as a single
// zero byte), exercising the branch the arithmetic has to special-case.
func buildExtendedTx(t *testing.T, inputs, outputs, prevScriptLen, unlockLen, lockLen int, setPrevHash bool) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	for i := 0; i < inputs; i++ {
		in := &bt.Input{
			PreviousTxOutIndex: uint32(i),
			PreviousTxSatoshis: uint64(1000 + i),
			SequenceNumber:     0xffffffff,
			UnlockingScript:    bscript.NewFromBytes(make([]byte, unlockLen)),
		}

		if setPrevHash {
			var h chainhash.Hash
			h[0] = byte(i + 1)
			require.NoError(t, in.PreviousTxIDAdd(&h))
		}

		if prevScriptLen >= 0 {
			s := bscript.Script(make([]byte, prevScriptLen))
			in.PreviousTxScript = &s
		}

		tx.Inputs = append(tx.Inputs, in)
	}

	for i := 0; i < outputs; i++ {
		tx.Outputs = append(tx.Outputs, &bt.Output{
			Satoshis:      uint64(500 + i),
			LockingScript: bscript.NewFromBytes(make([]byte, lockLen)),
		})
	}

	return tx
}

// TestExtendedTxSizeMatchesExtendedBytes pins the arithmetic against the real
// serialization across the shapes that stress each term: varying input and
// output counts, nil previous scripts, previous scripts long enough to need a
// 3-byte and a 5-byte length prefix, and inputs with no previous-txid hash
// (which ExtendedBytes omits but bt.Input.Size counts).
func TestExtendedTxSizeMatchesExtendedBytes(t *testing.T) {
	cases := []struct {
		name                                               string
		inputs, outputs, prevScriptLen, unlockLen, lockLen int
		setPrevHash                                        bool
	}{
		{"single in single out", 1, 1, 25, 107, 25, true},
		{"multi in multi out", 5, 3, 25, 107, 25, true},
		{"nil previous script", 3, 2, -1, 107, 25, true},
		{"zero length scripts", 2, 2, 0, 0, 0, true},
		{"three byte prev script prefix", 2, 1, 300, 107, 25, true},
		{"five byte prev script prefix", 1, 1, 70000, 107, 25, true},
		{"unset previous txid hash", 2, 1, 25, 107, 25, false},
		{"many inputs", 20, 20, 25, 107, 25, true},
		{"no inputs", 0, 2, 25, 107, 25, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := buildExtendedTx(t, tc.inputs, tc.outputs, tc.prevScriptLen, tc.unlockLen, tc.lockLen, tc.setPrevHash)

			require.Equal(t, len(tx.ExtendedBytes()), ExtendedTxSize(tx),
				"arithmetic size must equal the real extended serialization")
		})
	}
}

// TestExtendedTxSizeAllocationFree pins the reason the arithmetic exists: it
// must not serialize the transaction. len(tx.ExtendedBytes()) allocates and
// copies the whole transaction twice on a path that reads every transaction of
// every block.
func TestExtendedTxSizeAllocationFree(t *testing.T) {
	tx := buildExtendedTx(t, 5, 5, 25, 107, 25, true)

	allocs := testing.AllocsPerRun(100, func() {
		_ = ExtendedTxSize(tx)
	})

	require.Zero(t, allocs, "ExtendedTxSize must not allocate")
}
