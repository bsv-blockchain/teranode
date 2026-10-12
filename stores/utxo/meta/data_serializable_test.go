package meta

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/stretchr/testify/require"
)

// output returns a minimal spendable output.
func output(satoshis uint64) *bt.Output {
	return &bt.Output{Satoshis: satoshis, LockingScript: bscript.NewFromBytes([]byte{0x51})}
}

// TestTxIsSerializable pins the predicate that guards every caller which turns a
// stored *bt.Tx back into bytes.
//
// A transaction stored from a UTXO-set snapshot (cmd/seeder) is written with nil
// outputs at every index that was not a live UTXO at snapshot time, and with no
// inputs at all. Both the external .outputs blob path
// (stores/utxo/aerospike/get.go getExternalTransaction) and the inline bins path
// (getTxFromBins) hand that shape back out. Serializing it panics inside go-bt,
// because Output.Size() and Output.appendTo() dereference a nil *Output.
func TestTxIsSerializable(t *testing.T) {
	tests := []struct {
		name string
		data *Data
		want bool
	}{
		{
			// Every current caller guards txMeta != nil first, but the predicate
			// must not become a landmine for the next one.
			name: "nil Data",
			data: nil,
			want: false,
		},
		{
			name: "nil Data.Tx",
			data: &Data{},
			want: false,
		},
		{
			name: "ordinary tx with inputs and outputs",
			data: &Data{Tx: &bt.Tx{
				Version: 1,
				Inputs:  []*bt.Input{{}},
				Outputs: []*bt.Output{output(1000)},
			}},
			want: true,
		},
		{
			name: "nil output hole below a live max index",
			data: &Data{Tx: &bt.Tx{
				Version: 1,
				Inputs:  []*bt.Input{{}},
				Outputs: []*bt.Output{nil, output(1000)},
			}},
			want: false,
		},
		{
			name: "output present but nil LockingScript",
			data: &Data{Tx: &bt.Tx{
				Version: 1,
				Inputs:  []*bt.Input{{}},
				Outputs: []*bt.Output{{Satoshis: 1000}},
			}},
			want: false,
		},
		{
			// The .outputs-blob reconstruction: no inputs at all, Version 0.
			name: "hollow snapshot reconstruction, nil Inputs",
			data: &Data{Tx: &bt.Tx{
				Outputs: []*bt.Output{output(1000)},
			}},
			want: false,
		},
		{
			// getTxFromBins sets Inputs to a non-nil empty slice, which makes
			// bt.Tx.IsExtended() report true. The predicate must test length,
			// never nil-ness.
			name: "hollow inline reconstruction, non-nil empty Inputs",
			data: &Data{Tx: &bt.Tx{
				Inputs:  []*bt.Input{},
				Outputs: []*bt.Output{output(1000)},
			}},
			want: false,
		},
		{
			// A seeded coinbase whose *trailing* output was spent has no nil
			// hole at all: restoreCoinbaseInput declines to restore the input
			// (cmd/seeder/seeder.go), and PadUTXOsWithNil pads only to
			// maxIndex+1, so the vector is simply short. All outputs non-nil,
			// zero inputs, IsCoinbase true. This shape does NOT panic on
			// serialize, so the zero-input check is the only thing rejecting it:
			// a predicate that trusts IsCoinbase would hand a caller a short,
			// wrong-txid transaction and report success — worse than the panic
			// it was meant to prevent.
			name: "seeded coinbase, trailing output spent, no nil hole",
			data: &Data{
				IsCoinbase: true,
				Tx: &bt.Tx{
					Outputs: []*bt.Output{output(1000)},
				},
			},
			want: false,
		},
		{
			name: "real coinbase has exactly one input",
			data: &Data{
				IsCoinbase: true,
				Tx: &bt.Tx{
					Version: 1,
					Inputs:  []*bt.Input{{}},
					Outputs: []*bt.Output{output(5000000000)},
				},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.data.TxIsSerializable())
		})
	}
}

// TestTxIsSerializableGatesTheGoBtPanic is the load-bearing claim: the predicate
// must return false for exactly those shapes that panic in go-bt. Without the
// gate these calls abort the process.
func TestTxIsSerializableGatesTheGoBtPanic(t *testing.T) {
	d := &Data{Tx: &bt.Tx{
		Version: 1,
		Inputs:  []*bt.Input{{}},
		Outputs: []*bt.Output{nil, output(1000)},
	}}

	require.False(t, d.TxIsSerializable(), "a nil output hole must not be reported serializable")

	require.Panics(t, func() { _ = d.Tx.ExtendedBytes() },
		"go-bt still panics on this shape — the predicate is the only thing standing between it and a caller")
	require.Panics(t, func() { _ = d.Tx.TxIDChainHash() },
		"TxIDChainHash panics too, so a txid comparison must be gated by the predicate, not the other way round")
}
