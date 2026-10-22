package utxo

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

func TestProcessSpendBatch(t *testing.T) {
	parent := chainhash.HashH([]byte("parent"))
	a := chainhash.HashH([]byte("a"))
	b := chainhash.HashH([]byte("b"))
	claim := func(vout uint32, txID *chainhash.Hash, vin int) *Spend {
		return &Spend{TxID: &parent, Vout: vout, SpendingData: spendpkg.NewSpendingData(txID, vin)}
	}
	first := claim(0, &a, 0)
	identical := claim(0, &a, 0)
	other := claim(0, &b, 0)
	otherVin := claim(0, &a, 1)
	independent := claim(1, &b, 0)
	invalid := &Spend{}
	for _, tc := range []struct {
		name  string
		items []*Spend
		want  [][]*Spend
	}{
		{name: "empty"},
		{"independent and identical", []*Spend{first, identical, independent}, [][]*Spend{{first, identical, independent}}},
		{"conflict", []*Spend{first, identical, other, independent}, [][]*Spend{{first, identical}, {other, independent}}},
		{"different vin", []*Spend{first, otherVin}, [][]*Spend{{first}, {otherVin}}},
		{"alternating owners", []*Spend{first, other, identical}, [][]*Spend{{first}, {other}, {identical}}},
		{"invalid items reach backend", []*Spend{nil, invalid, first, other}, [][]*Spend{{nil, invalid, first}, {other}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got [][]*Spend
			ProcessSpendBatch(tc.items, func(s *Spend) *Spend { return s }, func(part []*Spend) {
				got = append(got, part)
			})
			require.Equal(t, tc.want, got)
		})
	}
}
