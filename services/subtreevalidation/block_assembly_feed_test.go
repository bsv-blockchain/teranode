package subtreevalidation

import (
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

func TestSameTxRuleEra(t *testing.T) {
	params := &chaincfg.Params{
		BIP0065Height:             -1, // unset heights are ignored
		BIP0066Height:             50,
		CSVHeight:                 576,
		UahfForkHeight:            0,
		GenesisActivationHeight:   100,
		ChronicleActivationHeight: 200,
	}

	tests := []struct {
		name string
		a, b uint32
		want bool
	}{
		{"same height", 150, 150, true},
		{"competitor at the tip height", 150, 151, true},
		{"block ahead of the tip, same era", 160, 151, true},
		{"just below Genesis", 99, 100, false},
		{"below Genesis, tip far above", 99, 151, false},
		{"at Genesis", 100, 101, true},
		{"just below Chronicle", 199, 200, false},
		{"across BIP66", 49, 51, false},
		{"across CSV", 575, 576, false},
		{"order does not matter", 200, 199, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, sameTxRuleEra(params, tt.a, tt.b))
		})
	}
}
