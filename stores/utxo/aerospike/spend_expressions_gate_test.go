package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

func TestUseExpressionSpend(t *testing.T) {
	tests := []struct {
		name              string
		enableExpressions bool
		utxoBatchSize     int
		want              bool
	}{
		{
			name:              "enabled and single utxo per record",
			enableExpressions: true,
			utxoBatchSize:     1,
			want:              true,
		},
		{
			name:              "enabled but multi utxo per record",
			enableExpressions: true,
			utxoBatchSize:     2,
			want:              false,
		},
		{
			name:              "enabled but invalid zero batch size",
			enableExpressions: true,
			utxoBatchSize:     0,
			want:              false,
		},
		{
			name:              "disabled with single utxo",
			enableExpressions: false,
			utxoBatchSize:     1,
			want:              false,
		},
		{
			name:              "disabled with multi utxo",
			enableExpressions: false,
			utxoBatchSize:     20000,
			want:              false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &Store{
				settings: &settings.Settings{
					Aerospike: settings.AerospikeSettings{
						EnableSpendFilterExpressions: tc.enableExpressions,
					},
				},
				utxoBatchSize: tc.utxoBatchSize,
			}

			require.Equal(t, tc.want, s.useExpressionSpend())
		})
	}
}
