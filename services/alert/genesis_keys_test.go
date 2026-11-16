package alert

import (
	"testing"

	"github.com/bsv-blockchain/go-alert-system/app/models"
	"github.com/stretchr/testify/require"
)

func TestUniqueKeys(t *testing.T) {
	require.Equal(t, []string{"a", "B"}, uniqueKeys([]string{" a ", "", "B", "A", "b", "  "}))
	require.Empty(t, uniqueKeys(nil))
}

func TestSameKeys(t *testing.T) {
	keys := func(ks ...string) []*models.PublicKey {
		out := make([]*models.PublicKey, 0, len(ks))
		for _, k := range ks {
			out = append(out, &models.PublicKey{Key: k})
		}

		return out
	}

	tests := []struct {
		name   string
		active []*models.PublicKey
		keys   []string
		want   bool
	}{
		{"same keys in another order and case", keys("AA", "bb"), []string{"bb", "aa"}, true},
		{"both empty", nil, nil, true},
		{"different key", keys("aa", "bb"), []string{"aa", "cc"}, false},
		{"fewer active keys", keys("aa"), []string{"aa", "bb"}, false},
		{"more active keys", keys("aa", "bb"), []string{"aa"}, false},
		{"repeated active key", keys("aa", "aa"), []string{"aa", "bb"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, sameKeys(tt.active, tt.keys))
		})
	}
}
