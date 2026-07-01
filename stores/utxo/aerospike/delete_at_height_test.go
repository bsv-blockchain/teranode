package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// TestDeleteAtHeightFor pins the single source of truth for the Go DAH formula and
// the retention guard shared by the create, spend and setMined paths. The
// filter-expression path (buildDeleteAtHeightExpression) and the Lua
// setDeleteAtHeight UDF must compute the same value.
func TestDeleteAtHeightFor(t *testing.T) {
	t.Run("retention enabled returns blockHeight + retention", func(t *testing.T) {
		s := &Store{settings: &settings.Settings{GlobalBlockHeightRetention: 288}}

		dah, ok := s.deleteAtHeightFor(1000)
		require.True(t, ok)
		require.Equal(t, uint32(1288), dah)
	})

	t.Run("retention disabled (0) stamps no DAH", func(t *testing.T) {
		s := &Store{settings: &settings.Settings{GlobalBlockHeightRetention: 0}}

		dah, ok := s.deleteAtHeightFor(1000)
		require.False(t, ok, "no DAH must be stamped when retention is disabled")
		require.Equal(t, uint32(0), dah)
	})
}
