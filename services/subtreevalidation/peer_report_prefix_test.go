package subtreevalidation_test

import (
	"testing"

	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/stretchr/testify/require"
)

// TestLegacyPeerIDPrefixMatchesBlockValidation keeps subtree validation's copy of the legacy peer
// prefix identical to the one netsync builds ids with. The package cannot import
// blockvalidation, which imports it, so it holds its own copy and this external test is what
// stops the two drifting apart.
func TestLegacyPeerIDPrefixMatchesBlockValidation(t *testing.T) {
	require.Equal(t, blockvalidation.LegacyPeerIDPrefix, subtreevalidation.LegacyPeerIDPrefix)
}
