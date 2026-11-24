package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The legacy and svp2p services bridge the same P2P network with the same
// legacy_* settings and ports; running both in one daemon must be refused.
func TestValidateServiceExclusions(t *testing.T) {
	require.NoError(t, validateServiceExclusions(false, false))
	require.NoError(t, validateServiceExclusions(true, false))
	require.NoError(t, validateServiceExclusions(false, true))

	err := validateServiceExclusions(true, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot run together")
}
