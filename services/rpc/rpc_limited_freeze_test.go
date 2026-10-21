package rpc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// freeze, unfreeze and reassign change which coins this node spends and, through the alert
// system's consensus freeze, which blocks it accepts. A limited user may not call them.
func TestRPCLimited_ExcludesFreezeCommands(t *testing.T) {
	for _, method := range []string{"freeze", "unfreeze", "reassign"} {
		_, ok := rpcLimited[method]
		require.False(t, ok, "%s must be admin-only", method)

		_, handled := rpcHandlersBeforeInit[method]
		require.True(t, handled, "%s must still be served to an admin", method)
	}
}
