package settings

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The reorg's store deadline defaults to 0, which keeps a store failure from failing a fork reorg
// (issue 2269: the fail half stays off until a measured default exists). Only a non-zero
// override read back proves NewSettings reads the key.
func TestBlockAssemblyReorgStoreDeadline(t *testing.T) {
	require.Zero(t, NewSettings().BlockAssembly.ReorgStoreDeadline, "off by default")

	t.Setenv("blockassembly_reorgStoreDeadline", "45s")
	require.Equal(t, 45*time.Second, NewSettings().BlockAssembly.ReorgStoreDeadline, "loader must read blockassembly_reorgStoreDeadline")
}
