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

// The stored-prefix template is off by default (issue 2269). Only a non-default override read back
// proves NewSettings reads the key.
func TestBlockAssemblyStoredPrefixTemplate(t *testing.T) {
	require.False(t, NewSettings().BlockAssembly.StoredPrefixTemplate, "off by default")

	t.Setenv("blockassembly_storedPrefixTemplate", "true")
	require.True(t, NewSettings().BlockAssembly.StoredPrefixTemplate, "loader must read blockassembly_storedPrefixTemplate")
}

// The store queue's byte limit (issue 2269) has a default that only applies with the stored-prefix
// template on. Only a non-default override read back proves NewSettings reads the key.
func TestBlockAssemblyStoreQueueMaxMB(t *testing.T) {
	require.Equal(t, 4096, NewSettings().BlockAssembly.StoreQueueMaxMB)

	t.Setenv("blockassembly_storeQueueMaxMB", "512")
	require.Equal(t, 512, NewSettings().BlockAssembly.StoreQueueMaxMB, "loader must read blockassembly_storeQueueMaxMB")
}
