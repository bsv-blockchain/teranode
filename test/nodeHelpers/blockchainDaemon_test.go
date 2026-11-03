package nodehelpers

import (
	"testing"

	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

func TestBlockchainDaemon(t *testing.T) {
	// Create a new blockchain daemon
	node, err := NewBlockchainDaemon(t)
	require.NoError(t, err)
	require.NotNil(t, node, "BlockchainDaemon should be created successfully")
	t.Cleanup(node.Stop)
	require.NoError(t, util.ValidateRequiredAdminAPIKey(node.Settings.GRPCAdminAPIKey),
		"the test daemon and its client need a valid shared service credential")

	// Start blockchain service
	err = node.StartBlockchainService()
	require.NoError(t, err, "Blockchain service should start without error")
}
