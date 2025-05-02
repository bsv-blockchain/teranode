package nodehelpers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlockchainDaemon(t *testing.T) {
	// Create a new blockchain daemon
	node, err := NewBlockchainDaemon(t)
	require.NoError(t, err)
	require.NotNil(t, node, "BlockchainDaemon should be created successfully")

	// Start blockchain service
	err = node.StartBlockchainService()
	require.NoError(t, err, "Blockchain service should start without error")
	// Stop the node
	node.Stop()
}
