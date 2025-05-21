package miningcandidate

import (
	"encoding/json"
	"testing"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMiningCandidate(t *testing.T) {
	t.Skip("Skipping test")

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC: true,
	})

	defer td.Stop(t)

	res, err := td.CallRPC("getminingcandidate", nil)
	require.NoError(t, err)

	var rpcResponse map[string]any
	err = json.Unmarshal([]byte(res), &rpcResponse)
	require.NoError(t, err)

	candidate, ok := rpcResponse["result"].(map[string]any)
	require.True(t, ok)

	assert.Equal(t, float64(1), candidate["num_tx"].(float64))
	assert.Equal(t, float64(80), candidate["sizeWithoutCoinbase"].(float64))
}
