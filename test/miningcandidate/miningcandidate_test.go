package miningcandidate

import (
	"encoding/json"
	"testing"

	"github.com/bitcoin-sv/teranode/chaincfg"
	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMiningCandidate(t *testing.T) {
	t.Skip("Skipping test")

	tSettings := settings.NewSettings()
	tSettings.ChainCfgParams = &chaincfg.RegressionNetParams

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		SettingsOverride: tSettings,
		EnableRPC:        true,
	})

	// t.Cleanup(func() {
	// 	td.Stop()
	// })

	res, err := td.CallRPC("getminingcandidate", nil)
	require.NoError(t, err)

	var rpcResponse map[string]interface{}
	err = json.Unmarshal([]byte(res), &rpcResponse)
	require.NoError(t, err)

	candidate, ok := rpcResponse["result"].(map[string]interface{})
	require.True(t, ok)

	assert.Equal(t, float64(1), candidate["num_tx"].(float64))
	assert.Equal(t, float64(80), candidate["sizeWithoutCoinbase"].(float64))
}
