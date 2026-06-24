package p2p

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPolicyFromSettings(t *testing.T) {
	t.Run("nil settings yield nil policy", func(t *testing.T) {
		assert.Nil(t, policyFromSettings(nil))
	})

	t.Run("happy path converts BSV/kB to sat/kB", func(t *testing.T) {
		p := &settings.PolicySettings{
			MinMiningTxFee:          0.000005,
			MaxScriptSizePolicy:     500_000,
			MaxTxSizePolicy:         10_485_760,
			MaxTxSigopsCountsPolicy: 4000,
		}

		fp := policyFromSettings(p)
		require.NotNil(t, fp)
		assert.Equal(t, uint64(500), fp.MiningFee.Satoshis)
		assert.Equal(t, uint64(1000), fp.MiningFee.Bytes)
		assert.Equal(t, uint64(500_000), fp.MaxScriptSizePolicy)
		assert.Equal(t, uint64(10_485_760), fp.MaxTxSizePolicy)
		assert.Equal(t, uint64(4000), fp.MaxTxSigopsCountsPolicy)
	})

	t.Run("negative fee yields nil", func(t *testing.T) {
		p := &settings.PolicySettings{MinMiningTxFee: -1}
		assert.Nil(t, policyFromSettings(p))
	})

	t.Run("zero fee is preserved", func(t *testing.T) {
		p := &settings.PolicySettings{MinMiningTxFee: 0}
		fp := policyFromSettings(p)
		require.NotNil(t, fp)
		assert.Equal(t, uint64(0), fp.MiningFee.Satoshis)
		assert.Equal(t, uint64(1000), fp.MiningFee.Bytes)
	})

	t.Run("NaN fee yields nil", func(t *testing.T) {
		p := &settings.PolicySettings{MinMiningTxFee: math.NaN()}
		assert.Nil(t, policyFromSettings(p))
	})

	t.Run("+Inf fee yields nil", func(t *testing.T) {
		p := &settings.PolicySettings{MinMiningTxFee: math.Inf(1)}
		assert.Nil(t, policyFromSettings(p))
	})

	t.Run("-Inf fee yields nil", func(t *testing.T) {
		p := &settings.PolicySettings{MinMiningTxFee: math.Inf(-1)}
		assert.Nil(t, policyFromSettings(p))
	})

	t.Run("negative MaxScriptSizePolicy yields nil", func(t *testing.T) {
		p := &settings.PolicySettings{MaxScriptSizePolicy: -1}
		assert.Nil(t, policyFromSettings(p))
	})

	t.Run("negative MaxTxSizePolicy yields nil", func(t *testing.T) {
		p := &settings.PolicySettings{MaxTxSizePolicy: -1}
		assert.Nil(t, policyFromSettings(p))
	})

	t.Run("negative MaxTxSigopsCountsPolicy yields nil", func(t *testing.T) {
		p := &settings.PolicySettings{MaxTxSigopsCountsPolicy: -1}
		assert.Nil(t, policyFromSettings(p))
	})
}

func TestNodeStatusMessage_FeePolicyJSONRoundTrip(t *testing.T) {
	fee := 0.000005
	msg := NodeStatusMessage{
		PeerID:         "12D3K",
		Type:           "node_status",
		BestHeight:     42,
		MinMiningTxFee: &fee,
		FeePolicy: &FeePolicy{
			MiningFee:               FeeAmount{Satoshis: 500, Bytes: 1000},
			MaxScriptSizePolicy:     500_000,
			MaxTxSizePolicy:         10_485_760,
			MaxTxSigopsCountsPolicy: 4000,
		},
	}

	raw, err := json.Marshal(msg)
	require.NoError(t, err)

	// Wire-format key must be "fee_policy" with nested ARC-shaped keys.
	assert.Contains(t, string(raw), `"fee_policy":{`)
	assert.Contains(t, string(raw), `"miningFee":{`)
	assert.Contains(t, string(raw), `"maxscriptsizepolicy":500000`)
	assert.Contains(t, string(raw), `"maxtxsizepolicy":10485760`)
	assert.Contains(t, string(raw), `"maxtxsigopscountspolicy":4000`)

	var decoded NodeStatusMessage
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.NotNil(t, decoded.FeePolicy)
	assert.Equal(t, msg.FeePolicy.MiningFee, decoded.FeePolicy.MiningFee)
	assert.Equal(t, msg.FeePolicy.MaxScriptSizePolicy, decoded.FeePolicy.MaxScriptSizePolicy)
	assert.Equal(t, msg.FeePolicy.MaxTxSizePolicy, decoded.FeePolicy.MaxTxSizePolicy)
	assert.Equal(t, msg.FeePolicy.MaxTxSigopsCountsPolicy, decoded.FeePolicy.MaxTxSigopsCountsPolicy)
	require.NotNil(t, decoded.MinMiningTxFee)
	assert.Equal(t, fee, *decoded.MinMiningTxFee)
}

func TestNodeStatusMessage_BackwardCompatNoFeePolicy(t *testing.T) {
	// A payload from an older peer that does not know about fee_policy.
	legacy := `{"peer_id":"12D3K","type":"node_status","best_height":7,"min_mining_tx_fee":0.000005}`

	var decoded NodeStatusMessage
	require.NoError(t, json.Unmarshal([]byte(legacy), &decoded))
	assert.Nil(t, decoded.FeePolicy)
	require.NotNil(t, decoded.MinMiningTxFee)
	assert.InDelta(t, 0.000005, *decoded.MinMiningTxFee, 1e-12)
}

func TestNodeStatusMessage_FeePolicyOmittedWhenNil(t *testing.T) {
	msg := NodeStatusMessage{
		PeerID:     "12D3K",
		Type:       "node_status",
		BestHeight: 1,
	}

	raw, err := json.Marshal(msg)
	require.NoError(t, err)
	assert.NotContains(t, string(raw), `"fee_policy"`)
}
