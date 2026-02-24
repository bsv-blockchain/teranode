package httpimpl

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetPolicy(t *testing.T) {
	initPrometheusMetrics()

	t.Run("success with default policy values", func(t *testing.T) {
		httpServer, _, echoContext, responseRecorder := GetMockHTTP(t, nil)

		httpServer.settings.Policy = &settings.PolicySettings{
			MaxScriptSizePolicy:     500000,
			MaxTxSigopsCountsPolicy: 0,
			MaxTxSizePolicy:         10485760,
			MinMiningTxFee:          0.000005,
		}

		echoContext.SetPath("/v1/policy")

		err := httpServer.GetPolicy(echoContext)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		var response PolicyResponse
		err = json.Unmarshal(responseRecorder.Body.Bytes(), &response)
		require.NoError(t, err)

		assert.False(t, response.Timestamp.IsZero())
		assert.Equal(t, uint64(500000), response.Policy.MaxScriptSizePolicy)
		assert.Equal(t, uint64(0), response.Policy.MaxTxSigopsCountsPolicy)
		assert.Equal(t, uint64(10485760), response.Policy.MaxTxSizePolicy)
		assert.Equal(t, uint64(500), response.Policy.MiningFee.Satoshis)
		assert.Equal(t, uint64(1000), response.Policy.MiningFee.Bytes)
		assert.True(t, response.Policy.StandardFormatSupported)
	})

	t.Run("nil policy returns error", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		httpServer.settings.Policy = nil
		echoContext.SetPath("/v1/policy")

		err := httpServer.GetPolicy(echoContext)
		require.Error(t, err)

		echoErr := &echo.HTTPError{}
		require.True(t, err.(*echo.HTTPError) != nil)
		require.ErrorAs(t, err, &echoErr)
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)
		assert.Equal(t, "policy settings not configured", echoErr.Message)
	})

	t.Run("zero fee", func(t *testing.T) {
		httpServer, _, echoContext, responseRecorder := GetMockHTTP(t, nil)

		httpServer.settings.Policy = &settings.PolicySettings{
			MinMiningTxFee: 0,
		}

		echoContext.SetPath("/v1/policy")

		err := httpServer.GetPolicy(echoContext)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		var response PolicyResponse
		err = json.Unmarshal(responseRecorder.Body.Bytes(), &response)
		require.NoError(t, err)

		assert.Equal(t, uint64(0), response.Policy.MiningFee.Satoshis)
		assert.Equal(t, uint64(1000), response.Policy.MiningFee.Bytes)
	})

	t.Run("negative fee returns error", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		httpServer.settings.Policy = &settings.PolicySettings{
			MinMiningTxFee: -0.001,
		}

		echoContext.SetPath("/v1/policy")

		err := httpServer.GetPolicy(echoContext)
		require.Error(t, err)

		echoErr := &echo.HTTPError{}
		require.ErrorAs(t, err, &echoErr)
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)
		assert.Equal(t, "invalid fee configuration", echoErr.Message)
	})

	t.Run("fee conversion precision", func(t *testing.T) {
		httpServer, _, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// 1 BSV/kB = 100,000,000 satoshis/kB
		httpServer.settings.Policy = &settings.PolicySettings{
			MinMiningTxFee: 1.0,
		}

		echoContext.SetPath("/v1/policy")

		err := httpServer.GetPolicy(echoContext)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		var response PolicyResponse
		err = json.Unmarshal(responseRecorder.Body.Bytes(), &response)
		require.NoError(t, err)

		assert.Equal(t, uint64(100_000_000), response.Policy.MiningFee.Satoshis)
	})

	t.Run("JSON format matches ARC spec", func(t *testing.T) {
		httpServer, _, echoContext, responseRecorder := GetMockHTTP(t, nil)

		httpServer.settings.Policy = &settings.PolicySettings{
			MaxScriptSizePolicy:     500000,
			MaxTxSigopsCountsPolicy: 0,
			MaxTxSizePolicy:         10485760,
			MinMiningTxFee:          0.000005,
		}

		echoContext.SetPath("/v1/policy")

		err := httpServer.GetPolicy(echoContext)
		require.NoError(t, err)

		var raw map[string]interface{}
		err = json.Unmarshal(responseRecorder.Body.Bytes(), &raw)
		require.NoError(t, err)

		// Verify top-level fields
		assert.Contains(t, raw, "timestamp")
		assert.Contains(t, raw, "policy")

		policy := raw["policy"].(map[string]interface{})
		assert.Contains(t, policy, "maxscriptsizepolicy")
		assert.Contains(t, policy, "maxtxsigopscountspolicy")
		assert.Contains(t, policy, "maxtxsizepolicy")
		assert.Contains(t, policy, "miningFee")
		assert.Contains(t, policy, "standardFormatSupported")

		miningFee := policy["miningFee"].(map[string]interface{})
		assert.Contains(t, miningFee, "satoshis")
		assert.Contains(t, miningFee, "bytes")
	})
}
