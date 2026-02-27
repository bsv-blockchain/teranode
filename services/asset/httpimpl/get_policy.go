package httpimpl

import (
	"math"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
)

// FeeAmount represents the mining fee as satoshis per bytes
type FeeAmount struct {
	Satoshis uint64 `json:"satoshis"`
	Bytes    uint64 `json:"bytes"`
}

// Policy contains the node's policy settings in ARC format
type Policy struct {
	MaxScriptSizePolicy     uint64    `json:"maxscriptsizepolicy"`
	MaxTxSigopsCountsPolicy uint64    `json:"maxtxsigopscountspolicy"`
	MaxTxSizePolicy         uint64    `json:"maxtxsizepolicy"`
	MiningFee               FeeAmount `json:"miningFee"`
	StandardFormatSupported bool      `json:"standardFormatSupported"`
}

// PolicyResponse is the response format for GET /v1/policy
type PolicyResponse struct {
	Timestamp time.Time `json:"timestamp"`
	Policy    Policy    `json:"policy"`
}

func (h *HTTP) GetPolicy(c echo.Context) error {
	policy := h.settings.Policy
	if policy == nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "policy settings not configured")
	}

	// Convert MinMiningTxFee (BSV/kB) to satoshis/bytes ratio
	// MinMiningTxFee is in BSV per kilobyte
	// 1 BSV = 100,000,000 satoshis, 1 kB = 1000 bytes
	feeInSatoshis := policy.MinMiningTxFee * 100_000_000
	if feeInSatoshis < 0 || feeInSatoshis > float64(math.MaxUint64) {
		return echo.NewHTTPError(http.StatusInternalServerError, "invalid fee configuration")
	}
	satoshisPerKB := uint64(feeInSatoshis)

	return c.JSON(http.StatusOK, &PolicyResponse{
		Timestamp: time.Now().UTC(),
		Policy: Policy{
			MaxScriptSizePolicy:     uint64(policy.MaxScriptSizePolicy),
			MaxTxSigopsCountsPolicy: uint64(policy.MaxTxSigopsCountsPolicy),
			MaxTxSizePolicy:         uint64(policy.MaxTxSizePolicy),
			MiningFee: FeeAmount{
				Satoshis: satoshisPerKB,
				Bytes:    1000,
			},
			StandardFormatSupported: true,
		},
	})
}
