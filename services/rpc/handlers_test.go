package rpc

import (
	"testing"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/stretchr/testify/assert"
)

func TestHandleGetMiningInfo(t *testing.T) {
	difficulty := 97415240192.16336
	expectedHashRate := 6.973254512622107e+17
	networkHashPS := calculateHashRate(difficulty, chaincfg.MainNetParams.TargetTimePerBlock.Seconds())
	assert.Equal(t, networkHashPS, expectedHashRate)
}
