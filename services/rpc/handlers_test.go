package rpc

import (
	"testing"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"gotest.tools/assert"
)

func TestHandleGetMiningInfo(t *testing.T) {
	difficulty := 97415240192.16336

	networkHashPS := calculateHashRate(difficulty, chaincfg.MainNetParams.TargetTimePerBlock.Seconds())
	assert.Equal(t, networkHashPS, 7.08831367103262e+17)
}
