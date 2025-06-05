package rpc

import (
	"testing"

	"github.com/bitcoin-sv/teranode/pkg/go-chaincfg"
	"github.com/stretchr/testify/assert"
)

func TestHandleGetMiningInfo(t *testing.T) {
	difficulty := 97415240192.16336
	expectedHashRate := 6.973254512622107e+17
	networkHashPS := calculateHashRate(difficulty, chaincfg.MainNetParams.TargetTimePerBlock.Seconds())
	assert.Equal(t, networkHashPS, expectedHashRate)
}

func TestIPOrSubnetValidation(t *testing.T) {
	ip := "172.19.0.8"

	assert.True(t, isIPOrSubnet(ip))

	// test subnet
	subnet := "172.19.0.0/24"
	assert.True(t, isIPOrSubnet(subnet))

	// test invalid ip
	invalidIP := "172.19.0.8.8"
	assert.False(t, isIPOrSubnet(invalidIP))

	// test invalid subnet
	invalidSubnet := "172.19.0.0/33"
	assert.False(t, isIPOrSubnet(invalidSubnet))

	// test ip with port
	invalidSubnet = "172.19.0.0:8080"
	assert.False(t, isIPOrSubnet(invalidSubnet))

	// test subnet with port
	invalidSubnet = "172.19.0.0/24:8080"
	assert.True(t, isIPOrSubnet(invalidSubnet))
}
