package util

import (
	"testing"

	"github.com/libsv/go-bt/v2/bscript"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidSigScript1(t *testing.T) {
	sigScript, err := bscript.NewFromHexString("0347520c2f7461616c2e636f6d2f79b010ec60689edf8d3a0000")
	require.NoError(t, err)

	height, miner, err := extractCoinbaseHeightAndText(*sigScript)
	require.NoError(t, err)

	assert.Equal(t, uint32(807495), height)
	assert.Equal(t, "/taal.com/", miner)
}

func TestValidSigScript2(t *testing.T) {
	sigScript, err := bscript.NewFromHexString("0100")
	require.NoError(t, err)

	height, miner, err := extractCoinbaseHeightAndText(*sigScript)
	require.NoError(t, err)

	assert.Equal(t, uint32(0), height)
	assert.Equal(t, "", miner)
}

func TestExtractMiner(t *testing.T) {
	miner := extractMiner("/taal.com/US/dksjk")
	assert.Equal(t, "/taal.com/US/", miner)
}

func TestExtractMiner2(t *testing.T) {
	miner := extractMiner("taal.com")
	assert.Equal(t, "taal.com", miner)
}

func TestExtractMiner3(t *testing.T) {
	miner := extractMiner("/taal.com")
	assert.Equal(t, "/", miner) // This is the current behaviour. Should it be "/taal.com"?
}
