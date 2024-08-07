package util

import (
	"testing"

	"github.com/libsv/go-bt/v2"

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

func TestValidHeight(t *testing.T) {
	tx, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff18030910002f6d352d6363312fdcce95f3c057431c486ae662ffffffff0a0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00000000")
	require.NoError(t, err)

	height, miner, err := extractCoinbaseHeightAndText(*tx.Inputs[0].UnlockingScript)
	require.NoError(t, err)

	assert.Equal(t, uint32(4105), height)
	assert.Equal(t, "/m5-cc1/", miner)
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

func TestExtractCoinbaseHeight(t *testing.T) {
	tx, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17032a120d2f71646c6e6b2ffa3e9e2068b1e1743dc80d00ffffffff014864a012000000001976a91417db35d440a673a218e70a5b9d07f895facf50d288ac00000000")
	require.NoError(t, err)

	height, err := ExtractCoinbaseHeight(tx)
	require.NoError(t, err)
	assert.Equal(t, uint32(856618), height)
}
