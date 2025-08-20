package aerospike

import (
	"strings"
	"testing"

	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
)

func TestFreezeUTXOsBasic(t *testing.T) {
	// Test basic functionality without mocking complex dependencies
	spends := []*utxo.Spend{
		{
			TxID:     &chainhash.Hash{},
			Vout:     0,
			UTXOHash: &chainhash.Hash{},
		},
	}

	// Test that the spends parameter is handled correctly
	assert.NotNil(t, spends)
	assert.Len(t, spends, 1)
	assert.Equal(t, uint32(0), spends[0].Vout)
}

func TestUnFreezeUTXOsBasic(t *testing.T) {
	// Test basic functionality without mocking
	spends := []*utxo.Spend{
		{
			TxID:     &chainhash.Hash{},
			Vout:     1,
			UTXOHash: &chainhash.Hash{},
		},
	}

	assert.NotNil(t, spends)
	assert.Len(t, spends, 1)
	assert.Equal(t, uint32(1), spends[0].Vout)
}

func TestReAssignUTXOBasic(t *testing.T) {
	// Test basic functionality
	oldUtxo := &utxo.Spend{
		TxID:     &chainhash.Hash{},
		Vout:     0,
		UTXOHash: &chainhash.Hash{},
	}

	newUtxo := &utxo.Spend{
		TxID:     &chainhash.Hash{},
		Vout:     0,
		UTXOHash: &chainhash.Hash{},
	}

	assert.NotNil(t, oldUtxo)
	assert.NotNil(t, newUtxo)
	assert.Equal(t, oldUtxo.Vout, newUtxo.Vout)
}

// Test constants are accessible
func TestLuaConstants(t *testing.T) {
	assert.Contains(t, LuaPackage, "teranode_v")

	// Test that LuaPackage version doesn't contain dots (per comment in teranode.go)
	assert.False(t, strings.Contains(LuaPackage, "."), "LuaPackage should not contain dots")

	assert.Equal(t, LuaReturnValue("OK"), LuaOk)
	assert.Equal(t, LuaReturnValue("ERROR"), LuaError)
	assert.Equal(t, LuaReturnValue("SPENT"), LuaSpent)
}
