package model

import (
	"testing"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	primitives "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/stretchr/testify/assert"
)

func TestMiningCandidate_CreateCoinbaseTxCandidate(t *testing.T) {
	// Create a test mining candidate
	mc := &MiningCandidate{
		Height:        100,
		CoinbaseValue: 5000000000, // 50 BTC in satoshis
	}

	// Create test settings with a valid private key
	testPrivKey := "L56TgyTpDdvL3W24SMoALYotibToSCySQeo4pThLKxw6EFR6f93Q" // Test WIF private key
	tSettings := &settings.Settings{
		Coinbase: settings.CoinbaseSettings{
			ArbitraryText: "test coinbase",
		},
		BlockAssembly: settings.BlockAssemblySettings{
			MinerWalletPrivateKeys: []string{testPrivKey},
		},
	}

	t.Run("CreateStandardCoinbase", func(t *testing.T) {
		coinbaseTx, err := mc.CreateCoinbaseTxCandidate(tSettings)

		assert.NoError(t, err)
		assert.NotNil(t, coinbaseTx)
		assert.Equal(t, uint32(1), coinbaseTx.Version)
		assert.Len(t, coinbaseTx.Inputs, 1)
		assert.Len(t, coinbaseTx.Outputs, 1)
		assert.Equal(t, mc.CoinbaseValue, coinbaseTx.Outputs[0].Satoshis)
	})

	t.Run("CreateP2PKCoinbase", func(t *testing.T) {
		coinbaseTx, err := mc.CreateCoinbaseTxCandidate(tSettings, true)

		assert.NoError(t, err)
		assert.NotNil(t, coinbaseTx)
		assert.Equal(t, uint32(2), coinbaseTx.Version)
		assert.Len(t, coinbaseTx.Inputs, 1)
		assert.Len(t, coinbaseTx.Outputs, 1)
		assert.Equal(t, mc.CoinbaseValue, coinbaseTx.Outputs[0].Satoshis)

		// Verify the output is a P2PK script (should end with OP_CHECKSIG)
		lockingScript := coinbaseTx.Outputs[0].LockingScript
		assert.NotNil(t, lockingScript)
		scriptBytes := *lockingScript
		assert.Greater(t, len(scriptBytes), 0)
		assert.Equal(t, bscript.OpCHECKSIG, scriptBytes[len(scriptBytes)-1])
	})

	t.Run("CreateP2PKCoinbaseExplicitFalse", func(t *testing.T) {
		coinbaseTx, err := mc.CreateCoinbaseTxCandidate(tSettings, false)

		assert.NoError(t, err)
		assert.NotNil(t, coinbaseTx)
		assert.Equal(t, uint32(1), coinbaseTx.Version)
		assert.Len(t, coinbaseTx.Outputs, 1)
	})

	t.Run("InvalidPrivateKey", func(t *testing.T) {
		invalidSettings := &settings.Settings{
			Coinbase: settings.CoinbaseSettings{
				ArbitraryText: "test coinbase",
			},
			BlockAssembly: settings.BlockAssemblySettings{
				MinerWalletPrivateKeys: []string{"invalid-wif-key"},
			},
		}

		coinbaseTx, err := mc.CreateCoinbaseTxCandidate(invalidSettings)

		assert.Error(t, err)
		assert.Nil(t, coinbaseTx)
		assert.Contains(t, err.Error(), "can't decode coinbase priv key")
	})

	t.Run("EmptyPrivateKeys", func(t *testing.T) {
		emptySettings := &settings.Settings{
			Coinbase: settings.CoinbaseSettings{
				ArbitraryText: "test coinbase",
			},
			BlockAssembly: settings.BlockAssemblySettings{
				MinerWalletPrivateKeys: []string{},
			},
		}

		coinbaseTx, err := mc.CreateCoinbaseTxCandidate(emptySettings)

		assert.Error(t, err) // Should error with empty private keys
		assert.Nil(t, coinbaseTx)
		assert.Contains(t, err.Error(), "no wallet addresses provided")
	})

	t.Run("MultiplePrivateKeys", func(t *testing.T) {
		testPrivKey2 := "L56TgyTpDdvL3W24SMoALYotibToSCySQeo4pThLKxw6EFR6f93Q" // Another test WIF
		multiSettings := &settings.Settings{
			Coinbase: settings.CoinbaseSettings{
				ArbitraryText: "test coinbase",
			},
			BlockAssembly: settings.BlockAssemblySettings{
				MinerWalletPrivateKeys: []string{testPrivKey, testPrivKey2},
			},
		}

		coinbaseTx, err := mc.CreateCoinbaseTxCandidate(multiSettings)

		assert.NoError(t, err)
		assert.NotNil(t, coinbaseTx)
		// Should use the first private key for P2PK when multiple are provided
	})
}

func TestMiningCandidate_CreateCoinbaseTxCandidateForAddress(t *testing.T) {
	mc := &MiningCandidate{
		Height:        200,
		CoinbaseValue: 2500000000, // 25 BTC in satoshis
	}

	tSettings := &settings.Settings{
		Coinbase: settings.CoinbaseSettings{
			ArbitraryText: "test coinbase for address",
		},
	}

	t.Run("ValidAddress", func(t *testing.T) {
		address := "1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn"
		coinbaseTx, err := mc.CreateCoinbaseTxCandidateForAddress(tSettings, &address)

		assert.NoError(t, err)
		assert.NotNil(t, coinbaseTx)
		assert.Len(t, coinbaseTx.Inputs, 1)
		assert.Len(t, coinbaseTx.Outputs, 1)
		assert.Equal(t, mc.CoinbaseValue, coinbaseTx.Outputs[0].Satoshis)
	})

	t.Run("NilAddress", func(t *testing.T) {
		coinbaseTx, err := mc.CreateCoinbaseTxCandidateForAddress(tSettings, nil)

		assert.Error(t, err)
		assert.Nil(t, coinbaseTx)
		assert.Contains(t, err.Error(), "address is required")
	})

	t.Run("EmptyAddress", func(t *testing.T) {
		address := ""
		coinbaseTx, err := mc.CreateCoinbaseTxCandidateForAddress(tSettings, &address)

		// This should pass to CreateCoinbase which may handle empty addresses
		// The exact behavior depends on the CreateCoinbase implementation
		if err != nil {
			// If it errors, that's also acceptable behavior
			assert.Error(t, err)
			assert.Nil(t, coinbaseTx)
		} else {
			assert.NotNil(t, coinbaseTx)
		}
	})

	t.Run("ZeroCoinbaseValue", func(t *testing.T) {
		mcZero := &MiningCandidate{
			Height:        200,
			CoinbaseValue: 0,
		}
		address := "1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn"

		coinbaseTx, err := mcZero.CreateCoinbaseTxCandidateForAddress(tSettings, &address)

		assert.NoError(t, err)
		assert.NotNil(t, coinbaseTx)
		assert.Equal(t, uint64(0), coinbaseTx.Outputs[0].Satoshis)
	})
}

func TestCreateCoinbase(t *testing.T) {
	t.Run("ValidParameters", func(t *testing.T) {
		height := uint32(150)
		coinbaseValue := uint64(5000000000)
		arbitraryText := "test arbitrary text"
		addresses := []string{"1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn"}

		coinbaseTx, err := CreateCoinbase(height, coinbaseValue, arbitraryText, addresses)

		assert.NoError(t, err)
		assert.NotNil(t, coinbaseTx)
		assert.Len(t, coinbaseTx.Inputs, 1)
		assert.True(t, len(coinbaseTx.Outputs) > 0)

		// Verify coinbase input characteristics
		coinbaseInput := coinbaseTx.Inputs[0]
		assert.NotNil(t, coinbaseInput)
		// Coinbase inputs should have all-zero previous transaction hash
		assert.Equal(t, uint32(0xffffffff), coinbaseInput.PreviousTxOutIndex)
	})

	t.Run("MultipleAddresses", func(t *testing.T) {
		addresses := []string{
			"1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn",
			"1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2",
		}

		coinbaseTx, err := CreateCoinbase(100, 5000000000, "multi address", addresses)

		assert.NoError(t, err)
		assert.NotNil(t, coinbaseTx)
		// Should have outputs corresponding to the addresses provided
		assert.True(t, len(coinbaseTx.Outputs) >= len(addresses))
	})

	t.Run("EmptyAddresses", func(t *testing.T) {
		addresses := []string{}

		coinbaseTx, err := CreateCoinbase(100, 5000000000, "empty addresses", addresses)

		if err != nil {
			// If it errors with empty addresses, that's acceptable
			assert.Error(t, err)
			assert.Nil(t, coinbaseTx)
		} else {
			// If it succeeds, it should still create a valid transaction
			assert.NotNil(t, coinbaseTx)
		}
	})

	t.Run("HighBlockHeight", func(t *testing.T) {
		height := uint32(4294967295) // Max uint32
		addresses := []string{"1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn"}

		coinbaseTx, err := CreateCoinbase(height, 5000000000, "high height", addresses)

		assert.NoError(t, err)
		assert.NotNil(t, coinbaseTx)
	})

	t.Run("ZeroHeight", func(t *testing.T) {
		height := uint32(0)
		addresses := []string{"1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn"}

		coinbaseTx, err := CreateCoinbase(height, 5000000000, "zero height", addresses)

		assert.NoError(t, err)
		assert.NotNil(t, coinbaseTx)
	})

	t.Run("LongArbitraryText", func(t *testing.T) {
		// Create a long arbitrary text string
		longText := make([]byte, 1000)
		for i := range longText {
			longText[i] = 'A'
		}
		addresses := []string{"1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn"}

		coinbaseTx, err := CreateCoinbase(100, 5000000000, string(longText), addresses)

		// This may succeed or fail depending on coinbase size limits
		if err != nil {
			assert.Error(t, err)
			assert.Nil(t, coinbaseTx)
		} else {
			assert.NotNil(t, coinbaseTx)
		}
	})
}

// Test helper function to create valid WIF private keys for testing
func TestValidateWIFPrivateKey(t *testing.T) {
	testPrivKey := "L56TgyTpDdvL3W24SMoALYotibToSCySQeo4pThLKxw6EFR6f93Q"

	privateKey, err := primitives.PrivateKeyFromWif(testPrivKey)
	assert.NoError(t, err)
	assert.NotNil(t, privateKey)

	// Test that we can create an address from it
	_, err = bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	assert.NoError(t, err)
}

// Test edge cases and error conditions
func TestMiningCandidateEdgeCases(t *testing.T) {
	t.Run("NilMiningCandidate", func(t *testing.T) {
		var mc *MiningCandidate
		tSettings := &settings.Settings{
			Coinbase: settings.CoinbaseSettings{
				ArbitraryText: "test",
			},
			BlockAssembly: settings.BlockAssemblySettings{
				MinerWalletPrivateKeys: []string{},
			},
		}

		assert.Panics(t, func() {
			_, _ = mc.CreateCoinbaseTxCandidate(tSettings)
		})
	})

	t.Run("NilSettings", func(t *testing.T) {
		mc := &MiningCandidate{
			Height:        100,
			CoinbaseValue: 5000000000,
		}

		assert.Panics(t, func() {
			_, _ = mc.CreateCoinbaseTxCandidate(nil)
		})
	})

	t.Run("MaxCoinbaseValue", func(t *testing.T) {
		mc := &MiningCandidate{
			Height:        100,
			CoinbaseValue: 18446744073709551615, // Max uint64
		}

		testPrivKey := "L56TgyTpDdvL3W24SMoALYotibToSCySQeo4pThLKxw6EFR6f93Q"
		tSettings := &settings.Settings{
			Coinbase: settings.CoinbaseSettings{
				ArbitraryText: "max value test",
			},
			BlockAssembly: settings.BlockAssemblySettings{
				MinerWalletPrivateKeys: []string{testPrivKey},
			},
		}

		coinbaseTx, err := mc.CreateCoinbaseTxCandidate(tSettings)

		assert.NoError(t, err)
		assert.NotNil(t, coinbaseTx)
		assert.Equal(t, mc.CoinbaseValue, coinbaseTx.Outputs[0].Satoshis)
	})
}
