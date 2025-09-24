package model

import (
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Pay-to-PubKeyHash address
func TestP2PKHAddressToScript(t *testing.T) {
	script, err := AddressToScript("1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn")
	if err != nil {
		t.Error(err)
	} else {
		h := hex.EncodeToString(script)
		expected := "76a9148be87b3978d8ef936b30ddd4ed903f8da7abd27788ac"

		if h != expected {
			t.Errorf("Expected %s, got %s", expected, h)
		}
	}
}

// Pay-to-ScriptHash address
func TestP2SHAddressToScript(t *testing.T) {
	script, err := AddressToScript("37BvY7rFguYQvEL872Y7Fo77Y3EBApC2EK")
	if err != nil {
		t.Error(err)
	} else {
		h := hex.EncodeToString(script)
		expected := "a9143c5031fd7b3f8dfc4aef2d98b76e74b1bb7a51b887"

		if h != expected {
			t.Errorf("Expected %s, got %s", expected, h)
		}
	}
}

func TestShortAddressToScript(t *testing.T) {
	_, err := AddressToScript("ADD8E55")
	require.Error(t, err)

	var teranodeError *errors.Error
	ok := errors.As(err, &teranodeError)
	require.True(t, ok)

	expected := "invalid address length for 'ADD8E55'"
	assert.Equal(t, expected, teranodeError.Message())
	assert.Equal(t, errors.ErrProcessing.Code(), teranodeError.Code())
}

func TestUnsupportedAddressToScript(t *testing.T) {
	_, err := AddressToScript("27BvY7rFguYQvEL872Y7Fo77Y3EBApC2EK")
	require.Error(t, err)

	var teranodeError *errors.Error
	ok := errors.As(err, &teranodeError)
	require.True(t, ok)

	expected := "address 27BvY7rFguYQvEL872Y7Fo77Y3EBApC2EK is not supported"
	assert.Equal(t, expected, teranodeError.Message())
	assert.Equal(t, errors.ErrProcessing.Code(), teranodeError.Code())
}

func TestBuildCoinbase(t *testing.T) {
	t.Run("BasicCoinbaseBuild", func(t *testing.T) {
		c1 := []byte{0x01, 0x02, 0x03}
		c2 := []byte{0x07, 0x08, 0x09}
		extraNonce1 := "0405"
		extraNonce2 := "06"

		result := BuildCoinbase(c1, c2, extraNonce1, extraNonce2)

		expected := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}
		assert.Equal(t, expected, result)
	})

	t.Run("EmptyExtraNonces", func(t *testing.T) {
		c1 := []byte{0x01, 0x02}
		c2 := []byte{0x03, 0x04}
		extraNonce1 := ""
		extraNonce2 := ""

		result := BuildCoinbase(c1, c2, extraNonce1, extraNonce2)

		expected := []byte{0x01, 0x02, 0x03, 0x04}
		assert.Equal(t, expected, result)
	})

	t.Run("InvalidHexInExtraNonce", func(t *testing.T) {
		c1 := []byte{0x01}
		c2 := []byte{0x02}
		extraNonce1 := "zz" // Invalid hex
		extraNonce2 := "ff"

		result := BuildCoinbase(c1, c2, extraNonce1, extraNonce2)

		// Should skip invalid hex but include valid hex
		expected := []byte{0x01, 0xff, 0x02}
		assert.Equal(t, expected, result)
	})

	t.Run("LongExtraNonces", func(t *testing.T) {
		c1 := []byte{0xaa}
		c2 := []byte{0xbb}
		extraNonce1 := "0102030405060708090a0b0c"
		extraNonce2 := "deadbeef"

		result := BuildCoinbase(c1, c2, extraNonce1, extraNonce2)

		expected := []byte{0xaa, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0xde, 0xad, 0xbe, 0xef, 0xbb}
		assert.Equal(t, expected, result)
	})

	t.Run("NilCoinbaseParts", func(t *testing.T) {
		result := BuildCoinbase(nil, nil, "", "")
		expected := []byte{}
		assert.Equal(t, expected, result)
	})
}

func TestGetCoinbaseParts(t *testing.T) {
	t.Run("ValidGetCoinbaseParts", func(t *testing.T) {
		height := uint32(518847)
		coinbaseValue := uint64(2504275756)
		coinbaseText := "Simon Ordish and Stuart Freeman made this happen"
		walletAddresses := []string{"1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn"}

		c1, c2, err := GetCoinbaseParts(height, coinbaseValue, coinbaseText, walletAddresses)

		assert.NoError(t, err)
		assert.NotNil(t, c1)
		assert.NotNil(t, c2)
		assert.Greater(t, len(c1), 0)
		assert.Greater(t, len(c2), 0)

		// Verify the coinbase1 contains the block height
		assert.Contains(t, c1, byte(0x03)) // Height bytes length
		// Height 518847 = 0x07EABF in little endian should be BF EA 07
		assert.Contains(t, c1, byte(0xbf))
		assert.Contains(t, c1, byte(0xea))
		assert.Contains(t, c1, byte(0x07))

		// Verify coinbase1 contains the coinbase text
		textBytes := []byte(coinbaseText)
		for _, b := range textBytes {
			assert.Contains(t, c1, b)
		}
	})

	t.Run("MultipleWalletAddresses", func(t *testing.T) {
		height := uint32(100000)
		coinbaseValue := uint64(5000000000)
		coinbaseText := "Multiple addresses test"
		walletAddresses := []string{
			"1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn",
			"1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
		}

		c1, c2, err := GetCoinbaseParts(height, coinbaseValue, coinbaseText, walletAddresses)

		assert.NoError(t, err)
		assert.NotNil(t, c1)
		assert.NotNil(t, c2)
		assert.Greater(t, len(c2), 0)
	})

	t.Run("EmptyWalletAddresses", func(t *testing.T) {
		height := uint32(100)
		coinbaseValue := uint64(5000000000)
		coinbaseText := "Empty addresses"
		walletAddresses := []string{}

		c1, c2, err := GetCoinbaseParts(height, coinbaseValue, coinbaseText, walletAddresses)

		assert.Error(t, err)
		// Note: c1 is created before the error occurs, so it's not nil
		assert.NotNil(t, c1)
		assert.Nil(t, c2)
		assert.Contains(t, err.Error(), "no wallet addresses provided")
	})

	t.Run("InvalidWalletAddress", func(t *testing.T) {
		height := uint32(100)
		coinbaseValue := uint64(5000000000)
		coinbaseText := "Invalid address"
		walletAddresses := []string{"invalid-address"}

		c1, c2, err := GetCoinbaseParts(height, coinbaseValue, coinbaseText, walletAddresses)

		assert.Error(t, err)
		// Note: c1 is created before the error occurs, so it's not nil
		assert.NotNil(t, c1)
		assert.Nil(t, c2)
	})

	t.Run("LongCoinbaseText", func(t *testing.T) {
		height := uint32(12345)
		coinbaseValue := uint64(1000000000)
		// Create a very long coinbase text that will be truncated
		coinbaseText := "This is a very long coinbase text that should be truncated because it exceeds the maximum allowed length for coinbase arbitrary data when combined with block height and space for extra nonce"
		walletAddresses := []string{"1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn"}

		c1, c2, err := GetCoinbaseParts(height, coinbaseValue, coinbaseText, walletAddresses)

		assert.NoError(t, err)
		assert.NotNil(t, c1)
		assert.NotNil(t, c2)

		// The coinbase text should have been truncated
		fullText := []byte(coinbaseText)
		truncated := false
		for i := range fullText[50:] { // Check if some chars from the end are missing
			if !containsByte(c1, fullText[50+i]) {
				truncated = true
				break
			}
		}
		assert.True(t, truncated, "Long coinbase text should be truncated")
	})

	t.Run("ZeroCoinbaseValue", func(t *testing.T) {
		height := uint32(1000)
		coinbaseValue := uint64(0)
		coinbaseText := "Zero value"
		walletAddresses := []string{"1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn"}

		c1, c2, err := GetCoinbaseParts(height, coinbaseValue, coinbaseText, walletAddresses)

		assert.NoError(t, err)
		assert.NotNil(t, c1)
		assert.NotNil(t, c2)
	})

	t.Run("HighBlockHeight", func(t *testing.T) {
		height := uint32(4294967295) // Max uint32
		coinbaseValue := uint64(100000)
		coinbaseText := "Max height"
		walletAddresses := []string{"1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn"}

		c1, c2, err := GetCoinbaseParts(height, coinbaseValue, coinbaseText, walletAddresses)

		assert.NoError(t, err)
		assert.NotNil(t, c1)
		assert.NotNil(t, c2)
	})
}

// Helper function to check if a byte slice contains a specific byte
func containsByte(slice []byte, b byte) bool {
	for _, item := range slice {
		if item == b {
			return true
		}
	}
	return false
}

func TestVarInt(t *testing.T) {
	t.Run("SmallInteger", func(t *testing.T) {
		result := VarInt(42)
		expected := []byte{42}
		assert.Equal(t, expected, result)
	})

	t.Run("ZeroValue", func(t *testing.T) {
		result := VarInt(0)
		expected := []byte{0}
		assert.Equal(t, expected, result)
	})

	t.Run("MaxSingleByte", func(t *testing.T) {
		result := VarInt(252)
		expected := []byte{252}
		assert.Equal(t, expected, result)
	})

	t.Run("TwoByteInteger", func(t *testing.T) {
		result := VarInt(253)
		expected := []byte{0xfd, 0xfd, 0x00}
		assert.Equal(t, expected, result)
	})

	t.Run("MaxTwoByteInteger", func(t *testing.T) {
		result := VarInt(65535)
		expected := []byte{0xfd, 0xff, 0xff}
		assert.Equal(t, expected, result)
	})

	t.Run("FourByteInteger", func(t *testing.T) {
		result := VarInt(65536)
		expected := []byte{0xfe, 0x00, 0x00, 0x01, 0x00}
		assert.Equal(t, expected, result)
	})

	t.Run("LargeFourByteInteger", func(t *testing.T) {
		result := VarInt(16777216) // 0x01000000
		expected := []byte{0xfe, 0x00, 0x00, 0x00, 0x01}
		assert.Equal(t, expected, result)
	})

	t.Run("MaxFourByteInteger", func(t *testing.T) {
		result := VarInt(4294967295) // Max uint32
		expected := []byte{0xfe, 0xff, 0xff, 0xff, 0xff}
		assert.Equal(t, expected, result)
	})

	t.Run("EightByteInteger", func(t *testing.T) {
		result := VarInt(4294967296) // 0x100000000
		expected := []byte{0xff, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00}
		assert.Equal(t, expected, result)
	})

	t.Run("VeryLargeInteger", func(t *testing.T) {
		result := VarInt(18446744073709551615) // Max uint64
		expected := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
		assert.Equal(t, expected, result)
	})

	t.Run("ConversionError_Uint16", func(t *testing.T) {
		// Test edge case where conversion might fail (though in practice it shouldn't with valid inputs)
		// Since we can't easily force a conversion error in tests, we'll test a large value that requires uint16
		result := VarInt(65534)
		expected := []byte{0xfd, 0xfe, 0xff}
		assert.Equal(t, expected, result)
	})

	t.Run("ConversionError_Uint32", func(t *testing.T) {
		// Test edge case where conversion might fail
		result := VarInt(4294967294)
		expected := []byte{0xfe, 0xfe, 0xff, 0xff, 0xff}
		assert.Equal(t, expected, result)
	})
}

func TestAddressToScript_ComprehensiveCoverage(t *testing.T) {
	t.Run("TestnetP2PKHAddress", func(t *testing.T) {
		// Testnet P2PKH address (starts with 'm' or 'n')
		script, err := AddressToScript("n2ZNV88uQbede7C5M5jzi6SyG4GVVr5JTt")
		assert.NoError(t, err)
		assert.NotNil(t, script)

		// Should contain OpDUP, OpHASH160, 0x14 (20 bytes), pubkey hash, OpEQUALVERIFY, OpCHECKSIG
		assert.Equal(t, byte(OpDUP), script[0])
		assert.Equal(t, byte(OpHASH160), script[1])
		assert.Equal(t, byte(0x14), script[2])
		assert.Equal(t, byte(OpEQUALVERIFY), script[23])
		assert.Equal(t, byte(OpCHECKSIG), script[24])
		assert.Equal(t, 25, len(script))
	})

	t.Run("TestnetP2SHAddress", func(t *testing.T) {
		// Testnet P2SH address (starts with '2')
		script, err := AddressToScript("2N2JD6wb56AfK4tfmM6PwdVmoYk2dCKf4Br")
		assert.NoError(t, err)
		assert.NotNil(t, script)

		// Should contain OpHASH160, 0x14 (20 bytes), script hash, OpEQUAL
		assert.Equal(t, byte(OpHASH160), script[0])
		assert.Equal(t, byte(0x14), script[1])
		assert.Equal(t, byte(OpEQUAL), script[22])
		assert.Equal(t, 23, len(script))
	})

	t.Run("InvalidBase58Address", func(t *testing.T) {
		_, err := AddressToScript("Invalid0OIl")
		assert.Error(t, err)
	})

	t.Run("AddressTooShort", func(t *testing.T) {
		// Address that's too short (will trigger length check)
		_, err := AddressToScript("1A")
		assert.Error(t, err)
	})
}

func TestMakeCoinbaseOutputTransactions_EdgeCases(t *testing.T) {
	t.Run("SingleSatoshiValue", func(t *testing.T) {
		result, err := makeCoinbaseOutputTransactions(1, []string{"1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn"})
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("ValueNotDivisibleByAddressCount", func(t *testing.T) {
		// 100 satoshis divided by 3 addresses = 33 each + 1 remainder goes to first address
		result, err := makeCoinbaseOutputTransactions(100, []string{
			"1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn",
			"1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
			"3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy",
		})
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Greater(t, len(result), 0)
	})

	t.Run("MaxUint64Value", func(t *testing.T) {
		result, err := makeCoinbaseOutputTransactions(18446744073709551615, []string{"1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn"})
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
}

func TestMakeCoinbase1_EdgeCases(t *testing.T) {
	t.Run("EmptyCoinbaseText", func(t *testing.T) {
		result := makeCoinbase1(12345, "")
		assert.NotNil(t, result)
		assert.Greater(t, len(result), 0)

		// Should contain version (4 bytes), input count (1 byte),
		// transaction hash (32 bytes), index (4 bytes), varint + height + empty text
		assert.Equal(t, uint32(1), binary.LittleEndian.Uint32(result[0:4])) // Version
		assert.Equal(t, byte(1), result[4])                                 // Input count
	})

	t.Run("MaxHeight", func(t *testing.T) {
		result := makeCoinbase1(4294967295, "test")
		assert.NotNil(t, result)
		assert.Greater(t, len(result), 0)
	})

	t.Run("LongCoinbaseTextTruncation", func(t *testing.T) {
		longText := make([]byte, 200)
		for i := range longText {
			longText[i] = 'A'
		}

		result := makeCoinbase1(12345, string(longText))
		assert.NotNil(t, result)

		// Should be truncated to fit within 100 bytes limit minus space for extra nonce (12 bytes)
		// The result should be shorter than if we included the full long text
		resultShort := makeCoinbase1(12345, "short")
		resultLongDiff := len(result) - len(resultShort)

		// The difference should be less than the full long text length
		assert.Less(t, resultLongDiff, len(longText))
	})
}

func TestMakeCoinbase2_Comprehensive(t *testing.T) {
	t.Run("ValidOutputTransactions", func(t *testing.T) {
		ot := []byte{0x01, 0x02, 0x03, 0x04}
		result := makeCoinbase2(ot)

		// Should contain sequence (4 bytes), original ot data, and locktime (4 bytes)
		expected := []byte{0xff, 0xff, 0xff, 0xff, 0x01, 0x02, 0x03, 0x04, 0x00, 0x00, 0x00, 0x00}
		assert.Equal(t, expected, result)
	})

	t.Run("EmptyOutputTransactions", func(t *testing.T) {
		result := makeCoinbase2([]byte{})
		expected := []byte{0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00}
		assert.Equal(t, expected, result)
	})

	t.Run("NilOutputTransactions", func(t *testing.T) {
		result := makeCoinbase2(nil)
		expected := []byte{0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00}
		assert.Equal(t, expected, result)
	})
}
