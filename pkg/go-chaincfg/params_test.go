// Copyright (c) 2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package chaincfg

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInvalidHashStr ensures the newShaHashFromStr function panics when used to
// with an invalid hash string.
func TestInvalidHashStr(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for invalid hash, got nil")
		}
	}()
	newHashFromStr("banana")
}

// TestMustRegisterPanic ensures the mustRegister function panics when used to
// register an invalid network.
func TestMustRegisterPanic(t *testing.T) {
	t.Parallel()

	// Set up a "defer function" to catch the expected panic to ensure it actually panicked.
	defer func() {
		if err := recover(); err == nil {
			t.Error("mustRegister did not panic as expected")
		}
	}()

	// Intentionally try to register duplicate params to force a panic.
	mustRegister(&MainNetParams)
}

// TestSeeds ensures the right seeds are defined.
func TestSeeds(t *testing.T) {
	expectedSeeds := []DNSSeed{
		{"seed.bitcoinsv.io", true},
	}

	if MainNetParams.DNSSeeds == nil {
		t.Error("Seed values are not set")
		return
	}

	if len(MainNetParams.DNSSeeds) != len(expectedSeeds) {
		t.Error("Incorrect number of seed values")
		return
	}

	for i := range MainNetParams.DNSSeeds {
		if MainNetParams.DNSSeeds[i] != expectedSeeds[i] {
			t.Error("Seed values are incorrect")
			return
		}
	}
}

// TestGetChainParams tests GetChainParams for all supported and unsupported networks.
func TestGetChainParams(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
		wantPtr *Params
	}{
		{"mainnet", "mainnet", false, &MainNetParams},
		{"testnet", "testnet", false, &TestNetParams},
		{"regtest", "regtest", false, &RegressionNetParams},
		{"stn", "stn", false, &StnParams},
		{"teratestnet", "teratestnet", false, &TeraTestNetParams},
		{"tstn", "tstn", false, &TeraScalingTestNetParams},
		{"unknown", "unknown", true, nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := GetChainParams(tc.input)
			if tc.wantErr {
				require.Error(t, err, "expected error for input %q", tc.input)
				assert.Nil(t, got, "expected nil result for input %q", tc.input)
			} else {
				require.NoError(t, err, "unexpected error for input %q", tc.input)
				assert.Equal(t, tc.wantPtr, got, "expected pointer for input %q", tc.input)
			}
		})
	}
}

// TestDNSSeedString tests the String method of the DNSSeed type.
func TestDNSSeedString(t *testing.T) {
	seed := DNSSeed{Host: "example.com", HasFiltering: true}
	assert.Equal(t, "example.com", seed.String(), "DNSSeed.String() should return the Host field")
}
