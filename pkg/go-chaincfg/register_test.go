package chaincfg

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Define some of the required parameters for a user-registered
// network.  This is necessary to test the registration of and
// lookup of encoding magics from the network.
var mockNetParams = Params{
	Name: "mocknet",
	Net:  1<<32 - 1,

	LegacyPubKeyHashAddrID: 0x9f,
	LegacyScriptHashAddrID: 0xf9,
	HDPrivateKeyID:         [4]byte{0x01, 0x02, 0x03, 0x04},
	HDPublicKeyID:          [4]byte{0x05, 0x06, 0x07, 0x08},
	CashAddressPrefix:      "bsvmock",
}

// TestRegister ensures that the Register function works as expected for
func TestRegister(t *testing.T) {
	type registerTest struct {
		name   string
		params *Params
		err    error
	}

	type magicTest struct {
		magic byte
		valid bool
	}

	type prefixTest struct {
		prefix string
		valid  bool
	}

	type hdTest struct {
		priv []byte
		want []byte
		err  error
	}

	tests := []struct {
		name             string
		register         []registerTest
		p2pkhMagics      []magicTest
		p2shMagics       []magicTest
		cashAddrPrefixes []prefixTest
		hdMagics         []hdTest
	}{
		{
			name: "default networks",
			register: []registerTest{
				{
					name:   "duplicate mainnet",
					params: &MainNetParams,
					err:    ErrDuplicateNet,
				},
				{
					name:   "duplicate regtest",
					params: &RegressionNetParams,
					err:    ErrDuplicateNet,
				},
				{
					name:   "duplicate testnet",
					params: &TestNetParams,
					err:    ErrDuplicateNet,
				},
			},
			p2pkhMagics: []magicTest{
				{
					magic: MainNetParams.LegacyPubKeyHashAddrID,
					valid: true,
				},
				{
					magic: TestNetParams.LegacyPubKeyHashAddrID,
					valid: true,
				},
				{
					magic: RegressionNetParams.LegacyPubKeyHashAddrID,
					valid: true,
				},
				{
					magic: mockNetParams.LegacyPubKeyHashAddrID,
					valid: false,
				},
				{
					magic: 0xFF,
					valid: false,
				},
			},
			p2shMagics: []magicTest{
				{
					magic: MainNetParams.LegacyScriptHashAddrID,
					valid: true,
				},
				{
					magic: TestNetParams.LegacyScriptHashAddrID,
					valid: true,
				},
				{
					magic: RegressionNetParams.LegacyScriptHashAddrID,
					valid: true,
				},
				{
					magic: mockNetParams.LegacyScriptHashAddrID,
					valid: false,
				},
				{
					magic: 0xFF,
					valid: false,
				},
			},
			cashAddrPrefixes: []prefixTest{
				{
					prefix: MainNetParams.CashAddressPrefix + ":",
					valid:  true,
				},
				{
					prefix: TestNetParams.CashAddressPrefix + ":",
					valid:  true,
				},
				{
					prefix: RegressionNetParams.CashAddressPrefix + ":",
					valid:  true,
				},
				{
					prefix: strings.ToUpper(MainNetParams.CashAddressPrefix + ":"),
					valid:  true,
				},
				{
					prefix: mockNetParams.CashAddressPrefix + ":",
					valid:  false,
				},
				{
					prefix: "abc1",
					valid:  false,
				},
				{
					prefix: "1",
					valid:  false,
				},
				{
					prefix: MainNetParams.CashAddressPrefix,
					valid:  false,
				},
			},
			hdMagics: []hdTest{
				{
					priv: MainNetParams.HDPrivateKeyID[:],
					want: MainNetParams.HDPublicKeyID[:],
					err:  nil,
				},
				{
					priv: TestNetParams.HDPrivateKeyID[:],
					want: TestNetParams.HDPublicKeyID[:],
					err:  nil,
				},
				{
					priv: RegressionNetParams.HDPrivateKeyID[:],
					want: RegressionNetParams.HDPublicKeyID[:],
					err:  nil,
				},
				{
					priv: mockNetParams.HDPrivateKeyID[:],
					err:  ErrUnknownHDKeyID,
				},
				{
					priv: []byte{0xff, 0xff, 0xff, 0xff},
					err:  ErrUnknownHDKeyID,
				},
				{
					priv: []byte{0xff},
					err:  ErrUnknownHDKeyID,
				},
			},
		},
		{
			name: "register mocknet",
			register: []registerTest{
				{
					name:   "mocknet",
					params: &mockNetParams,
					err:    nil,
				},
			},
			p2pkhMagics: []magicTest{
				{
					magic: MainNetParams.LegacyPubKeyHashAddrID,
					valid: true,
				},
				{
					magic: TestNetParams.LegacyPubKeyHashAddrID,
					valid: true,
				},
				{
					magic: RegressionNetParams.LegacyPubKeyHashAddrID,
					valid: true,
				},
				{
					magic: mockNetParams.LegacyPubKeyHashAddrID,
					valid: true,
				},
				{
					magic: 0xFF,
					valid: false,
				},
			},
			p2shMagics: []magicTest{
				{
					magic: MainNetParams.LegacyScriptHashAddrID,
					valid: true,
				},
				{
					magic: TestNetParams.LegacyScriptHashAddrID,
					valid: true,
				},
				{
					magic: RegressionNetParams.LegacyScriptHashAddrID,
					valid: true,
				},
				{
					magic: mockNetParams.LegacyScriptHashAddrID,
					valid: true,
				},
				{
					magic: 0xFF,
					valid: false,
				},
			},
			cashAddrPrefixes: []prefixTest{
				{
					prefix: MainNetParams.CashAddressPrefix + ":",
					valid:  true,
				},
				{
					prefix: TestNetParams.CashAddressPrefix + ":",
					valid:  true,
				},
				{
					prefix: RegressionNetParams.CashAddressPrefix + ":",
					valid:  true,
				},
				{
					prefix: strings.ToUpper(MainNetParams.CashAddressPrefix + ":"),
					valid:  true,
				},
				{
					prefix: mockNetParams.CashAddressPrefix + ":",
					valid:  true,
				},
				{
					prefix: "abc1",
					valid:  false,
				},
				{
					prefix: "1",
					valid:  false,
				},
				{
					prefix: MainNetParams.CashAddressPrefix,
					valid:  false,
				},
			},
			hdMagics: []hdTest{
				{
					priv: mockNetParams.HDPrivateKeyID[:],
					want: mockNetParams.HDPublicKeyID[:],
					err:  nil,
				},
			},
		},
		{
			name: "more duplicates",
			register: []registerTest{
				{
					name:   "duplicate mainnet",
					params: &MainNetParams,
					err:    ErrDuplicateNet,
				},
				{
					name:   "duplicate regtest",
					params: &RegressionNetParams,
					err:    ErrDuplicateNet,
				},
				{
					name:   "duplicate testnet",
					params: &TestNetParams,
					err:    ErrDuplicateNet,
				},
				{
					name:   "duplicate mocknet",
					params: &mockNetParams,
					err:    ErrDuplicateNet,
				},
			},
			p2pkhMagics: []magicTest{
				{
					magic: MainNetParams.LegacyPubKeyHashAddrID,
					valid: true,
				},
				{
					magic: TestNetParams.LegacyPubKeyHashAddrID,
					valid: true,
				},
				{
					magic: RegressionNetParams.LegacyPubKeyHashAddrID,
					valid: true,
				},
				{
					magic: mockNetParams.LegacyPubKeyHashAddrID,
					valid: true,
				},
				{
					magic: 0xFF,
					valid: false,
				},
			},
			p2shMagics: []magicTest{
				{
					magic: MainNetParams.LegacyScriptHashAddrID,
					valid: true,
				},
				{
					magic: TestNetParams.LegacyScriptHashAddrID,
					valid: true,
				},
				{
					magic: RegressionNetParams.LegacyScriptHashAddrID,
					valid: true,
				},
				{
					magic: mockNetParams.LegacyScriptHashAddrID,
					valid: true,
				},
				{
					magic: 0xFF,
					valid: false,
				},
			},
			cashAddrPrefixes: []prefixTest{
				{
					prefix: MainNetParams.CashAddressPrefix + ":",
					valid:  true,
				},
				{
					prefix: TestNetParams.CashAddressPrefix + ":",
					valid:  true,
				},
				{
					prefix: RegressionNetParams.CashAddressPrefix + ":",
					valid:  true,
				},
				{
					prefix: strings.ToUpper(MainNetParams.CashAddressPrefix + ":"),
					valid:  true,
				},
				{
					prefix: mockNetParams.CashAddressPrefix + ":",
					valid:  true,
				},
				{
					prefix: "abc1",
					valid:  false,
				},
				{
					prefix: "1",
					valid:  false,
				},
				{
					prefix: MainNetParams.CashAddressPrefix,
					valid:  false,
				},
			},
			hdMagics: []hdTest{
				{
					priv: MainNetParams.HDPrivateKeyID[:],
					want: MainNetParams.HDPublicKeyID[:],
					err:  nil,
				},
				{
					priv: TestNetParams.HDPrivateKeyID[:],
					want: TestNetParams.HDPublicKeyID[:],
					err:  nil,
				},
				{
					priv: RegressionNetParams.HDPrivateKeyID[:],
					want: RegressionNetParams.HDPublicKeyID[:],
					err:  nil,
				},
				{
					priv: mockNetParams.HDPrivateKeyID[:],
					want: mockNetParams.HDPublicKeyID[:],
					err:  nil,
				},
				{
					priv: []byte{0xff, 0xff, 0xff, 0xff},
					err:  ErrUnknownHDKeyID,
				},
				{
					priv: []byte{0xff},
					err:  ErrUnknownHDKeyID,
				},
			},
		},
	}

	for _, test := range tests {
		for _, regTest := range test.register {
			err := Register(regTest.params)
			if regTest.err != nil {
				assert.ErrorIs(t, err, regTest.err, "%s:%s: Registered network with unexpected error", test.name, regTest.name)
			} else {
				assert.NoError(t, err, "%s:%s: Unexpected error registering network", test.name, regTest.name)
			}
		}

		for i, magTest := range test.p2pkhMagics {
			valid := IsPubKeyHashAddrID(magTest.magic)
			assert.Equalf(t, magTest.valid, valid, "%s: P2PKH magic %d valid mismatch", test.name, i)
		}

		for i, magTest := range test.p2shMagics {
			valid := IsScriptHashAddrID(magTest.magic)
			assert.Equalf(t, magTest.valid, valid, "%s: P2SH magic %d valid mismatch", test.name, i)
		}

		for i, prxTest := range test.cashAddrPrefixes {
			valid := IsCashAddressPrefix(prxTest.prefix)
			assert.Equalf(t, prxTest.valid, valid, "%s: cash address prefix %s (%d) valid mismatch", test.name, prxTest.prefix, i)
		}

		for i, magTest := range test.hdMagics {
			pubKey, err := HDPrivateKeyToPublicKeyID(magTest.priv)
			assert.Equalf(t, magTest.err, err, "%s: HD magic %d mismatched error", test.name, i)

			if magTest.err == nil {
				assert.Truef(t, bytes.Equal(pubKey, magTest.want), "%s: HD magic %d private and public mismatch: got %v expected %v ", test.name, i, pubKey, magTest.want)
			}
		}
	}
}
