package consensus

import (
	"encoding/hex"
	
	"github.com/bitcoin-sv/teranode/services/legacy/bsvutil"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
)

// Test keys from C++ implementation
var (
	vchKey0 = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	vchKey1 = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0}
	vchKey2 = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0}
)

// KeyData contains test keys in various formats
type KeyData struct {
	Key0   *bec.PrivateKey // Uncompressed key 0
	Key0C  *bec.PrivateKey // Compressed key 0
	Key1   *bec.PrivateKey // Uncompressed key 1
	Key1C  *bec.PrivateKey // Compressed key 1
	Key2   *bec.PrivateKey // Uncompressed key 2
	Key2C  *bec.PrivateKey // Compressed key 2
	
	Pubkey0  []byte // Uncompressed pubkey 0
	Pubkey0C []byte // Compressed pubkey 0
	Pubkey0H []byte // Hybrid pubkey 0
	Pubkey0U []byte // Uncompressed pubkey 0 (alias)
	Pubkey1  []byte // Uncompressed pubkey 1
	Pubkey1C []byte // Compressed pubkey 1
	Pubkey1U []byte // Uncompressed pubkey 1 (alias)
	Pubkey2  []byte // Uncompressed pubkey 2
	Pubkey2C []byte // Compressed pubkey 2
	Pubkey2U []byte // Uncompressed pubkey 2 (alias)
	
	Pubkey0Hash []byte // Hash160 of Pubkey0
	Pubkey1Hash []byte // Hash160 of Pubkey1
	Pubkey2Hash []byte // Hash160 of Pubkey2
}

// NewKeyData creates a new KeyData instance with test keys
func NewKeyData() *KeyData {
	kd := &KeyData{}
	
	// Create private keys
	kd.Key0, _ = bec.PrivateKeyFromBytes(vchKey0)
	kd.Key0C = kd.Key0 // In go-bt, compression is handled differently
	
	kd.Key1, _ = bec.PrivateKeyFromBytes(vchKey1)
	kd.Key1C = kd.Key1
	
	kd.Key2, _ = bec.PrivateKeyFromBytes(vchKey2)
	kd.Key2C = kd.Key2
	
	// Get public keys
	kd.Pubkey0 = kd.Key0.PubKey().Uncompressed()
	kd.Pubkey0C = kd.Key0.PubKey().Compressed()
	
	// Create hybrid pubkey (0x06 or 0x07 prefix based on Y coordinate)
	kd.Pubkey0H = make([]byte, 65)
	copy(kd.Pubkey0H, kd.Pubkey0)
	if kd.Pubkey0[64]&1 == 1 {
		kd.Pubkey0H[0] = 0x07
	} else {
		kd.Pubkey0H[0] = 0x06
	}
	
	kd.Pubkey1 = kd.Key1.PubKey().Uncompressed()
	kd.Pubkey1C = kd.Key1.PubKey().Compressed()
	
	kd.Pubkey2 = kd.Key2.PubKey().Uncompressed()
	kd.Pubkey2C = kd.Key2.PubKey().Compressed()
	
	// Set up aliases
	kd.Pubkey0U = kd.Pubkey0
	kd.Pubkey1U = kd.Pubkey1
	kd.Pubkey2U = kd.Pubkey2
	
	// Calculate hash160s
	kd.Pubkey0Hash = bsvutil.Hash160(kd.Pubkey0)
	kd.Pubkey1Hash = bsvutil.Hash160(kd.Pubkey1)
	kd.Pubkey2Hash = bsvutil.Hash160(kd.Pubkey2)
	
	return kd
}

// GetPubkeyBytes returns pubkey bytes for use in scripts
func (kd *KeyData) GetPubkeyBytes(compressed bool, keyNum int) []byte {
	switch keyNum {
	case 0:
		if compressed {
			return kd.Pubkey0C
		}
		return kd.Pubkey0
	case 1:
		if compressed {
			return kd.Pubkey1C
		}
		return kd.Pubkey1
	case 2:
		if compressed {
			return kd.Pubkey2C
		}
		return kd.Pubkey2
	default:
		panic("invalid key number")
	}
}

// GetPrivateKey returns a private key
func (kd *KeyData) GetPrivateKey(keyNum int) *bec.PrivateKey {
	switch keyNum {
	case 0:
		return kd.Key0
	case 1:
		return kd.Key1
	case 2:
		return kd.Key2
	default:
		panic("invalid key number")
	}
}

// Additional test vectors for specific tests
var (
	// Test pubkey for script tests (from C++)
	TestPubkey1Hex = "0479BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8"
	TestPubkey2Hex = "02C7E8823A38FBD61FB5DD88F8B60EFEC227C853462AA217D9442DE47BC96B3E33"
	
	TestPubkey1, _ = hex.DecodeString(TestPubkey1Hex)
	TestPubkey2, _ = hex.DecodeString(TestPubkey2Hex)
)