// Copyright (c) 2013-2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package bsvutil provides Bitcoin SV utility functions and data types for the legacy protocol implementation.
package bsvutil

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/bsv-blockchain/go-chaincfg"
	base58 "github.com/bsv-blockchain/go-sdk/compat/base58" //nolint:depguard
	"github.com/bsv-blockchain/teranode/services/legacy/bsvec"
	"golang.org/x/crypto/ripemd160" //nolint:gosec // this is a known safe use of ripemd160
)

var (
	// ErrChecksumMismatch describes an error where decoding failed due
	// to a bad checksum.
	ErrChecksumMismatch = errors.New("checksum mismatch")

	// ErrUnknownAddressType describes an error where an address can not
	// decoded as a specific address type due to the string encoding
	// beginning with an identifier byte unknown to any standard or
	// registered (via chaincfg.Register) network.
	ErrUnknownAddressType = errors.New("unknown address type")

	// ErrAddressCollision describes an error where an address can not
	// be uniquely determined as either a pay-to-pubkey-hash or
	// pay-to-script-hash address since the leading identifier is used for
	// describing both address kinds, but for different networks.  Rather
	// than assuming or defaulting to one or the other, this error is
	// returned and the caller must decide how to decode the address.
	ErrAddressCollision = errors.New("address collision")

	// ErrInvalidFormat describes an error where decoding failed due to invalid version
	ErrInvalidFormat = errors.New("invalid format: version and/or checksum bytes missing")
)

// Address is an interface type for any type of destination a transaction
// output may spend to.  This includes pay-to-pubkey (P2PK), pay-to-pubkey-hash
// (P2PKH), and pay-to-script-hash (P2SH).  Address is designed to be generic
// enough that other kinds of addresses may be added in the future without
// changing the decoding and encoding API.
type Address interface {
	// String returns the string encoding of the transaction output
	// destination.
	//
	// Please note that String differs subtly from EncodeAddress: String
	// will return the value as a string without any conversion, while
	// EncodeAddress may convert destination types (for example,
	// converting pubkeys to P2PKH addresses) before encoding as a
	// payment address string.
	String() string

	// EncodeAddress returns the string encoding of the payment address
	// associated with the Address value.  See the comment on String
	// for how this method differs from String.
	EncodeAddress() string

	// ScriptAddress returns the raw bytes of the address to be used
	// when inserting the address into a txout's script.
	ScriptAddress() []byte

	// IsForNet returns whether or not the address is associated with the
	// passed bitcoin network.
	IsForNet(*chaincfg.Params) bool
}

// DecodeAddress decodes the string encoding of an address and returns
// the Address if addr is a valid encoding for a known address type.
//
// The bitcoin network the address is associated with is extracted if possible.
// When the address does not encode the network, such as in the case of a raw
// public key, the address will be associated with the passed defaultNet.
func DecodeAddress(addr string, defaultNet *chaincfg.Params) (Address, error) {
	pre := defaultNet.CashAddressPrefix
	if len(addr) < len(pre)+2 {
		return nil, errors.New("invalid length address")
	}

	// Add prefix if it does not exist
	addrWithPrefix := addr
	if addr[:len(pre)+1] != pre+":" {
		addrWithPrefix = pre + ":" + addr
	}

	// Switch on decoded length to determine the type.
	decoded, _, typ, err := checkDecodeCashAddress(addrWithPrefix)
	if err == nil {
		switch len(decoded) {
		case ripemd160.Size: // P2PKH or P2SH
			switch typ {
			case AddrTypePayToPubKeyHash:
				return newAddressPubKeyHash(decoded, defaultNet)
			case AddrTypePayToScriptHash:
				return newAddressScriptHashFromHash(decoded, defaultNet)
			default:
				return nil, ErrUnknownAddressType
			}
		default:
			return nil, errors.New("decoded address is of unknown size")
		}
	} else if errors.Is(err, ErrChecksumMismatch) {
		return nil, ErrChecksumMismatch
	}

	// Serialized public keys are either 65 bytes (130 hex chars) if
	// uncompressed/hybrid or 33 bytes (66 hex chars) if compressed.
	if len(addr) == 130 || len(addr) == 66 {
		serializedPubKey, err := hex.DecodeString(addr)
		if err != nil {
			return nil, err
		}

		return NewAddressPubKey(serializedPubKey, defaultNet)
	}

	// Switch on decoded length to determine the type.
	decoded, netID, err := checkDecode(addr)
	if err != nil {
		return nil, errors.New("decoded address is of unknown format")
	}

	switch len(decoded) {
	case ripemd160.Size: // P2PKH or P2SH
		isP2PKH := chaincfg.IsPubKeyHashAddrID(defaultNet.Net, netID)
		isP2SH := chaincfg.IsScriptHashAddrID(defaultNet.Net, netID)

		switch hash160 := decoded; {
		case isP2PKH && isP2SH:
			return nil, ErrAddressCollision
		case isP2PKH:
			return newLegacyAddressPubKeyHash(hash160, netID)
		case isP2SH:
			return newLegacyAddressScriptHashFromHash(hash160, netID)
		default:
			return nil, ErrUnknownAddressType
		}

	default:
		return nil, errors.New("decoded address is of unknown size")
	}
}

// checksum computes a 4-byte checksum from the input using double SHA256
func checksum(input []byte) [4]byte {
	var cksum [4]byte

	h := sha256.Sum256(input)
	h2 := sha256.Sum256(h[:])
	copy(cksum[:], h2[:4])

	return cksum
}

// checkEncode encodes input with version byte and checksum, then base58 encodes
func checkEncode(input []byte, version byte) string {
	b := make([]byte, 0, 1+len(input)+4)
	b = append(b, version)
	b = append(b, input...)
	cksum := checksum(b)
	b = append(b, cksum[:]...)

	return base58.Encode(b)
}

// checkDecode decodes a base58check string and validates checksum
func checkDecode(input string) (result []byte, version byte, err error) {
	decoded, err := base58.Decode(input)
	if err != nil {
		return nil, 0, err
	}

	if len(decoded) < 5 {
		return nil, 0, ErrInvalidFormat
	}

	version = decoded[0]

	var cksum [4]byte

	copy(cksum[:], decoded[len(decoded)-4:])

	if checksum(decoded[:len(decoded)-4]) != cksum {
		return nil, 0, ErrChecksumMismatch
	}

	payload := decoded[1 : len(decoded)-4]
	result = append(result, payload...)

	return
}

// encodeLegacyAddress returns a human-readable payment address given a ripemd160 hash
// and netID which encodes the bitcoin network and address type.  It is used
// in both legacy pay-to-pubkey-hash (P2PKH) and pay-to-script-hash (P2SH) address
// encoding.
func encodeLegacyAddress(hash160 []byte, netID byte) string {
	// Format is 1 byte for a network and address class (i.e. P2PKH vs
	// P2SH), 20 bytes for a RIPEMD160 hash, and 4 bytes of checksum.
	return checkEncode(hash160[:ripemd160.Size], netID)
}

// encodeCashAddress returns a human-readable payment address given a ripemd160 hash
// and prefix which encodes the bitcoin cash network and address type.  It is used
// in both pay-to-pubkey-hash (P2PKH) and pay-to-script-hash (P2SH) address
// encoding.
func encodeCashAddress(hash160 []byte, prefix string, t AddressType) string {
	return checkEncodeCashAddress(hash160[:ripemd160.Size], prefix, t)
}

// AddressPubKeyHash is an Address for a pay-to-pubkey-hash (P2PKH)
// transaction.
type AddressPubKeyHash struct {
	hash   [ripemd160.Size]byte
	prefix string
}

// NewAddressPubKeyHash returns a new AddressPubKeyHash.  pkHash mustbe 20
// bytes.
func NewAddressPubKeyHash(pkHash []byte, net *chaincfg.Params) (*AddressPubKeyHash, error) {
	return newAddressPubKeyHash(pkHash, net)
}

// newAddressPubKeyHash is the internal API to create a pubkey hash address
// with a known leading identifier byte for a network, rather than looking
// it up through its parameters.  This is useful when creating a new address
// structure from a string encoding where the identifier byte is already
// known.
func newAddressPubKeyHash(pkHash []byte, net *chaincfg.Params) (*AddressPubKeyHash, error) {
	// Check for a valid pubkey hash length.
	if len(pkHash) != ripemd160.Size {
		return nil, errors.New("pkHash must be 20 bytes")
	}

	addr := &AddressPubKeyHash{prefix: net.CashAddressPrefix}
	copy(addr.hash[:], pkHash)

	return addr, nil
}

// EncodeAddress returns the string encoding of a pay-to-pubkey-hash
// address.  Part of the Address interface.
func (a *AddressPubKeyHash) EncodeAddress() string {
	return encodeCashAddress(a.hash[:], a.prefix, AddrTypePayToPubKeyHash)
}

// ScriptAddress returns the bytes to be included in a txout script to pay
// to a pubkey hash.  Part of the Address interface.
func (a *AddressPubKeyHash) ScriptAddress() []byte {
	return a.hash[:]
}

// IsForNet returns whether or not the pay-to-pubkey-hash address is associated
// with the passed bitcoin cash network.
func (a *AddressPubKeyHash) IsForNet(net *chaincfg.Params) bool {
	return a.prefix == net.CashAddressPrefix
}

// String returns a human-readable string for the pay-to-pubkey-hash address.
// This is equivalent to calling EncodeAddress, but is provided so the type can
// be used as a fmt.Stringer.
func (a *AddressPubKeyHash) String() string {
	return a.EncodeAddress()
}

// Hash160 returns the underlying array of the pubkey hash.  This can be useful
// when an array is more appropriate than a slice (for example, when used as map
// keys).
func (a *AddressPubKeyHash) Hash160() *[ripemd160.Size]byte {
	return &a.hash
}

// AddressScriptHash is an Address for a pay-to-script-hash (P2SH)
// transaction.
type AddressScriptHash struct {
	hash   [ripemd160.Size]byte
	prefix string
}

// NewAddressScriptHash returns a new AddressScriptHash.
func NewAddressScriptHash(serializedScript []byte, net *chaincfg.Params) (*AddressScriptHash, error) {
	scriptHash := Hash160(serializedScript)
	return newAddressScriptHashFromHash(scriptHash, net)
}

// NewAddressScriptHashFromHash returns a new AddressScriptHash.  scriptHash
// must be 20 bytes.
func NewAddressScriptHashFromHash(scriptHash []byte, net *chaincfg.Params) (*AddressScriptHash, error) {
	return newAddressScriptHashFromHash(scriptHash, net)
}

// newAddressScriptHashFromHash is the internal API to create a script hash
// address with a known leading identifier byte for a network, rather than
// looking it up through its parameters.  This is useful when creating a new
// address structure from a string encoding where the identifier byte is already
// known.
func newAddressScriptHashFromHash(scriptHash []byte, net *chaincfg.Params) (*AddressScriptHash, error) {
	// Check for a valid script hash length.
	if len(scriptHash) != ripemd160.Size {
		return nil, errors.New("scriptHash must be 20 bytes")
	}

	addr := &AddressScriptHash{prefix: net.CashAddressPrefix}
	copy(addr.hash[:], scriptHash)

	return addr, nil
}

// EncodeAddress returns the string encoding of a pay-to-script-hash
// address.  Part of the Address interface.
func (a *AddressScriptHash) EncodeAddress() string {
	return encodeCashAddress(a.hash[:], a.prefix, AddrTypePayToScriptHash)
}

// ScriptAddress returns the bytes to be included in a txout script to pay
// to a script hash.  Part of the Address interface.
func (a *AddressScriptHash) ScriptAddress() []byte {
	return a.hash[:]
}

// IsForNet returns whether or not the pay-to-script-hash address is associated
// with the passed bitcoin cash network.
func (a *AddressScriptHash) IsForNet(net *chaincfg.Params) bool {
	return net.CashAddressPrefix == a.prefix
}

// String returns a human-readable string for the pay-to-script-hash address.
// This is equivalent to calling EncodeAddress, but is provided so the type can
// be used as a fmt.Stringer.
func (a *AddressScriptHash) String() string {
	return a.EncodeAddress()
}

// Hash160 returns the underlying array of the script hash.  This can be useful
// when an array is more appropriate than a slice (for example, when used as map
// keys).
func (a *AddressScriptHash) Hash160() *[ripemd160.Size]byte {
	return &a.hash
}

// LegacyAddressPubKeyHash is an Address for a pay-to-pubkey-hash (P2PKH)
// transaction in the legacy format.
type LegacyAddressPubKeyHash struct {
	hash  [ripemd160.Size]byte
	netID byte
}

// NewLegacyAddressPubKeyHash returns a new AddressPubKeyHash in the legacy
// format. pkHash mustbe 20 bytes.
func NewLegacyAddressPubKeyHash(pkHash []byte, net *chaincfg.Params) (*LegacyAddressPubKeyHash, error) {
	return newLegacyAddressPubKeyHash(pkHash, net.LegacyPubKeyHashAddrID)
}

// newLegacyAddressPubKeyHash is the internal API to create a pubkey hash address
// with a known leading identifier byte for a network, rather than looking
// it up through its parameters.  This is useful when creating a new address
// structure from a string encoding where the identifier byte is already
// known.
func newLegacyAddressPubKeyHash(pkHash []byte, netID byte) (*LegacyAddressPubKeyHash, error) {
	// Check for a valid pubkey hash length.
	if len(pkHash) != ripemd160.Size {
		return nil, errors.New("pkHash must be 20 bytes")
	}

	addr := &LegacyAddressPubKeyHash{netID: netID}
	copy(addr.hash[:], pkHash)

	return addr, nil
}

// EncodeAddress returns the string encoding of a pay-to-pubkey-hash
// address.  Part of the Address interface.
func (a *LegacyAddressPubKeyHash) EncodeAddress() string {
	return encodeLegacyAddress(a.hash[:], a.netID)
}

// ScriptAddress returns the bytes to be included in a txout script to pay
// to a pubkey hash.  Part of the Address interface.
func (a *LegacyAddressPubKeyHash) ScriptAddress() []byte {
	return a.hash[:]
}

// IsForNet returns whether or not the pay-to-pubkey-hash address is associated
// with the passed bitcoin network.
func (a *LegacyAddressPubKeyHash) IsForNet(net *chaincfg.Params) bool {
	return a.netID == net.LegacyPubKeyHashAddrID
}

// String returns a human-readable string for the pay-to-pubkey-hash address.
// This is equivalent to calling EncodeAddress, but is provided so the type can
// be used as a fmt.Stringer.
func (a *LegacyAddressPubKeyHash) String() string {
	return a.EncodeAddress()
}

// Hash160 returns the underlying array of the pubkey hash.  This can be useful
// when an array is more appropriate than a slice (for example, when used as map
// keys).
func (a *LegacyAddressPubKeyHash) Hash160() *[ripemd160.Size]byte {
	return &a.hash
}

// LegacyAddressScriptHash is an Address for a pay-to-script-hash (P2SH)
// transaction in legacy format.
type LegacyAddressScriptHash struct {
	hash  [ripemd160.Size]byte
	netID byte
}

// NewLegacyAddressScriptHash returns a new LegacyAddressScriptHash.
func NewLegacyAddressScriptHash(serializedScript []byte, net *chaincfg.Params) (*LegacyAddressScriptHash, error) {
	scriptHash := Hash160(serializedScript)

	return newLegacyAddressScriptHashFromHash(scriptHash, net.LegacyScriptHashAddrID)
}

// NewLegacyAddressScriptHashFromHash returns a new AddressScriptHash.  scriptHash
// must be 20 bytes.
func NewLegacyAddressScriptHashFromHash(scriptHash []byte, net *chaincfg.Params) (*LegacyAddressScriptHash, error) {
	return newLegacyAddressScriptHashFromHash(scriptHash, net.LegacyScriptHashAddrID)
}

// newLegacyAddressScriptHashFromHash is the internal API to create a script hash
// address with a known leading identifier byte for a network, rather than
// looking it up through its parameters.  This is useful when creating a new
// address structure from a string encoding where the identifier byte is already
// known.
func newLegacyAddressScriptHashFromHash(scriptHash []byte, netID byte) (*LegacyAddressScriptHash, error) {
	// Check for a valid script hash length.
	if len(scriptHash) != ripemd160.Size {
		return nil, errors.New("scriptHash must be 20 bytes")
	}

	addr := &LegacyAddressScriptHash{netID: netID}
	copy(addr.hash[:], scriptHash)

	return addr, nil
}

// EncodeAddress returns the string encoding of a pay-to-script-hash
// address.  Part of the Address interface.
func (a *LegacyAddressScriptHash) EncodeAddress() string {
	return encodeLegacyAddress(a.hash[:], a.netID)
}

// ScriptAddress returns the bytes to be included in a txout script to pay
// to a script hash.  Part of the Address interface.
func (a *LegacyAddressScriptHash) ScriptAddress() []byte {
	return a.hash[:]
}

// IsForNet returns whether or not the pay-to-script-hash address is associated
// with the passed bitcoin network.
func (a *LegacyAddressScriptHash) IsForNet(net *chaincfg.Params) bool {
	return a.netID == net.LegacyScriptHashAddrID
}

// String returns a human-readable string for the pay-to-script-hash address.
// This is equivalent to calling EncodeAddress, but is provided so the type can
// be used as a fmt.Stringer.
func (a *LegacyAddressScriptHash) String() string {
	return a.EncodeAddress()
}

// Hash160 returns the underlying array of the script hash.  This can be useful
// when an array is more appropriate than a slice (for example, when used as map
// keys).
func (a *LegacyAddressScriptHash) Hash160() *[ripemd160.Size]byte {
	return &a.hash
}

// PubKeyFormat describes what format to use for a pay-to-pubkey address.
type PubKeyFormat int

const (
	// PKFUncompressed indicates the pay-to-pubkey address format is an
	// uncompressed public key.
	PKFUncompressed PubKeyFormat = iota

	// PKFCompressed indicates the pay-to-pubkey address format is a
	// compressed public key.
	PKFCompressed

	// PKFHybrid indicates the pay-to-pubkey address format is a hybrid
	// public key.
	PKFHybrid
)

// AddressPubKey is an Address for a pay-to-pubkey transaction.
type AddressPubKey struct {
	pubKeyFormat PubKeyFormat
	pubKey       *bsvec.PublicKey
	pubKeyHashID byte
}

// NewAddressPubKey returns a new AddressPubKey which represents a pay-to-pubkey
// address.  The serializedPubKey parameter must be a valid pubkey and can be
// uncompressed, compressed, or hybrid.
func NewAddressPubKey(serializedPubKey []byte, net *chaincfg.Params) (*AddressPubKey, error) {
	pubKey, err := bsvec.ParsePubKey(serializedPubKey, bsvec.S256())
	if err != nil {
		return nil, err
	}

	// Set the format of the pubkey.  This probably should be returned
	// from bsvec, but do it here to avoid API churn.  We already know the
	// pubkey is valid since it parsed above, so it's safe to simply examine
	// the leading byte to get the format.
	pkFormat := PKFUncompressed

	switch serializedPubKey[0] {
	case 0x02, 0x03:
		pkFormat = PKFCompressed
	case 0x06, 0x07:
		pkFormat = PKFHybrid
	}

	return &AddressPubKey{
		pubKeyFormat: pkFormat,
		pubKey:       pubKey,
		pubKeyHashID: net.LegacyPubKeyHashAddrID,
	}, nil
}

// serialize returns the serialization of the public key according to the
// format associated with the address.
func (a *AddressPubKey) serialize() []byte {
	switch a.pubKeyFormat {
	default:
		fallthrough
	case PKFUncompressed:
		return a.pubKey.SerializeUncompressed()

	case PKFCompressed:
		return a.pubKey.SerializeCompressed()

	case PKFHybrid:
		return a.pubKey.SerializeHybrid()
	}
}

// EncodeAddress returns the string encoding of the public key as a
// pay-to-pubkey-hash.  Note that the public key format (uncompressed,
// compressed, etc) will change the resulting address.  This is expected since
// pay-to-pubkey-hash is a hash of the serialized public key which obviously
// differs with the format.  At the time of this writing, most Bitcoin addresses
// are pay-to-pubkey-hash constructed from the uncompressed public key.
//
// Part of the Address interface.
func (a *AddressPubKey) EncodeAddress() string {
	return encodeLegacyAddress(Hash160(a.serialize()), a.pubKeyHashID)
}

// ScriptAddress returns the bytes to be included in a txout script to pay
// to a public key.  Setting the public key format will affect the output of
// this function accordingly.  Part of the Address interface.
func (a *AddressPubKey) ScriptAddress() []byte {
	return a.serialize()
}

// IsForNet returns whether or not the pay-to-pubkey address is associated
// with the passed bitcoin network.
func (a *AddressPubKey) IsForNet(net *chaincfg.Params) bool {
	return a.pubKeyHashID == net.LegacyPubKeyHashAddrID
}

// String returns the hex-encoded human-readable string for the pay-to-pubkey
// address.  This is not the same as calling EncodeAddress.
func (a *AddressPubKey) String() string {
	return hex.EncodeToString(a.serialize())
}

// Format returns the format (uncompressed, compressed, etc) of the
// pay-to-pubkey address.
func (a *AddressPubKey) Format() PubKeyFormat {
	return a.pubKeyFormat
}

// SetFormat sets the format (uncompressed, compressed, etc) of the
// pay-to-pubkey address.
func (a *AddressPubKey) SetFormat(pkFormat PubKeyFormat) {
	a.pubKeyFormat = pkFormat
}

// AddressPubKeyHash returns the pay-to-pubkey address converted to a
// pay-to-pubkey-hash address.  Note that the public key format (uncompressed,
// compressed, etc) will change the resulting address.  This is expected since
// pay-to-pubkey-hash is a hash of the serialized public key which obviously
// differs with the format.  At the time of this writing, most Bitcoin addresses
// are pay-to-pubkey-hash constructed from the uncompressed public key.
func (a *AddressPubKey) AddressPubKeyHash() *AddressPubKeyHash {
	params := paramsFromNetID(a.pubKeyHashID)
	addr := &AddressPubKeyHash{prefix: params.CashAddressPrefix}
	copy(addr.hash[:], Hash160(a.serialize()))

	return addr
}

// PubKey returns the underlying public key for the address.
func (a *AddressPubKey) PubKey() *bsvec.PublicKey {
	return a.pubKey
}

func paramsFromNetID(netID byte) *chaincfg.Params {
	switch netID {
	case chaincfg.TestNetParams.LegacyPubKeyHashAddrID:
		return &chaincfg.TestNetParams
	case chaincfg.RegressionNetParams.LegacyPubKeyHashAddrID:
		return &chaincfg.RegressionNetParams
	case chaincfg.TestNetParams.LegacyScriptHashAddrID:
		return &chaincfg.TestNetParams
	case chaincfg.RegressionNetParams.LegacyScriptHashAddrID:
		return &chaincfg.RegressionNetParams
	default:
		return &chaincfg.MainNetParams
	}
}

func checkEncodeCashAddress(input []byte, prefix string, t AddressType) string {
	k, err := packAddressData(t, input)
	if err != nil {
		return ""
	}

	return encode(prefix, k)
}

// checkDecode decodes a string that was encoded with checkEncode and verifies the checksum.
func checkDecodeCashAddress(input string) (result []byte, prefix string, t AddressType, err error) {
	prefix, data, err := DecodeCashAddress(input)
	if err != nil {
		return data, prefix, AddrTypePayToPubKeyHash, err
	}

	data, err = convertBits(data, 5, 8, false)
	if err != nil {
		return data, prefix, AddrTypePayToPubKeyHash, err
	}

	if len(data) != 21 {
		return data, prefix, AddrTypePayToPubKeyHash, errors.New("incorrect data length")
	}

	switch data[0] {
	case 0x00:
		t = AddrTypePayToPubKeyHash
	case 0x08:
		t = AddrTypePayToScriptHash
	}

	return data[1:21], prefix, t, nil
}

// AddressType represents the type of address and is used
// when encoding the cashaddr.
type AddressType int

const (
	// AddrTypePayToPubKeyHash is the numeric identifier for
	// a cashaddr PayToPubkeyHash address
	AddrTypePayToPubKeyHash AddressType = 0

	// AddrTypePayToScriptHash is the numeric identifier for
	// a cashaddr PayToPubkeyHash address
	AddrTypePayToScriptHash AddressType = 1
)

// Charset is the base32 character set for the cashaddr.
const Charset string = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"

// CharsetRev is the cashaddr character set for decoding.
var CharsetRev = [128]int8{
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 15, -1, 10, 17, 21, 20, 26, 30, 7,
	5, -1, -1, -1, -1, -1, -1, -1, 29, -1, 24, 13, 25, 9, 8, 23, -1, 18, 22,
	31, 27, 19, -1, 1, 0, 3, 16, 11, 28, 12, 14, 6, 4, 2, -1, -1, -1, -1,
	-1, -1, 29, -1, 24, 13, 25, 9, 8, 23, -1, 18, 22, 31, 27, 19, -1, 1, 0,
	3, 16, 11, 28, 12, 14, 6, 4, 2, -1, -1, -1, -1, -1,
}

// This function will compute what 8 5-bit values to XOR into the last 8 input
// values, in order to make the checksum 0. These 8 values are packed together
// in a single 40-bit integer. The higher bits correspond to earlier values.
func polyMod(v []byte) uint64 {
	/**
	 * The input is interpreted as a list of coefficients of a polynomial over F
	 * = GF(32), with an implicit 1 in front. If the input is [v0,v1,v2,v3,v4],
	 * that polynomial is v(x) = 1*x^5 + v0*x^4 + v1*x^3 + v2*x^2 + v3*x + v4.
	 * The implicit 1 guarantees that [v0,v1,v2,...] has a distinct checksum
	 * from [0,v0,v1,v2,...].
	 *
	 * The output is a 40-bit integer whose 5-bit groups are the coefficients of
	 * the remainder of v(x) mod g(x), where g(x) is the cashaddr generator, x^8
	 * + {19}*x^7 + {3}*x^6 + {25}*x^5 + {11}*x^4 + {25}*x^3 + {3}*x^2 + {19}*x
	 * + {1}. g(x) is chosen in such a way that the resulting code is a BSV
	 * code, guaranteeing detection of up to 4 errors within a window of 1025
	 * characters. Among the various possible BSV codes, one was selected to in
	 * fact guarantee detection of up to 5 errors within a window of 160
	 * characters and 6 erros within a window of 126 characters. In addition,
	 * the code guarantee the detection of a burst of up to 8 errors.
	 *
	 * Note that the coefficients are elements of GF(32), here represented as
	 * decimal numbers between {}. In this finite field, addition is just XOR of
	 * the corresponding numbers. For example, {27} + {13} = {27 ^ 13} = {22}.
	 * Multiplication is more complicated, and requires treating the bits of
	 * values themselves as coefficients of a polynomial over a smaller field,
	 * GF(2), and multiplying those polynomials mod a^5 + a^3 + 1. For example,
	 * {5} * {26} = (a^2 + 1) * (a^4 + a^3 + a) = (a^4 + a^3 + a) * a^2 + (a^4 +
	 * a^3 + a) = a^6 + a^5 + a^4 + a = a^3 + 1 (mod a^5 + a^3 + 1) = {9}.
	 *
	 * During the course of the loop below, `c` contains the bitpacked
	 * coefficients of the polynomial constructed from just the values of v that
	 * were processed so far, mod g(x). In the above example, `c` initially
	 * corresponds to 1 mod (x), and after processing 2 inputs of v, it
	 * corresponds to x^2 + v0*x + v1 mod g(x). As 1 mod g(x) = 1, that is the
	 * starting value for `c`.
	 */
	c := uint64(1)
	for _, d := range v {
		/**
		 * We want to update `c` to correspond to a polynomial with one extra
		 * term. If the initial value of `c` consists of the coefficients of
		 * c(x) = f(x) mod g(x), we modify it to correspond to
		 * c'(x) = (f(x) * x + d) mod g(x), where d is the next input to
		 * process.
		 *
		 * Simplifying:
		 * c'(x) = (f(x) * x + d) mod g(x)
		 *         ((f(x) mod g(x)) * x + d) mod g(x)
		 *         (c(x) * x + d) mod g(x)
		 * If c(x) = c0*x^5 + c1*x^4 + c2*x^3 + c3*x^2 + c4*x + c5, we want to
		 * compute
		 * c'(x) = (c0*x^5 + c1*x^4 + c2*x^3 + c3*x^2 + c4*x + c5) * x + d
		 *                                                             mod g(x)
		 *       = c0*x^6 + c1*x^5 + c2*x^4 + c3*x^3 + c4*x^2 + c5*x + d
		 *                                                             mod g(x)
		 *       = c0*(x^6 mod g(x)) + c1*x^5 + c2*x^4 + c3*x^3 + c4*x^2 +
		 *                                                             c5*x + d
		 * If we call (x^6 mod g(x)) = k(x), this can be written as
		 * c'(x) = (c1*x^5 + c2*x^4 + c3*x^3 + c4*x^2 + c5*x + d) + c0*k(x)
		 */
		// First, determine the value of c0:
		c0 := byte(c >> 35)

		// Then compute c1*x^5 + c2*x^4 + c3*x^3 + c4*x^2 + c5*x + d:
		c = ((c & 0x07ffffffff) << 5) ^ uint64(d)

		// Finally, for each set bit n in c0, conditionally add {2^n}k(x):
		if c0&0x01 > 0 {
			// k(x) = {19}*x^7 + {3}*x^6 + {25}*x^5 + {11}*x^4 + {25}*x^3 +
			//        {3}*x^2 + {19}*x + {1}
			c ^= 0x98f2bc8e61
		}

		if c0&0x02 > 0 {
			// {2}k(x) = {15}*x^7 + {6}*x^6 + {27}*x^5 + {22}*x^4 + {27}*x^3 +
			//           {6}*x^2 + {15}*x + {2}
			c ^= 0x79b76d99e2
		}

		if c0&0x04 > 0 {
			// {4}k(x) = {30}*x^7 + {12}*x^6 + {31}*x^5 + {5}*x^4 + {31}*x^3 +
			//           {12}*x^2 + {30}*x + {4}
			c ^= 0xf33e5fb3c4
		}

		if c0&0x08 > 0 {
			// {8}k(x) = {21}*x^7 + {24}*x^6 + {23}*x^5 + {10}*x^4 + {23}*x^3 +
			//           {24}*x^2 + {21}*x + {8}
			c ^= 0xae2eabe2a8
		}

		if c0&0x10 > 0 {
			// {16}k(x) = {3}*x^7 + {25}*x^6 + {7}*x^5 + {20}*x^4 + {7}*x^3 +
			//            {25}*x^2 + {3}*x + {16}
			c ^= 0x1e4f43e470
		}
	}

	/**
	 * PolyMod computes what value to xor into the final values to make the
	 * checksum 0. However, if we required that the checksum was 0, it would be
	 * the case that appending a 0 to a valid list of values would result in a
	 * new valid list. For that reason, cashaddr requires the resulting checksum
	 * to be 1 instead.
	 */
	return c ^ 1
}

func cat(x, y []byte) []byte {
	return append(x, y...)
}

func lowerCase(c byte) byte {
	// ASCII black magic.
	return c | 0x20
}

func expandPrefix(prefix string) []byte {
	ret := make([]byte, len(prefix)+1)
	for i := 0; i < len(prefix); i++ {
		ret[i] = prefix[i] & 0x1f
	}

	ret[len(prefix)] = 0

	return ret
}

func verifyChecksum(prefix string, payload []byte) bool {
	return polyMod(cat(expandPrefix(prefix), payload)) == 0
}

func createChecksum(prefix string, payload []byte) []byte {
	enc := cat(expandPrefix(prefix), payload)
	// Append 8 zeroes.
	enc = cat(enc, []byte{0, 0, 0, 0, 0, 0, 0, 0})
	// Determine what to XOR into those 8 zeroes.
	mod := polyMod(enc)
	ret := make([]byte, 8)

	for i := 0; i < 8; i++ {
		// Convert the 5-bit groups in mod to checksum values.
		ret[i] = byte((mod >> uint(5*(7-i))) & 0x1f)
	}

	return ret
}

func encode(prefix string, payload []byte) string {
	checksum := createChecksum(prefix, payload)
	combined := cat(payload, checksum)
	ret := ""

	for _, c := range combined {
		ret += string(Charset[c])
	}

	return ret
}

// DecodeCashAddress decodes a cashaddr string and returns the
// prefix and data element.
func DecodeCashAddress(str string) (string, []byte, error) {
	// Go over the string and do some sanity checks.
	lower, upper := false, false
	prefixSize := 0

	for i := 0; i < len(str); i++ {
		c := str[i]
		if c >= 'a' && c <= 'z' {
			lower = true
			continue
		}

		if c >= 'A' && c <= 'Z' {
			upper = true
			continue
		}

		if c >= '0' && c <= '9' {
			// We cannot have numbers in the prefix.
			if prefixSize == 0 {
				return "", nil, errors.New("addresses cannot have numbers in the prefix")
			}

			continue
		}

		if c == ':' {
			// The separator must not be the first character, and there must not
			// be 2 separators.
			if i == 0 || prefixSize != 0 {
				return "", nil, errors.New("the separator must not be the first character")
			}

			prefixSize = i

			continue
		}

		// We have an unexpected character.
		return "", nil, errors.New("unexpected character")
	}

	// We must have a prefix and a data part and we can't have both uppercase
	// and lowercase.
	if prefixSize == 0 {
		return "", nil, errors.New("address must have a prefix")
	}

	if upper && lower {
		return "", nil, errors.New("addresses cannot use both upper and lower case characters")
	}

	// Get the prefix.
	var prefix string
	for i := 0; i < prefixSize; i++ {
		prefix += string(lowerCase(str[i]))
	}

	// Decode values.
	valuesSize := len(str) - 1 - prefixSize
	values := make([]byte, valuesSize)

	for i := 0; i < valuesSize; i++ {
		c := str[i+prefixSize+1]
		// We have an invalid char in there.
		if c > 127 || CharsetRev[c] == -1 {
			return "", nil, errors.New("invalid character")
		}

		values[i] = byte(CharsetRev[c])
	}

	// Verify the checksum.
	if !verifyChecksum(prefix, values) {
		return "", nil, ErrChecksumMismatch
	}

	return prefix, values[:len(values)-8], nil
}

// Base32 conversion contains some licensed code
// https://github.com/sipa/bech32/blob/master/ref/go/src/bech32/bech32.go
// Copyright (c) 2017 Takatoshi Nakagawa
// MIT License
func convertBits(data []byte, fromBits uint, tobits uint, pad bool) ([]byte, error) {
	// General power-of-2 base conversion.
	var uintArr []uint //nolint:prealloc // Consider pre-allocatin
	for _, i := range data {
		uintArr = append(uintArr, uint(i))
	}

	acc := uint(0)
	bits := uint(0)

	var ret []uint

	maxv := uint((1 << tobits) - 1)
	maxAcc := uint((1 << (fromBits + tobits - 1)) - 1)

	for _, value := range uintArr {
		acc = ((acc << fromBits) | value) & maxAcc

		bits += fromBits
		for bits >= tobits {
			bits -= tobits
			ret = append(ret, (acc>>bits)&maxv)
		}
	}

	if pad {
		if bits > 0 {
			ret = append(ret, (acc<<(tobits-bits))&maxv)
		}
	} else if bits >= fromBits || ((acc<<(tobits-bits))&maxv) != 0 {
		return []byte{}, errors.New("encoding padding error")
	}

	var dataArr []byte //nolint:prealloc // Consider pre-allocatin
	for _, i := range ret {
		dataArr = append(dataArr, byte(i))
	}

	return dataArr, nil
}

func packAddressData(addrType AddressType, addrHash []byte) ([]byte, error) {
	// Pack addr data with version byte.
	if addrType != AddrTypePayToPubKeyHash && addrType != AddrTypePayToScriptHash {
		return nil, errors.New("invalid AddressType")
	}

	versionByte := uint(addrType) << 3
	encodedSize := (uint(len(addrHash)) - 20) / 4

	if (len(addrHash)-20)%4 != 0 {
		return nil, errors.New("invalid address hash size")
	}

	if encodedSize > 8 {
		return nil, errors.New("encoded size out of valid range")
	}

	versionByte |= encodedSize

	var addrHashUint []byte
	addrHashUint = append(addrHashUint, addrHash...)
	data := append([]byte{byte(versionByte)}, addrHashUint...)
	packedData, err := convertBits(data, 8, 5, true)

	if err != nil {
		return []byte{}, err
	}

	return packedData, nil
}
