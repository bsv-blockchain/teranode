// Package seedcheckpoint signs and verifies the compact UTXO-set commitment
// checkpoint (height, blockHash, setHash) that a new miner trusts when verifying
// a downloaded seed.
package seedcheckpoint

import (
	"bytes"
	"encoding/binary"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
)

// FormatVersion is the signed-checkpoint serialization version.
const FormatVersion uint32 = 1

const (
	pubKeyLen       = 33
	signedHeaderLen = 4 + 4 + 4 + 32 + 32 + pubKeyLen + 2 // formatVersion, commitmentVersion, height, blockHash, setHash, pubkey, sigLen = 111
	maxSigLen       = 80                                  // DER secp256k1 signatures are <= ~72 bytes
)

// Checkpoint is the signed tuple committing to the UTXO set at a block.
type Checkpoint struct {
	// CommitmentVersion identifies the frozen UTXO commitment construction (the
	// element byte layout plus the MuHash mapping) that SetHash was built with.
	// A consumer that does not implement this version must refuse the seed
	// rather than recompute a digest under a different construction. It is part
	// of the signed digest, so it cannot be altered without invalidating the
	// signature.
	CommitmentVersion uint32
	Height            uint32
	BlockHash         chainhash.Hash
	SetHash           [32]byte
}

// digest returns the 32-byte message hash that is signed: the double-SHA256 of
// commitmentVersion(4 LE) | netMagic(4 LE) | height(4 LE) | blockHash(32) |
// setHash(32). This layout is frozen.
//
// netMagic is the local network's wire magic (settings.ChainCfgParams.Net). It
// is supplied by the caller on both the signing and verifying side and is NOT
// carried in the serialized checkpoint, so a checkpoint signed for one network
// fails signature verification on another (domain separation) rather than being
// re-validated under its own claimed network.
func (c Checkpoint) digest(netMagic uint32) []byte {
	msg := make([]byte, 0, 4+4+4+32+32)
	msg = binary.LittleEndian.AppendUint32(msg, c.CommitmentVersion)
	msg = binary.LittleEndian.AppendUint32(msg, netMagic)
	msg = binary.LittleEndian.AppendUint32(msg, c.Height)
	msg = append(msg, c.BlockHash[:]...)
	msg = append(msg, c.SetHash[:]...)

	return chainhash.DoubleHashB(msg)
}

// SignedCheckpoint is a checkpoint plus the signer's compressed public key and
// the DER-encoded secp256k1 signature over the checkpoint digest.
type SignedCheckpoint struct {
	Checkpoint Checkpoint
	PubKey     [pubKeyLen]byte
	Sig        []byte
}

// Sign produces a SignedCheckpoint for c using priv (secp256k1, RFC6979),
// binding the signature to netMagic (the local network's wire magic).
func Sign(priv *bec.PrivateKey, c Checkpoint, netMagic uint32) (*SignedCheckpoint, error) {
	sig, err := priv.Sign(c.digest(netMagic))
	if err != nil {
		return nil, errors.NewProcessingError("error signing checkpoint", err)
	}

	var pub [pubKeyLen]byte
	copy(pub[:], priv.PubKey().Compressed())

	return &SignedCheckpoint{Checkpoint: c, PubKey: pub, Sig: sig.Serialize()}, nil
}

// Verify checks that the signature is valid for the embedded public key and
// checkpoint under netMagic (the local network's wire magic). It does NOT
// establish that the public key is a trusted authority.
func (sc *SignedCheckpoint) Verify(netMagic uint32) error {
	pub, err := bec.ParsePubKey(sc.PubKey[:])
	if err != nil {
		return errors.NewProcessingError("invalid checkpoint public key", err)
	}

	sig, err := bec.ParseDERSignature(sc.Sig)
	if err != nil {
		return errors.NewProcessingError("invalid checkpoint signature encoding", err)
	}

	if !sig.Verify(sc.Checkpoint.digest(netMagic), pub) {
		return errors.NewProcessingError("checkpoint signature does not verify")
	}

	return nil
}

// VerifyWithKey checks the signature (under netMagic) AND that it was made by
// trustedPubKey (a 33-byte compressed key). This is the check a seed consumer
// performs.
func (sc *SignedCheckpoint) VerifyWithKey(trustedPubKey []byte, netMagic uint32) error {
	if !bytes.Equal(sc.PubKey[:], trustedPubKey) {
		return errors.NewProcessingError("checkpoint signed by untrusted key")
	}

	return sc.Verify(netMagic)
}

// Serialize encodes the signed checkpoint as:
//
//	version(4 LE) | commitmentVersion(4 LE) | height(4 LE) | blockHash(32) | setHash(32) | pubkey(33) | sigLen(2 LE) | sig
func (sc *SignedCheckpoint) Serialize() []byte {
	out := make([]byte, 0, signedHeaderLen+len(sc.Sig))
	out = binary.LittleEndian.AppendUint32(out, FormatVersion)
	out = binary.LittleEndian.AppendUint32(out, sc.Checkpoint.CommitmentVersion)
	out = binary.LittleEndian.AppendUint32(out, sc.Checkpoint.Height)
	out = append(out, sc.Checkpoint.BlockHash[:]...)
	out = append(out, sc.Checkpoint.SetHash[:]...)
	out = append(out, sc.PubKey[:]...)
	out = binary.LittleEndian.AppendUint16(out, uint16(len(sc.Sig)))
	out = append(out, sc.Sig...)

	return out
}

// ParseSignedCheckpoint decodes and structurally validates a signed checkpoint.
func ParseSignedCheckpoint(b []byte) (*SignedCheckpoint, error) {
	if len(b) < signedHeaderLen {
		return nil, errors.NewProcessingError("signed checkpoint too short: %d bytes", len(b))
	}

	version := binary.LittleEndian.Uint32(b[0:4])
	if version != FormatVersion {
		return nil, errors.NewProcessingError("unsupported signed checkpoint version %d", version)
	}

	sc := &SignedCheckpoint{}
	sc.Checkpoint.CommitmentVersion = binary.LittleEndian.Uint32(b[4:8])
	sc.Checkpoint.Height = binary.LittleEndian.Uint32(b[8:12])
	copy(sc.Checkpoint.BlockHash[:], b[12:44])
	copy(sc.Checkpoint.SetHash[:], b[44:76])
	copy(sc.PubKey[:], b[76:109])

	sigLen := int(binary.LittleEndian.Uint16(b[109:111]))
	if sigLen == 0 || sigLen > maxSigLen {
		return nil, errors.NewProcessingError("invalid checkpoint signature length %d", sigLen)
	}

	if len(b) != signedHeaderLen+sigLen {
		return nil, errors.NewProcessingError("signed checkpoint length %d, expected %d", len(b), signedHeaderLen+sigLen)
	}

	sc.Sig = make([]byte, sigLen)
	copy(sc.Sig, b[signedHeaderLen:])

	return sc, nil
}
