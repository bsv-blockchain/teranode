//go:build native

package main

/*
	#cgo CFLAGS: -I./include
	#cgo LDFLAGS: -lsecp256k1 -lgmp
	#include <stdlib.h>
	#include <secp256k1.h>
*/
import "C"
import (
	"crypto/ecdsa"
	"log"
	"unsafe"

	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2/bscript/interpreter"
	"github.com/ordishs/gocore"
)

func init() {
	if gocore.Config().GetBool("use_cgo_verifier", false) {
		log.Println("Using CGO verifier - VerifySignature")
		interpreter.InjectExternalVerifySignatureFn(VerifySignature)
	}
}

func cBuf(goSlice []byte) *C.uchar {
	return (*C.uchar)(unsafe.Pointer(&goSlice[0]))
}

func VerifySignature(message []byte, signature []byte, publicKey []byte) bool {
	// Create a secp256k1 context
	ctx := C.secp256k1_context_create(C.SECP256K1_CONTEXT_SIGN | C.SECP256K1_CONTEXT_VERIFY)
	defer C.free(unsafe.Pointer(ctx))

	// Allocate memory for the message, signature, and public key
	cMessage := C.CBytes(message)
	defer C.free(cMessage)
	cSignature := C.CBytes(signature)
	defer C.free(cSignature)
	cPublicKey := C.CBytes(publicKey)
	defer C.free(cPublicKey)

	// Create a secp256k1 signature object
	var cSig C.secp256k1_ecdsa_signature
	if C.secp256k1_ecdsa_signature_parse_der(ctx, &cSig, (*C.uchar)(cSignature), C.size_t(len(signature))) != 1 {
		return false
	}

	// Create a secp256k1 public key object
	var cPubKey C.secp256k1_pubkey
	if C.secp256k1_ec_pubkey_parse(ctx, &cPubKey, (*C.uchar)(cPublicKey), C.size_t(len(publicKey))) != 1 {
		return false
	}

	// Verify the signature
	if C.secp256k1_ecdsa_verify(ctx, &cSig, (*C.uchar)(cMessage), &cPubKey) != 1 {
		return false
	}

	return true
}

// VerifySignature verifies that the given signature for the given message was signed by the given public key.
func VerifySignature2(message []byte, signature []byte, publicKey []byte) bool {
	// Create a secp256k1 context
	ctx := C.secp256k1_context_create(C.SECP256K1_CONTEXT_SIGN | C.SECP256K1_CONTEXT_VERIFY)
	defer C.free(unsafe.Pointer(ctx))

	// Allocate memory for the message, signature, and public key
	cMessage := cBuf(message)
	cSignature := cBuf(signature)
	cPublicKey := cBuf(publicKey)

	// Create a secp256k1 signature object
	var cSig C.secp256k1_ecdsa_signature
	if C.secp256k1_ecdsa_signature_parse_der(ctx, &cSig, cSignature, C.size_t(len(signature))) != 1 {
		return false
	}

	// Create a secp256k1 public key object
	var cPubKey C.secp256k1_pubkey
	if C.secp256k1_ec_pubkey_parse(ctx, &cPubKey, cPublicKey, C.size_t(len(publicKey))) != 1 {
		return false
	}

	// Verify the signature
	if C.secp256k1_ecdsa_verify(ctx, &cSig, cMessage, &cPubKey) != 1 {
		return false
	}

	return true
}

func VerifySignatureGo(message []byte, signature []byte, publicKey []byte) bool {
	sig, _ := bec.ParseSignature(signature, bec.S256())

	pubKey, err := bec.ParsePubKey(publicKey, bec.S256())
	if err != nil {
		panic(err)
	}

	return ecdsa.Verify(pubKey.ToECDSA(), message, sig.R, sig.S)
}
