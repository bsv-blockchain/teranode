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
	"log"
	"unsafe"

	"github.com/libsv/go-bt/v2/bscript/interpreter"
	"github.com/ordishs/gocore"
)

func init() {
	if gocore.Config().GetBool("use_cgo_verifier", false) {
		log.Println("Using CGO verifier")
		interpreter.InjectExternalVerifySignatureFn(VerifySignature2)
	}
}

func cBuf(goSlice []byte) *C.uchar {
	return (*C.uchar)(unsafe.Pointer(&goSlice[0]))
}

// VerifySignature verifies that the given signature for the given message was signed by the given public key.
func VerifySignature2(message []byte, signature []byte, publicKey []byte) bool {
	// Create a secp256k1 context
	ctx := C.secp256k1_context_create(C.SECP256K1_CONTEXT_SIGN | C.SECP256K1_CONTEXT_VERIFY)

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
