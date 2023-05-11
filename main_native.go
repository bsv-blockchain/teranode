////go:build native

package main

/*
	#cgo CFLAGS: -I./include
	#cgo LDFLAGS: -lsecp256k1 -lgmp
	#include <stdlib.h>
	#include <secp256k1.h>
*/
import "C"
import (
	"fmt"
	"log"
	"unsafe"

	"github.com/libsv/go-bt/v2/bscript/interpreter"
	"github.com/ordishs/gocore"
)

func init() {
	if gocore.Config().GetBool("use_cgo_verifier", false) {
		log.Println("Using CGO verifier - VerifySignature")
		log.Println("Using CGO verifier - VerifySignature2")
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

func SignMessage(message []byte, privateKey []byte) ([]byte, error) {
	// Create a secp256k1 context
	ctx := C.secp256k1_context_create(C.SECP256K1_CONTEXT_SIGN | C.SECP256K1_CONTEXT_VERIFY)
	defer C.free(unsafe.Pointer(ctx))

	if len(message) != 32 {
		return nil, fmt.Errorf("message must be 32 bytes")
	}
	if len(privateKey) != 32 {
		return nil, fmt.Errorf("private key must be 32 bytes")
	}

	// Allocate memory for the message, signature, and public key
	cMessage := C.CBytes(message)
	defer C.free(cMessage)
	cPrivateKey := C.CBytes(privateKey)
	defer C.free(cPrivateKey)

	// Create a secp256k1 signature object
	var cSig C.secp256k1_ecdsa_signature
	result := int(C.secp256k1_ecdsa_sign(ctx, &cSig, (*C.uchar)(cMessage), (*C.uchar)(cPrivateKey), nil, nil))

	if result != 1 {
		return nil, fmt.Errorf("error signing message: %d", result)
	}

	serializedSig := make([]C.uchar, 72)
	outputLen := C.size_t(len(serializedSig))

	result = int(C.secp256k1_ecdsa_signature_serialize_der(ctx, &serializedSig[0], &outputLen, &cSig))

	if result != 1 {
		return nil, fmt.Errorf("error serializing signature: %d", result)
	}

	return C.GoBytes(unsafe.Pointer(&serializedSig[0]), C.int(outputLen)), nil
}
