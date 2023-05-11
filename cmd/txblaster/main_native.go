//go:build native

package main

/*
	#cgo CFLAGS: -I./include
	#cgo LDFLAGS: -lsecp256k1
	#include <stdlib.h>
	#include <secp256k1.h>
*/
import "C"
import (
	"fmt"
	"log"
	"unsafe"

	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/gocore"
)

func init() {
	if gocore.Config().GetBool("use_cgo_signer", false) {
		log.Println("Using CGO signer - SignMessage")
		unlocker.InjectExternalSignerFn(SignMessage)
	}
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
