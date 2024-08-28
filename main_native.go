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
	"log"

	sdkinterpreter "github.com/bitcoin-sv/go-sdk/script/interpreter"
	"github.com/bitcoin-sv/ubsv/native"
	"github.com/libsv/go-bt/v2/bscript/interpreter"
	"github.com/ordishs/gocore"
)

func init() {
	// Create a secp256k1 context
	if gocore.Config().GetBool("use_cgo_verifier", false) {
		log.Println("Using CGO verifier - VerifySignature")
		interpreter.InjectExternalVerifySignatureFn(native.VerifySignature)
		sdkinterpreter.InjectExternalVerifySignatureFn(native.VerifySignature)
	}
}
