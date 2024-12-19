package daemon

import (
	"log"

	bdksecp256k1 "github.com/bitcoin-sv/bdk/module/gobdk/secp256k1"
	sdkinterpreter "github.com/bitcoin-sv/go-sdk/script/interpreter"
	"github.com/libsv/go-bt/v2/bscript/interpreter"
	"github.com/ordishs/gocore"
)

func init() {
	// Create a secp256k1 context
	if gocore.Config().GetBool("use_cgo_verifier", true) {
		log.Println("Using BDK secp256k1 verifyer - VerifySignature")
		interpreter.InjectExternalVerifySignatureFn(bdksecp256k1.VerifySignature)
		sdkinterpreter.InjectExternalVerifySignatureFn(bdksecp256k1.VerifySignature)
	}
}
