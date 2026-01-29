package daemon

import (
	bdksecp256k1 "github.com/bitcoin-sv/bdk/module/gobdk/secp256k1"
	"github.com/bsv-blockchain/go-bt/v2/bscript/interpreter"
	sdkinterpreter "github.com/bsv-blockchain/go-sdk/script/interpreter"
	"github.com/ordishs/gocore"
)

func init() {
	// Create a secp256k1 context
	if gocore.Config().GetBool("use_cgo_verifier", true) {
		// log.Println("Using BDK secp256k1 verifier - VerifySignature")
		interpreter.InjectExternalVerifySignatureFn(bdksecp256k1.VerifySignature)
		sdkinterpreter.InjectExternalVerifySignatureFn(bdksecp256k1.VerifySignature)
	}
}
