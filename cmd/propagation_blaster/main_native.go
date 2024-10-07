//go:build bdk

package main

/*
	#cgo LDFLAGS: -lsecp256k1
	#include <stdlib.h>
	#include <secp256k1/include/secp256k1.h>
*/
import "C"
import (
	"log"

	bdksecp256k1 "github.com/bitcoin-sv/bdk/module/gobdk/secp256k1"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/gocore"
)

func init() {
	if gocore.Config().GetBool("use_cgo_signer", false) {
		log.Println("Using BDK signer - SignMessage")
		unlocker.InjectExternalSignerFn(bdksecp256k1.SignMessage)
	}
}
