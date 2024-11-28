package main

import (
	"log"

	bdksecp256k1 "github.com/bitcoin-sv/bdk/module/gobdk/secp256k1"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/gocore"
)

func init() {
	if gocore.Config().GetBool("use_cgo_signer", false) {
		log.Println("Using CGO signer - SignMessage")
		unlocker.InjectExternalSignerFn(bdksecp256k1.SignMessage)
	}
}
