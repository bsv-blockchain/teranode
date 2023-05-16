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

	"github.com/TAAL-GmbH/ubsv/native"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/gocore"
)

func init() {
	if gocore.Config().GetBool("use_cgo_signer", false) {
		log.Println("Using CGO signer - SignMessage")
		unlocker.InjectExternalSignerFn(native.SignMessage)
	}
}
