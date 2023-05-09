//go:build linux
// +build linux

package main

import (
	"log"

	"github.com/libsv/go-bt/v2/bscript/interpreter"
	"github.com/ordishs/gocore"
	"github.com/ordishs/verifysignature"
)

func init() {
	if gocore.Config().GetBool("use_cgo_verifier", false) {
		log.Println("Using CGO verifier")
		interpreter.InjectExternalVerifySignatureFn(verifysignature.VerifySignature2)
	}
}
