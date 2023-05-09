//go:build linux
// +build linux

package main

func init() {
	if gocore.Config().GetBool("use_cgo_verifier", false) {
		log.Println("Using CGO verifier")
		interpreter.InjectExternalVerifySignatureFn(verifysignature.VerifySignature2)
	}
}
