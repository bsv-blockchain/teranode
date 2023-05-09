//go:build linux
// +build linux

package main

func init() {
	if gocore.Config().GetBool("use_gco_verifier", false) {
		interpreter.InjectExternalVerifySignatureFn(verifysignature.VerifySignature2)
	}
}
