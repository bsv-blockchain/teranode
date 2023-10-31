//go:build native

package main

import (
	"github.com/bitcoin-sv/ubsv/cmd/propagation_blaster/propagation_blaster"
	"github.com/libsv/go-bt/v2/unlocker"
)

func init() {
	unlocker.InjectExternalSignerFn(native.SignMessage)
}

func main() {
	propagation_blaster.Init()
	propagation_blaster.Start()
}
