package main

import (
	"github.com/bitcoin-sv/ubsv/cmd/blockassembly_blaster/blockassembly_blaster"
	"github.com/bitcoin-sv/ubsv/settings"
)

func main() {
	blockassembly_blaster.Init(settings.NewSettings())
	blockassembly_blaster.Start()
}
