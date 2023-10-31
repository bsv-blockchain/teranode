package main

import "github.com/bitcoin-sv/ubsv/cmd/blockassembly_blaster/blockassembly_blaster"

func main() {
	blockassembly_blaster.Init()
	blockassembly_blaster.Start()
}
