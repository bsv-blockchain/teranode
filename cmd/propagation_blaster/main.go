package main

import "github.com/bitcoin-sv/ubsv/cmd/propagation_blaster/propagation_blaster"

func main() {
	propagation_blaster.Init()
	propagation_blaster.Start()
}
