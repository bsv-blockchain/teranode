package main

import "github.com/bitcoin-sv/ubsv/cmd/txblaster/txblaster"

func main() {
	txblaster.Init()
	txblaster.Start()
}
