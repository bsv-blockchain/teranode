package main

import "github.com/bitcoin-sv/ubsv/cmd/utxostore_blaster/utxostore_blaster"

func main() {
	utxostore_blaster.Init()
	utxostore_blaster.Start()
}
