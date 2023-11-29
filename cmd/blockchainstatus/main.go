package main

import "github.com/bitcoin-sv/ubsv/cmd/blockchainstatus/blockchainstatus"

func main() {
	blockchainstatus.Init()
	blockchainstatus.Start()
}
