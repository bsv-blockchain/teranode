package main

import "github.com/bitcoin-sv/teranode/cmd/blockchainstatus/blockchainstatus"

func main() {
	blockchainstatus.Init()
	blockchainstatus.Start()
}
