package main

import "github.com/bitcoin-sv/ubsv/cmd/status/status"

func main() {
	status.Init()
	status.Start()
}
