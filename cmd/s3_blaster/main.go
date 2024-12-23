package main

import "github.com/bitcoin-sv/teranode/cmd/s3_blaster/s3_blaster"

func main() {
	s3_blaster.Init()
	s3_blaster.Start()
}
