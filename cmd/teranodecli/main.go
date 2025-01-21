package main

import (
	"os"

	"github.com/bitcoin-sv/teranode/cmd/teranodecli/teranodecli"
)

func main() {
	teranodecli.Start(os.Args[1:], "", "")
}
