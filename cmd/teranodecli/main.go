package main

import (
	"os"

	"github.com/bsv-blockchain/teranode/cmd/teranodecli/teranodecli"
)

func main() {
	teranodecli.Start(os.Args[1:], "", "")
}
