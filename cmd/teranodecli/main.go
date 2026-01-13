package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"

	"github.com/bsv-blockchain/teranode/cmd/teranodecli/teranodecli"
)

func main() {
	if strconv.IntSize != 64 {
		fmt.Fprintf(os.Stderr, "Error: teranodecli requires a 64-bit architecture. Current architecture: %s\n", runtime.GOARCH)
		os.Exit(1)
	}

	teranodecli.Start(os.Args[1:], "", "")
}
