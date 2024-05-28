package main

import (
	"flag"
	"fmt"
	"github.com/bitcoinsv/bsvutil"
	"os"
)

func usage(msg string) {
	if msg != "" {
		fmt.Printf("Error: %s\n\n", msg)
	}
	fmt.Printf("Usage: verifyblock <filename>\n\n")
	os.Exit(1)
}

func main() {
	flag.Parse()

	if len(flag.Args()) != 1 {
		usage("filename or hash required")
	}

	// read block from file and verify with go-bt

	f, err := os.Open(flag.Arg(0))
	if err != nil {
		fmt.Printf("error opening file: %v\n", err)
		os.Exit(1)
	}

	// drop the first 8 bytes of the block
	_, err = f.Seek(8, 0)
	if err != nil {
		fmt.Printf("error seeking file: %v\n", err)
		os.Exit(1)
	}

	// read the block
	block, err := bsvutil.NewBlockFromReader(f)
	if err != nil {
		fmt.Printf("error reading block: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Block %s with %d transactions processed correctly\n", block.Hash().String(), len(block.Transactions()))
}
