package main

import (
	"fmt"
	"os"

	"github.com/bitcoin-sv/teranode/cmd/teranode"
)

// Name used by build script for the binaries. (Please keep on single line)
const progname = "teranode"

// Version & commit strings injected at build with -ldflags -X...
var version string
var commit string

func main() {
	// Check for --version flag before any initialization
	for _, arg := range os.Args[1:] {
		if arg == "--version" || arg == "-v" {
			fmt.Printf("%s version %s (commit: %s)\n", progname, version, commit)
			os.Exit(0)
		}
	}

	// If not showing version, run the daemon
	teranode.RunDaemon(progname, version, commit)
}
