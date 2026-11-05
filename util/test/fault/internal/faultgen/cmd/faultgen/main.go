// Command faultgen regenerates the fault decorators in the current directory. It is run by
// `go generate ./util/test/fault`.
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/bsv-blockchain/teranode/util/test/fault/internal/faultgen"
)

func main() {
	files, err := faultgen.Generate(".")
	if err != nil {
		fmt.Fprintln(os.Stderr, "faultgen:", err)
		os.Exit(1)
	}

	for name, src := range files {
		if err := os.WriteFile(filepath.Join(".", name), src, 0o600); err != nil {
			fmt.Fprintln(os.Stderr, "faultgen:", err)
			os.Exit(1)
		}
	}
}
