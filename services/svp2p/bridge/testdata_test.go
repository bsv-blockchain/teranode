package bridge

// Test-only fixture loader for the relocated handle_block test suite.
//
// Adapted from the legacy netsync package's own testdata fixture reader, with
// the bsvutil import swapped to this package's own relocated bsvutil
// (services/svp2p/bridge/bsvutil), so ReadBlockFromFile returns the type the
// bridge pipeline actually consumes. This package imports no legacy code.

import (
	"bufio"
	"encoding/hex"
	"io"
	"os"
	"strings"

	"github.com/bsv-blockchain/teranode/services/svp2p/bridge/bsvutil"
)

type binReader struct {
	r io.Reader
}

func (br *binReader) Read(p []byte) (n int, err error) {
	return br.r.Read(p)
}

// ReadBlockFromFile helps to read the test data from the file, returns a BSV block
func ReadBlockFromFile(filePath string) (*bsvutil.Block, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var reader io.Reader

	if strings.HasSuffix(filePath, ".hex") {
		// Create a hex stream reader
		reader = hex.NewDecoder(file)
	} else {
		// Create a binReader that does nothing to the stream
		reader = &binReader{r: file}
	}

	// buffer the reader
	bufferedReader := bufio.NewReaderSize(reader, 1024*64)

	block, err := bsvutil.NewBlockFromReader(bufferedReader)
	if err != nil {
		return nil, err
	}

	return block, nil
}
