package testdata

import (
	"bufio"
	"encoding/hex"
	"io"
	"os"
	"strings"

	"github.com/bitcoin-sv/ubsv/services/legacy/bsvutil"
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
	bufferedReader := bufio.NewReaderSize(reader, 1024*1024*4) // 4MB buffer

	block, err := bsvutil.NewBlockFromReader(bufferedReader)
	if err != nil {
		return nil, err
	}

	return block, nil
}
