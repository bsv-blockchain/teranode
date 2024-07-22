package testdata

import (
	"os"

	"github.com/bitcoin-sv/ubsv/services/legacy/bsvutil"
)

// This function helps reading the test data from the file, returns a BSV block
func ReadBlockFromFile(filePath string) (*bsvutil.Block, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	block, err := bsvutil.NewBlockFromReader(file)
	if err != nil {
		return nil, err
	}
	return block, nil
}
