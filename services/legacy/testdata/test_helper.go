package testdata

import (
	"encoding/hex"
	"io"
	"os"
	"strings"

	"github.com/bitcoin-sv/ubsv/services/legacy/bsvutil"
)

// This function helps reading the test data from the file, returns a BSV block
func ReadBlockFromFile(filePath string) (*bsvutil.Block, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	hexData, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	cleanHexData := strings.TrimSpace(string(hexData))

	blockBytes, err := hex.DecodeString(cleanHexData)
	if err != nil {
		return nil, err
	}

	block, err := bsvutil.NewBlockFromBytes(blockBytes)
	if err != nil {
		return nil, err
	}
	return block, nil
}
