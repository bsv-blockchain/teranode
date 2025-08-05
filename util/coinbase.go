package util

import (
	"encoding/binary"
	"strings"
	"unicode"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
)

const (
	// minerSlashTruncationCount defines the number of slashes after which to truncate miner tags
	minerSlashTruncationCount = 2
)

func ExtractCoinbaseHeight(coinbaseTx *bt.Tx) (uint32, error) {
	height, _, err := extractCoinbaseHeightAndText(*coinbaseTx.Inputs[0].UnlockingScript)
	return height, err
}

func ExtractCoinbaseMiner(coinbaseTx *bt.Tx) (string, error) {
	_, miner, err := extractCoinbaseHeightAndText(*coinbaseTx.Inputs[0].UnlockingScript)
	if err != nil && errors.Is(err, errors.ErrBlockCoinbaseMissingHeight) {
		err = nil
	}

	return miner, err
}

func extractCoinbaseHeightAndText(sigScript bscript.Script) (uint32, string, error) {
	if len(sigScript) < 1 {
		return 0, "", errors.NewBlockCoinbaseMissingHeightError("the coinbase signature script must start with the length of the serialized block height")
	}

	serializedLen := int(sigScript[0])

	// Support both 2-byte and 3-byte height encodings for compatibility
	// Most blocks use 3-byte encoding, but some CPU miners use 2-byte
	if serializedLen != 2 && serializedLen != 3 {
		return 0, "", errors.NewBlockCoinbaseMissingHeightError("the coinbase signature script must start with the length of the serialized block height (0x02 or 0x03)")
	}

	if len(sigScript[1:]) < serializedLen {
		return 0, "", errors.NewBlockCoinbaseMissingHeightError("the coinbase signature script must start with the serialized block height")
	}

	serializedHeightBytes := sigScript[1 : serializedLen+1]
	if len(serializedHeightBytes) > 8 {
		return 0, "", errors.NewBlockCoinbaseMissingHeightError("serialized block height too large")
	}

	heightBytes := make([]byte, 8)
	copy(heightBytes, serializedHeightBytes)
	serializedHeight := binary.LittleEndian.Uint64(heightBytes)

	arbitraryTextBytes := sigScript[serializedLen+1:]
	arbitraryText := string(arbitraryTextBytes)

	return uint32(serializedHeight), extractMiner(arbitraryText), nil
}

func extractMiner(data string) string {
	if len(data) == 0 {
		return ""
	}

	// Simple approach: keep only printable UTF-8 characters
	// This preserves human-readable text while removing binary data
	var result strings.Builder

	for _, r := range data {
		// Keep printable characters that are valid UTF-8
		if unicode.IsPrint(r) && r != 0xFFFD { // 0xFFFD is the Unicode replacement character
			result.WriteRune(r)
		}
	}

	// Trim any leading/trailing spaces and quotes
	cleaned := strings.TrimSpace(result.String())
	cleaned = strings.Trim(cleaned, "\"")

	// Find the first slash
	firstSlash := strings.Index(cleaned, "/")
	if firstSlash == -1 {
		// No slashes, return as is
		return cleaned
	}

	// Remove everything before the first slash
	cleaned = cleaned[firstSlash:]

	// If it has 2 slashes, remove everything after the 2nd slash
	slashCount := 0
	for i, r := range cleaned {
		if r == '/' {
			slashCount++
			if slashCount == minerSlashTruncationCount {
				// Truncate after this slash (the 2nd slash)
				return cleaned[:i+1]
			}
		}
	}

	return cleaned
}

// func extractCoinbaseHeightAndText(coinbaseTx *bt.Tx) (uint32, string, error) {
// 	sigScript := *coinbaseTx.Inputs[0].UnlockingScript
// 	if len(sigScript) < 1 {
// 		str := "the coinbase signature script for blocks of " +
// 			"version %d or greater must start with the " +
// 			"length of the serialized block height"
// 		str = fmt.Sprintf(str, serializedHeightVersion)
// 		//return 0, ruleError(ErrMissingCoinbaseHeight, str)
// 		return 0, "", fmt.Errorf("ErrMissingCoinbaseHeight: %s", str)
// 	}

// 	// Detect the case when the block height is a small integer encoded with
// 	// as single byte.
// 	opcode := int(sigScript[0])
// 	if opcode == txscript.OP_0 {
// 		return 0, "", nil
// 	}
// 	if opcode >= txscript.OP_1 && opcode <= txscript.OP_16 {
// 		return uint32(opcode - (txscript.OP_1 - 1)), "", nil
// 	}

// 	// Otherwise, the opcode is the length of the following bytes which
// 	// encode in the block height.
// 	serializedLen := int(sigScript[0])
// 	if len(sigScript[1:]) < serializedLen {
// 		str := "the coinbase signature script for blocks of " +
// 			"version %d or greater must start with the " +
// 			"serialized block height"
// 		str = fmt.Sprintf(str, serializedLen)
// 		return 0, "", fmt.Errorf("ErrMissingCoinbaseHeight: %s", str)
// 	}

// 	serializedHeightBytes := make([]byte, 8)
// 	copy(serializedHeightBytes, sigScript[1:serializedLen+1])
// 	serializedHeight := binary.LittleEndian.Uint64(serializedHeightBytes)

// 	return uint32(serializedHeight), "", nil
// }
