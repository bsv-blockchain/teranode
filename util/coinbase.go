package util

import (
	"encoding/binary"
	"strings"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
)

// var (
// 	serializedHeightVersion = 2
// )

func ExtractCoinbaseHeight(coinbaseTx *bt.Tx) (uint32, error) {
	height, _, err := extractCoinbaseHeightAndText(*coinbaseTx.Inputs[0].UnlockingScript)
	return height, err
}

func ExtractCoinbaseMiner(coinbaseTx *bt.Tx) (string, error) {
	_, miner, err := extractCoinbaseHeightAndText(*coinbaseTx.Inputs[0].UnlockingScript)
	if err != nil && errors.Is(err, errors.ErrTxCoinbaseMissingBlockHeight) {
		err = nil
	}
	return miner, err
}

func extractCoinbaseHeightAndText(sigScript bscript.Script) (uint32, string, error) {
	if len(sigScript) < 1 {
		return 0, "", errors.New(errors.ERR_TX_COINBASE_MISSING_BLOCK_HEIGHT, "the coinbase signature script must start with the length of the serialized block height")
	}

	serializedLen := int(sigScript[0])
	if len(sigScript[1:]) < serializedLen {
		return 0, "", errors.New(errors.ERR_TX_COINBASE_MISSING_BLOCK_HEIGHT, "the coinbase signature script must start with the serialized block height")
	}

	serializedHeightBytes := sigScript[1 : serializedLen+1]
	if len(serializedHeightBytes) > 8 {
		return 0, "", errors.New(errors.ERR_TX_COINBASE_MISSING_BLOCK_HEIGHT, "serialized block height too large")
	}

	heightBytes := make([]byte, 8)
	copy(heightBytes, serializedHeightBytes)
	serializedHeight := binary.LittleEndian.Uint64(heightBytes)

	arbitraryTextBytes := sigScript[serializedLen+1:]
	arbitraryText := string(arbitraryTextBytes)

	return uint32(serializedHeight), extractMiner(arbitraryText), nil
}

func extractMiner(str string) string {
	str = strings.ToValidUTF8(str, "?")

	// Split the arbitrary text by "/"
	parts := strings.Split(str, "/")
	if len(parts) == 1 {
		return str
	}

	// Join all the parts except the last one
	str = strings.Join(parts[:len(parts)-1], "/")

	return str + "/"
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
