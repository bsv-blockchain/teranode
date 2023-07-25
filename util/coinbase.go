package util

import (
	"encoding/binary"
	"fmt"

	"github.com/bitcoinsv/bsvd/txscript"
	"github.com/libsv/go-bt/v2"
)

var (
	serializedHeightVersion = 2
)

func ExtractCoinbaseHeight(coinbaseTx *bt.Tx) (uint32, error) {
	sigScript := *coinbaseTx.Inputs[0].UnlockingScript
	if len(sigScript) < 1 {
		str := "the coinbase signature script for blocks of " +
			"version %d or greater must start with the " +
			"length of the serialized block height"
		str = fmt.Sprintf(str, serializedHeightVersion)
		//return 0, ruleError(ErrMissingCoinbaseHeight, str)
		return 0, fmt.Errorf("ErrMissingCoinbaseHeight: %s", str)
	}

	// Detect the case when the block height is a small integer encoded with
	// as single byte.
	opcode := int(sigScript[0])
	if opcode == txscript.OP_0 {
		return 0, nil
	}
	if opcode >= txscript.OP_1 && opcode <= txscript.OP_16 {
		return uint32(opcode - (txscript.OP_1 - 1)), nil
	}

	// Otherwise, the opcode is the length of the following bytes which
	// encode in the block height.
	serializedLen := int(sigScript[0])
	if len(sigScript[1:]) < serializedLen {
		str := "the coinbase signature script for blocks of " +
			"version %d or greater must start with the " +
			"serialized block height"
		str = fmt.Sprintf(str, serializedLen)
		return 0, fmt.Errorf("ErrMissingCoinbaseHeight: %s", str)
	}

	serializedHeightBytes := make([]byte, 8)
	copy(serializedHeightBytes, sigScript[1:serializedLen+1])
	serializedHeight := binary.LittleEndian.Uint64(serializedHeightBytes)

	return uint32(serializedHeight), nil
}
