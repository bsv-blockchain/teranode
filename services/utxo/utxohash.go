package utxo

import (
	"fmt"

	"github.com/libsv/go-bk/crypto"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

func GetInputUtxoHash(input *bt.Input) (*chainhash.Hash, error) {
	voutBytes := bt.VarInt(input.PreviousTxOutIndex).Bytes()

	if input.PreviousTxScript == nil || len(*input.PreviousTxScript) == 0 {
		return nil, fmt.Errorf("previous tx script is nil")
	}
	previousScript := []byte(*input.PreviousTxScript)

	if input.PreviousTxSatoshis == 0 {
		return nil, fmt.Errorf("previous tx satoshis is 0")
	}
	previousSatoshis := bt.VarInt(input.PreviousTxSatoshis).Bytes()

	// input.PreviousTxID() is in bytes, but not reversed FFS
	utxoHashBytes := append(bt.ReverseBytes(input.PreviousTxID()), voutBytes...)
	utxoHashBytes = append(utxoHashBytes, previousScript...)
	utxoHashBytes = append(utxoHashBytes, previousSatoshis...)

	hash := crypto.Sha256(utxoHashBytes)
	chHash, err := chainhash.NewHash(hash)
	if err != nil {
		return nil, err
	}

	return chHash, nil
}

func GetOutputUtxoHash(txID []byte, output *bt.Output, vOut uint64) (*chainhash.Hash, error) {
	voutBytes := bt.VarInt(vOut).Bytes()

	if output.LockingScript == nil || len(*output.LockingScript) == 0 {
		return nil, fmt.Errorf("output script is nil")
	}
	outputScript := []byte(*output.LockingScript)

	if output.Satoshis == 0 {
		return nil, fmt.Errorf("satoshis is 0")
	}
	satoshiBytes := bt.VarInt(output.Satoshis).Bytes()

	utxoHashBytes := append(txID, voutBytes...)
	utxoHashBytes = append(utxoHashBytes, outputScript...)
	utxoHashBytes = append(utxoHashBytes, satoshiBytes...)

	hash := crypto.Sha256(utxoHashBytes)
	chHash, err := chainhash.NewHash(hash)
	if err != nil {
		return nil, err
	}

	return chHash, nil
}
