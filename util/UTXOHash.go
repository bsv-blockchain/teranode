package util

import (
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
)

// UTXOHash returns the hash of the UTXO for the given input parameters.
// The hash is calculated by concatenating the previous txid, the output index, the locking script and the satoshis.
// The hash is then hashed using SHA256.
func UTXOHash(previousTxid *chainhash.Hash, index uint32, lockingScript *bscript.Script, satoshis uint64) (*chainhash.Hash, error) {
	if lockingScript == nil {
		return nil, errors.NewProcessingError("locking script is nil")
	}

	utxoHash := make([]byte, 0, 256)
	utxoHash = append(utxoHash, previousTxid.CloneBytes()...)
	utxoHash = append(utxoHash, bt.VarInt(index).Bytes()...)
	utxoHash = append(utxoHash, *lockingScript...)
	utxoHash = append(utxoHash, bt.VarInt(satoshis).Bytes()...)

	chHash := chainhash.HashH(utxoHash)
	return &chHash, nil
}

// UTXOHashFromInput returns the hash of the UTXO for the given input.
// The hash is calculated by concatenating the previous txid, the output index, the locking script and the satoshis.
// The hash is then hashed using SHA256.
func UTXOHashFromInput(input *bt.Input) (*chainhash.Hash, error) {
	hash := input.PreviousTxIDChainHash()

	if input.PreviousTxScript == nil {
		return nil, errors.NewProcessingError("locking script is nil")
	}

	return UTXOHash(hash, input.PreviousTxOutIndex, input.PreviousTxScript, input.PreviousTxSatoshis)
}

// UTXOHashFromOutput returns the hash of the UTXO for the given output.
// The hash is calculated by concatenating the previous txid, the output index, the locking script and the satoshis.
// The hash is then hashed using SHA256.
func UTXOHashFromOutput(hash *chainhash.Hash, output *bt.Output, vOut uint32) (*chainhash.Hash, error) {
	return UTXOHash(hash, vOut, output.LockingScript, output.Satoshis)
}
