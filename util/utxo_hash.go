package util

import (
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// UTXOHashInto appends the UTXO preimage (previous txid || VarInt(index) ||
// locking script || VarInt(satoshis)) into scratch[:0], hashes it with a single
// SHA-256, and returns the hash by value plus the (possibly grown) scratch for
// reuse. The returned scratch may share backing with the input; callers should
// rebind it (h, scratch, err = UTXOHashInto(scratch, ...)).
//
// Zero heap allocations when scratch has sufficient capacity.
func UTXOHashInto(scratch []byte, previousTxid *chainhash.Hash, index uint32,
	lockingScript *bscript.Script, satoshis uint64) (chainhash.Hash, []byte, error) {
	if lockingScript == nil {
		return chainhash.Hash{}, scratch, errors.NewProcessingError("locking script is nil")
	}

	scratch = scratch[:0]
	scratch = append(scratch, previousTxid[:]...)
	scratch = bt.VarInt(index).AppendTo(scratch)
	scratch = append(scratch, *lockingScript...)
	scratch = bt.VarInt(satoshis).AppendTo(scratch)

	return chainhash.HashH(scratch), scratch, nil
}

// UTXOHash returns the hash of the UTXO for the given input parameters.
// The hash is calculated by concatenating the previous txid, the output index,
// the locking script and the satoshis, then hashing with SHA-256.
func UTXOHash(previousTxid *chainhash.Hash, index uint32, lockingScript *bscript.Script, satoshis uint64) (*chainhash.Hash, error) {
	h, _, err := UTXOHashInto(nil, previousTxid, index, lockingScript, satoshis)
	if err != nil {
		return nil, err
	}
	return &h, nil
}

// UTXOHashFromInput returns the hash of the UTXO for the given input.
func UTXOHashFromInput(input *bt.Input) (*chainhash.Hash, error) {
	if input.PreviousTxScript == nil {
		return nil, errors.NewProcessingError("locking script is nil")
	}
	return UTXOHash(input.PreviousTxIDChainHash(), input.PreviousTxOutIndex, input.PreviousTxScript, input.PreviousTxSatoshis)
}

// UTXOHashFromOutput returns the hash of the UTXO for the given output.
//
// output may be nil: the UTXO store encodes "this output is not a live UTXO" as a
// nil entry in the output vector, and a caller that walks the whole vector rather
// than a chosen index can hand one straight over. stores/utxo/sql SetConflicting
// does exactly that — it ranges over txMeta.Tx.Outputs with no nil check and
// passes a precomputed hash — so this guard turns a nil-deref panic there into an
// error return.
//
// It is only defence-in-depth for the callers that pass tx.TxIDChainHash() as the
// hash argument (services/alert/node.go, stores/utxo/aerospike/conflicting.go):
// Go evaluates that argument first, and it faults on the same nil-holed vector
// before this function is entered, so the guard is unreachable for them until
// bsv-blockchain/go-bt#162 is fixed upstream. Reject nil regardless, rather than
// letting the field access fault before UTXOHashInto's own nil check can report
// it.
func UTXOHashFromOutput(hash *chainhash.Hash, output *bt.Output, vOut uint32) (*chainhash.Hash, error) {
	if output == nil {
		return nil, errors.NewProcessingError("output is nil")
	}

	return UTXOHash(hash, vOut, output.LockingScript, output.Satoshis)
}
