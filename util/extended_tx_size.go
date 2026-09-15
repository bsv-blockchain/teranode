package util

import (
	"github.com/bsv-blockchain/go-bt/v2"
)

// ExtendedTxSize returns len(tx.ExtendedBytes()) without serializing the tx.
// Mirrors go-bt's extended layout: standard size, plus the 6-byte EF marker,
// plus per-input PreviousTxSatoshis(8) and the previous-script varint+bytes
// (a nil PreviousTxScript serializes as a single 0x00 == VarInt(0)).
//
// bt.Input.Size() counts 32 bytes for the previous txid unconditionally, but
// ExtendedBytes() omits them when previousTxIDHash is nil; the correction below
// keeps this exact for inputs with an unset hash too (production txs always set
// it — via decode or WireTxToGoBtTx — but we don't rely on that).
//
// Callers use this instead of len(tx.ExtendedBytes()) on hot paths: the latter
// allocates and copies the whole transaction twice (go-bt sizes its buffer from
// the standard size, then regrows for the larger extended form) purely to learn
// a length. util/extended_tx_size_test.go pins the two against each other.
func ExtendedTxSize(tx *bt.Tx) int {
	size := tx.Size() + 6

	for _, in := range tx.Inputs {
		if in.PreviousTxIDChainHash() == nil {
			size -= 32
		}

		size += 8

		if in.PreviousTxScript == nil {
			size++
		} else {
			l := len(*in.PreviousTxScript)
			size += bt.VarInt(uint64(l)).Length() + l
		}
	}

	return size
}
