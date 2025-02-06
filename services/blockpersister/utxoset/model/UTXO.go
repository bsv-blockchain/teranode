package model

// UTXO represents an unspent transaction output with its key and value
type UTXO struct {
	// Key uniquely identifies the UTXO
	Key UTXOKey
	// Val contains the UTXO's data
	Val *UTXOValue
}
