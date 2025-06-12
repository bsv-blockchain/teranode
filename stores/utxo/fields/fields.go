// Package fields defines the field names used for accessing and storing data in the UTXO store.
//
// This package provides constants and utilities for working with database field names
// in a type-safe manner. These field names are used across various UTXO store implementations
// (Aerospike, SQL, in-memory) for consistent data access patterns.
package fields

// FieldName represents database bin/column names when getting data from UTXO store.
// It provides a type-safe way to reference field names across different database implementations,
// ensuring consistency and preventing typos when accessing UTXO data.
type FieldName string

const (
	// Tx represents the full serialized transaction data
	Tx FieldName = "tx"
	// Inputs represents the transaction inputs data
	Inputs FieldName = "inputs"
	// Outputs represents the transaction outputs data
	Outputs FieldName = "outputs"
	// External indicates external data associated with the transaction
	External FieldName = "external"
	// LockTime is the block height or timestamp until which the transaction is locked
	LockTime FieldName = "locktime"
	// Version is the transaction version number
	Version FieldName = "version"
	// Fee is the transaction fee in satoshis
	Fee FieldName = "fee"
	// SizeInBytes is the serialized size of the transaction in bytes
	SizeInBytes FieldName = "sizeInBytes"
	// ExtendedSize is the size of the transaction including additional metadata
	ExtendedSize FieldName = "extendedSize"
	// TxInpoints contains the transaction inpoints, which are references to previous transaction outputs
	TxInpoints FieldName = "txInpoints"
	// IsCoinbase indicates whether the transaction is a coinbase transaction
	IsCoinbase FieldName = "isCoinbase"
	// Conflicting indicates whether the transaction conflicts with another transaction
	Conflicting FieldName = "conflicting"
	// ConflictingChildren contains transactions that spend from this conflicting transaction
	ConflictingChildren FieldName = "conflictingCs" // bin Fieldname can only be max 15 chars in aerospike
	// Unspendable indicates whether the transaction outputs are marked as unspendable
	Unspendable FieldName = "unspendable"
	// UtxoSpendableIn indicates the number of blocks after which the UTXO becomes spendable
	UtxoSpendableIn FieldName = "utxoSpendableIn"
	// SpendingHeight is the block height at which the UTXO was spent
	SpendingHeight FieldName = "spendingHeight"
	// Utxos represents the UTXOs associated with a transaction
	Utxos FieldName = "utxos"
	// TotalUtxos is the total number of UTXOs created by a transaction
	TotalUtxos FieldName = "totalUtxos"
	// RecordUtxos represents UTXOs that are stored in the record
	RecordUtxos FieldName = "recordUtxos"
	// SpentUtxos is the number of UTXOs that have been spent
	SpentUtxos FieldName = "spentUtxos"
	// TotalExtraRecs is the total number of extra records
	TotalExtraRecs FieldName = "totalExtraRecs"
	// SpentExtraRecs is the number of spent extra records
	SpentExtraRecs FieldName = "spentExtraRecs"
	// BlockIDs contains the block IDs where this transaction appears
	BlockIDs FieldName = "blockIDs"
	// BlockHeights contains the block heights where this transaction appears
	BlockHeights FieldName = "blockHeights"
	// SubtreeIdxs contains the subtree indexes where this transaction appears
	SubtreeIdxs FieldName = "subtreeIdxs"
	// Reassignments tracks UTXOs that have been reassigned
	Reassignments FieldName = "reassignments"
	// DeleteAtHeight specifies the block height at which the record should be deleted
	DeleteAtHeight FieldName = "deleteAtHeight"
	//  NotMined is an integer field that is set to 1 when a transaction is unminded and removed (nil) when it has.
	// The idea is to create a secondary index on this field which will only contain records that have this field set."
	NotMined FieldName = "notMined"
)

// String returns the string representation of the FieldName.
// This method implements the Stringer interface, allowing FieldName
// to be used in contexts requiring a string.
func (f FieldName) String() string {
	return string(f)
}

// FieldNamesToStrings converts a slice of FieldName to a slice of strings.
// This is useful when interacting with APIs that require string identifiers
// instead of the typed FieldName values.
//
// Parameters:
//   - fieldNames: Slice of FieldName values to convert
//
// Returns:
//   - Slice of strings containing the string representation of each FieldName
func FieldNamesToStrings(fieldNames []FieldName) []string {
	fieldStrings := make([]string, len(fieldNames))
	for i, fieldName := range fieldNames {
		fieldStrings[i] = string(fieldName)
	}

	return fieldStrings
}
