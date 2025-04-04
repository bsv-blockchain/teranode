package fields

// FieldName database bin names when getting data from utxo store
type FieldName string

const (
	Tx                  FieldName = "tx"
	Inputs              FieldName = "inputs"
	Outputs             FieldName = "outputs"
	External            FieldName = "external"
	LockTime            FieldName = "locktime"
	Version             FieldName = "version"
	Fee                 FieldName = "fee"
	SizeInBytes         FieldName = "sizeInBytes"
	ExtendedSize        FieldName = "extendedSize"
	ParentTxHashes      FieldName = "parentTxHashes"
	IsCoinbase          FieldName = "isCoinbase"
	Conflicting         FieldName = "conflicting"
	ConflictingChildren FieldName = "conflictingCs" // bin Fieldname can only be max 15 chars in aerospike
	Unspendable         FieldName = "unspendable"
	UtxoSpendableIn     FieldName = "utxoSpendableIn"
	SpendingHeight      FieldName = "spendingHeight"
	Utxos               FieldName = "utxos"
	TotalUtxos          FieldName = "totalUtxos"
	RecordUtxos         FieldName = "recordUtxos"
	SpentUtxos          FieldName = "spentUtxos"
	TotalExtraRecs      FieldName = "totalExtraRecs"
	SpentExtraRecs      FieldName = "spentExtraRecs"
	BlockIDs            FieldName = "blockIDs"
	BlockHeights        FieldName = "blockHeights"
	SubtreeIdxs         FieldName = "subtreeIdxs"
)

func (f FieldName) String() string {
	return string(f)
}

func FieldNamesToStrings(fieldNames []FieldName) []string {
	fieldStrings := make([]string, len(fieldNames))
	for i, fieldName := range fieldNames {
		fieldStrings[i] = string(fieldName)
	}

	return fieldStrings
}
