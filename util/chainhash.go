package util

import "github.com/bsv-blockchain/go-bt/v2/chainhash"

// HashPointersToValues converts a slice of hash pointers to a slice of hash values.
// This is useful when converting between wire protocol types (which use pointers) and
// internal types (which use values).
//
// Nil pointers in the input slice are converted to zero hashes in the output.
func HashPointersToValues(hashes []*chainhash.Hash) []chainhash.Hash {
	result := make([]chainhash.Hash, len(hashes))
	for i, hash := range hashes {
		if hash != nil {
			result[i] = *hash
		}
	}
	return result
}

// HashValuesToPointers converts a slice of hash values to a slice of hash pointers.
// This is useful when converting from internal types (which use values) to
// wire protocol types (which use pointers).
func HashValuesToPointers(hashes []chainhash.Hash) []*chainhash.Hash {
	result := make([]*chainhash.Hash, len(hashes))
	for i := range hashes {
		hash := hashes[i]
		result[i] = &hash
	}
	return result
}
