package util

import "sync"

func ConvertSyncMapToUint32Slice(oldBlockIDs *sync.Map) ([]uint32, bool) {
	var referencedOldBlockIDs []uint32
	// Flag to check if the oldBlockIDs map has elements
	var hasTransactionsReferencingOldBlocks bool

	oldBlockIDs.Range(func(key, _ interface{}) bool {
		hasTransactionsReferencingOldBlocks = true
		val := key.(uint32)
		referencedOldBlockIDs = append(referencedOldBlockIDs, val)

		return true // Continue iteration to collect all elements
	})

	return referencedOldBlockIDs, hasTransactionsReferencingOldBlocks
}
