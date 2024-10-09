package util

import "sync"

func ConvertSyncMapToUint32Slice(oldBlockIDs *sync.Map) ([]uint32, bool) {
	var referencedOldBlockIDs []uint32
	var hasTransactionsReferencingOldBlocks bool

	oldBlockIDs.Range(func(key, value interface{}) bool {
		hasTransactionsReferencingOldBlocks = true
		val := value.(uint32)
		referencedOldBlockIDs = append(referencedOldBlockIDs, val)

		return true
	})

	return referencedOldBlockIDs, hasTransactionsReferencingOldBlocks
}
