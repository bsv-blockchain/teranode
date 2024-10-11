package util

import "sync"

func ConvertSyncMapToUint32Slice(syncMap *sync.Map) ([]uint32, bool) {
	var sliceWithMapElements []uint32
	mapHasAnyElements := false

	syncMap.Range(func(key, _ interface{}) bool {
		mapHasAnyElements = true
		val := key.(uint32)
		sliceWithMapElements = append(sliceWithMapElements, val)

		return true
	})

	return sliceWithMapElements, mapHasAnyElements
}
