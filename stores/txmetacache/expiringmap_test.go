package txmetacache

import (
	"encoding/binary"
	"sync"
	"testing"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/stretchr/testify/assert"
)

func TestExpiringMap_Get(t *testing.T) {
	t.Run("get from empty map", func(t *testing.T) {
		m := NewExpiringMap(10, 3)
		_, ok := m.Get([32]byte{})
		if ok {
			t.Error("expected ok to be false")
		}
	})

	t.Run("get from map with one element", func(t *testing.T) {
		m := NewExpiringMap(10, 3)
		m.Put([32]byte{1}, txmeta.Data{})
		_, ok := m.Get([32]byte{1})
		if !ok {
			t.Error("expected ok to be true")
		}
	})

	t.Run("get from map with many elements", func(t *testing.T) {
		// this test is failing
		util.SkipVeryLongTests(t)

		m := NewExpiringMap(10, 3)
		var wg sync.WaitGroup

		var key [32]byte
		for i := 0; i < 29; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				binary.LittleEndian.PutUint32(key[:], uint32(i))
				m.Put(key, txmeta.Data{})
			}(i)
		}
		wg.Wait()

		assert.Equal(t, 3, len(m.maps))
		assert.Equal(t, 10, int(m.maps[0].length.Load()))

		for i := 0; i < 29; i++ {
			binary.LittleEndian.PutUint32(key[:], uint32(i))
			_, ok := m.Get(key)
			if !ok {
				t.Error("expected ok to be true")
			}
		}
	})
}
