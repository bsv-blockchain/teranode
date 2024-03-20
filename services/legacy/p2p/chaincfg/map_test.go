package chaincfg

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
)

// Test to see if the chainhash.Hash can be used as a key in a map.

func TestMap(t *testing.T) {
	m := make(map[chainhash.Hash]int)

	for i := 0; i < 10; i++ {
		hash := chainhash.HashH([]byte(fmt.Sprintf("test_%d", i)))
		m[hash] = i
	}

	var hashes []chainhash.Hash
	for hash := range m {
		hashes = append(hashes, hash)
	}

	// Sort the keys...
	sort.SliceStable(hashes, func(i, j int) bool {
		return bytes.Compare(hashes[i][:], hashes[j][:]) < 0
	})

	// First key in the sorted list should be:
	assert.Equal(t, "6439ce00396fb2f2ee1a33be05ef6258cf5ac1727f0389917e9d9c886972141d", hashes[0].String())
	assert.Equal(t, 8, m[hashes[0]], 1)

	hash := chainhash.HashH([]byte("test_1"))
	expected := 1

	got := m[hash]

	assert.Equal(t, expected, got)

}
