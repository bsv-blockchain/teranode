package worker_test

import (
	"testing"

	"github.com/bitcoin-sv/teranode/cmd/txblaster/worker"
	"github.com/stretchr/testify/assert"
)

func TestRollingCache(t *testing.T) {
	cache := worker.NewRollingCache(5)

	cache.Add("tx1")
	cache.Add("tx2")
	cache.Add("tx3")
	cache.Add("tx4")
	cache.Add("tx5")
	cache.Add("tx6") // This will remove tx1 from the cache

	// Check if a txId exists in the cache
	assert.True(t, cache.Contains("tx2"))  // true
	assert.True(t, cache.Contains("tx6"))  // true
	assert.False(t, cache.Contains("tx1")) // false
}
