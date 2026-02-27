package blockvalidation

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeDuplicatorDeduplicate(t *testing.T) {
	t.Run("test", func(t *testing.T) {
		nrCalled := &atomic.Uint32{}

		fn := func() error {
			time.Sleep(100 * time.Millisecond)
			nrCalled.Add(1)

			return nil
		}

		d := NewDeDuplicator(2)
		ctx := context.Background()

		_, err := d.DeDuplicate(ctx, chainhash.Hash{}, 0, fn)
		require.NoError(t, err)

		_, err = d.DeDuplicate(ctx, chainhash.Hash{}, 0, fn)
		require.NoError(t, err)

		_, err = d.DeDuplicate(ctx, chainhash.Hash{}, 0, fn)
		require.NoError(t, err)

		assert.Equal(t, uint32(1), nrCalled.Load())
	})
}

func TestDeDuplicatorGoroutines(t *testing.T) {
	nrCalled := &atomic.Uint32{}

	fn := func() error {
		time.Sleep(100 * time.Millisecond)
		nrCalled.Add(1)
		return nil
	}

	d := NewDeDuplicator(2)

	ctx := context.Background()

	// Test that multiple concurrent calls for the same hash only execute once
	testHash := chainhash.Hash{1, 2, 3}

	// Use channels to coordinate goroutines
	done := make(chan error, 5)

	for i := 0; i < 5; i++ {
		go func() {
			_, err := d.DeDuplicate(ctx, testHash, 0, fn)
			done <- err
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 5; i++ {
		err := <-done
		require.NoError(t, err)
	}

	// Should only be called once despite 5 concurrent attempts
	assert.Equal(t, uint32(1), nrCalled.Load())
}
