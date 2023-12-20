package deduplicator

import (
	"context"
	"testing"
	"time"

	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestDeDuplicator_DeDuplicate(t *testing.T) {
	t.Run("test", func(t *testing.T) {
		nrCalled := atomic.NewUint32(0)

		fn := func() error {
			time.Sleep(100 * time.Millisecond)
			nrCalled.Inc()
			return nil
		}

		d := New(2)
		ctx := context.Background()

		err := d.DeDuplicate(ctx, chainhash.Hash{}, fn)
		require.NoError(t, err)

		err = d.DeDuplicate(ctx, chainhash.Hash{}, fn)
		require.NoError(t, err)

		err = d.DeDuplicate(ctx, chainhash.Hash{}, fn)
		require.NoError(t, err)

		assert.Equal(t, uint32(1), nrCalled.Load())
	})
}
