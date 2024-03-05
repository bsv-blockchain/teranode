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

func TestDeDuplicatorDeduplicate(t *testing.T) {
	t.Run("test", func(t *testing.T) {
		nrCalled := atomic.NewUint32(0)

		fn := func() error {
			time.Sleep(100 * time.Millisecond)
			nrCalled.Inc()
			return nil
		}

		d := New(2)
		ctx := context.Background()

		_, err := d.DeDuplicate(ctx, chainhash.Hash{}, fn)
		require.NoError(t, err)

		_, err = d.DeDuplicate(ctx, chainhash.Hash{}, fn)
		require.NoError(t, err)

		_, err = d.DeDuplicate(ctx, chainhash.Hash{}, fn)
		require.NoError(t, err)

		assert.Equal(t, uint32(1), nrCalled.Load())
	})
}

//func TestDeDuplicatorGoroutines(t *testing.T) {
//	nrCalled := atomic.NewUint32(0)
//
//	fn := func() error {
//		time.Sleep(100 * time.Millisecond)
//		nrCalled.Inc()
//		return nil
//	}
//
//	d := New(2)
//
//	ctx := context.Background()
//
//	g := errgroup.Group{}
//
//	for i := 0; i < 5; i++ {
//		g.Go(func() error {
//			err := d.DeDuplicate(ctx, chainhash.Hash{}, fn)
//			require.NoError(t, err)
//			require.NoError(t, err)
//			assert.Equal(t, "Hash is 0000000000000000000000000000000000000000000000000000000000000000", res)
//
//			return nil
//		})
//	}
//
//	err := g.Wait()
//	require.NoError(t, err)
//
//	assert.Equal(t, uint32(1), nrCalled.Load())
//}
