package utxo

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

func TestSpendAdmission(t *testing.T) {
	t.Run("expired deadline before timeout handler runs", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		var admission SpendAdmission
		require.ErrorIs(t, admission.Start(ctx), errors.ErrServiceUnavailable)
		require.ErrorIs(t, admission.Start(context.Background()), errors.ErrServiceUnavailable)
	})
	t.Run("context cancelled with a deadline cause is a timeout", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(context.DeadlineExceeded)
		var admission SpendAdmission
		require.ErrorIs(t, admission.Start(ctx), errors.ErrServiceUnavailable)
	})
	t.Run("cancelled context before dispatch", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var admission SpendAdmission
		require.ErrorIs(t, admission.Start(ctx), errors.ErrContextCanceled)
		require.False(t, admission.Abandon(), "the refused start already abandoned the item")
	})
	t.Run("abandon before start", func(t *testing.T) {
		var admission SpendAdmission
		require.True(t, admission.Abandon())
		require.ErrorIs(t, admission.Start(context.Background()), errors.ErrServiceUnavailable)
	})
	t.Run("started operation retains responsibility", func(t *testing.T) {
		var admission SpendAdmission
		require.NoError(t, admission.Start(context.Background()))
		require.False(t, admission.Abandon())
		require.ErrorIs(t, admission.Start(context.Background()), errors.ErrServiceUnavailable)
	})
	t.Run("racing dispatch and abandonment have exactly one winner", func(t *testing.T) {
		for range 1000 {
			var admission SpendAdmission
			ready := make(chan struct{})
			started, abandoned := make(chan bool, 1), make(chan bool, 1)
			go func() { <-ready; started <- admission.Start(context.Background()) == nil }()
			go func() { <-ready; abandoned <- admission.Abandon() }()
			close(ready)
			require.NotEqual(t, <-started, <-abandoned)
		}
	})
}

func TestSpendAdmissionStarted(t *testing.T) {
	var queued, started, abandoned SpendAdmission

	require.NoError(t, started.Start(context.Background()))
	require.True(t, abandoned.Abandon())

	require.False(t, queued.Started())
	require.True(t, started.Started())
	require.False(t, abandoned.Started())
}

func TestSpendOrphanClaimsExactlyOnce(t *testing.T) {
	var unmarked SpendOrphan
	require.False(t, unmarked.Claim(), "an item whose caller still waits must not be claimed")

	for i := 0; i < 200; i++ {
		var (
			orphan SpendOrphan
			wins   atomic.Int32
			wg     sync.WaitGroup
		)

		orphan.Mark()

		for j := 0; j < 8; j++ {
			wg.Add(1)

			go func() {
				defer wg.Done()

				if orphan.Claim() {
					wins.Add(1)
				}
			}()
		}

		wg.Wait()
		require.Equal(t, int32(1), wins.Load(), "exactly one side must win the rollback")
	}
}
