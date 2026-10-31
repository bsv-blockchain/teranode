package utxo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSpendAdmission(t *testing.T) {
	t.Run("expired deadline before timeout handler runs", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		var admission SpendAdmission
		require.Error(t, admission.Start(ctx))
		require.Error(t, admission.Start(context.Background()))
	})
	t.Run("abandon before start", func(t *testing.T) {
		var admission SpendAdmission
		require.True(t, admission.Abandon())
		require.Error(t, admission.Start(context.Background()))
	})
	t.Run("started operation retains responsibility", func(t *testing.T) {
		var admission SpendAdmission
		require.NoError(t, admission.Start(context.Background()))
		require.False(t, admission.Abandon())
		require.Error(t, admission.Start(context.Background()))
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
