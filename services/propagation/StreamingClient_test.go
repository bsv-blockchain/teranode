package propagation_test

import (
	"context"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/services/propagation"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamingClient(t *testing.T) {
	logger := gocore.Log("test")
	sc, err := propagation.NewStreamingClient(context.Background(), logger, 0, true)
	require.NoError(t, err)

	numberOfIterations := 100

	for i := 0; i < numberOfIterations; i++ {
		err := sc.ProcessTransaction([]byte("test"))
		require.NoError(t, err)
	}

	total, count := sc.GetTimings()
	assert.Greater(t, total, time.Duration(0))
	t.Logf("total time: %s", total)
	t.Logf("average time: %s", total/time.Duration(count))
	assert.Equal(t, numberOfIterations, count)
}
