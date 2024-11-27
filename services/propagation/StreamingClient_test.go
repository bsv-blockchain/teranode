package propagation_test

import (
	"context"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/services/propagation"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamingClient(t *testing.T) {
	logger := ulogger.New("test")
	tSettings := test.CreateBaseTestSettings()
	sc, err := propagation.NewStreamingClient(context.Background(), logger, tSettings, 0, true)
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
