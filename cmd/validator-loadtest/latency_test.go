package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLatencyHistogram_RecordAndPercentiles(t *testing.T) {
	h := newLatencyHistogram()
	for i := 1; i <= 100; i++ {
		h.record(time.Duration(i) * time.Millisecond)
	}
	require.Equal(t, int64(100), h.count())
	// 50th, 95th, 99th of [1ms..100ms] (nearest-rank).
	require.Equal(t, 50*time.Millisecond, h.percentile(50))
	require.Equal(t, 95*time.Millisecond, h.percentile(95))
	require.Equal(t, 99*time.Millisecond, h.percentile(99))
}

func TestLatencyHistogram_EmptyReturnsZero(t *testing.T) {
	h := newLatencyHistogram()
	require.Equal(t, int64(0), h.count())
	require.Equal(t, time.Duration(0), h.percentile(50))
}

func TestLatencyHistogram_Concurrent(t *testing.T) {
	h := newLatencyHistogram()
	const N = 1000
	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < N; j++ {
				h.record(time.Microsecond)
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
	require.Equal(t, int64(10*N), h.count())
}
