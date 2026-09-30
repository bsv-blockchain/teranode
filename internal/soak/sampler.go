// Package soak provides the sampling and trend-analysis primitives for endurance (soak) tests: a sampler that
// records heap and goroutine counts into a time series, and an analyser that fails when that series trends upward
// rather than when it crosses a fixed ceiling.
package soak

import (
	"context"
	"encoding/csv"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
)

// Sample is a single point-in-time reading of the process's memory and goroutine counts, taken after a forced GC.
type Sample struct {
	Elapsed      time.Duration
	HeapInuse    uint64
	HeapAlloc    uint64
	NumGoroutine int
}

// Sampler records a Sample at a fixed interval until it is stopped.
type Sampler struct {
	interval time.Duration

	mu      sync.Mutex
	samples []Sample
	start   time.Time
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewSampler returns a sampler that takes a reading every interval.
func NewSampler(interval time.Duration) *Sampler {
	return &Sampler{interval: interval}
}

// Start takes an initial reading and then samples in the background until ctx is cancelled or Stop is called.
func (s *Sampler) Start(ctx context.Context) {
	ctx, s.cancel = context.WithCancel(ctx)
	s.done = make(chan struct{})
	s.start = time.Now()
	s.record()

	go func() {
		defer close(s.done)

		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.record()
			}
		}
	}()
}

// Stop halts sampling and returns a copy of every sample taken.
func (s *Sampler) Stop() []Sample {
	if s.cancel != nil {
		s.cancel()
		<-s.done
		s.cancel = nil
	}

	return s.Samples()
}

// Samples returns a copy of the samples taken so far.
func (s *Sampler) Samples() []Sample {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]Sample, len(s.samples))
	copy(out, s.samples)

	return out
}

func (s *Sampler) record() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.samples = append(s.samples, TakeSample(time.Since(s.start)))
}

// TakeSample forces a GC so that the heap figures reflect live memory rather than uncollected garbage, then reads
// the heap and goroutine counts.
func TakeSample(elapsed time.Duration) Sample {
	runtime.GC()

	var m runtime.MemStats

	runtime.ReadMemStats(&m)

	return Sample{
		Elapsed:      elapsed,
		HeapInuse:    m.HeapInuse,
		HeapAlloc:    m.HeapAlloc,
		NumGoroutine: runtime.NumGoroutine(),
	}
}

// WriteCSV writes the samples to path as CSV with a header row.
func WriteCSV(path string, samples []Sample) error {
	f, err := os.Create(path)
	if err != nil {
		return errors.NewProcessingError("failed to create soak CSV %s", path, err)
	}

	w := csv.NewWriter(f)

	rows := make([][]string, 0, len(samples)+1)
	rows = append(rows, []string{"elapsed_seconds", "heap_inuse_bytes", "heap_alloc_bytes", "goroutines"})

	for _, s := range samples {
		rows = append(rows, []string{
			strconv.FormatFloat(s.Elapsed.Seconds(), 'f', 3, 64),
			strconv.FormatUint(s.HeapInuse, 10),
			strconv.FormatUint(s.HeapAlloc, 10),
			strconv.Itoa(s.NumGoroutine),
		})
	}

	if err = w.WriteAll(rows); err != nil {
		_ = f.Close()
		return errors.NewProcessingError("failed to write soak CSV %s", path, err)
	}

	if err = f.Close(); err != nil {
		return errors.NewProcessingError("failed to close soak CSV %s", path, err)
	}

	return nil
}
