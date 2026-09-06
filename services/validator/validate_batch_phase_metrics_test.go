package validator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPhaseMetrics_AddAndRead(t *testing.T) {
	m := &phaseMetrics{}
	m.add(PhaseGetParents, 10*time.Millisecond)
	m.add(PhaseGetParents, 5*time.Millisecond)
	m.add(PhaseSpend, 7*time.Millisecond)

	snap := m.snapshot()
	require.Equal(t, int64(2), snap[PhaseGetParents].Count)
	require.Equal(t, int64(15_000_000), snap[PhaseGetParents].TotalNs)
	require.Equal(t, int64(1), snap[PhaseSpend].Count)
	require.Equal(t, int64(7_000_000), snap[PhaseSpend].TotalNs)
}

func TestPhaseSnapshot_PopulatedAfterValidateBatch(t *testing.T) {
	v := newValidatorForTest(t)
	v.settings.Validator.UseBatchValidation = true
	// Empty batch is a no-op (returns immediately) — must NOT panic and
	// must NOT populate counters because no phase runs.
	results, err := v.ValidateBatch(context.Background(), nil, 0)
	require.NoError(t, err)
	require.Len(t, results, 0)
	snap := v.PhaseSnapshot()
	require.Equal(t, int64(0), snap[PhaseGetParents].Count)
}

func TestPhaseMetrics_ConcurrentSafe(t *testing.T) {
	m := &phaseMetrics{}
	const N = 1000
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < N; i++ {
				m.add(PhaseSpend, time.Microsecond)
			}
		}()
	}
	wg.Wait()
	snap := m.snapshot()
	require.Equal(t, int64(8*N), snap[PhaseSpend].Count)
	require.Equal(t, int64(8*N*1000), snap[PhaseSpend].TotalNs)
}
