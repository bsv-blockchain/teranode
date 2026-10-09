package main

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/services/validator"
)

// telemetrySample is one 1s data point.
type telemetrySample struct {
	At              time.Time
	Goroutines      int
	AeroServerStats map[string]string // parsed from `asadm -e "info statistics"`
}

type telemetry struct {
	mu            sync.Mutex
	samples       []telemetrySample
	stop          chan struct{}
	wg            sync.WaitGroup
	v             *validator.Validator
	containerName string
}

func startTelemetry(ctx context.Context, v *validator.Validator, containerName string) *telemetry {
	t := &telemetry{
		v:             v,
		containerName: containerName,
		stop:          make(chan struct{}),
	}
	t.wg.Add(1)
	go t.run(ctx)
	return t
}

func (t *telemetry) run(ctx context.Context) {
	defer t.wg.Done()
	tick := time.NewTicker(1 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			t.sample()
		case <-t.stop:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (t *telemetry) sample() {
	s := telemetrySample{
		At:         time.Now(),
		Goroutines: runtime.NumGoroutine(),
	}
	if t.containerName != "" {
		s.AeroServerStats = readAerospikeServerStats(t.containerName)
	}
	t.mu.Lock()
	t.samples = append(t.samples, s)
	t.mu.Unlock()
}

func (t *telemetry) close() {
	close(t.stop)
	t.wg.Wait()
}

// readAerospikeServerStats shells `docker exec <name> asadm -e "info statistics"`
// and parses the output into a flat key/value map. Failures are non-fatal
// — we just emit an empty map for that sample.
func readAerospikeServerStats(containerName string) map[string]string {
	cmd := exec.Command("docker", "exec", containerName, "asadm", "-e", "info statistics")
	out, err := cmd.Output()
	if err != nil {
		return map[string]string{}
	}
	m := map[string]string{}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// asadm output mixes key=value pairs separated by ;
		for _, kv := range strings.Split(line, ";") {
			parts := strings.SplitN(strings.TrimSpace(kv), "=", 2)
			if len(parts) == 2 {
				m[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			}
		}
	}
	return m
}

// telemetrySummary captures peak/final values for printing.
type telemetrySummary struct {
	PeakGoroutines   int
	PeakRWInProgress int
	Phases           map[validator.ValidatePhase]validator.PhaseStat
}

func (t *telemetry) summary() telemetrySummary {
	t.mu.Lock()
	defer t.mu.Unlock()
	var sum telemetrySummary
	for _, s := range t.samples {
		if s.Goroutines > sum.PeakGoroutines {
			sum.PeakGoroutines = s.Goroutines
		}
		if rwStr, ok := s.AeroServerStats["rw_in_progress"]; ok {
			if v := atoi(rwStr); v > sum.PeakRWInProgress {
				sum.PeakRWInProgress = v
			}
		}
	}
	if t.v != nil {
		sum.Phases = t.v.PhaseSnapshot()
	}
	return sum
}

func atoi(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return n
		}
		n = n*10 + int(c-'0')
	}
	return n
}

func (s telemetrySummary) format() string {
	var b strings.Builder
	fmt.Fprintf(&b, "Peak goroutines: %d\n", s.PeakGoroutines)
	if s.PeakRWInProgress > 0 {
		fmt.Fprintf(&b, "Aerospike server peak rw_in_progress: %d\n", s.PeakRWInProgress)
	}
	if len(s.Phases) > 0 {
		fmt.Fprintln(&b, "Per-phase wall time (cumulative across run):")
		order := []validator.ValidatePhase{
			validator.PhaseGetParents,
			validator.PhaseCPU,
			validator.PhaseSpend,
			validator.PhaseCreate,
			validator.PhaseBlockAssembly,
			validator.PhaseSetLocked,
		}
		var total int64
		for _, p := range order {
			total += s.Phases[p].TotalNs
		}
		for _, p := range order {
			st := s.Phases[p]
			pct := 0.0
			if total > 0 {
				pct = float64(st.TotalNs) * 100.0 / float64(total)
			}
			fmt.Fprintf(&b, "  %-22s count=%-10d total=%-14s (%.1f%%)\n",
				phaseName(p), st.Count, time.Duration(st.TotalNs), pct)
		}
	}
	return b.String()
}

func phaseName(p validator.ValidatePhase) string {
	switch p {
	case validator.PhaseGetParents:
		return "PhaseGetParents"
	case validator.PhaseCPU:
		return "PhaseCPU"
	case validator.PhaseSpend:
		return "PhaseSpend"
	case validator.PhaseCreate:
		return "PhaseCreate"
	case validator.PhaseBlockAssembly:
		return "PhaseBlockAssembly"
	case validator.PhaseSetLocked:
		return "PhaseSetLocked"
	default:
		return fmt.Sprintf("Phase(%d)", p)
	}
}
