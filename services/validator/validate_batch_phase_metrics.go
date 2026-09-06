package validator

import (
	"sync/atomic"
	"time"
)

// phaseMetrics holds atomic per-phase counters for the validator's
// ValidateBatch six-phase pipeline. Updates are atomic adds; snapshot
// reads atomic loads. Sub-nanosecond per call; no allocation.
//
// The accessor PhaseSnapshot is exported so external diagnostic tools
// (the loadtest binary) can read counts/total nanoseconds at the end
// of a run without touching internal state.
type phaseMetrics struct {
	count   [phaseMax]atomic.Int64
	totalNs [phaseMax]atomic.Int64
}

// phaseMax is one past the last valid ValidatePhase constant. Must
// be kept in sync with ValidatePhase additions.
const phaseMax = int(PhaseSetLocked) + 1

func (m *phaseMetrics) add(phase ValidatePhase, d time.Duration) {
	if int(phase) >= phaseMax {
		return
	}
	m.count[phase].Add(1)
	m.totalNs[phase].Add(int64(d))
}

// PhaseStat is the per-phase view returned by PhaseSnapshot.
type PhaseStat struct {
	Count   int64
	TotalNs int64
}

func (m *phaseMetrics) snapshot() [phaseMax]PhaseStat {
	var s [phaseMax]PhaseStat
	for i := 0; i < phaseMax; i++ {
		s[i] = PhaseStat{
			Count:   m.count[i].Load(),
			TotalNs: m.totalNs[i].Load(),
		}
	}
	return s
}

// PhaseSnapshot returns the per-phase counters from this Validator's
// most-recent activity. Intended for external diagnostic tools (the
// loadtest binary calls this at end-of-run).
func (v *Validator) PhaseSnapshot() map[ValidatePhase]PhaseStat {
	snap := v.phaseMetrics.snapshot()
	out := make(map[ValidatePhase]PhaseStat, phaseMax)
	for i, s := range snap {
		out[ValidatePhase(i)] = s
	}
	return out
}
