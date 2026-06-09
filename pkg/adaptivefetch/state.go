package adaptivefetch

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// State is the adaptive fetch state machine. Safe for concurrent use.
type State struct {
	mu sync.Mutex
	// mode is the current live mode (pessimistic or optimistic). Auto is a
	// bootstrap-only value and never appears here once New returns.
	mode Mode
	// allowPessToOpt records whether the operator opted into automatic
	// Pess→Opt transitions. Only BootstrapMode=ModeAuto sets this true, and
	// only once Arm has run; pinned ModePessimistic stays pessimistic forever
	// ("always fetch") and pinned ModeOptimistic, having started in optimistic,
	// also never trips Pess→Opt. The Opt→Pess safety trip is always enabled
	// regardless of bootstrap so a degraded optimistic deployment can still
	// self-recover. Rationale: pinned pessimistic is the documented "always
	// fetch subtreeData" safe default; only auto-opted operators get drift.
	allowPessToOpt bool
	// armed gates whether the configured BootstrapMode behaviour has been
	// activated. A freshly constructed State is always pinned pessimistic and
	// unarmed: it cannot drift optimistic no matter how many observations it
	// sees. The integrating service calls Arm exactly once, the first time the
	// node reaches a fully-synced state (blockchain FSM RUNNING), at which
	// point bootstrapIntent is applied. This keeps a node pessimistic through
	// cold-start IBD (LEGACYSYNCING / CATCHINGBLOCKS) and only lets optimism be
	// earned once it has been live. The latch is one-way — it never re-locks,
	// so a later catch-up burst after RUNNING may still use optimistic mode.
	armed bool
	// bootstrapIntent is the configured BootstrapMode, applied by Arm. It is
	// held here rather than acted on in New so optimism is deferred until the
	// node is proven synced.
	bootstrapIntent Mode
	window          []Observation // ring buffer, cap = cfg.WindowSize
	windowHead      int           // next write position
	cfg             Config
	serviceName     string
	metrics         *metrics
}

// New constructs a State with the given Config, a label used for metrics, and
// a prometheus.Registerer to register the two collectors against.
// Pass prometheus.DefaultRegisterer in production and prometheus.NewRegistry()
// in tests to avoid inter-test collector conflicts.
//
// The returned State is ALWAYS pinned pessimistic and unarmed, regardless of
// cfg.BootstrapMode. The configured bootstrap behaviour is deferred until Arm
// is called — see Arm and the armed field. This guarantees a node never skips
// subtreeData during cold-start IBD; optimism must be earned after the node is
// proven synced.
func New(cfg Config, serviceName string, reg prometheus.Registerer) (*State, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	m := newMetrics(serviceName, reg)
	s := &State{
		mode:            ModePessimistic,
		allowPessToOpt:  false,
		bootstrapIntent: cfg.BootstrapMode,
		window:          make([]Observation, 0, cfg.WindowSize),
		cfg:             cfg,
		serviceName:     serviceName,
		metrics:         m,
	}
	s.emitMode()
	return s, nil
}

// Arm activates the configured BootstrapMode behaviour. It is the one-way latch
// that the integrating service trips the first time the node reaches a
// fully-synced state (blockchain FSM RUNNING). Until Arm runs the State is
// pinned pessimistic no matter how many observations it records, so optimistic
// subtreeData skipping can never happen during cold-start IBD
// (LEGACYSYNCING / CATCHINGBLOCKS).
//
// Effect of Arm by configured bootstrap intent:
//   - ModeAuto: enables automatic Pess→Opt transitions (the machine may now
//     warm up to optimistic once a full window clears the hit-rate threshold).
//   - ModeOptimistic: switches to optimistic immediately. The Opt→Pess safety
//     trip remains enabled so a degraded deployment still self-recovers, but
//     Pess→Opt stays disabled (matching pinned-optimistic semantics).
//   - ModePessimistic: no-op; stays pinned pessimistic forever.
//
// On arming, the rolling window is cleared so observations gathered while
// pinned (e.g. the fake-perfect synthetic samples emitted during IBD) cannot
// instantly satisfy the Pess→Opt threshold. The machine re-learns from a clean
// window. Arm is idempotent, never re-locks, and is safe for concurrent use.
// A nil receiver is a no-op.
func (s *State) Arm() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.armed {
		return
	}
	s.armed = true

	// Discard any observations gathered while pinned pessimistic so post-arm
	// assessment starts from a clean window.
	s.window = s.window[:0]
	s.windowHead = 0

	switch s.bootstrapIntent {
	case ModeAuto:
		s.allowPessToOpt = true
	case ModeOptimistic:
		s.mode = ModeOptimistic
		s.emitMode()
	case ModePessimistic:
		// Stays pinned pessimistic — nothing to do.
	}
}

// Armed reports whether Arm has been called. Exposed for tests and metrics.
// A nil receiver returns false.
func (s *State) Armed() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.armed
}

// emitMode updates the mode gauge to reflect s.mode. Callers must hold s.mu
// (or be in a single-threaded context such as New). The prometheus GaugeVec
// is itself concurrent-safe.
func (s *State) emitMode() {
	val := 0.0
	if s.mode == ModeOptimistic {
		val = 1.0
	}
	s.metrics.modeGauge.WithLabelValues(s.serviceName).Set(val)
}

// Mode returns the current mode.
// A nil receiver returns ModePessimistic.
func (s *State) Mode() Mode {
	if s == nil {
		return ModePessimistic
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mode
}

// ShouldSkipSubtreeData reports whether the caller should skip the
// subtreeData download for the block/subtree it is about to process.
// A nil receiver returns false (pessimistic — always fetch subtreeData).
func (s *State) ShouldSkipSubtreeData() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mode == ModeOptimistic
}

// Record adds an observation to the rolling window and may transition modes.
// A nil receiver is a no-op.
func (s *State) Record(obs Observation) {
	s.recordWithMode(obs, false, ModePessimistic)
}

// RecordIfMode is like Record but discards the observation when the
// current mode no longer matches observedAt. It exists to close a
// race in the validation hot paths: callers sample ShouldSkipSubtreeData
// (or Mode) at the start of a unit of work, perform mode-specific work,
// and then call back to record the result. With concurrent workers the
// runtime mode can transition between sample and Record, which would
// otherwise apply a pessimistic-mode observation to the optimistic
// window (or vice versa) and skew transition decisions.
//
// observedAt is the mode the caller saw when it chose its code path.
// If the current mode differs at Record time the observation is dropped
// silently — losing one observation is far cheaper than corrupting the
// rolling window. A nil receiver is a no-op.
func (s *State) RecordIfMode(observedAt Mode, obs Observation) {
	s.recordWithMode(obs, true, observedAt)
}

// RecordSyntheticWarmup records one unit of completed validation work (a block
// in blockvalidation, a subtree in subtreevalidation) as an observation for the
// state machine. It is the single shared recording path for both services, so
// the rationale below lives here once rather than being duplicated at each call
// site.
//
// What "synthetic" means today: the validation hot paths do not yet measure a
// real local-hit rate, so both callers pass missingFetches = 0, which makes
// LocalHits = totalTxs (a fake-perfect sample). With perfect samples the
// Pess→Opt transition is effectively a WindowSize warm-up timer rather than a
// hit-rate measurement, and the automatic Opt→Pess average-miss trip cannot
// fire (the single-observation OptToPessMissThreshold trip still can). This is
// safe: a wrong optimistic guess only costs bandwidth — downstream validation
// still recovers any genuinely missing txs, so correctness is never at risk.
// The mode gauge can lag reality (report optimistic while quietly paying that
// bandwidth) until real counts are wired.
//
// TODO(adaptivefetch): plumb the real recovered-tx count
// (len(missingTxHashesCompacted) in subtreevalidation.ValidateSubtreeInternal)
// as missingFetches so the windowed Opt→Pess auto-recovery measures actual
// locality instead of a constant 0. The missingFetches parameter exists so that
// future change touches only the call sites, not this signature.
//
// observedAt is the mode the caller sampled before its fetch decision; the
// observation is dropped if the live mode has since transitioned (see
// RecordIfMode), keeping cross-mode samples out of the rolling window.
// totalTxs <= 0 is ignored, and a nil receiver is a no-op.
func (s *State) RecordSyntheticWarmup(observedAt Mode, totalTxs, missingFetches int) {
	if totalTxs <= 0 {
		return
	}
	s.RecordIfMode(observedAt, Observation{
		TotalTxs:       totalTxs,
		LocalHits:      totalTxs - missingFetches,
		MissingFetches: missingFetches,
	})
}

// recordWithMode is the shared body of Record and RecordIfMode. When
// requireMode is true and the live mode differs from observedAt, the
// observation is dropped before any window mutation or metric update.
func (s *State) recordWithMode(obs Observation, requireMode bool, observedAt Mode) {
	if s == nil {
		return
	}
	// Defensive: ignore observations with nonsense counts. These should never
	// occur in production but a silently-corrupted observation would skew the
	// rolling average and either block a Pess→Opt transition or spuriously
	// trigger one.
	if obs.TotalTxs <= 0 {
		return
	}
	if obs.LocalHits < 0 || obs.LocalHits > obs.TotalTxs {
		return
	}
	if obs.MissingFetches < 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Mode-snapshot guard: if the caller sampled the mode at decision time
	// and the mode has since transitioned, the observation belongs to the
	// previous mode's window and must not contaminate the current one.
	if requireMode && s.mode != observedAt {
		return
	}

	if len(s.window) < s.cfg.WindowSize {
		s.window = append(s.window, obs)
	} else {
		s.window[s.windowHead] = obs
		s.windowHead = (s.windowHead + 1) % s.cfg.WindowSize
	}

	prev := s.mode
	s.maybeTransition()
	if prev != s.mode {
		// Reset the rolling window on every mode transition. Each mode's
		// thresholds must be evaluated against observations collected while
		// in that mode — leaving stale observations from the previous mode
		// in the ring causes bouncing (e.g. an Opt→Pess trip would leave
		// the window full of perfect-hit-rate optimistic samples, and the
		// very next pessimistic Record would instantly satisfy the
		// Pess→Opt threshold and flip back). See pkg/adaptivefetch/state_test.go
		// TestTransition_ClearsWindow_NoImmediateBackflip for the
		// regression case.
		s.window = s.window[:0]
		s.windowHead = 0
		s.metrics.transitions.WithLabelValues(s.serviceName, prev.String(), s.mode.String()).Inc()
		s.emitMode()
	}
}

func (s *State) maybeTransition() {
	switch s.mode {
	case ModePessimistic:
		// Pess→Opt is only allowed when the operator chose BootstrapMode=auto.
		// Pinned ModePessimistic means "always fetch subtreeData" and must
		// never drift to optimistic. The Opt→Pess safety trip below remains
		// always-on so a degraded optimistic deployment can still recover.
		if !s.allowPessToOpt {
			return
		}
		if len(s.window) < s.cfg.WindowSize {
			return
		}
		if s.avgHitRateLocked() >= s.cfg.PessToOptHitRateThreshold {
			s.mode = ModeOptimistic
		}

	case ModeOptimistic:
		// Threshold semantics are inclusive (>=): a configured threshold
		// value is the *trip point*, not the first value above it. So
		// MissingFetches == OptToPessMissThreshold trips, and an average
		// equal to OptToPessAvgMissThreshold trips. This matches the
		// natural reading of "miss-count threshold of N misses".
		last := s.window[s.lastIndexLocked()]
		if last.MissingFetches >= s.cfg.OptToPessMissThreshold {
			s.mode = ModePessimistic
			return
		}
		if len(s.window) < s.cfg.WindowSize {
			return
		}
		if s.avgMissesLocked() >= s.cfg.OptToPessAvgMissThreshold {
			s.mode = ModePessimistic
		}
	}
}

func (s *State) avgMissesLocked() float64 {
	var sum int
	for _, o := range s.window {
		sum += o.MissingFetches
	}
	return float64(sum) / float64(len(s.window))
}

func (s *State) lastIndexLocked() int {
	if len(s.window) < s.cfg.WindowSize {
		return len(s.window) - 1
	}
	return (s.windowHead - 1 + s.cfg.WindowSize) % s.cfg.WindowSize
}

func (s *State) avgHitRateLocked() float64 {
	var sum float64
	for _, o := range s.window {
		sum += float64(o.LocalHits) / float64(o.TotalTxs)
	}
	return sum / float64(len(s.window))
}
