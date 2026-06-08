package adaptivefetch

import (
	"fmt"
	"strings"
)

// Mode is the state of the adaptive fetch gate.
type Mode int

const (
	// ModePessimistic always fetches subtreeData (safe, higher bandwidth).
	ModePessimistic Mode = 0
	// ModeOptimistic skips subtreeData and recovers missing txs individually.
	ModeOptimistic Mode = 1
	// ModeAuto is a bootstrap-only value meaning "start pessimistic".
	// Must not appear as the runtime mode on State.
	ModeAuto Mode = 2
)

// String is used for metric labels and log messages.
func (m Mode) String() string {
	switch m {
	case ModePessimistic:
		return "pessimistic"
	case ModeOptimistic:
		return "optimistic"
	case ModeAuto:
		return "auto"
	default:
		return fmt.Sprintf("unknown(%d)", int(m))
	}
}

// Config controls how the state machine learns.
type Config struct {
	// WindowSize is how many recent observations are held for rolling-average logic.
	WindowSize int

	// PessToOptHitRateThreshold is the minimum avg local-hit rate (0..1)
	// over a full window that triggers a switch from pessimistic to optimistic.
	PessToOptHitRateThreshold float64

	// OptToPessMissThreshold is the absolute missing-tx count in a single
	// observation that immediately trips back to pessimistic.
	OptToPessMissThreshold int

	// OptToPessAvgMissThreshold is the average missing-tx count over a
	// full window that trips back to pessimistic.
	OptToPessAvgMissThreshold float64

	// BootstrapMode is the initial mode. ModeAuto resolves to ModePessimistic.
	BootstrapMode Mode
}

// ParseBootstrapMode converts a settings string into a Mode. Empty string
// and "auto" both resolve to ModeAuto. Unknown values return (ModeAuto, error)
// so the caller can either surface the error or fall back silently.
func ParseBootstrapMode(s string) (Mode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "auto":
		return ModeAuto, nil
	case "pessimistic":
		return ModePessimistic, nil
	case "optimistic":
		return ModeOptimistic, nil
	default:
		return ModeAuto, fmt.Errorf("adaptivefetch: unknown bootstrap mode %q", s)
	}
}

// DefaultConfig returns the canonical default configuration. Both services
// use this as a fallback when operator-provided settings fail validation,
// and tests use it to build representative State instances without
// duplicating the 6-line literal. Tune thresholds here rather than in
// caller-side literals so the defaults stay in one place.
func DefaultConfig() Config {
	return Config{
		WindowSize:                10,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeAuto,
	}
}

// Validate returns a non-nil error if cfg has nonsense values.
func (c Config) Validate() error {
	if c.WindowSize < 1 {
		return fmt.Errorf("adaptivefetch: WindowSize must be >= 1 (got %d)", c.WindowSize)
	}
	if c.PessToOptHitRateThreshold < 0 || c.PessToOptHitRateThreshold > 1 {
		return fmt.Errorf("adaptivefetch: PessToOptHitRateThreshold must be in [0,1] (got %v)", c.PessToOptHitRateThreshold)
	}
	// Thresholds must be strictly positive: maybeTransition uses >=, so
	// a threshold of 0 trips on the first observation even when no fetches
	// missed, which collapses optimistic mode entirely.
	if c.OptToPessMissThreshold < 1 {
		return fmt.Errorf("adaptivefetch: OptToPessMissThreshold must be >= 1 (got %d)", c.OptToPessMissThreshold)
	}
	if c.OptToPessAvgMissThreshold <= 0 {
		return fmt.Errorf("adaptivefetch: OptToPessAvgMissThreshold must be > 0 (got %v)", c.OptToPessAvgMissThreshold)
	}
	switch c.BootstrapMode {
	case ModePessimistic, ModeOptimistic, ModeAuto:
	default:
		return fmt.Errorf("adaptivefetch: BootstrapMode must be one of Pessimistic/Optimistic/Auto (got %v)", c.BootstrapMode)
	}
	return nil
}
