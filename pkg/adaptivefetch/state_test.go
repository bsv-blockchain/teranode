package adaptivefetch

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestNew_ReturnsPessimisticByDefault(t *testing.T) {
	s, err := New(Config{
		WindowSize:                10,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeAuto,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)
	require.Equal(t, ModePessimistic, s.Mode())
	require.False(t, s.ShouldSkipSubtreeData())
	require.False(t, s.Armed())
}

// TestNew_AlwaysStartsPessimisticUntilArmed pins the cold-start safety
// invariant: regardless of BootstrapMode, a freshly constructed State is
// pinned pessimistic and unarmed. Optimism is only applied by Arm (tripped by
// the service on first FSM RUNNING). This guarantees no subtreeData skipping
// during cold-start IBD.
func TestNew_AlwaysStartsPessimisticUntilArmed(t *testing.T) {
	for _, bootstrap := range []Mode{ModeAuto, ModeOptimistic, ModePessimistic} {
		t.Run(bootstrap.String(), func(t *testing.T) {
			s, err := New(Config{
				WindowSize:                10,
				PessToOptHitRateThreshold: 0.99,
				OptToPessMissThreshold:    100,
				OptToPessAvgMissThreshold: 10,
				BootstrapMode:             bootstrap,
			}, "test", prometheus.NewRegistry())
			require.NoError(t, err)
			require.Equal(t, ModePessimistic, s.Mode(),
				"must start pessimistic before Arm regardless of BootstrapMode")
			require.False(t, s.ShouldSkipSubtreeData())
			require.False(t, s.Armed())
		})
	}
}

// TestArm_OptimisticBootstrap_SwitchesOnArm verifies a ModeOptimistic bootstrap
// stays pessimistic until armed, then switches to optimistic immediately.
func TestArm_OptimisticBootstrap_SwitchesOnArm(t *testing.T) {
	s, err := New(Config{
		WindowSize:                10,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeOptimistic,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)
	require.Equal(t, ModePessimistic, s.Mode(), "pessimistic until armed")

	s.Arm()
	require.True(t, s.Armed())
	require.Equal(t, ModeOptimistic, s.Mode(), "optimistic bootstrap applies on Arm")
	require.True(t, s.ShouldSkipSubtreeData())
}

// TestArm_AutoDoesNotTransitionBeforeArm verifies the latch: with ModeAuto, no
// number of perfect observations can flip Pess→Opt until Arm is called.
func TestArm_AutoDoesNotTransitionBeforeArm(t *testing.T) {
	s, err := New(Config{
		WindowSize:                3,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeAuto,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)

	// Many full windows of perfect observations must NOT flip while unarmed.
	for i := 0; i < 30; i++ {
		s.Record(Observation{TotalTxs: 1000, LocalHits: 1000, MissingFetches: 0})
		require.Equal(t, ModePessimistic, s.Mode(),
			"unarmed auto must stay pessimistic at observation %d", i)
	}

	// Arming clears the pre-arm window, so it takes a fresh full window to flip.
	s.Arm()
	require.Equal(t, ModePessimistic, s.Mode(), "Arm clears window; not instantly optimistic")
	s.Record(Observation{TotalTxs: 1000, LocalHits: 1000, MissingFetches: 0})
	s.Record(Observation{TotalTxs: 1000, LocalHits: 1000, MissingFetches: 0})
	require.Equal(t, ModePessimistic, s.Mode(), "two-of-three after Arm must not flip")
	s.Record(Observation{TotalTxs: 1000, LocalHits: 1000, MissingFetches: 0})
	require.Equal(t, ModeOptimistic, s.Mode(), "fresh full window after Arm flips Pess→Opt")
}

// TestArm_ClearsWindow_NoInstantFlip explicitly guards that observations
// gathered while pinned (e.g. fake-perfect IBD samples) cannot instantly
// satisfy the Pess→Opt threshold the moment the node is armed.
func TestArm_ClearsWindow_NoInstantFlip(t *testing.T) {
	s, err := New(Config{
		WindowSize:                3,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeAuto,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)

	// Fill a full window while unarmed.
	for i := 0; i < 3; i++ {
		s.Record(Observation{TotalTxs: 1000, LocalHits: 1000, MissingFetches: 0})
	}
	require.Equal(t, ModePessimistic, s.Mode())

	// Arm must NOT see the pre-arm full window as a satisfied threshold.
	s.Arm()
	require.Equal(t, ModePessimistic, s.Mode(),
		"pre-arm window must be discarded on Arm; no instant flip")
}

// TestArm_Idempotent verifies Arm is a one-way latch safe to call repeatedly.
func TestArm_Idempotent(t *testing.T) {
	s, err := New(Config{
		WindowSize:                10,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeOptimistic,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)

	s.Arm()
	s.Arm()
	s.Arm()
	require.True(t, s.Armed())
	require.Equal(t, ModeOptimistic, s.Mode(), "repeated Arm is stable")
}

// TestArm_NilReceiver locks in the nil-safe contract for Arm/Armed.
func TestArm_NilReceiver(t *testing.T) {
	var s *State
	require.NotPanics(t, func() {
		s.Arm()
	})
	require.False(t, s.Armed())
}

func TestNew_RejectsInvalidConfig(t *testing.T) {
	base := Config{
		WindowSize:                10,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeAuto,
	}

	cases := []struct {
		name   string
		mutate func(*Config)
		needle string
	}{
		{"zero window", func(c *Config) { c.WindowSize = 0 }, "WindowSize"},
		{"negative window", func(c *Config) { c.WindowSize = -1 }, "WindowSize"},
		{"hit rate below zero", func(c *Config) { c.PessToOptHitRateThreshold = -0.1 }, "PessToOptHitRateThreshold"},
		{"hit rate above one", func(c *Config) { c.PessToOptHitRateThreshold = 1.1 }, "PessToOptHitRateThreshold"},
		{"negative miss threshold", func(c *Config) { c.OptToPessMissThreshold = -1 }, "OptToPessMissThreshold"},
		{"zero miss threshold", func(c *Config) { c.OptToPessMissThreshold = 0 }, "OptToPessMissThreshold"},
		{"negative avg miss threshold", func(c *Config) { c.OptToPessAvgMissThreshold = -1 }, "OptToPessAvgMissThreshold"},
		{"zero avg miss threshold", func(c *Config) { c.OptToPessAvgMissThreshold = 0 }, "OptToPessAvgMissThreshold"},
		{"invalid bootstrap mode", func(c *Config) { c.BootstrapMode = Mode(99) }, "BootstrapMode"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			tc.mutate(&c)
			_, err := New(c, "test", prometheus.NewRegistry())
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.needle)
		})
	}
}

func TestRecord_PessToOpt_HighHitRateFullWindow(t *testing.T) {
	// BootstrapMode=Auto enables transitions once armed; pinned pessimistic
	// would not transition (TestBootstrapMode_PinnedPessimisticDoesNotTransition).
	s, err := New(Config{
		WindowSize:                5,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeAuto,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)
	s.Arm()

	for i := 0; i < 4; i++ {
		s.Record(Observation{TotalTxs: 1000, LocalHits: 1000, MissingFetches: 0})
		require.Equal(t, ModePessimistic, s.Mode(), "block %d: window not full, must stay pessimistic", i)
	}

	s.Record(Observation{TotalTxs: 1000, LocalHits: 1000, MissingFetches: 0})
	require.Equal(t, ModeOptimistic, s.Mode())
}

func TestRecord_PessStays_WhenHitRateBelowThreshold(t *testing.T) {
	// BootstrapMode=Auto, armed, so transitions are enabled; the test verifies
	// the threshold gates the transition (hit rate < threshold = no transition).
	s, err := New(Config{
		WindowSize:                3,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeAuto,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)
	s.Arm()

	s.Record(Observation{TotalTxs: 1000, LocalHits: 1000})
	s.Record(Observation{TotalTxs: 1000, LocalHits: 1000})
	s.Record(Observation{TotalTxs: 1000, LocalHits: 950})
	require.Equal(t, ModePessimistic, s.Mode())
}

func TestRecord_OptToPess_SingleBadBlockTrips(t *testing.T) {
	s, err := New(Config{
		WindowSize:                10,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeOptimistic,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)
	s.Arm()
	require.Equal(t, ModeOptimistic, s.Mode())

	s.Record(Observation{TotalTxs: 10000, LocalHits: 9800, MissingFetches: 200})
	require.Equal(t, ModePessimistic, s.Mode(), "single block with 200 misses must trip immediately")
}

func TestRecord_OptStays_WhenMissesBelowSingleBlockThreshold(t *testing.T) {
	s, err := New(Config{
		WindowSize:                10,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeOptimistic,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)
	s.Arm()

	s.Record(Observation{TotalTxs: 10000, LocalHits: 9950, MissingFetches: 50})
	require.Equal(t, ModeOptimistic, s.Mode())
}

func TestRecord_OptToPess_RollingAverageTrip(t *testing.T) {
	s, err := New(Config{
		WindowSize:                5,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeOptimistic,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)
	s.Arm()

	for i := 0; i < 4; i++ {
		s.Record(Observation{TotalTxs: 10000, LocalHits: 9980, MissingFetches: 20})
		require.Equal(t, ModeOptimistic, s.Mode(), "block %d: window not full yet", i)
	}
	s.Record(Observation{TotalTxs: 10000, LocalHits: 9980, MissingFetches: 20})
	require.Equal(t, ModePessimistic, s.Mode())
}

// TestRecord_OptToPess_ThresholdBoundaryIsInclusive locks in the documented
// inclusive (>=) threshold semantics for both Opt→Pess trips. A misses count
// or rolling-average exactly equal to the configured threshold MUST trip back
// to pessimistic; otherwise a misconfigured node could sit at threshold-value
// forever without ever recovering. Regression guard for review-round-2.
func TestRecord_OptToPess_ThresholdBoundaryIsInclusive(t *testing.T) {
	t.Run("single-block boundary trips", func(t *testing.T) {
		s, err := New(Config{
			WindowSize:                10,
			PessToOptHitRateThreshold: 0.99,
			OptToPessMissThreshold:    100,
			OptToPessAvgMissThreshold: 1000, // never trips on average
			BootstrapMode:             ModeOptimistic,
		}, "test", prometheus.NewRegistry())
		require.NoError(t, err)
		s.Arm()

		// MissingFetches == OptToPessMissThreshold MUST trip (inclusive).
		s.Record(Observation{TotalTxs: 10000, LocalHits: 9900, MissingFetches: 100})
		require.Equal(t, ModePessimistic, s.Mode(),
			"misses == single-block threshold must trip (inclusive)")
	})

	t.Run("rolling-average boundary trips", func(t *testing.T) {
		s, err := New(Config{
			WindowSize:                5,
			PessToOptHitRateThreshold: 0.99,
			OptToPessMissThreshold:    1000, // never trips on single block
			OptToPessAvgMissThreshold: 10,
			BootstrapMode:             ModeOptimistic,
		}, "test", prometheus.NewRegistry())
		require.NoError(t, err)
		s.Arm()

		// 5 observations of 10 misses each → average exactly 10 → MUST trip.
		for i := 0; i < 4; i++ {
			s.Record(Observation{TotalTxs: 10000, LocalHits: 9990, MissingFetches: 10})
			require.Equal(t, ModeOptimistic, s.Mode(), "block %d: window not full yet", i)
		}
		s.Record(Observation{TotalTxs: 10000, LocalHits: 9990, MissingFetches: 10})
		require.Equal(t, ModePessimistic, s.Mode(),
			"avg-misses == avg-threshold must trip (inclusive)")
	})
}

func TestRecord_ConcurrentIsRaceClean(t *testing.T) {
	s, err := New(Config{
		WindowSize:                64,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    1000,
		OptToPessAvgMissThreshold: 100,
		BootstrapMode:             ModeAuto,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)
	s.Arm()

	const goroutines = 16
	const perGoroutine = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				s.Record(Observation{TotalTxs: 1000, LocalHits: 1000})
				_ = s.ShouldSkipSubtreeData()
				_ = s.Mode()
			}
		}()
	}
	wg.Wait()

	require.Equal(t, ModeOptimistic, s.Mode())
}

// TestArm_ConcurrentWithRecordIsRaceClean exercises Arm racing against
// concurrent Record/Mode calls to prove the latch is race-clean under -race.
func TestArm_ConcurrentWithRecordIsRaceClean(t *testing.T) {
	s, err := New(Config{
		WindowSize:                64,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    1000,
		OptToPessAvgMissThreshold: 100,
		BootstrapMode:             ModeAuto,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)

	const goroutines = 16
	const perGoroutine = 200

	var wg sync.WaitGroup
	wg.Add(goroutines + 1)
	go func() {
		defer wg.Done()
		s.Arm()
	}()
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				s.Record(Observation{TotalTxs: 1000, LocalHits: 1000})
				_ = s.ShouldSkipSubtreeData()
				_ = s.Mode()
				_ = s.Armed()
			}
		}()
	}
	wg.Wait()
	require.True(t, s.Armed())
}

func TestRecord_IgnoresInvalidObservations(t *testing.T) {
	s, err := New(Config{
		WindowSize:                3,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeAuto,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)
	s.Arm()

	// Each of these should be silently dropped — window should stay empty,
	// so a subsequent Pess→Opt should not fire until 3 VALID observations arrive.
	s.Record(Observation{TotalTxs: 0, LocalHits: 0})
	s.Record(Observation{TotalTxs: -5, LocalHits: 10})
	s.Record(Observation{TotalTxs: 100, LocalHits: -1})
	s.Record(Observation{TotalTxs: 100, LocalHits: 200}) // LocalHits > TotalTxs
	s.Record(Observation{TotalTxs: 100, LocalHits: 50, MissingFetches: -1})

	require.Equal(t, ModePessimistic, s.Mode(), "invalid observations must not alter window")

	// Now 3 valid perfect observations must be enough to flip Pess→Opt (WindowSize=3).
	s.Record(Observation{TotalTxs: 100, LocalHits: 100})
	s.Record(Observation{TotalTxs: 100, LocalHits: 100})
	s.Record(Observation{TotalTxs: 100, LocalHits: 100})
	require.Equal(t, ModeOptimistic, s.Mode())
}

func TestRecord_RingBufferWraparound(t *testing.T) {
	s, err := New(Config{
		WindowSize:                3,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    1000, // never triggers
		OptToPessAvgMissThreshold: 1000, // never triggers
		BootstrapMode:             ModeOptimistic,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)
	s.Arm()

	// Write 2×WindowSize observations to force wraparound. Mode should stay optimistic
	// because all observations are clean.
	for i := 0; i < 6; i++ {
		s.Record(Observation{TotalTxs: 100, LocalHits: 100, MissingFetches: 0})
	}
	require.Equal(t, ModeOptimistic, s.Mode())
}

// TestRecordIfMode_DropsCrossModeObservation verifies that an observation
// tagged with one mode is discarded when the live mode has transitioned
// to the other before Record is called. This is the explicit guard against
// concurrent workers writing observations from a previous mode into the
// current mode's rolling window.
func TestRecordIfMode_DropsCrossModeObservation(t *testing.T) {
	s, err := New(Config{
		WindowSize:                3,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeAuto,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)
	s.Arm()

	// Fill window with valid pessimistic observations so the next perfect
	// pessimistic observation will flip the state to optimistic.
	for i := 0; i < 3; i++ {
		s.RecordIfMode(ModePessimistic, Observation{TotalTxs: 100, LocalHits: 100})
	}
	require.Equal(t, ModeOptimistic, s.Mode(),
		"three perfect pessimistic samples must trip Pess→Opt")

	// Now the live mode is optimistic. A late observation tagged as
	// pessimistic (e.g. recorded by a worker that started before the
	// transition) must be dropped — otherwise it would contaminate the
	// optimistic-mode window with synthetic LocalHits that bypass the
	// real OptToPess gates.
	s.RecordIfMode(ModePessimistic, Observation{TotalTxs: 1000, LocalHits: 1000, MissingFetches: 0})
	require.Equal(t, ModeOptimistic, s.Mode(),
		"cross-mode observation must not alter mode")

	// Conversely, an observation tagged with the live mode is recorded.
	// A miss large enough to trip the immediate OptToPess threshold proves
	// the observation actually entered the window.
	s.RecordIfMode(ModeOptimistic, Observation{TotalTxs: 1000, LocalHits: 800, MissingFetches: 200})
	require.Equal(t, ModePessimistic, s.Mode(),
		"matching-mode observation with MissingFetches above threshold must trip Opt→Pess")
}

// TestRecordSyntheticWarmup verifies the shared warm-up recording helper:
// totalTxs<=0 is ignored, LocalHits is derived as totalTxs-missingFetches, and
// the cross-mode guard from RecordIfMode is honoured.
func TestRecordSyntheticWarmup(t *testing.T) {
	s, err := New(Config{
		WindowSize:                3,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeAuto,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)
	s.Arm()

	// totalTxs <= 0 must be a no-op (window stays empty → no flip).
	s.RecordSyntheticWarmup(ModePessimistic, 0, 0)
	s.RecordSyntheticWarmup(ModePessimistic, -10, 0)

	// Three perfect (missingFetches=0) warm-ups fill the window and flip.
	for i := 0; i < 3; i++ {
		s.RecordSyntheticWarmup(ModePessimistic, 1000, 0)
	}
	require.Equal(t, ModeOptimistic, s.Mode(),
		"three perfect synthetic warm-ups must flip Pess→Opt")

	// A warm-up carrying a real miss count above the single-block threshold,
	// tagged with the live (optimistic) mode, must trip back.
	s.RecordSyntheticWarmup(ModeOptimistic, 10000, 200)
	require.Equal(t, ModePessimistic, s.Mode(),
		"warm-up with missingFetches above threshold must trip Opt→Pess")
}

// TestRecordSyntheticWarmup_NilReceiver locks in the nil-safe contract.
func TestRecordSyntheticWarmup_NilReceiver(t *testing.T) {
	var s *State
	require.NotPanics(t, func() {
		s.RecordSyntheticWarmup(ModePessimistic, 100, 0)
	})
}

// TestBootstrapMode_PinnedPessimisticDoesNotTransition pins the design
// invariant that an explicitly-configured ModePessimistic stays pessimistic
// regardless of how many perfect observations arrive, EVEN AFTER Arm. Only
// BootstrapMode=Auto enables the Pess→Opt transition. See PR #745 review.
func TestBootstrapMode_PinnedPessimisticDoesNotTransition(t *testing.T) {
	s, err := New(Config{
		WindowSize:                3,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModePessimistic,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)
	s.Arm() // even armed, pinned pessimistic must not drift
	require.Equal(t, ModePessimistic, s.Mode())

	// Many full windows of perfect observations must NOT trip Pess→Opt.
	for i := 0; i < 30; i++ {
		s.Record(Observation{TotalTxs: 1000, LocalHits: 1000, MissingFetches: 0})
		require.Equal(t, ModePessimistic, s.Mode(),
			"pinned pessimistic must stay pessimistic at observation %d", i)
	}
}

// TestBootstrapMode_PinnedOptimisticStillTripsOptToPess pins the design
// invariant that pinned optimistic deployments retain the Opt→Pess safety
// trip — only Pess→Opt is gated on Auto. A degraded optimistic deployment
// must still self-recover when misses are observed.
func TestBootstrapMode_PinnedOptimisticStillTripsOptToPess(t *testing.T) {
	s, err := New(Config{
		WindowSize:                10,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeOptimistic,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)
	s.Arm()
	require.Equal(t, ModeOptimistic, s.Mode())

	// A single observation at or above OptToPessMissThreshold trips immediately.
	s.Record(Observation{TotalTxs: 10000, LocalHits: 9900, MissingFetches: 100})
	require.Equal(t, ModePessimistic, s.Mode(),
		"pinned optimistic must still trip Opt→Pess on observed misses")
}

// TestTransition_ClearsWindow_NoImmediateBackflip locks in the invariant
// that a mode transition resets the rolling window so the next mode's
// thresholds are computed only from observations collected while in that
// mode. Without this, an Opt→Pess trip would leave the window full of
// optimistic-mode observations (all LocalHits=TotalTxs from the perfect-
// hit synthetic samples) and the very next pessimistic Record would
// instantly satisfy the Pess→Opt threshold and bounce back. Regression
// guard for review-round-6.
func TestTransition_ClearsWindow_NoImmediateBackflip(t *testing.T) {
	s, err := New(Config{
		WindowSize:                3,
		PessToOptHitRateThreshold: 0.99,
		OptToPessMissThreshold:    100,
		OptToPessAvgMissThreshold: 10,
		BootstrapMode:             ModeAuto,
	}, "test", prometheus.NewRegistry())
	require.NoError(t, err)
	s.Arm()

	// Fill window with perfect pessimistic observations → trip Pess→Opt.
	for i := 0; i < 3; i++ {
		s.Record(Observation{TotalTxs: 1000, LocalHits: 1000, MissingFetches: 0})
	}
	require.Equal(t, ModeOptimistic, s.Mode())

	// Trip Opt→Pess immediately with a single big-miss observation.
	// Note: hit-rate = 1.0 (LocalHits=TotalTxs) but MissingFetches=100 still
	// trips the single-block threshold. Without window reset on transition
	// this leaves the window with three hit-rate=1.0 observations.
	s.Record(Observation{TotalTxs: 1000, LocalHits: 1000, MissingFetches: 100})
	require.Equal(t, ModePessimistic, s.Mode())

	// The window must be cleared on transition. A single perfect pessimistic
	// observation must NOT immediately re-trip to optimistic, because the
	// new pessimistic window must fill from scratch (len < WindowSize).
	// Without the reset, the previous three hit-rate=1.0 observations would
	// still be in the ring and the new Record would push the window to full
	// with avgHitRate=1.0, instantly bouncing back to optimistic.
	s.Record(Observation{TotalTxs: 1000, LocalHits: 1000, MissingFetches: 0})
	require.Equal(t, ModePessimistic, s.Mode(),
		"first post-Opt→Pess observation must not back-flip; window must reset")

	// Two more perfect observations to fill the new pessimistic window → flip.
	s.Record(Observation{TotalTxs: 1000, LocalHits: 1000, MissingFetches: 0})
	require.Equal(t, ModePessimistic, s.Mode(),
		"two-of-three observations must not yet flip; window must fill from scratch")
	s.Record(Observation{TotalTxs: 1000, LocalHits: 1000, MissingFetches: 0})
	require.Equal(t, ModeOptimistic, s.Mode(),
		"three perfect pessimistic observations after reset must flip Pess→Opt")
}

// TestRecordIfMode_NilReceiver locks in the nil-safe contract.
func TestRecordIfMode_NilReceiver(t *testing.T) {
	var s *State
	require.NotPanics(t, func() {
		s.RecordIfMode(ModePessimistic, Observation{TotalTxs: 100, LocalHits: 100})
	})
}

// TestParseBootstrapMode covers ParseBootstrapMode's accepted input set,
// case-insensitivity, the empty-string-as-auto convention, and the
// error path for unknown values.
func TestParseBootstrapMode(t *testing.T) {
	cases := []struct {
		in   string
		want Mode
		err  bool
	}{
		{"pessimistic", ModePessimistic, false},
		{"optimistic", ModeOptimistic, false},
		{"auto", ModeAuto, false},
		{"", ModeAuto, false},
		{"Optimistic", ModeOptimistic, false},
		{"nonsense", ModeAuto, true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseBootstrapMode(tc.in)
			if tc.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestNoWallClockOrFSMDependency pins the design invariant that the gate
// is NOT driven by FSM state or wall-clock time. If a future edit imports
// blockchain_api for FSM checks or time for age-based logic inside this
// package, this test's grep-style check fails and forces a review.
//
// Rationale: PR #598 was reverted via PR #647 because clock/FSM gating
// cascaded under load. The adaptive-fetch design deliberately avoids that
// whole class of bug by driving transitions solely from counts. The
// service-layer Arm latch keeps FSM (sync-state) knowledge OUT of this
// package — Arm is a generic trigger, so this invariant still holds.
func TestNoWallClockOrFSMDependency(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)

	forbidden := []string{
		`"time"`,
		"blockchain_api",
		"FSMStateType",
		"time.Now",
		"time.Since",
	}

	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		data, err := os.ReadFile(f)
		require.NoError(t, err)
		src := string(data)
		for _, needle := range forbidden {
			require.NotContainsf(t, src, needle,
				"adaptivefetch package must not reference %q (found in %s). "+
					"See TestNoWallClockOrFSMDependency docstring for why.", needle, f)
		}
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	require.NoError(t, cfg.Validate(), "DefaultConfig must pass Validate")

	// Pin the semantics — changing these values is a behaviour change that
	// should be reviewed explicitly. Not a locked contract, but a speed bump.
	require.Equal(t, 10, cfg.WindowSize)
	require.InDelta(t, 0.99, cfg.PessToOptHitRateThreshold, 0.0001)
	require.Equal(t, 100, cfg.OptToPessMissThreshold)
	require.InDelta(t, 10.0, cfg.OptToPessAvgMissThreshold, 0.0001)
	require.Equal(t, ModeAuto, cfg.BootstrapMode)
}
