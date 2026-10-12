package adaptivefetch

import (
	"github.com/prometheus/client_golang/prometheus"
)

// metrics groups the prometheus collectors used by State.
//
// All collectors are registered with the supplied registry at construction
// time so the caller chooses between the global promauto registry (via
// prometheus.DefaultRegisterer) and a private one (for tests).
//
// Only signals that carry real, varying data are exported. The state
// machine also tracks per-observation hit rate and missing-fetch counts
// internally (see Observation / maybeTransition), but until the validation
// hot paths plumb through real LocalHits / MissingFetches counts those
// values are synthetic (LocalHits=TotalTxs, MissingFetches=0) and would
// publish a permanently-perfect, meaningless series. They are therefore
// deliberately NOT exported as metrics yet — add them here when real counts
// are plumbed through.
type metrics struct {
	modeGauge   *prometheus.GaugeVec
	transitions *prometheus.CounterVec
}

// registerOrReuse registers c with reg. If the metric is already registered,
// it returns the previously-registered collector of the same type. This allows
// production code to call New() more than once against prometheus.DefaultRegisterer
// (e.g. in tests that call server.New() in multiple subtests) without panicking.
func registerOrReuse[C prometheus.Collector](reg prometheus.Registerer, c C) C {
	if err := reg.Register(c); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return are.ExistingCollector.(C) //nolint:forcetypeassert // same type was registered
		}
		panic(err) // unexpected error — surface it
	}
	return c
}

func newMetrics(serviceName string, reg prometheus.Registerer) *metrics {
	m := &metrics{
		modeGauge: registerOrReuse(reg, prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "teranode",
				Subsystem: "adaptive_fetch",
				Name:      "mode",
				Help:      "Current adaptive fetch mode (0=pessimistic, 1=optimistic), by service.",
			},
			[]string{"service"},
		)),
		transitions: registerOrReuse(reg, prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "teranode",
				Subsystem: "adaptive_fetch",
				Name:      "mode_transitions_total",
				Help:      "Count of mode transitions, by service and direction.",
			},
			[]string{"service", "from", "to"},
		)),
	}
	// Initialise all series for this service so dashboards show a line even before first Record.
	// Gauges (Set) and counters (Add) are genuine no-ops at zero, so they are safe to initialise.
	m.modeGauge.WithLabelValues(serviceName).Set(0)
	m.transitions.WithLabelValues(serviceName, ModePessimistic.String(), ModeOptimistic.String()).Add(0)
	m.transitions.WithLabelValues(serviceName, ModeOptimistic.String(), ModePessimistic.String()).Add(0)
	return m
}
