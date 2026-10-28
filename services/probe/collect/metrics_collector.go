package collect

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/bsv-blockchain/teranode/settings"
)

type MetricsCollector struct {
	src   MetricSource
	rates *Rates
	cfg   *settings.Settings
	now   func() time.Time
}

func NewMetricsCollector(src MetricSource, r *Rates, cfg *settings.Settings) *MetricsCollector {
	return &MetricsCollector{src: src, rates: r, cfg: cfg, now: time.Now}
}

func (c *MetricsCollector) Name() string {
	return "metrics"
}

func (c *MetricsCollector) Collect(ctx context.Context) (snapshot.Patch, error) {
	set, err := c.src.Gather(ctx)
	if err != nil {
		return nil, err
	}

	c.rates.Observe(c.now(), set)
	view := c.rates.frozen()

	return func(s *snapshot.Snapshot) {
		Apply(s, set, view, c.cfg)
	}, err
}
