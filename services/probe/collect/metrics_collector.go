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
}

func NewMetricsCollector(src MetricSource, r *Rates, cfg *settings.Settings) *MetricsCollector {
	return &MetricsCollector{src: src, rates: r, cfg: cfg}
}

func (c *MetricsCollector) Name() string {
	return "metrics"
}

func (c *MetricsCollector) Collect(ctx context.Context) (snapshot.Patch, error) {
	set, err := c.src.Gather(ctx)
	if set == nil || (err != nil && len(set) == 0) {
		return nil, err
	}

	c.rates.Observe(time.Now(), set)
	view := c.rates.frozen()

	return func(s *snapshot.Snapshot) {
		Apply(s, set, view, c.cfg)
	}, err
}
