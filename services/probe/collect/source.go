package collect

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

const maxBodyBytes = 64 << 20

type MetricSet map[string]*dto.MetricFamily

type MetricSource interface {
	Gather(ctx context.Context) (MetricSet, error)
}

func (m MetricSet) Sum(name string) (float64, bool) {
	family, found := m[name]
	if !found || len(family.GetMetric()) == 0 {
		return 0, false
	}

	var total float64

	for _, metric := range family.GetMetric() {
		switch family.GetType() {
		case dto.MetricType_COUNTER:
			total += metric.GetCounter().GetValue()
		case dto.MetricType_GAUGE:
			total += metric.GetGauge().GetValue()
		case dto.MetricType_UNTYPED:
			total += metric.GetUntyped().GetValue()
		case dto.MetricType_HISTOGRAM, dto.MetricType_GAUGE_HISTOGRAM:
			total += metric.GetHistogram().GetSampleSum()
		case dto.MetricType_SUMMARY:
			total += metric.GetSummary().GetSampleSum()
		}
	}

	return total, true
}

func merge(dst MetricSet, src map[string]*dto.MetricFamily) {
	for name, family := range src {
		existing, found := dst[name]
		if !found {
			dst[name] = family
			continue
		}

		existing.Metric = append(existing.Metric, family.Metric...)
	}
}

type localSource struct {
	gatherer prometheus.Gatherer
}

func NewLocalSource() MetricSource {
	return newLocalSource(prometheus.DefaultGatherer)
}

func newLocalSource(gatherer prometheus.Gatherer) MetricSource {
	return &localSource{gatherer: gatherer}
}

func (s *localSource) Gather(ctx context.Context) (MetricSet, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	families, err := s.gatherer.Gather()

	set := make(MetricSet, len(families))
	for _, family := range families {
		set[family.GetName()] = family
	}

	return set, err
}

type httpSource struct {
	targets []string
	timeout time.Duration
	client  *http.Client
}

func NewHTTPSource(targets []string, timeout time.Duration) MetricSource {
	return &httpSource{
		targets: append([]string(nil), targets...),
		timeout: timeout,
		client:  &http.Client{Timeout: timeout},
	}
}

type scrapeResult struct {
	families map[string]*dto.MetricFamily
	err      error
}

func (s *httpSource) Gather(ctx context.Context) (MetricSet, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	results := make([]scrapeResult, len(s.targets))

	var wg sync.WaitGroup

	for i, target := range s.targets {
		wg.Add(1)

		go func(i int, target string) {
			defer wg.Done()

			families, err := s.scrape(ctx, target)
			if err != nil {
				err = fmt.Errorf("scrape %s: %w", target, err)
			}

			results[i] = scrapeResult{families: families, err: err}
		}(i, target)
	}

	wg.Wait()

	set := make(MetricSet)

	var errs []error

	for _, result := range results {
		if result.err != nil {
			errs = append(errs, result.err)
			continue
		}

		merge(set, result.families)
	}

	return set, errors.Join(errs...)
}

func (s *httpSource) scrape(ctx context.Context, target string) (map[string]*dto.MetricFamily, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %s", resp.Status)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
	if err != nil {
		return nil, err
	}

	if len(body) > maxBodyBytes {
		return nil, fmt.Errorf("response body exceeds %d bytes", maxBodyBytes)
	}

	parser := expfmt.NewTextParser(model.UTF8Validation)

	return parser.TextToMetricFamilies(bytes.NewReader(body))
}
