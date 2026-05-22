package tracing

import (
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// sampleRateOverrideKey is the context key for per-span sample rate overrides.
type sampleRateOverrideKey struct{}

// overrideSampler wraps a base sampler and checks the context for a
// per-span sample rate override. If found, it uses the override rate
// instead of delegating to the base sampler.
type overrideSampler struct {
	base sdktrace.Sampler
}

func newOverrideSampler(base sdktrace.Sampler) sdktrace.Sampler {
	return &overrideSampler{base: base}
}

func (s *overrideSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	rate, ok := p.ParentContext.Value(sampleRateOverrideKey{}).(*float64)
	if !ok || rate == nil {
		return s.base.ShouldSample(p)
	}

	r := *rate
	if r >= 1.0 {
		return sdktrace.AlwaysSample().ShouldSample(p)
	}
	if r <= 0.0 {
		return sdktrace.NeverSample().ShouldSample(p)
	}

	return sdktrace.TraceIDRatioBased(r).ShouldSample(p)
}

func (s *overrideSampler) Description() string {
	return "OverrideSampler{" + s.base.Description() + "}"
}
