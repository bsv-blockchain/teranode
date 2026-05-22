package tracing

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestOverrideSampler_NoOverride_DelegatesToAlwaysSample(t *testing.T) {
	sampler := newOverrideSampler(sdktrace.AlwaysSample())

	result := sampler.ShouldSample(sdktrace.SamplingParameters{
		ParentContext: context.Background(),
		TraceID:       trace.TraceID{1, 2, 3},
		Name:          "test-span",
	})

	assert.Equal(t, sdktrace.RecordAndSample, result.Decision)
}

func TestOverrideSampler_NoOverride_DelegatesToNeverSample(t *testing.T) {
	sampler := newOverrideSampler(sdktrace.NeverSample())

	result := sampler.ShouldSample(sdktrace.SamplingParameters{
		ParentContext: context.Background(),
		TraceID:       trace.TraceID{1, 2, 3},
		Name:          "test-span",
	})

	assert.Equal(t, sdktrace.Drop, result.Decision)
}

func TestOverrideSampler_OverrideAlwaysSample(t *testing.T) {
	// Base sampler would drop, but override forces sampling
	sampler := newOverrideSampler(sdktrace.NeverSample())

	rate := 1.0
	ctx := context.WithValue(context.Background(), sampleRateOverrideKey{}, &rate)

	result := sampler.ShouldSample(sdktrace.SamplingParameters{
		ParentContext: ctx,
		TraceID:       trace.TraceID{1, 2, 3},
		Name:          "test-span",
	})

	assert.Equal(t, sdktrace.RecordAndSample, result.Decision)
}

func TestOverrideSampler_OverrideNeverSample(t *testing.T) {
	// Base sampler would record, but override suppresses sampling
	sampler := newOverrideSampler(sdktrace.AlwaysSample())

	rate := 0.0
	ctx := context.WithValue(context.Background(), sampleRateOverrideKey{}, &rate)

	result := sampler.ShouldSample(sdktrace.SamplingParameters{
		ParentContext: ctx,
		TraceID:       trace.TraceID{1, 2, 3},
		Name:          "test-span",
	})

	assert.Equal(t, sdktrace.Drop, result.Decision)
}

func TestOverrideSampler_OverrideIgnoresParentDecision(t *testing.T) {
	// ParentBased with NeverSample base — would normally drop child spans
	// when parent wasn't sampled. Override should force sampling anyway.
	baseSampler := sdktrace.ParentBased(sdktrace.NeverSample())
	sampler := newOverrideSampler(baseSampler)

	rate := 1.0
	ctx := context.WithValue(context.Background(), sampleRateOverrideKey{}, &rate)

	result := sampler.ShouldSample(sdktrace.SamplingParameters{
		ParentContext: ctx,
		TraceID:       trace.TraceID{1, 2, 3},
		Name:          "test-span",
	})

	assert.Equal(t, sdktrace.RecordAndSample, result.Decision)
}

func TestOverrideSampler_OverrideRatio(t *testing.T) {
	sampler := newOverrideSampler(sdktrace.NeverSample())

	rate := 0.5
	ctx := context.WithValue(context.Background(), sampleRateOverrideKey{}, &rate)

	sampled := 0
	total := 10000

	for i := 0; i < total; i++ {
		// TraceIDRatioBased compares binary.BigEndian.Uint64(traceID[8:16]) >> 1
		// against a threshold derived from the ratio, so we need trace IDs that
		// span the full uint64 range in bytes 8-15.
		traceID := trace.TraceID{}
		val := uint64(i) * (^uint64(0) / uint64(total))
		binary.BigEndian.PutUint64(traceID[8:], val)

		result := sampler.ShouldSample(sdktrace.SamplingParameters{
			ParentContext: ctx,
			TraceID:       traceID,
			Name:          "test-span",
		})

		if result.Decision == sdktrace.RecordAndSample {
			sampled++
		}
	}

	ratio := float64(sampled) / float64(total)
	require.InDelta(t, 0.5, ratio, 0.1, "expected ~50%% sampling, got %.2f%%", ratio*100)
}

func TestOverrideSampler_Description(t *testing.T) {
	sampler := newOverrideSampler(sdktrace.AlwaysSample())

	desc := sampler.Description()
	assert.Contains(t, desc, "OverrideSampler")
	assert.Contains(t, desc, "AlwaysOnSampler")
}
