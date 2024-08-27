package tracing

import (
	"context"

	"github.com/opentracing/opentracing-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Span struct {
	Ctx               context.Context
	span              opentracing.Span
	otSpan            trace.Span
	opentracingActive bool
	otelActive        bool
}

func Start(ctx context.Context, name string) Span {
	span := Span{
		Ctx: ctx,
	}

	span.opentracingActive = opentracing.IsGlobalTracerRegistered()
	if span.opentracingActive {
		span.span, span.Ctx = opentracing.StartSpanFromContext(span.Ctx, name)
	}

	// check whether otel is active
	span.otelActive = otel.Tracer("") != nil
	if span.otelActive {
		span.Ctx, span.otSpan = otel.Tracer("").Start(span.Ctx, "PeerHandler:HandleBlock")
	}

	return span
}

func (s *Span) SetTag(key, value string) {
	if s.opentracingActive {
		s.span.SetTag(key, value)
	}

	if s.otelActive {
		s.otSpan.SetAttributes(attribute.String(key, value))
	}
}

func (s *Span) RecordError(err error) {
	if s.opentracingActive {
		s.span.SetTag("error", true)
		s.span.LogKV("error", err)
	}

	if s.otelActive {
		s.otSpan.RecordError(err)
	}
}

func (s *Span) Finish() {
	if s.opentracingActive {
		s.span.Finish()
	}

	if s.otelActive {
		s.otSpan.End()
	}
}
