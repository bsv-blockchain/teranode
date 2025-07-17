package tracing

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func TestUTracer_WithError(t *testing.T) {
	gocore.SetInfo("name", "v0.1.2b", "76b9cdd7e5ff85b62f6fec6cc20cfe02b4a12c17")

	tSettings := test.CreateBaseTestSettings()
	tSettings.TracingSampleRate = 1.0

	err := InitTracer(tSettings)
	require.NoError(t, err)

	defer func() {
		_ = ShutdownTracer(context.Background())
	}()

	logger := newLineLogger()

	tracer := Tracer("test-service")

	// Start span
	_, _, endFn := tracer.Start(context.Background(), "TestOperationWithError",
		WithLogMessage(logger, "Processing operation"),
	)

	// Simulate error
	testErr := errors.NewProcessingError("test error occurred")

	// End with error
	endFn(testErr)

	// Verify error logging
	assert.Contains(t, logger.lastLog, "Processing operation DONE in")
	assert.Contains(t, logger.lastLog, "with error: PROCESSING (4): test error occurred")
}

func TestUTracer_ChildSpans(t *testing.T) {
	gocore.SetInfo("name", "v0.1.2b", "76b9cdd7e5ff85b62f6fec6cc20cfe02b4a12c17")

	tSettings := test.CreateBaseTestSettings()
	tSettings.TracingSampleRate = 1.0

	err := InitTracer(tSettings)
	require.NoError(t, err)

	defer func() {
		_ = ShutdownTracer(context.Background())
	}()

	tracer := Tracer("test-service", nil)

	// Start parent span
	ctx, parentSpan, endParent := tracer.Start(
		context.Background(),
		"ParentOperation",
		WithTag("TXID", "d286fcdf58754b59691528cf857850d47ed529608b0a6fd8da5317303beffe8b"),
	)

	// Start child span
	_, childSpan, endChild1 := tracer.Start(ctx, "ChildOperation",
		WithTag("child.id", "child-1"),
	)

	// Verify child has parent's stat as parent
	assert.NotNil(t, childSpan)
	assert.NotNil(t, parentSpan)

	// End child
	endChild1()

	// Start another child
	_, _, endChild2 := tracer.Start(ctx, "ChildOperation2")
	endChild2()

	// End parent
	endParent()
}

func TestSimpleTracing(t *testing.T) {
	// skip tracing test, manually run it
	t.Skip()

	// Initialize tracer
	tSettings := test.CreateBaseTestSettings()
	tSettings.TracingSampleRate = 1.0

	err := InitTracer(tSettings)
	require.NoError(t, err)

	defer func() {
		_ = ShutdownTracer(context.Background())
	}()

	logger := ulogger.NewVerboseTestLogger(t)

	tracer := Tracer("test-service")

	ctx, span, endFn := tracer.Start(
		context.Background(),
		"operation 1",
		WithTag("foo", "bar"),
		WithLogMessage(logger, "Starting operation 1"),
	)

	time.Sleep(1 * time.Second)

	_, _, childEndFn := tracer.Start(ctx, "operation 2")

	time.Sleep(1 * time.Second)

	childEndFn()

	span.AddEvent("bang", trace.WithAttributes(attribute.String("foo", "bar")))

	time.Sleep(1 * time.Second)

	endFn()

	time.Sleep(5 * time.Second)
}

type lineLogger struct {
	lastLog string
}

func newLineLogger() *lineLogger {
	return &lineLogger{}
}

func (l *lineLogger) New(service string, options ...ulogger.Option) ulogger.Logger {
	return nil
}
func (l *lineLogger) Duplicate(options ...ulogger.Option) ulogger.Logger { return l }

func (l *lineLogger) LogLevel() int {
	return 0
}
func (l *lineLogger) SetLogLevel(level string) {}

func (l *lineLogger) Debugf(format string, args ...interface{}) {
	l.log("DEBUG", format, args...)
}

func (l *lineLogger) Infof(format string, args ...interface{}) {
	l.log("INFO", format, args...)
}

func (l *lineLogger) Warnf(format string, args ...interface{}) {
	l.log("WARN", format, args...)
}

func (l *lineLogger) Errorf(format string, args ...interface{}) {
	l.log("ERROR", format, args...)
}

func (l *lineLogger) Fatalf(format string, args ...interface{}) {
	l.log("FATAL", format, args...)
}

func (l *lineLogger) log(_ string, format string, args ...interface{}) {
	l.lastLog = fmt.Sprintf(format, args...)
}
