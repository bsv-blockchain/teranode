package validator

import (
	"net/url"
	"testing"
	"time"

	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestValidateInternalEndsItsSpan pins that validateInternal finishes its own span
// and observes its histogram on the success path, i.e. after the decoupled span is
// started for the spend/store/block-assembly phase.
//
// validateInternal ends its span from a deferred closure that reads the end
// function when the function returns. The decoupled span used to be assigned to the
// same variable, so on every transaction that reached that phase the closure ended
// the decoupled span a second time and validateInternal's span was never ended:
// it was never exported (its children showed up in traces with a missing parent)
// and teranode_validator_transactions_validate_total never got an observation.
func TestValidateInternalEndsItsSpan(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(recorder),
	)

	previousProvider := otel.GetTracerProvider()
	previousEnabled := tracing.IsTracingEnabled()

	otel.SetTracerProvider(tp)
	tracing.SetTracingEnabled(true)

	t.Cleanup(func() {
		tracing.SetTracingEnabled(previousEnabled)
		otel.SetTracerProvider(previousProvider)
		_ = tp.Shutdown(t.Context())
	})

	ctx := t.Context()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.Disabled = true

	utxoStoreURL, err := url.Parse("sqlitememory:///validate_internal_span")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)
	utxoStore.RawDB().SetMaxOpenConns(1)
	require.NoError(t, utxoStore.SetBlockHeight(100))
	require.NoError(t, utxoStore.SetMedianBlockTime(uint32(time.Now().Unix()))) //nolint:gosec

	privateKey, publicKey := bec.PrivateKeyFromBytes([]byte("VALIDATE_INTERNAL_SPAN_TEST_KEY!"))
	parentTx := transactions.Create(t,
		transactions.WithCoinbaseData(1, "/validate internal span test/"),
		transactions.WithP2PKHOutputs(1, 100_000, publicKey),
	)
	_, err = utxoStore.Create(ctx, parentTx, 1, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 1, BlockHeight: 1, SubtreeIdx: 0}))
	require.NoError(t, err)

	tx := transactions.Create(t,
		transactions.WithPrivateKey(privateKey),
		transactions.WithInput(parentTx, 0, privateKey),
		transactions.WithP2PKHOutputs(1, 90_000, publicKey),
	)

	validator, err := New(ctx, logger, tSettings, utxoStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	observationsBefore := validateTotalSampleCount(t)

	_, err = validator.(*Validator).Validate(ctx, tx, 100)
	require.NoError(t, err)

	var (
		validateInternalSpan sdktrace.ReadOnlySpan
		decoupledSpan        sdktrace.ReadOnlySpan
	)

	for _, span := range recorder.Ended() {
		switch span.Name() {
		case "validateInternal":
			validateInternalSpan = span
		case "decoupledSpan":
			decoupledSpan = span
		}
	}

	require.NotNil(t, decoupledSpan, "the success path must reach the decoupled phase, otherwise this test does not cover it")
	require.NotNil(t, validateInternalSpan, "validateInternal span was never ended")
	require.Equal(t, validateInternalSpan.SpanContext().SpanID(), decoupledSpan.Parent().SpanID(),
		"decoupledSpan must be a child of validateInternal")

	require.Equal(t, observationsBefore+1, validateTotalSampleCount(t),
		"teranode_validator_transactions_validate_total must observe the validation")
}

func validateTotalSampleCount(t *testing.T) uint64 {
	t.Helper()

	var m dto.Metric
	require.NoError(t, prometheusTransactionValidateTotal.Write(&m))

	return m.GetHistogram().GetSampleCount()
}
