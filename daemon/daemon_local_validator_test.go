package daemon

import (
	"context"
	"net/url"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/stretchr/testify/require"
)

func mustParseURL(t *testing.T, s string) *url.URL {
	t.Helper()

	u, err := url.Parse(s)
	require.NoError(t, err)

	return u
}

// recordingProducer is a KafkaAsyncProducerI test double that counts Stop()
// calls so the construction-error cleanup path can be observed without relying
// on unexported kafka internals.
type recordingProducer struct {
	mu    sync.Mutex
	stops int
}

func (r *recordingProducer) Start(_ context.Context, _ chan *kafka.Message) {}

func (r *recordingProducer) Stop() error {
	r.mu.Lock()
	r.stops++
	r.mu.Unlock()

	return nil
}

func (r *recordingProducer) BrokersURL() []string             { return nil }
func (r *recordingProducer) Publish(_ *kafka.Message)         {}
func (r *recordingProducer) TryPublish(_ *kafka.Message) bool { return false }

func (r *recordingProducer) stopCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.stops
}

// Test_GetValidatorClient_LocalValidator_StopsOrphanedProducersOnError proves the
// orphan-cleanup path actually runs: when local-validator construction fails
// after some producers were created, those already-created producers are
// Stop()ped (not leaked) and nothing is memoized/retained.
//
// It substitutes the producer factory seam with recording doubles and injects a
// failure at the third producer, leaving txmeta + rejectedTx as orphans. The
// test FAILS if the cleanup defer is removed (the stop counts stay at 0), so it
// is discriminating for the fix rather than a false positive.
func Test_GetValidatorClient_LocalValidator_StopsOrphanedProducersOnError(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	appSettings := settings.NewSettings()
	appSettings.Validator.UseLocalValidator = true

	d := &Stores{}

	// GetUtxoStore runs before the producers; pre-seed it (real sqlitememory
	// store, no containers) so construction reaches the producer/orphan path.
	utxoStore, err := utxosql.New(ctx, logger, appSettings, mustParseURL(t, "sqlitememory:///lv_orphan_seam"))
	require.NoError(t, err)

	d.mainUtxoStore = utxoStore

	txmeta := &recordingProducer{}
	rejectedTx := &recordingProducer{}

	orig := localValidatorKafkaProducers
	t.Cleanup(func() { localValidatorKafkaProducers = orig })

	// Simulate a failure at the policy-rejected (3rd) producer: txmeta and
	// rejectedTx were already created and must be Stopped by the cleanup defer.
	localValidatorKafkaProducers = func(_ context.Context, _ ulogger.Logger, _ *settings.Settings) (
		kafka.KafkaAsyncProducerI, kafka.KafkaAsyncProducerI, kafka.KafkaAsyncProducerI, error,
	) {
		return txmeta, rejectedTx, nil, errors.NewServiceError("injected policy-rejected producer failure")
	}

	client, err := d.GetValidatorClient(ctx, logger, appSettings)
	require.Error(t, err)
	require.Nil(t, client)

	require.Equal(t, 1, txmeta.stopCount(), "orphaned txmeta producer must be Stopped on construction failure")
	require.Equal(t, 1, rejectedTx.stopCount(), "orphaned rejectedTx producer must be Stopped on construction failure")

	require.Nil(t, d.mainValidatorClient, "no validator should be memoized on construction failure")
	require.Empty(t, d.constructedClients, "no client should be retained on construction failure")
}

// Test_localValidatorKafkaProducers_ReturnsPartialOnError verifies the producer
// factory hands the already-created producer(s) back to the caller on a later
// creation failure — without which the caller's deferred cleanup would have
// nothing to Stop. The txmeta producer is creatable (in-memory) but rejectedTx
// has no config, so creation fails at the second producer.
func Test_localValidatorKafkaProducers_ReturnsPartialOnError(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	appSettings := settings.NewSettings()
	appSettings.Kafka.TxMetaConfig = mustParseURL(t, "memory://txmeta")
	appSettings.Kafka.RejectedTxConfig = nil

	txmeta, rejectedTx, policyRejected, err := localValidatorKafkaProducers(ctx, logger, appSettings)
	require.Error(t, err)
	require.ErrorContains(t, err, "rejectedTx")
	require.NotNil(t, txmeta, "the already-created txmeta producer must be returned so the caller can Stop it")
	require.Nil(t, rejectedTx)
	require.Nil(t, policyRejected)

	// Stop the real returned producer so the test leaks nothing.
	require.NoError(t, txmeta.Stop())
}
