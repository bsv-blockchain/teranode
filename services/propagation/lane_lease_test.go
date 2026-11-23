package propagation

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

// noLease is a lane position source with the allocator's Start and Ready that holds a lease only
// once leased is set.
type noLease struct {
	leased atomic.Bool
	n      atomic.Uint64
}

func (s *noLease) Start(context.Context) error { return nil }

func (s *noLease) Ready() error {
	if !s.leased.Load() {
		return errors.NewServiceUnavailableError("no lane lease yet")
	}

	return nil
}

func (s *noLease) Next(context.Context) (uint64, error) {
	if !s.leased.Load() {
		return lane.None, errors.NewServiceUnavailableError("no lane lease")
	}

	return lane.Pack(6, s.n.Add(1)), nil
}

// newLaneLeasePropagation is a propagation server with a local validator over a SQLite store whose
// lane position source is source.
func newLaneLeasePropagation(t *testing.T, source *noLease) (*PropagationServer, *sql.Store) {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Propagation.GRPCListenAddress = ""
	tSettings.Propagation.HTTPListenAddress = ""
	tSettings.BlockAssembly.Disabled = true

	storeURL, err := url.Parse("sqlitememory:///lanelease")
	require.NoError(t, err)

	store, err := sql.New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)

	_ = store.SetBlockHeight(101)
	_ = store.SetMedianBlockTime(1700000000)

	taker, ok := utxo.As[utxo.LanePositionTaker](store)
	require.True(t, ok)
	taker.SetLanePositionSource(source)

	v, err := validator.New(ctx, logger, tSettings, store, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	return &PropagationServer{logger: logger, validator: v, settings: tSettings}, store
}

// Propagation's readiness asks its local validator, so a pod whose validator holds no lane lease is
// not ready and gets no traffic.
func TestPropagationIsNotReadyWhenItsLocalValidatorHasNoLease(t *testing.T) {
	source := &noLease{}
	ps, _ := newLaneLeasePropagation(t, source)

	status, msg, _ := ps.Health(context.Background(), false)
	require.Equal(t, http.StatusServiceUnavailable, status)
	require.Contains(t, msg, "LaneLease: BAD")

	source.leased.Store(true)

	_, msg, _ = ps.Health(context.Background(), false)
	require.Contains(t, msg, "LaneLease: GOOD")
}

// A create that gets no lane position fails with service unavailable through the validator's wrap,
// and propagation answers 503, so a client resubmits rather than drops the transaction.
func TestPropagationAnswers503WhenACreateGetsNoPosition(t *testing.T) {
	source := &noLease{}
	ps, store := newLaneLeasePropagation(t, source)

	privKey, pubKey := bec.PrivateKeyFromBytes([]byte("THIS_IS_A_DETERMINISTIC_PRIVATE_KEY"))

	coinbase := transactions.Create(t,
		transactions.WithCoinbaseData(100, "/Test miner/"),
		transactions.WithP2PKHOutputs(1, 50e6, pubKey),
	)

	source.leased.Store(true)
	_, err := store.Create(context.Background(), coinbase, 1)
	require.NoError(t, err)
	source.leased.Store(false)

	tx := transactions.Create(t,
		transactions.WithPrivateKey(privKey),
		transactions.WithInput(coinbase, 0),
		transactions.WithP2PKHOutputs(1, 1000),
	)

	rec := httptest.NewRecorder()
	c := echo.New().NewContext(httptest.NewRequest(http.MethodPost, "/tx", bytes.NewBuffer(tx.ExtendedBytes())), rec)
	require.NoError(t, ps.handleSingleTx(context.Background())(c))

	require.Equal(t, http.StatusServiceUnavailable, rec.Code, "body: %s", rec.Body.String())
	// The control: with a lease, the same transaction is accepted, so the 503 was the missing position's.
	source.leased.Store(true)

	rec = httptest.NewRecorder()
	c = echo.New().NewContext(httptest.NewRequest(http.MethodPost, "/tx", bytes.NewBuffer(tx.ExtendedBytes())), rec)
	require.NoError(t, ps.handleSingleTx(context.Background())(c))
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
}
