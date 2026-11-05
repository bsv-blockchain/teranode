package fault_test

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/test/fault"
	"github.com/stretchr/testify/require"
)

var errInjected = errors.NewStorageError("injected")

func key(s string) []byte { return []byte(s) }

// blobStore returns a decorated in-memory blob store holding key "a".
func blobStore(t *testing.T, plan *fault.Plan) *fault.BlobStore {
	t.Helper()

	inner := blobmemory.New()
	require.NoError(t, inner.Set(context.Background(), key("a"), fileformat.FileTypeSubtree, []byte("A")))

	return fault.NewBlobStore(inner, plan)
}

// existsResults calls Exists n times and reports which calls failed.
func existsResults(t *testing.T, s *fault.BlobStore, n int) []bool {
	t.Helper()

	out := make([]bool, n)

	for i := range out {
		_, err := s.Exists(context.Background(), key("a"), fileformat.FileTypeSubtree)
		out[i] = err != nil
	}

	return out
}

func TestTriggers(t *testing.T) {
	tests := []struct {
		name string
		rule func(r *fault.Rule) *fault.Rule
		want []bool
	}{
		{"always", func(r *fault.Rule) *fault.Rule { return r.Always() }, []bool{true, true, true, true}},
		{"nth", func(r *fault.Rule) *fault.Rule { return r.Nth(2) }, []bool{false, true, false, false}},
		{"first n", func(r *fault.Rule) *fault.Rule { return r.FirstN(2) }, []bool{true, true, false, false}},
		{"after n", func(r *fault.Rule) *fault.Rule { return r.AfterN(2) }, []bool{false, false, true, true}},
		{"every", func(r *fault.Rule) *fault.Rule { return r.Every(2) }, []bool{false, true, false, true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := fault.NewPlan()
			tt.rule(plan.On("Exists")).Fail(errInjected)

			require.Equal(t, tt.want, existsResults(t, blobStore(t, plan), len(tt.want)))
		})
	}
}

func TestProbabilityIsReproducibleFromItsSeed(t *testing.T) {
	run := func() []bool {
		plan := fault.NewPlan()
		plan.On("Exists").Probability(0.5, 42).Fail(errInjected)

		return existsResults(t, blobStore(t, plan), 64)
	}

	first := run()
	require.Equal(t, first, run(), "the same seed must inject the same faults")
	require.Contains(t, first, true)
	require.Contains(t, first, false)
}

func TestFailDoesNotCallTheWrappedStore(t *testing.T) {
	plan := fault.NewPlan()
	plan.On("Set").Fail(errInjected)

	inner := blobmemory.New()
	s := fault.NewBlobStore(inner, plan)

	err := s.Set(context.Background(), key("b"), fileformat.FileTypeSubtree, []byte("B"))
	require.ErrorIs(t, err, errInjected)

	exists, err := inner.Exists(context.Background(), key("b"), fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.False(t, exists, "a failed call must not reach the wrapped store")
}

func TestApplyThenFailAppliesTheEffect(t *testing.T) {
	plan := fault.NewPlan()
	plan.On("Set").ApplyThenFail(errInjected)

	inner := blobmemory.New()
	s := fault.NewBlobStore(inner, plan)

	err := s.Set(context.Background(), key("b"), fileformat.FileTypeSubtree, []byte("B"))
	require.ErrorIs(t, err, errInjected, "the caller is told the call failed")

	exists, err := inner.Exists(context.Background(), key("b"), fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, exists, "the wrapped store applied the call")
}

func TestDelay(t *testing.T) {
	plan := fault.NewPlan()
	plan.On("Get").Delay(50 * time.Millisecond)

	s := blobStore(t, plan)

	start := time.Now()
	v, err := s.Get(context.Background(), key("a"), fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.Equal(t, []byte("A"), v)
	require.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
}

func TestBlockHoldsTheCallUntilReleased(t *testing.T) {
	release := make(chan struct{})

	plan := fault.NewPlan()
	plan.On("Get").Block(release)

	s := blobStore(t, plan)

	done := make(chan error, 1)

	go func() {
		_, err := s.Get(context.Background(), key("a"), fileformat.FileTypeSubtree)
		done <- err
	}()

	select {
	case <-done:
		t.Fatal("the call returned before it was released")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the call was not released")
	}
}

func TestBlockEndsOnContextCancellation(t *testing.T) {
	plan := fault.NewPlan()
	plan.On("Get").Block(make(chan struct{}))

	s := blobStore(t, plan)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := s.Get(ctx, key("a"), fileformat.FileTypeSubtree)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestPanic(t *testing.T) {
	plan := fault.NewPlan()
	plan.On("Get").Panic("boom")

	s := blobStore(t, plan)

	require.PanicsWithValue(t, "boom", func() {
		_, _ = s.Get(context.Background(), key("a"), fileformat.FileTypeSubtree)
	})
}

func TestObserveSeesTheCallAndPassesItThrough(t *testing.T) {
	var seen []fault.Call

	plan := fault.NewPlan()
	plan.On("Get").Observe(func(c fault.Call) { seen = append(seen, c) })

	s := blobStore(t, plan)

	v, err := s.Get(context.Background(), key("a"), fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.Equal(t, []byte("A"), v)

	require.Len(t, seen, 1)
	require.Equal(t, "Get", seen[0].Method)
	require.Equal(t, key("a"), seen[0].Args[0], "the context is not recorded; the key is the first argument")
}

func TestWhereFiltersBeforeCounting(t *testing.T) {
	plan := fault.NewPlan()
	plan.On("Exists").
		Where(func(c fault.Call) bool { return string(c.Args[0].([]byte)) == "b" }).
		Nth(1).
		Fail(errInjected)

	s := blobStore(t, plan)

	_, err := s.Exists(context.Background(), key("a"), fileformat.FileTypeSubtree)
	require.NoError(t, err, "a call the filter rejects passes through")

	_, err = s.Exists(context.Background(), key("b"), fileformat.FileTypeSubtree)
	require.ErrorIs(t, err, errInjected, "and does not count towards Nth")
}

func TestFirstMatchingRuleWins(t *testing.T) {
	other := errors.NewProcessingError("second rule")

	plan := fault.NewPlan()
	plan.On("Exists").FirstN(1).Fail(errInjected)
	plan.On("Exists").Fail(other)

	s := blobStore(t, plan)

	_, err := s.Exists(context.Background(), key("a"), fileformat.FileTypeSubtree)
	require.ErrorIs(t, err, errInjected)

	_, err = s.Exists(context.Background(), key("a"), fileformat.FileTypeSubtree)
	require.ErrorIs(t, err, other)
}

func TestRuleWithoutAnActionIsInert(t *testing.T) {
	plan := fault.NewPlan()
	plan.On("Exists").FirstN(1) // never completed with an action
	plan.On("Exists").Nth(1).Fail(errInjected)

	s := blobStore(t, plan)

	_, err := s.Exists(context.Background(), key("a"), fileformat.FileTypeSubtree)
	require.ErrorIs(t, err, errInjected, "an incomplete rule must not shadow the rule after it")
}

func TestRuleSetupErrorsPanic(t *testing.T) {
	plan := fault.NewPlan()

	require.PanicsWithValue(t, `fault: no decorated interface has a method "Exsits"`, func() { plan.On("Exsits") })
	require.PanicsWithValue(t, `fault: blob.Store has no method "AddBlock"`, func() { plan.On("AddBlock").In(fault.StoreBlob) })
	require.PanicsWithValue(t, "fault: Fail on utxo.Store.GetBlockHeight, which returns no error", func() {
		plan.On("GetBlockHeight").Fail(errInjected)
	})
}

// chainStack is a sqlitememory blockchain store, decorated, under a LocalClient, decorated, both
// on one plan.
func chainStack(t *testing.T, plan *fault.Plan) (blockchain.ClientI, *fault.BlockchainStore) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	logger := ulogger.TestLogger{}

	inner, err := blockchainstore.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	store := fault.NewBlockchainStore(inner, plan)

	client, err := blockchain.NewLocalClient(logger, tSettings, store, nil, nil)
	require.NoError(t, err)

	return fault.NewBlockchainClient(client, plan), store
}

func TestSharedPlanLogsCallsAcrossDecoratorsInOrder(t *testing.T) {
	plan := fault.NewPlan()
	client, _ := chainStack(t, plan)

	_, _, err := client.GetBestBlockHeader(context.Background())
	require.NoError(t, err)

	var trace []string

	for i, c := range plan.Calls() {
		require.Equal(t, i+1, c.Seq)
		trace = append(trace, c.Store+"."+c.Method)
	}

	// The client call is logged when it starts, before the store call it makes.
	require.Equal(t, fault.StoreBlockchain+".GetBestBlockHeader", trace[0])
	require.Contains(t, trace[1:], fault.StoreBlockchainStore+".GetBestBlockHeader", "trace: %s", strings.Join(trace, ", "))
}

func TestInNarrowsARuleToOneDecorator(t *testing.T) {
	plan := fault.NewPlan()
	plan.On("GetBestBlockHeader").In(fault.StoreBlockchainStore).Fail(errInjected)

	client, _ := chainStack(t, plan)

	_, _, err := client.GetBestBlockHeader(context.Background())
	require.ErrorIs(t, err, errInjected, "the store fault surfaces through the client")

	calls := plan.CallsTo("GetBestBlockHeader")
	require.Len(t, calls, 2)
	require.Equal(t, fault.StoreBlockchain, calls[0].Store)
	require.False(t, calls[0].Injected, "the client call matched no rule")
	require.Equal(t, fault.StoreBlockchainStore, calls[1].Store)
	require.True(t, calls[1].Injected)
	require.ErrorIs(t, calls[1].Err, errInjected, "the log records the returned error")
}

func TestResetClearsTheLogAndRestartsTriggers(t *testing.T) {
	plan := fault.NewPlan()
	plan.On("Exists").FirstN(1).Fail(errInjected)

	s := blobStore(t, plan)

	require.Equal(t, []bool{true, false}, existsResults(t, s, 2))
	require.Equal(t, 2, plan.Count("Exists"))

	plan.Reset()

	require.Empty(t, plan.Calls())
	require.Equal(t, []bool{true, false}, existsResults(t, s, 2))
}

func TestUTXOStoreUnwrapsForCapabilityLookup(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///fault_unwrap")
	require.NoError(t, err)

	inner, err := utxosql.New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	s := fault.NewUTXOStore(inner, fault.NewPlan())

	found, ok := utxo.As[*utxosql.Store](s)
	require.True(t, ok, "utxo.As must see through the decorator")
	require.Same(t, inner, found)
}
