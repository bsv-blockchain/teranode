package validator

import (
	"context"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	sqlstore "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestEnsureMTPLoaded_ReadersContinueDuringFetch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tSettings := test.CreateBaseTestSettings(t)
	params := *tSettings.ChainCfgParams
	params.CSVHeight = 0
	tSettings.ChainCfgParams = &params
	logger := ulogger.TestLogger{}
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	store, err := sqlstore.New(logger, storeURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	service, err := blockchain.New(ctx, logger, tSettings, store, nil)
	require.NoError(t, err)
	require.NoError(t, service.Init(ctx))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if info.FullMethod == "/blockchain_api.BlockchainAPI/GetMedianTimePastByHeights" {
			entered <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return next(ctx, req)
	}))
	blockchain_api.RegisterBlockchainAPIServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	client, err := blockchain.NewClientWithAddress(ctx, logger, tSettings, listener.Addr().String(), "mtp-reader-test")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.(*blockchain.Client).Close()) })
	// Store length 20 puts the loader's reorg-patch window at [20-mtpReorgOverlap, 20)
	// = [8, 20); height 1 sits well below it, so it is both covered and untouched by
	// the in-flight fetch and must take the fast path without waiting.
	initial := make([]uint32, 20)
	initial[1] = 123
	v := &Validator{logger: logger, settings: tSettings, blockchainClient: client, mtpStore: initial}
	loaded := make(chan error, 1)
	go func() { loaded <- v.EnsureMTPLoaded(ctx, 21) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("MTP request did not reach the real server")
	}
	require.True(t, v.mtpMu.TryRLock(), "an RPC must not hold the MTP readers' lock")
	value := v.mtpStore[1]
	v.mtpMu.RUnlock()
	require.Equal(t, uint32(123), value)
	covered := make(chan error, 1)
	go func() { covered <- v.EnsureMTPLoaded(ctx, 1) }()
	select {
	case err := <-covered:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("already-covered height outside the in-flight patch window waited for an unrelated fetch")
	}
	unblock()
	require.NoError(t, <-loaded)
	require.Len(t, v.mtpStore, 22)
}

// TestEnsureMTPLoaded_InFlightWindowBlocksCoveredFastPath demonstrates the fix
// for the fast-path race: a caller whose requested height is already "covered"
// (len(mtpStore) > needed) must still wait for an in-flight loader that is about
// to patch that exact height for reorg repair, instead of racing ahead and
// observing (or racing to overwrite) the pre-patch value.
//
// This is a logical race on values, not a memory race, so -race alone cannot
// catch it. The test makes it deterministic by gating the real
// GetMedianTimePastByHeights RPC mid-flight with a channel: a second caller
// requesting a height inside the loader's published patch window
// [fromHeight, currentLen) must not return while the RPC is gated. On the
// pre-fix code, the second call's fast path only checked store length and
// returned immediately, well before the in-flight loader had a chance to
// correct the stale entry.
func TestEnsureMTPLoaded_InFlightWindowBlocksCoveredFastPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tSettings := test.CreateBaseTestSettings(t)
	params := *tSettings.ChainCfgParams
	params.CSVHeight = 0
	tSettings.ChainCfgParams = &params
	logger := ulogger.TestLogger{}
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	store, err := sqlstore.New(logger, storeURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	service, err := blockchain.New(ctx, logger, tSettings, store, nil)
	require.NoError(t, err)
	require.NoError(t, service.Init(ctx))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if info.FullMethod == "/blockchain_api.BlockchainAPI/GetMedianTimePastByHeights" {
			entered <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return next(ctx, req)
	}))
	blockchain_api.RegisterBlockchainAPIServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	client, err := blockchain.NewClientWithAddress(ctx, logger, tSettings, listener.Addr().String(), "mtp-inflight-window-test")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.(*blockchain.Client).Close()) })

	// Seed a store of length 15 with a deliberately wrong ("reorg-stale") value
	// at height 10. currentLen=15 > mtpReorgOverlap=12, so the loader's patch
	// window will be [15-12, 15) = [3, 15), which covers height 10.
	const staleHeight = 10
	initial := make([]uint32, 15)
	initial[staleHeight] = 999
	v := &Validator{logger: logger, settings: tSettings, blockchainClient: client, mtpStore: initial}

	// Trigger a load that extends the store to height 16 and, as a side effect,
	// re-fetches and patches [3, 15).
	loaded := make(chan error, 1)
	go func() { loaded <- v.EnsureMTPLoaded(ctx, 16) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("MTP request did not reach the real server")
	}

	// height 10 is already "covered" (len(mtpStore)=15 > 10) but falls inside the
	// in-flight loader's published window, so this call must wait rather than
	// take the fast path.
	covered := make(chan error, 1)
	go func() { covered <- v.EnsureMTPLoaded(ctx, staleHeight) }()

	select {
	case <-covered:
		t.Fatal("covered-but-in-flight height returned before the loader finished patching it")
	case <-time.After(300 * time.Millisecond):
		// expected: still blocked on mtpLoadMu behind the gated RPC
	}

	unblock()
	require.NoError(t, <-loaded)
	require.NoError(t, <-covered)
	require.Len(t, v.mtpStore, 17)
	// The stale entry must have been corrected by the patch (no block beyond
	// genesis is persisted, so the real MTP for height 10 is 0), and the second
	// caller must not have observed or re-introduced the stale value.
	require.Equal(t, uint32(0), v.mtpStore[staleHeight])
	v.mtpMu.RLock()
	require.Empty(t, v.mtpLoadWindows, "load window must be removed once the loader returns")
	v.mtpMu.RUnlock()
}
