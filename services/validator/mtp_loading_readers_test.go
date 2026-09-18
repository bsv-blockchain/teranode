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
	v := &Validator{logger: logger, settings: tSettings, blockchainClient: client, mtpStore: []uint32{0, 123}}
	loaded := make(chan error, 1)
	go func() { loaded <- v.EnsureMTPLoaded(ctx, 3) }()
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
		t.Fatal("already-covered height waited for an unrelated fetch")
	}
	unblock()
	require.NoError(t, <-loaded)
	require.Len(t, v.mtpStore, 4)
}
