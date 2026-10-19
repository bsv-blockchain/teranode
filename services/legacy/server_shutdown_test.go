package legacy

import (
	"context"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/addrmgr"
	"github.com/bsv-blockchain/teranode/services/legacy/connmgr"
	"github.com/bsv-blockchain/teranode/services/legacy/netsync"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestStop_JoinsInternalServerForEveryCaller(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.GRPCListenAddress = "127.0.0.1:0"
	tSettings.GRPCAdminAPIKey = "test-only-key"
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	store, err := blockchainstore.NewStore(logger, storeURL, tSettings)
	require.NoError(t, err)
	closer, ok := store.(interface{ Close() error })
	require.True(t, ok)
	t.Cleanup(func() { require.NoError(t, closer.Close()) })
	client, err := blockchain.NewLocalClient(logger, tSettings, store, nil, nil)
	require.NoError(t, err)
	manager, err := netsync.New(ctx, logger, tSettings, client, nil, nil, nil, nil, nil, nil,
		&netsync.Config{ChainParams: tSettings.ChainCfgParams, DisableCheckpoints: true})
	require.NoError(t, err)
	connections, err := connmgr.New(logger, &connmgr.Config{Dial: func(addr net.Addr) (net.Conn, error) {
		return net.Dial(addr.Network(), addr.String())
	}})
	require.NoError(t, err)
	oldConfig := cfg
	cfg = &config{DisableDNSSeed: true}
	t.Cleanup(func() { cfg = oldConfig })
	inner := &server{
		ctx: ctx, logger: logger, settings: tSettings, blockchainClient: client,
		syncManager: manager, addrManager: addrmgr.New(logger, "", net.LookupIP),
		connManager: connections, quit: make(chan struct{}),
	}
	service := &Server{logger: logger, settings: tSettings, blockchainClient: client, server: inner}

	// Hold the real internal server's WaitForShutdown beyond the sync-manager
	// drain. A Stop implementation that only joins the manager returns too early.
	inner.wg.Add(1)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(inner.wg.Done) }
	t.Cleanup(func() {
		release()
		cancel()
		require.NoError(t, service.Stop(context.Background()))
	})
	started := make(chan error, 1)
	ready := make(chan struct{})
	go func() { started <- service.Start(ctx, ready) }()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("service did not become ready")
	}
	stopped := make(chan error, 2)
	go func() { stopped <- service.Stop(context.Background()) }()
	select {
	case <-inner.quit:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not signal internal shutdown")
	}
	go func() { stopped <- service.Stop(context.Background()) }()
	require.Never(t, func() bool { return len(stopped) > 0 }, 100*time.Millisecond, time.Millisecond,
		"every Stop caller must wait for the internal server")
	release()
	for range 2 {
		select {
		case err := <-stopped:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("Stop did not finish after internal shutdown")
		}
	}
	select {
	case err := <-started:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not cancel and join Start")
	}
	require.Error(t, service.Start(ctx, make(chan struct{})), "a stopped service cannot restart")
}

func TestStop_PreventsLaterStart(t *testing.T) {
	service := &Server{logger: ulogger.TestLogger{}}
	require.NoError(t, service.Stop(context.Background()))
	// No blockchain dependency is needed: Start must reject shutdown before
	// trying to wait for the FSM or launch the internal server.
	require.Error(t, service.Start(context.Background(), make(chan struct{})))
}
