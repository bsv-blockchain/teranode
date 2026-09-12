package legacy

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/netsync"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/settings"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestServerStop_JoinsPeerShutdown(t *testing.T) {
	inner := &server{logger: ulogger.TestLogger{}, quit: make(chan struct{})}
	inner.wg.Add(1) // peerHandler is still disconnecting peers
	service := &Server{server: inner}
	returned := make(chan error, 2)
	go func() { returned <- service.Stop(context.Background()) }()
	<-inner.quit
	go func() { returned <- service.Stop(context.Background()) }()
	select {
	case <-returned:
		t.Error("service Stop returned before the peer handler completed")
	case <-time.After(50 * time.Millisecond):
	}
	inner.wg.Done()
	for i := 0; i < 2; i++ {
		select {
		case err := <-returned:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("service Stop did not join peer shutdown")
		}
	}
	inner.Start()
	require.Zero(t, atomic.LoadInt32(&inner.started), "a stopped server must not register new workers")
}

func TestOnBlock_ShutdownCancelsWait(t *testing.T) {
	for _, waitingFor := range []string{"ban query send", "ban query reply", "block reply"} {
		t.Run(waitingFor, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cfg := test.CreateBaseTestSettings(t)
			cfg.Kafka = settings.KafkaSettings{}
			storeURL, err := url.Parse("sqlitememory:///")
			require.NoError(t, err)
			store, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, cfg)
			require.NoError(t, err)
			defer func() { require.NoError(t, store.(interface{ Close() error }).Close()) }()
			client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, cfg, store, nil, nil)
			require.NoError(t, err)
			sm, err := netsync.New(ctx, ulogger.TestLogger{}, cfg, client, nil, nil, nil, nil, nil, nil,
				&netsync.Config{ChainParams: cfg.ChainCfgParams, DisableCheckpoints: true})
			require.NoError(t, err)
			defer func() { cancel(); require.NoError(t, sm.Stop()) }()
			s := &server{ctx: ctx, logger: ulogger.TestLogger{}, settings: cfg, blockchainClient: client,
				syncManager: sm, quit: make(chan struct{}), query: make(chan interface{})}
			sp := newServerPeer(s, false)
			sp.Peer, err = peer.NewOutboundPeer(ulogger.TestLogger{}, cfg, &peer.Config{}, "127.0.0.1:8333")
			require.NoError(t, err)
			finished := make(chan struct{})
			go func() {
				sp.OnBlock(sp.Peer, &wire.MsgBlock{}, nil)
				close(finished)
			}()
			if waitingFor != "ban query send" {
				select {
				case query := <-s.query:
					if waitingFor == "block reply" {
						query.(*isPeerBannedMsg).reply <- false
					}
				case <-time.After(5 * time.Second):
					t.Fatal("OnBlock did not submit its ban query")
				}
			}
			select {
			case <-finished:
				t.Fatal("OnBlock returned before its reply or shutdown")
			case <-time.After(50 * time.Millisecond):
			}
			require.NoError(t, s.stopAndWait())
			select {
			case <-finished:
			case <-time.After(5 * time.Second):
				t.Fatal("OnBlock remained blocked after shutdown")
			}
		})
	}
}
