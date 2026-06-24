package blockchain

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// zombieFakeServer is a gRPC server whose Subscribe never sends anything
// (the stream context is kept alive but no frames are written). This
// simulates the half-zombie state from issue #872.
type zombieFakeServer struct {
	blockchain_api.UnimplementedBlockchainAPIServer
	// hangCh: each Subscribe call blocks until this channel is closed or the
	// stream context is cancelled. Closing it allows clean shutdown in tests.
	hangCh       chan struct{}
	connectCount atomic.Int32
}

func (z *zombieFakeServer) Subscribe(_ *blockchain_api.SubscribeRequest, stream blockchain_api.BlockchainAPI_SubscribeServer) error {
	z.connectCount.Add(1)
	select {
	case <-z.hangCh:
	case <-stream.Context().Done():
	}
	return nil
}

func (z *zombieFakeServer) HealthGRPC(_ context.Context, _ *emptypb.Empty) (*blockchain_api.HealthResponse, error) {
	return &blockchain_api.HealthResponse{Ok: true, Details: "ok", Timestamp: timestamppb.Now()}, nil
}

func (z *zombieFakeServer) GetFSMCurrentState(_ context.Context, _ *emptypb.Empty) (*blockchain_api.GetFSMStateResponse, error) {
	return &blockchain_api.GetFSMStateResponse{State: blockchain_api.FSMStateType_RUNNING}, nil
}

// heartbeatFakeServer sends periodic PINGs to keep the watchdog satisfied.
type heartbeatFakeServer struct {
	blockchain_api.UnimplementedBlockchainAPIServer
	interval time.Duration
}

func (h *heartbeatFakeServer) Subscribe(_ *blockchain_api.SubscribeRequest, stream blockchain_api.BlockchainAPI_SubscribeServer) error {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
			if err := stream.Send(&blockchain_api.Notification{
				Type: model.NotificationType_PING,
			}); err != nil {
				return err
			}
		}
	}
}

func (h *heartbeatFakeServer) HealthGRPC(_ context.Context, _ *emptypb.Empty) (*blockchain_api.HealthResponse, error) {
	return &blockchain_api.HealthResponse{Ok: true, Details: "ok", Timestamp: timestamppb.Now()}, nil
}

func (h *heartbeatFakeServer) GetFSMCurrentState(_ context.Context, _ *emptypb.Empty) (*blockchain_api.GetFSMStateResponse, error) {
	return &blockchain_api.GetFSMStateResponse{State: blockchain_api.FSMStateType_RUNNING}, nil
}

// blockFakeServer sends periodic Block notifications with a valid 32-byte hash.
// Block notifications are forwarded to local Subscribe subscribers (unlike PINGs).
type blockFakeServer struct {
	blockchain_api.UnimplementedBlockchainAPIServer
	interval time.Duration
}

func (b *blockFakeServer) Subscribe(_ *blockchain_api.SubscribeRequest, stream blockchain_api.BlockchainAPI_SubscribeServer) error {
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()
	seq := byte(0)
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
			hash := make([]byte, 32)
			hash[0] = seq
			seq++
			if err := stream.Send(&blockchain_api.Notification{
				Type: model.NotificationType_Block,
				Hash: hash,
			}); err != nil {
				return err
			}
		}
	}
}

func (b *blockFakeServer) HealthGRPC(_ context.Context, _ *emptypb.Empty) (*blockchain_api.HealthResponse, error) {
	return &blockchain_api.HealthResponse{Ok: true, Details: "ok", Timestamp: timestamppb.Now()}, nil
}

func (b *blockFakeServer) GetFSMCurrentState(_ context.Context, _ *emptypb.Empty) (*blockchain_api.GetFSMStateResponse, error) {
	return &blockchain_api.GetFSMStateResponse{State: blockchain_api.FSMStateType_RUNNING}, nil
}

// startFakeGRPC starts a gRPC server on a free local port and returns its address
// and a stop function. The caller owns the stop call.
func startFakeGRPC(t *testing.T, srv blockchain_api.BlockchainAPIServer) (addr string, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := grpc.NewServer()
	blockchain_api.RegisterBlockchainAPIServer(s, srv)
	go s.Serve(lis) //nolint:errcheck
	return lis.Addr().String(), s.Stop
}

// TestSubscribeToServer_WatchdogClosesZombieStream verifies that when the
// server's Subscribe handler never sends a frame (zombie / half-zombie stream),
// the client watchdog eventually cancels the stream context and the reconnect
// loop re-establishes the subscription.
//
// We use a short HeartbeatInterval (50ms) so zombieTimeout = 100ms and the
// watchdog tick = 50ms. The test asserts that Subscribe is called at least
// twice on the server within a tight deadline, proving the watchdog fired and
// the reconnect loop ran.
func TestSubscribeToServer_WatchdogClosesZombieStream(t *testing.T) {
	const heartbeat = 50 * time.Millisecond

	hangCh := make(chan struct{})
	srv := &zombieFakeServer{hangCh: hangCh}

	addr, stopSrv := startFakeGRPC(t, srv)
	defer stopSrv()
	defer close(hangCh)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockChain.GRPCAddress = addr
	tSettings.BlockChain.HeartbeatInterval = heartbeat
	tSettings.BlockChain.MaxRetries = 0

	_, err := NewClientWithAddress(ctx, logger, tSettings, addr, "watchdog-test")
	require.NoError(t, err)

	// Wait for the watchdog to fire and trigger a reconnect:
	//   initial connect: immediate
	//   zombieTimeout:   100ms
	//   watchdog tick:    50ms worst-case extra
	//   reconnect sleep:   1s
	//   second connect:   immediate
	//
	// Total: ~1.2s. Give 2s total to be safe on slow CI.
	require.Eventually(t, func() bool {
		return srv.connectCount.Load() >= 2
	}, 2*time.Second, 20*time.Millisecond,
		"expected at least 2 Subscribe calls (initial + watchdog-triggered reconnect); got %d",
		srv.connectCount.Load())
}

// TestSubscribeToServer_WatchdogDoesNotFireWhenStreamProgresses verifies that
// a healthy stream delivering regular Block notifications does not get cancelled
// by the watchdog before we explicitly cancel the context.
//
// Block notifications (unlike PINGs) are forwarded to local Subscribe
// subscribers, making them directly observable.
func TestSubscribeToServer_WatchdogDoesNotFireWhenStreamProgresses(t *testing.T) {
	const heartbeat = 50 * time.Millisecond
	zombieTimeout := 2 * heartbeat // 100ms

	// Send blocks faster than zombieTimeout so the watchdog cannot trip.
	srv := &blockFakeServer{interval: heartbeat / 2}

	addr, stopSrv := startFakeGRPC(t, srv)
	defer stopSrv()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockChain.GRPCAddress = addr
	tSettings.BlockChain.HeartbeatInterval = heartbeat
	tSettings.BlockChain.MaxRetries = 0

	c, err := NewClientWithAddress(ctx, logger, tSettings, addr, "watchdog-healthy-test")
	require.NoError(t, err)
	client := c.(*Client)

	// Wait for the gRPC subscription to be established.
	require.Eventually(t, func() bool {
		if client.subscriptionReady == nil {
			return false
		}
		select {
		case <-client.subscriptionReady:
			return true
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond, "subscription should become ready")

	// Add a local subscriber to receive forwarded Block notifications.
	ch, subErr := c.Subscribe(ctx, "watchdog-healthy-test")
	require.NoError(t, subErr)

	// Collect notifications for 5× the zombie timeout. If the watchdog fires
	// incorrectly, the gRPC stream gets cancelled and Recv errors, which causes
	// the client to stop delivering to local subscribers — the channel would
	// timeout instead of delivering.
	received := 0
	deadline := time.Now().Add(5 * zombieTimeout)
	for time.Now().Before(deadline) {
		select {
		case n, ok := <-ch:
			if !ok {
				t.Fatal("subscription channel closed unexpectedly — watchdog may have fired incorrectly")
			}
			if n.Type == model.NotificationType_Block {
				received++
			}
		case <-time.After(zombieTimeout + 200*time.Millisecond):
			t.Fatalf("no Block notification received within zombieTimeout — stream may be stalled or watchdog fired incorrectly")
		}
	}

	require.Greater(t, received, 0, "should have received at least one Block notification from healthy stream")
}
