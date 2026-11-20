package svp2p

import (
	"context"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer_api"
	"github.com/bsv-blockchain/teranode/services/svp2p/protocol"
	"github.com/bsv-blockchain/teranode/services/svp2p/svp2ptest"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

// newIntegrationServer builds a full svp2p stack on real TCP ports. When
// connectTo is non-empty it is used as the outbound dial target. Each server
// gets a unique settings context: util.GetListener caches gRPC listeners by
// (context, serviceName), and two same-named in-process servers must not
// share one listener.
func newIntegrationServer(t *testing.T, role string, connectTo []string) (*Server, string) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Context = tSettings.Context + "-" + role
	tSettings.Legacy.ListenAddresses = []string{"127.0.0.1:0"}
	tSettings.Legacy.GRPCListenAddress = svp2ptest.FreePort(t)
	tSettings.Legacy.WorkingDir = t.TempDir()
	tSettings.Legacy.ConnectPeers = connectTo
	tSettings.GRPCAdminAPIKey = "test-admin-key"

	store, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, store, nil, nil)
	require.NoError(t, err)

	return New(ulogger.TestLogger{}, tSettings, blockchainClient), tSettings.Legacy.GRPCListenAddress
}

func TestIntegrationHandshakeBetweenTwoServers(t *testing.T) {
	ctx := context.Background()

	// Server A listens; we learn its P2P port after Init+Start.
	serverA, grpcA := newIntegrationServer(t, "a", nil)
	cancelA := startServer(t, serverA)

	defer cancelA()
	defer func() { require.NoError(t, serverA.Stop(ctx)) }()

	listenAddrs := serverA.manager.ListenAddrs()
	require.Len(t, listenAddrs, 1)

	// Server B dials A.
	serverB, grpcB := newIntegrationServer(t, "b", []string{listenAddrs[0]})
	cancelB := startServer(t, serverB)

	defer cancelB()
	defer func() { require.NoError(t, serverB.Stop(ctx)) }()

	clientA := dialPeerAPI(t, grpcA)
	clientB := dialPeerAPI(t, grpcB)

	// Both sides converge on one established connection.
	require.Eventually(t, func() bool {
		respA, errA := clientA.GetPeerCount(ctx, &emptypb.Empty{})
		respB, errB := clientB.GetPeerCount(ctx, &emptypb.Empty{})

		return errA == nil && errB == nil && respA.Count == 1 && respB.Count == 1
	}, 10*time.Second, 100*time.Millisecond, "peers did not connect")

	// A sees B's user agent from the version handshake.
	peersA, err := clientA.GetPeers(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	require.Len(t, peersA.Peers, 1)
	require.Equal(t, protocol.UserAgent, peersA.Peers[0].SubVer)
	require.True(t, peersA.Peers[0].Inbound)

	// The connection holds through an idle window: no flap.
	time.Sleep(3 * time.Second)

	respA, err := clientA.GetPeerCount(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, int32(1), respA.Count)

	// Banning B's host on A disconnects it and blocks reconnection.
	peerHost, _, err := net.SplitHostPort(peersA.Peers[0].Addr)
	require.NoError(t, err)

	_, err = serverA.BanPeer(ctx, &peer_api.BanPeerRequest{
		Addr:  peerHost,
		Until: time.Now().Add(time.Hour).Unix(),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		resp, err := clientA.GetPeerCount(ctx, &emptypb.Empty{})
		return err == nil && resp.Count == 0
	}, 10*time.Second, 100*time.Millisecond, "banned peer was not disconnected")

	// B keeps redialing (base 5s backoff); A must keep refusing. Hold long
	// enough for at least one redial attempt.
	time.Sleep(6 * time.Second)

	resp, err := clientA.GetPeerCount(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, int32(0), resp.Count, "banned peer reconnected")
}
