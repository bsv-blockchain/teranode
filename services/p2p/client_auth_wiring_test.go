package p2p

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Exercise the production client constructor and server auth options over a real
// connection. A probe after authentication stops before handler validation so
// every RPC can be checked without manufacturing unrelated peer state.
func TestClientCredentialsCoverServerProtectedMethods(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.GRPCAdminAPIKey = "p2p-client-wiring-test-secret-32-chars"
	tSettings.SecurityLevelGRPC = 0
	tSettings.P2P.GRPCListenAddress = "127.0.0.1:0"
	logger := ulogger.NewErrorTestLogger(t)
	server := &Server{settings: tSettings, logger: logger}
	auth, err := server.grpcAuthOptions()
	require.NoError(t, err)
	lis, err := net.Listen("tcp", tSettings.P2P.GRPCListenAddress)
	require.NoError(t, err)
	grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(
		util.CreateAuthInterceptor(auth.APIKey, auth.ProtectedMethods),
		func(ctx context.Context, _ any, info *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
			md, _ := metadata.FromIncomingContext(ctx)
			if !auth.ProtectedMethods[info.FullMethod] && len(md.Get("x-api-key")) != 0 {
				return nil, status.Error(codes.Internal, "credential leaked onto public RPC")
			}
			return nil, status.Error(codes.Aborted, "wiring probe reached")
		},
	))
	p2p_api.RegisterPeerServiceServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clientI, err := NewClientWithAddress(ctx, logger, lis.Addr().String(), tSettings)
	require.NoError(t, err)
	client := clientI.(*Client)
	defer func() { require.NoError(t, client.Close()) }()
	for _, method := range p2p_api.PeerService_ServiceDesc.Methods {
		name := "/p2p_api.PeerService/" + method.MethodName
		err := client.conn.Invoke(ctx, name, &emptypb.Empty{}, &emptypb.Empty{})
		require.Equal(t, codes.Aborted, status.Code(err), "%s: %v", name, err)
	}
}
