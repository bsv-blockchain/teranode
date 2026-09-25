package blockchain

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	sqlstore "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	reflectionv1alpha "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

const blockchainAuthTestKey = "blockchain-auth-transport-test-key"

func startAuthenticatedBlockchain(t *testing.T, reflection bool) (*Blockchain, *settings.Settings, *grpc.ClientConn) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	s := test.CreateBaseTestSettings(t)
	s.Context = t.Name()
	s.GRPCAdminAPIKey = blockchainAuthTestKey
	s.GRPCEnableReflection = reflection
	s.BlockChain.GRPCListenAddress = "127.0.0.1:0"
	s.BlockChain.HTTPListenAddress = "127.0.0.1:0"
	s.BlockChain.PeerRegistryStore = nil
	s.BlockChain.GRPCAddress = ""
	logger := ulogger.NewErrorTestLogger(t)
	store, err := sqlstore.New(logger, &url.URL{Scheme: "sqlitememory"}, s)
	require.NoError(t, err)
	producer, err := kafka.NewKafkaAsyncProducerFromURL(ctx, logger, &url.URL{Scheme: "memory", Host: "auth-test", Path: "/" + t.Name()}, &s.Kafka)
	require.NoError(t, err)
	b, err := New(ctx, logger, s, store, producer)
	require.NoError(t, err)
	require.NoError(t, b.Init(ctx))
	ready := make(chan struct{})
	stopped := make(chan error, 1)
	go func() { stopped <- b.Start(ctx, ready) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-stopped:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Error("server did not stop")
		}
		require.NoError(t, b.Stop(context.Background()))
		util.CleanupListeners(s.Context)
		require.NoError(t, store.Close(context.Background()))
	})
	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		t.Fatal("server did not start")
	}
	_, _, address, err := util.GetListener(s.Context, "blockchain", "", s.BlockChain.GRPCListenAddress)
	require.NoError(t, err)
	s.BlockChain.GRPCAddress = address
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return b, s, conn
}

func TestBlockchainRequiredAuthStartup(t *testing.T) {
	for _, key := range []string{"", "   ", "testkey", "ChangeMe", "short", " padded-secret-long-enough "} {
		t.Run("invalid_"+strings.TrimSpace(key), func(t *testing.T) {
			// Nil dependencies deliberately prove validation precedes workers and listeners.
			b := &Blockchain{settings: &settings.Settings{GRPCAdminAPIKey: key}}
			ready := make(chan struct{})
			err := b.Start(context.Background(), ready)
			require.ErrorContains(t, err, "grpc_admin_api_key is required")
			if strings.TrimSpace(key) != "" {
				require.NotContains(t, err.Error(), key)
			}
			select {
			case <-ready:
			default:
				t.Fatal("failed startup did not signal completion")
			}
		})
	}
}

func TestBlockchainRequiredAuthTransport(t *testing.T) {
	b, _, conn := startAuthenticatedBlockchain(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(t, b.store.SetState(ctx, "BlockAssembler", []byte("original-checkpoint")))
	services := blockchain_api.File_services_blockchain_blockchain_api_blockchain_api_proto.Services()
	for i := 0; i < services.Len(); i++ {
		svc := services.Get(i)
		for j := 0; j < svc.Methods().Len(); j++ {
			method := svc.Methods().Get(j)
			fullMethod := "/" + string(svc.FullName()) + "/" + string(method.Name())
			if fullMethod == blockchain_api.BlockchainAPI_HealthGRPC_FullMethodName {
				continue
			}
			t.Run(fullMethod, func(t *testing.T) {
				for _, values := range [][]string{nil, {"x-api-key", ""}, {"x-api-key", "wrong"}, {"x-api-key", blockchainAuthTestKey, "x-api-key", blockchainAuthTestKey}} {
					callCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(values...))
					req := dynamicpb.NewMessage(method.Input())
					if method.Name() == "SetState" {
						req.Set(method.Input().Fields().ByName("key"), protoreflect.ValueOfString("BlockAssembler"))
						req.Set(method.Input().Fields().ByName("data"), protoreflect.ValueOfBytes([]byte("corrupt")))
					}
					var err error
					if method.IsStreamingClient() || method.IsStreamingServer() {
						var stream grpc.ClientStream
						stream, err = conn.NewStream(callCtx, &grpc.StreamDesc{ServerStreams: method.IsStreamingServer(), ClientStreams: method.IsStreamingClient()}, fullMethod)
						if err == nil {
							_ = stream.SendMsg(req)
							_ = stream.CloseSend()
							err = stream.RecvMsg(dynamicpb.NewMessage(method.Output()))
						}
					} else {
						err = conn.Invoke(callCtx, fullMethod, req, dynamicpb.NewMessage(method.Output()))
					}
					require.Equal(t, codes.Unauthenticated, status.Code(err))
				}
			})
		}
	}
	state, err := b.store.GetState(ctx, "BlockAssembler")
	require.NoError(t, err)
	require.Equal(t, []byte("original-checkpoint"), state)
	api := blockchain_api.NewBlockchainAPIClient(conn)
	_, err = api.HealthGRPC(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	authCtx := metadata.AppendToOutgoingContext(ctx, "x-api-key", blockchainAuthTestKey)
	_, err = api.SetState(authCtx, &blockchain_api.SetStateRequest{Key: "BlockAssembler", Data: []byte("authorized-checkpoint")})
	require.NoError(t, err)
	got, err := api.GetState(authCtx, &blockchain_api.GetStateRequest{Key: "BlockAssembler"})
	require.NoError(t, err)
	require.Equal(t, []byte("authorized-checkpoint"), got.Data)
}

func TestBlockchainRequiredAuthClients(t *testing.T) {
	b, s, _ := startAuthenticatedBlockchain(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := NewClient(ctx, ulogger.NewErrorTestLogger(t), s, "auth-client")
	require.NoError(t, err)
	defer func() { require.NoError(t, client.(*Client).Close()) }()
	require.NoError(t, client.SetState(ctx, "auth-client", []byte("state")))
	state, err := client.GetState(ctx, "auth-client")
	require.NoError(t, err)
	require.Equal(t, []byte("state"), state)
	// A subscription must deliver initial state, not just establish a TCP connection.
	concrete := client.(*Client)
	select {
	case <-concrete.subscriptionReady:
	case <-ctx.Done():
		t.Fatal("authenticated subscription did not become ready")
	}
	// Evict the live server subscription, then prove the client's retry authenticates.
	require.Eventually(t, func() bool {
		b.subscribersMu.RLock()
		defer b.subscribersMu.RUnlock()
		return len(b.subscribers) > 0
	}, 5*time.Second, 10*time.Millisecond)
	var old subscriber
	b.subscribersMu.RLock()
	for sub := range b.subscribers {
		old = sub
		break
	}
	b.subscribersMu.RUnlock()
	require.NotNil(t, old.subscription)
	b.deadSubscriptions <- old
	require.Eventually(t, func() bool {
		b.subscribersMu.RLock()
		defer b.subscribersMu.RUnlock()
		for sub := range b.subscribers {
			if sub != old {
				return true
			}
		}
		return false
	}, 10*time.Second, 10*time.Millisecond)
	for _, fromConn := range []bool{false, true} {
		var registry PeerRegistryClientI
		if fromConn {
			registry = NewPeerRegistryClientFromConn(concrete.conn)
		} else {
			registry, err = NewPeerRegistryClient(ctx, s.BlockChain.GRPCAddress, s)
			require.NoError(t, err)
		}
		require.NoError(t, registry.RegisterPeer(ctx, &PeerInfo{ID: "auth-peer"}))
		peer, found, err := registry.GetPeer(ctx, "auth-peer")
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "auth-peer", peer.ID)
		require.NoError(t, registry.Close())
	}
}

func TestBlockchainRequiredAuthHTTP(t *testing.T) {
	_, s, _ := startAuthenticatedBlockchain(t, false)
	_, _, address, err := util.GetListener(s.Context, "blockchain", "http://", s.BlockChain.HTTPListenAddress)
	require.NoError(t, err)
	address = "http://" + address
	client := &http.Client{Timeout: 5 * time.Second}
	for _, route := range []string{"invalidate", "revalidate"} {
		for _, tc := range []struct {
			method, key string
			want        int
		}{
			{http.MethodGet, "", http.StatusMethodNotAllowed},
			{http.MethodGet, blockchainAuthTestKey, http.StatusMethodNotAllowed},
			{http.MethodPost, "", http.StatusUnauthorized},
			{http.MethodPost, "wrong", http.StatusUnauthorized},
			{http.MethodPost, blockchainAuthTestKey, http.StatusBadRequest},
		} {
			req, err := http.NewRequestWithContext(context.Background(), tc.method, address+"/"+route+"/invalid-hash", nil)
			require.NoError(t, err)
			if tc.key != "" {
				req.Header.Set("x-api-key", tc.key)
			}
			resp, err := client.Do(req)
			require.NoError(t, err)
			_, err = io.Copy(io.Discard, resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, tc.want, resp.StatusCode)
		}
	}
	resp, err := client.Get(address + "/health")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestBlockchainRequiredAuthReflection(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(t *testing.T) {
			_, _, conn := startAuthenticatedBlockchain(t, enabled)
			for _, auth := range []bool{false, true} {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if auth {
					ctx = metadata.AppendToOutgoingContext(ctx, "x-api-key", blockchainAuthTestKey)
				}
				stream, err := reflectionv1.NewServerReflectionClient(conn).ServerReflectionInfo(ctx)
				require.NoError(t, err)
				_ = stream.Send(&reflectionv1.ServerReflectionRequest{MessageRequest: &reflectionv1.ServerReflectionRequest_ListServices{ListServices: ""}})
				_, err = stream.Recv()
				want := codes.Unimplemented
				if enabled {
					want = codes.Unauthenticated
					if auth {
						want = codes.OK
					}
				}
				require.Equal(t, want, status.Code(err))
				alpha, err := reflectionv1alpha.NewServerReflectionClient(conn).ServerReflectionInfo(ctx)
				require.NoError(t, err)
				_ = alpha.Send(&reflectionv1alpha.ServerReflectionRequest{MessageRequest: &reflectionv1alpha.ServerReflectionRequest_ListServices{ListServices: ""}})
				_, err = alpha.Recv()
				require.Equal(t, want, status.Code(err))
				cancel()
			}
		})
	}
}
