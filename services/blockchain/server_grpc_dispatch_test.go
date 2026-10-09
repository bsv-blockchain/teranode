package blockchain

import (
	"context"
	"net"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestBlockchainAPIServer_GRPCDispatch guards against a *Blockchain method whose
// name has drifted from the blockchain_api.BlockchainAPIServer interface (as
// GetLatestBlockHeaderFromBlockLocator and GetBlockHeadersFromOldest once did,
// both carrying a stray "Request" suffix). That drift still compiles, because the
// embedded blockchain_api.UnimplementedBlockchainAPIServer supplies a default for
// any interface method *Blockchain doesn't itself implement — so a compile-time
// `var _ blockchain_api.BlockchainAPIServer = (*Blockchain)(nil)` assertion cannot
// catch it. The only way to catch it is to dispatch through a real gRPC server, as
// this test does, and check the real RPCs don't come back as codes.Unimplemented.
func TestBlockchainAPIServer_GRPCDispatch(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///grpc_dispatch")
	require.NoError(t, err)

	blockchainStore, err := sql.New(logger, storeURL, tSettings)
	require.NoError(t, err)

	mockProducer := kafka.NewKafkaAsyncProducerMock()

	server, err := New(ctx, logger, tSettings, blockchainStore, mockProducer)
	require.NoError(t, err)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	blockchain_api.RegisterBlockchainAPIServer(grpcServer, server)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := blockchain_api.NewBlockchainAPIClient(conn)

	best := chainhash.DoubleHashH([]byte("best"))
	locator := chainhash.DoubleHashH([]byte("locator"))

	t.Run("GetLatestBlockHeaderFromBlockLocator is not Unimplemented", func(t *testing.T) {
		_, err := client.GetLatestBlockHeaderFromBlockLocator(ctx, &blockchain_api.GetLatestBlockHeaderFromBlockLocatorRequest{
			BestBlockHash:      best.CloneBytes(),
			BlockLocatorHashes: [][]byte{locator.CloneBytes()},
		})

		st, ok := status.FromError(err)
		require.True(t, ok, "expected a gRPC status error, got: %v", err)
		require.NotEqual(t, codes.Unimplemented, st.Code(), "RPC method name has drifted from the BlockchainAPIServer interface")
	})

	t.Run("GetBlockHeadersFromOldest is not Unimplemented", func(t *testing.T) {
		chainTip := chainhash.DoubleHashH([]byte("tip"))
		target := chainhash.DoubleHashH([]byte("target"))

		_, err := client.GetBlockHeadersFromOldest(ctx, &blockchain_api.GetBlockHeadersFromOldestRequest{
			ChainTipHash:    chainTip.CloneBytes(),
			TargetHash:      target.CloneBytes(),
			NumberOfHeaders: 1,
		})

		st, ok := status.FromError(err)
		require.True(t, ok, "expected a gRPC status error, got: %v", err)
		require.NotEqual(t, codes.Unimplemented, st.Code(), "RPC method name has drifted from the BlockchainAPIServer interface")
	})
}
