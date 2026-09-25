package blockchain

import (
	"context"
	"math"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	sqlstore "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestClientGetMedianTimePastRange_RealServer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Context = t.Name()
	params := *tSettings.ChainCfgParams
	params.CSVHeight = 0
	tSettings.ChainCfgParams = &params
	tSettings.BlockChain.MaxMedianTimePastHeights = 10
	tSettings.BlockChain.GRPCListenAddress = "127.0.0.1:0"
	tSettings.BlockChain.HTTPListenAddress = "127.0.0.1:0"
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	store, err := sqlstore.New(logger, storeURL, tSettings)
	require.NoError(t, err)
	kafkaURL, err := url.Parse("memory://localhost/pr1565-final-blocks")
	require.NoError(t, err)
	producer, err := kafka.NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL, &tSettings.Kafka)
	require.NoError(t, err)
	defer func() { require.NoError(t, producer.Stop()) }()
	server, err := New(ctx, logger, tSettings, store, producer)
	require.NoError(t, err)
	require.NoError(t, server.Init(ctx))
	for height := uint32(1); height < 30; height++ {
		block := createTestBlockAtHeight(&testContext{server: server}, t, height, 1000000+height*600)
		_, _, err := store.StoreBlock(ctx, block, "")
		require.NoError(t, err)
	}
	ready := make(chan struct{})
	stopped := make(chan error, 1)
	go func() { stopped <- server.Start(ctx, ready) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-stopped:
		case <-time.After(10 * time.Second):
			t.Error("server did not stop")
		}
		util.RemoveListener(tSettings.Context, "blockchain", "")
		util.RemoveListener(tSettings.Context, "blockchain", "http://")
		require.NoError(t, store.Close(context.Background()))
	})
	select {
	case <-ready:
	case err := <-stopped:
		t.Fatalf("server failed to start: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("server did not start")
	}
	_, address, _, err := util.GetListener(tSettings.Context, "blockchain", "", tSettings.BlockChain.GRPCListenAddress)
	require.NoError(t, err)
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	rpcClient := blockchain_api.NewBlockchainAPIClient(conn)
	callCtx, callCancel := context.WithTimeout(ctx, 10*time.Second)
	defer callCancel()
	// An older, unchunked caller really is rejected by the production handler.
	_, err = rpcClient.GetMedianTimePastByHeights(callCtx, &blockchain_api.GetMedianTimePastByHeightsRequest{Heights: make([]uint32, 31)})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	want, err := server.GetMedianTimePastRange(callCtx, 0, 30)
	require.NoError(t, err)
	require.Equal(t, uint32(1014400), want[30]) // median of heights 19..29
	for _, clientCap := range []int{10, 100, 0, -1} {
		clientSettings := *tSettings
		clientSettings.BlockChain.MaxMedianTimePastHeights = clientCap
		client := &Client{client: rpcClient, settings: &clientSettings}
		got, err := client.GetMedianTimePastRange(callCtx, 0, 30)
		require.NoError(t, err)
		require.Equal(t, want, got, "includes a one-height final chunk and an unpersisted tip")
		got, err = client.GetMedianTimePastRange(callCtx, 29, 30)
		require.NoError(t, err)
		require.Equal(t, want[29:], got)
		got, err = client.GetMedianTimePastRange(callCtx, 30, 29)
		require.NoError(t, err)
		require.Empty(t, got)
		canceled, stop := context.WithCancel(callCtx)
		stop()
		_, err = client.GetMedianTimePastRange(canceled, 0, 30)
		require.Error(t, err)
	}
}

func TestPublicReadCountsAreBounded(t *testing.T) {
	server := setup(t).server
	ctx := context.Background()
	hash := make([]byte, chainhash.HashSize)
	for name, call := range map[string]func() error{
		"blocks": func() error {
			_, err := server.GetBlocks(ctx, &blockchain_api.GetBlocksRequest{Hash: hash, Count: math.MaxUint32})
			return err
		},
		"headers from height": func() error {
			_, err := server.GetBlockHeadersFromHeight(ctx, &blockchain_api.GetBlockHeadersFromHeightRequest{Limit: math.MaxUint32})
			return err
		},
		"last blocks": func() error {
			_, err := server.GetLastNBlocks(ctx, &blockchain_api.GetLastNBlocksRequest{NumberOfBlocks: math.MaxUint32})
			return err
		},
		"invalid blocks": func() error {
			_, err := server.GetLastNInvalidBlocks(ctx, &blockchain_api.GetLastNInvalidBlocksRequest{N: math.MaxUint32})
			return err
		},
		"ancestor headers": func() error {
			_, err := server.GetBlockHeadersToCommonAncestor(ctx, &blockchain_api.GetBlockHeadersToCommonAncestorRequest{MaxHeaders: math.MaxUint32})
			return err
		},
		"zero ancestor headers": func() error {
			_, err := server.GetBlockHeadersToCommonAncestor(ctx, &blockchain_api.GetBlockHeadersToCommonAncestorRequest{})
			return err
		},
		"headers from common ancestor": func() error {
			_, err := server.GetBlockHeadersFromCommonAncestor(ctx, &blockchain_api.GetBlockHeadersFromCommonAncestorRequest{
				TargetHash: hash, MaxHeaders: math.MaxUint32,
			})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) { require.Equal(t, codes.InvalidArgument, status.Code(call())) })
	}
}

// TestBlockSummaryReadsUseBlockShapedBounds pins that GetLastNBlocks and
// GetLastNInvalidBlocks are bounded separately from the header-only RPCs:
// they return []model.BlockInfo, not bare 80-byte headers, so reusing
// maxBlockHeadersPerRequest (sized for a whole-chain header response) would
// leave the same OOM primitive open for the heavier block-summary shape.
//
// GetLastNInvalidBlocks in particular cannot reuse maxBlocksByHeightRange's
// tighter 2000 default: the RPC service's reconsiderInvalidChildren asks for
// 10000 in production (services/rpc/handlers.go), so its bound must
// accommodate that in-tree caller.
func TestBlockSummaryReadsUseBlockShapedBounds(t *testing.T) {
	server := setup(t).server
	ctx := context.Background()

	// GetLastNBlocks: bounded by maxBlocksByHeightRange (2000 default), not
	// maxBlockHeadersPerRequest (1,000,000 default).
	_, err := server.GetLastNBlocks(ctx, &blockchain_api.GetLastNBlocksRequest{
		NumberOfBlocks: int64(server.maxBlocksByHeightRange()),
	})
	require.NoError(t, err, "count at the block-range cap must be accepted")

	_, err = server.GetLastNBlocks(ctx, &blockchain_api.GetLastNBlocksRequest{
		NumberOfBlocks: int64(server.maxBlocksByHeightRange()) + 1,
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "count above the block-range cap must be rejected")

	// GetLastNInvalidBlocks: bounded by maxInvalidBlocksPerRequest (10000
	// default), which must cover reconsiderInvalidChildren's fixed request
	// of 10000.
	_, err = server.GetLastNInvalidBlocks(ctx, &blockchain_api.GetLastNInvalidBlocksRequest{N: 10000})
	require.NoError(t, err, "reconsiderInvalidChildren's request size of 10000 must be accepted")

	_, err = server.GetLastNInvalidBlocks(ctx, &blockchain_api.GetLastNInvalidBlocksRequest{
		N: int64(server.maxInvalidBlocksPerRequest()) + 1,
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "count above the invalid-block cap must be rejected")
}

// Block assembly requests tip height + 1 IDs during startup and Reset. That
// request must keep working as the chain grows beyond the header-response cap.
func TestClientGetBlockHeaderIDs_AboveHeaderCap(t *testing.T) {
	fixture := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for height := uint32(1); height <= 3; height++ {
		block := createTestBlockAtHeight(fixture, t, height, 1000000+height*600)
		_, _, err := fixture.server.store.StoreBlock(ctx, block, "")
		require.NoError(t, err)
	}
	tip, _, err := fixture.server.store.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	want, err := fixture.server.store.GetBlockHeaderIDs(ctx, tip.Hash(), 4)
	require.NoError(t, err)
	require.Len(t, want, 4)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	blockchain_api.RegisterBlockchainAPIServer(server, fixture.server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := &Client{client: blockchain_api.NewBlockchainAPIClient(conn)}

	// Both the default million-header boundary and a lowered operator cap are
	// independent of the ID query. The store stops at genesis, retaining order.
	for _, cap := range []int{defaultMaxBlockHeadersPerRequest, 1} {
		fixture.server.settings.BlockChain.MaxBlockHeadersPerRequest = cap
		got, err := client.GetBlockHeaderIDs(ctx, tip.Hash(), defaultMaxBlockHeadersPerRequest+1)
		require.NoError(t, err)
		require.Equal(t, want, got)
		_, err = client.client.GetBlockHeaders(ctx, &blockchain_api.GetBlockHeadersRequest{
			StartHash: tip.Hash().CloneBytes(), NumberOfHeaders: defaultMaxBlockHeadersPerRequest + 1,
		})
		require.Equal(t, codes.InvalidArgument, status.Code(err), "full headers remain capped")
	}
}
