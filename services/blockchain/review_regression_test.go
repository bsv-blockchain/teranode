package blockchain

import (
	"context"
	"math"
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
	for _, clientCap := range []int{10, 100} {
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
	} {
		t.Run(name, func(t *testing.T) { require.Equal(t, codes.InvalidArgument, status.Code(call())) })
	}
	server.settings.BlockChain.MaxBlockHeadersPerRequest = 1_000_001
	genesis, _, err := server.store.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	_, err = server.GetBlockHeaderIDs(ctx, &blockchain_api.GetBlockHeadersRequest{StartHash: genesis.Hash().CloneBytes(), NumberOfHeaders: 1_000_001})
	require.NoError(t, err, "operators can raise the cap for whole-chain startup reads")
}
