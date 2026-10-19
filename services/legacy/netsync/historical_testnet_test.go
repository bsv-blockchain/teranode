//go:build longtest

package netsync

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/blockvalidation_api"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// This exercises headers-first provenance, real transaction/UTXO processing and
// the ProcessBlock gRPC handoff. No checkpoint proof is added to that RPC: the
// historical calculator must work with release/v0.15's stricter proof gate too.
func TestLegacyHistoricalTestnetSync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := ulogger.TestLogger{}
	s := test.CreateBaseTestSettings(t)
	params := chaincfg.TestNetParams
	s.ChainCfgParams = &params
	s.BlockValidation.OptimisticMining = false
	s.BlockValidation.IsParentMinedRetryMaxRetry = 1
	s.BlockAssembly.Disabled = true
	s.GlobalBlockHeightRetention = 1000
	s.Kafka.InvalidBlocksConfig = &url.URL{Scheme: "memory", Host: "localhost", Path: "/historical-invalid-blocks"}
	s.Kafka.InvalidSubtreesConfig = &url.URL{Scheme: "memory", Host: "localhost", Path: "/historical-invalid-subtrees"}
	s.UtxoStore.UtxoStore = &url.URL{Scheme: "sqlitememory"}
	chainStore, err := blockchainstore.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, s)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, chainStore.(interface{ Close() error }).Close()) })
	utxos, err := utxosql.New(ctx, logger, s, s.UtxoStore.UtxoStore)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, utxos.Close(context.Background())) })
	// Fixture blobs remain available until the temporary directories are cleaned up.
	subtrees, err := file.New(logger, &url.URL{Scheme: "file", Path: t.TempDir()}, bloboptions.WithDisableDAH(true))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, subtrees.Close(context.Background())) })
	txs, err := file.New(logger, &url.URL{Scheme: "file", Path: t.TempDir()}, bloboptions.WithDisableDAH(true))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, txs.Close(context.Background())) })
	chain, err := blockchain.NewLocalClient(logger, s, chainStore, subtrees, utxos)
	require.NoError(t, err)
	require.NoError(t, chain.SetBlockMinedSet(ctx, params.GenesisHash))
	validate, err := validator.New(ctx, logger, s, utxos, nil, nil, nil, chain)
	require.NoError(t, err)
	subtreeServer, err := subtreevalidation.New(ctx, logger, s, subtrees, txs, utxos, validate, chain, nil, nil, nil)
	require.NoError(t, err)
	require.NoError(t, subtreeServer.Init(ctx))
	t.Cleanup(func() { require.NoError(t, subtreeServer.Stop(context.Background())) })
	serve := func(register func(*grpc.Server)) string {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		server := grpc.NewServer()
		register(server)
		go func() { _ = server.Serve(listener) }()
		t.Cleanup(server.Stop)
		return listener.Addr().String()
	}
	s.SubtreeValidation.GRPCAddress = serve(func(server *grpc.Server) {
		subtreevalidation_api.RegisterSubtreeValidationAPIServer(server, subtreeServer)
	})
	consumer, err := kafka.NewKafkaConsumerGroupFromURL(logger, &url.URL{Scheme: "memory", Host: "localhost", Path: "/historical-blocks"}, "historical", true, &s.Kafka)
	require.NoError(t, err)
	blockServer := blockvalidation.New(logger, s, subtrees, txs, utxos, validate, chain, consumer, nil, nil)
	require.NoError(t, blockServer.Init(ctx))
	t.Cleanup(func() {
		cancel()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		require.NoError(t, blockServer.Stop(stopCtx))
	})
	s.BlockValidation.GRPCAddress = serve(func(server *grpc.Server) {
		blockvalidation_api.RegisterBlockValidationAPIServer(server, blockServer)
	})
	blockClient, err := blockvalidation.NewClient(ctx, logger, s, "historical-testnet")
	require.NoError(t, err)
	p := peer.NewInboundPeer(logger, s, &peer.Config{})
	state := &peerSyncState{requestedBlocks: expiringmap.New[chainhash.Hash, blockRequestOrigin](time.Hour)}
	t.Cleanup(state.requestedBlocks.Stop)
	checkpoint := params.Checkpoints[0]
	sm := &SyncManager{
		ctx: ctx, logger: logger, settings: s, chainParams: &params,
		blockchainClient: chain, utxoStore: utxos, subtreeStore: subtrees,
		validationClient: validate, blockValidation: blockClient,
		peerStates:      txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks: expiringmap.New[chainhash.Hash, blockRequestOrigin](time.Hour),
		orphanTxs:       expiringmap.New[chainhash.Hash, *orphanTxAndParents](time.Hour),
		headerList:      list.New(), blockSizeTracker: newBlockSizeTracker(10),
		nextCheckpoint: &checkpoint,
	}
	t.Cleanup(sm.requestedBlocks.Stop)
	t.Cleanup(sm.orphanTxs.Stop)
	sm.peerStates.Set(p, state)
	sm.storeSyncPeer(p, &syncPeerState{})
	sm.headersFirstMode.Store(true)
	sm.headerList.PushBack(&headerNode{height: 0, hash: params.GenesisHash})
	initPrometheusMetrics()

	data, err := os.ReadFile("testdata/testnet_blocks_1_547.gz")
	require.NoError(t, err)
	reader, err := gzip.NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })
	blocks := make([]*wire.MsgBlock, 547)
	headers := wire.NewMsgHeaders()
	for i := range blocks {
		var size uint32
		require.NoError(t, binary.Read(reader, binary.LittleEndian, &size))
		require.LessOrEqual(t, size, uint32(100_000))
		data := make([]byte, size)
		_, err := io.ReadFull(reader, data)
		require.NoError(t, err)
		blocks[i] = &wire.MsgBlock{}
		require.NoError(t, blocks[i].Deserialize(bytes.NewReader(data)))
		if i < 546 {
			require.NoError(t, headers.AddBlockHeader(&blocks[i].Header))
		}
	}
	sm.handleHeadersMsg(&headersMsg{headers: headers, peer: p})
	for i, block := range blocks {
		height := uint32(i + 1)
		hash := block.Header.BlockHash()
		origin := sm.blockOrigin(state, hash)
		if height <= 546 {
			require.True(t, origin.headerProven, "height %d must come from the verified header run", height)
		}
		if height == 149 {
			require.Equal(t, "00000000291d8e6f5d0d2a59de8f0f206917f3e00ff53edc8f6dcaddd61f3fe9", hash.String())
			exists, err := chain.GetBlockExists(ctx, params.Checkpoints[0].Hash)
			require.NoError(t, err)
			require.False(t, exists, "checkpoint evidence is only in netsync, not blockchain storage")
		}
		// HandleBlockDirect retains the origin while ProcessBlock serializes only
		// the block and ordinary RPC fields, reproducing the release handoff.
		require.NoError(t, sm.HandleBlockDirect(ctx, p, hash, block, origin), "height %d", height)
		_, meta, err := chain.GetBlockHeader(ctx, &hash)
		require.NoError(t, err)
		require.Equal(t, height, meta.Height)
		require.False(t, meta.Invalid)
		// Block assembly normally creates the accepted block's coinbase UTXOs.
		// Complete that separate service's step before delivering the next block.
		accepted, err := model.NewBlockFromMsgBlock(block, s)
		require.NoError(t, err)
		_, err = utxos.Create(ctx, accepted.CoinbaseTx, height, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: meta.ID, BlockHeight: height}))
		require.NoError(t, err)
		require.NoError(t, chain.SetBlockMinedSet(ctx, &hash))
		// Advance the production request queue as delivered headers are consumed.
		if height <= 546 {
			state.requestedBlocks.Delete(hash)
			sm.requestedBlocks.Delete(hash)
			sm.headerList.Remove(sm.headerList.Front())
			sm.fetchHeaderBlocks()
		}
	}
	_, best, err := chain.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(547), best.Height)
}
