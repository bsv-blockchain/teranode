package netsync

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/blockvalidation_api"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// legacyValidationStackCounter gives every stack its own Kafka memory topic
// paths and store URL labels, so two subtests in one process cannot share a
// topic and their log lines tell apart. It does not separate the databases:
// util.InitSQLiteDB opens every sqlitememory store under a random name and
// ignores the URL path, so two stacks never share one whichever path they pass.
var legacyValidationStackCounter atomic.Int64

// legacyValidationStack is the real service stack a legacy block commits
// through, with no mock anywhere on the path: a sqlitememory blockchain store
// behind a LocalClient, a sqlitememory SQL UTXO store, file blob stores for the
// subtrees and the transactions, the validator, a subtreevalidation server and a
// blockvalidation server, each behind a real gRPC listener, and the
// blockvalidation client netsync's ProcessBlock calls. The blockvalidation server
// gets an in-memory Kafka consumer because its Stop closes the consumer with no
// nil guard (services/blockvalidation/Server.go, Stop).
//
// It is what the historical testnet replay (historical_testnet_test.go, build tag
// longtest) and the seam test (pipeline_quick_validate_seam_test.go) share, so
// the shipped default route and the unified route are both proven against the
// same services a node runs. It lives in an untagged file so the tagged test can
// call it; the reverse would not compile outside a longtest build.
type legacyValidationStack struct {
	ctx         context.Context
	chainStore  blockchainstore.Store
	utxos       *utxosql.Store
	subtrees    blob.Store
	txs         blob.Store
	chain       blockchain.ClientI
	validator   validator.Interface
	blockClient blockvalidation.Interface
}

// newLegacyValidationStack builds the stack over s, which must already carry
// ChainCfgParams. It sets the settings the stack needs to come up and nothing
// about the route: Kafka URLs in memory, a sqlitememory UTXO store URL,
// the two gRPC addresses, and Legacy.TempStore pointing at the subtree file store
// so newBlockPark accepts it. Every service is stopped and every store closed in
// t.Cleanup, in the reverse of the order they were built.
func newLegacyValidationStack(t *testing.T, s *settings.Settings) *legacyValidationStack {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	logger := ulogger.TestLogger{}
	n := legacyValidationStackCounter.Add(1)

	s.Kafka.InvalidBlocksConfig = &url.URL{Scheme: "memory", Host: "localhost", Path: fmt.Sprintf("/legacy-stack-invalid-blocks-%d", n)}
	s.Kafka.InvalidSubtreesConfig = &url.URL{Scheme: "memory", Host: "localhost", Path: fmt.Sprintf("/legacy-stack-invalid-subtrees-%d", n)}
	s.UtxoStore.UtxoStore = &url.URL{Scheme: "sqlitememory", Path: fmt.Sprintf("/legacy_stack_utxo_%d", n)}
	// One stamping batch at a time. model.UpdateTxMinedStatus runs
	// MaxMinedRoutines SetMinedMulti batches in parallel, and two of them on a
	// sqlitememory store deadlock each other on the table lock (SQLITE_LOCKED,
	// "database table is locked: database is deadlocked"). setMinedMultiChunk
	// has no lock retry of its own, unlike the store's create and spend paths,
	// so the setMined worker retries the whole block for a minute and never
	// sets mined_set. The sqlite engine has one writer; the default of 128 is
	// sized for the production stores.
	s.UtxoStore.MaxMinedRoutines = 1

	chainStore, err := blockchainstore.NewStore(logger, &url.URL{Scheme: "sqlitememory", Path: fmt.Sprintf("/legacy_stack_chain_%d", n)}, s)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, chainStore.Close(context.Background())) })

	utxos, err := utxosql.New(ctx, logger, s, s.UtxoStore.UtxoStore)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, utxos.Close(context.Background())) })

	subtreesURL := &url.URL{Scheme: "file", Path: t.TempDir()}
	subtrees, err := file.New(logger, subtreesURL, bloboptions.WithDisableDAH(true))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, subtrees.Close(context.Background())) })

	// The park scans this directory on restart, so it must be the file store the
	// sink writes into: one store for the subtree files and the converted record.
	s.Legacy.TempStore = subtreesURL

	txs, err := file.New(logger, &url.URL{Scheme: "file", Path: t.TempDir()}, bloboptions.WithDisableDAH(true))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, txs.Close(context.Background())) })

	chain, err := blockchain.NewLocalClient(logger, s, chainStore, subtrees, utxos)
	require.NoError(t, err)
	require.NoError(t, chain.SetBlockMinedSet(ctx, s.ChainCfgParams.GenesisHash))

	validate, err := validator.New(ctx, logger, s, utxos, nil, nil, nil, nil, chain)
	require.NoError(t, err)

	subtreeServer, err := subtreevalidation.New(ctx, logger, s, subtrees, txs, utxos, validate, chain, nil, nil, nil, nil)
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

	consumer, err := kafka.NewKafkaConsumerGroupFromURL(logger, &url.URL{Scheme: "memory", Host: "localhost", Path: fmt.Sprintf("/legacy-stack-blocks-%d", n)}, "legacy-stack", true, &s.Kafka)
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

	blockClient, err := blockvalidation.NewClient(ctx, logger, s, "legacy-stack")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, blockClient.Close()) })

	return &legacyValidationStack{
		ctx:         ctx,
		chainStore:  chainStore,
		utxos:       utxos,
		subtrees:    subtrees,
		txs:         txs,
		chain:       chain,
		validator:   validate,
		blockClient: blockClient,
	}
}

// newLegacySyncManager builds the SyncManager that commits through stack: the
// fields New sets that the sink, the park and the commit path read, over the
// stack's own stores and clients. params is the chain the header cache judges
// checkpoints against, so it must be the same object s.ChainCfgParams points at
// and must already carry the checkpoints a test wants proven.
//
// The manager is a struct literal, not New: there is no peer server, no block
// handler goroutine and no dispatcher, so a test drives pipelineBlockSink and
// commitParkedBlock itself. That bypasses the download ledger (handleBlockOnDiskMsg
// is never called, so blockDownloads owners set by the wanted-range pass are never
// released) and the pass stops asking after perPeerDepth blocks. Harmless for a
// commit, and stated here so nobody reads the quiet pass as a live download loop.
func newLegacySyncManager(t *testing.T, stack *legacyValidationStack, s *settings.Settings, params *chaincfg.Params) *SyncManager {
	t.Helper()

	logger := ulogger.TestLogger{}

	// HandleConvertedBlock sets a gauge on every commit; a manager that never
	// went through New has not registered it.
	initPrometheusMetrics()

	// The header cache New builds: checkpoints, the proof-of-work ceiling and
	// SV Node's contextual header rules reading the committed chain through
	// this stack's blockchain client, so a headers message here is judged as a
	// production node judges it.
	rules, err := newHeaderRules(logger, s, params, stack.chain)
	require.NoError(t, err)
	require.NotNil(t, rules)

	sm := &SyncManager{
		ctx:                  stack.ctx,
		logger:               logger,
		settings:             s,
		chainParams:          params,
		blockchainClient:     stack.chain,
		utxoStore:            stack.utxos,
		subtreeStore:         stack.subtrees,
		validationClient:     stack.validator,
		blockValidation:      stack.blockClient,
		peerStates:           txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		orphanTxs:            expiringmap.New[chainhash.Hash, *orphanTxAndParents](time.Hour),
		rejectedTxns:         txmap.NewSyncedMap[chainhash.Hash, struct{}](100),
		blockDownloads:       newBlockDownloadTracker(blockRequestAssignmentTTL),
		blockSizeTracker:     newBlockSizeTracker(10),
		commitRate:           newCommitRateTracker(),
		streams:              newStreamRegistry(),
		headerCache:          newHeaderCache().WithCheckpoints(params.Checkpoints).WithPowLimit(model.PowLimitCeiling(params)).WithHeaderRules(rules),
		recentlyFailedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	t.Cleanup(sm.orphanTxs.Stop)
	t.Cleanup(sm.recentlyFailedBlocks.Stop)

	sm.blockPark = mustNewBlockPark(t, logger, s, stack.subtrees)
	enablePrefetchBudgetForTest(t, sm, 64)

	return sm
}

// requireBlockMinedByValidation hands block validation the one signal this
// stack's LocalClient does not send, and waits for block validation's own
// setMined worker to finish with the block.
//
// On the full route ValidateBlockWithOptions ends with SetBlockSubtreesSet. The
// blockchain server sends a BlockSubtreesSet notification inside that call
// (services/blockchain/Server.go, SetBlockSubtreesSet), block validation's
// subscriber (BlockValidation.go, the setMined goroutine) turns it into
// setTxMinedStatus, which stamps every transaction of the block with its block
// id and then sets mined_set. The LocalClient's SetBlockSubtreesSet writes the
// flag and sends nothing, so without this the transactions of a block committed
// on the full route are never stamped and the next block's spends of them fail
// as floaters, which is how this gap was found (historical replay, height 383
// spending height 381). The test sends the notification; everything after it is
// the worker's. Setting mined_set by hand instead would make the worker skip the
// block on its MinedSet guard and leave the transactions unstamped.
//
// On the unified route quick validation commits with mined_set already true and
// the worker skips the block, so the wait returns at once.
func requireBlockMinedByValidation(t *testing.T, stack *legacyValidationStack, hash chainhash.Hash) {
	t.Helper()

	require.NoError(t, stack.chain.SendNotification(stack.ctx, &blockchain_api.Notification{
		Type: model.NotificationType_BlockSubtreesSet,
		Hash: hash.CloneBytes(),
	}))

	require.Eventually(t, func() bool {
		mined, err := stack.chain.GetBlockIsMined(stack.ctx, &hash)
		require.NoError(t, err)

		return mined
	}, 30*time.Second, 5*time.Millisecond, "block validation's setMined worker must stamp %s and set mined_set", hash)
}

// outpointOnlyBlocksMetric is block validation's count of blocks that entered the
// below-checkpoint quick route (services/blockvalidation quickValidateBlock and
// quickValidateBlockAsync, the only two places it is incremented). The full route
// never touches it, which is what lets a test tell the two routes apart: a block
// that commits on either route looks the same in the chain.
const outpointOnlyBlocksMetric = "teranode_blockvalidation_outpoint_only_blocks_total"

// outpointOnlyBlocks reads outpointOnlyBlocksMetric from the default Prometheus
// registry, where block validation's promauto counters are registered when its
// server is built. The counter is process-wide, so callers compare a reading
// before and after the work they mean to measure.
func outpointOnlyBlocks(t *testing.T) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, family := range families {
		if family.GetName() != outpointOnlyBlocksMetric {
			continue
		}

		require.Len(t, family.GetMetric(), 1, "%s is a plain counter with one series", outpointOnlyBlocksMetric)

		return family.GetMetric()[0].GetCounter().GetValue()
	}

	require.Failf(t, "metric not registered", "%s is not in the default registry; a blockvalidation server must be built first", outpointOnlyBlocksMetric)

	return 0
}
