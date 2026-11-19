package blockvalidation

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/catchup"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/settings"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jellydator/ttlcache/v3"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// banScoreRecordingP2PClient records malicious reports and ban-score escalations
// so tests can assert that a confirmed consensus violation does both.
type banScoreRecordingP2PClient struct {
	maliciousAbortP2PClient

	mu             sync.Mutex
	maliciousPeers []string
	banCalls       []string // "peerID:reason"
	banErr         error
}

func (b *banScoreRecordingP2PClient) RecordCatchupMalicious(_ context.Context, peerID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.maliciousPeers = append(b.maliciousPeers, peerID)

	return nil
}

func (b *banScoreRecordingP2PClient) AddBanScore(_ context.Context, peerID string, reason string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.banCalls = append(b.banCalls, peerID+":"+reason)

	return b.banErr
}

func (b *banScoreRecordingP2PClient) snapshot() ([]string, []string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]string(nil), b.maliciousPeers...), append([]string(nil), b.banCalls...)
}

// TestReportCatchupMaliciousAndBan verifies that confirmed catchup misbehaviour is
// escalated to the ban-score system, not just penalised in reputation (issue #1161).
func TestReportCatchupMaliciousAndBan(t *testing.T) {
	t.Run("EscalatesToBanScore", func(t *testing.T) {
		client := &banScoreRecordingP2PClient{}
		server := &Server{logger: ulogger.TestLogger{}, p2pClient: client}

		server.reportCatchupMaliciousAndBan(context.Background(), "peer-1", "invalid_block_validation", p2p.ReasonInvalidBlock)

		malicious, bans := client.snapshot()
		require.Equal(t, []string{"peer-1"}, malicious)
		require.Equal(t, []string{"peer-1:" + p2p.ReasonInvalidBlock}, bans)
	})

	t.Run("RepeatedInvalidBlocksAccumulateBanScore", func(t *testing.T) {
		client := &banScoreRecordingP2PClient{}
		server := &Server{logger: ulogger.TestLogger{}, p2pClient: client}

		for i := 0; i < 5; i++ {
			server.reportCatchupMaliciousAndBan(context.Background(), "peer-2", "invalid_block", p2p.ReasonInvalidBlock)
		}

		malicious, bans := client.snapshot()
		require.Len(t, malicious, 5)
		require.Len(t, bans, 5, "every confirmed invalid block must add ban score")
	})

	t.Run("BanScoreErrorIsNonFatal", func(t *testing.T) {
		client := &banScoreRecordingP2PClient{banErr: errors.NewServiceError("p2p down")}
		server := &Server{logger: ulogger.TestLogger{}, p2pClient: client}

		require.NotPanics(t, func() {
			server.reportCatchupMaliciousAndBan(context.Background(), "peer-3", "invalid_block", p2p.ReasonInvalidBlock)
		})

		malicious, bans := client.snapshot()
		require.Equal(t, []string{"peer-3"}, malicious)
		require.Len(t, bans, 1)
	})

	t.Run("EmptyPeerIDIsIgnored", func(t *testing.T) {
		client := &banScoreRecordingP2PClient{}
		server := &Server{logger: ulogger.TestLogger{}, p2pClient: client}

		server.reportCatchupMaliciousAndBan(context.Background(), "", "invalid_block", p2p.ReasonInvalidBlock)

		malicious, bans := client.snapshot()
		require.Empty(t, malicious)
		require.Empty(t, bans)
	})

	t.Run("SoftMaliciousReportDoesNotBan", func(t *testing.T) {
		// Ambiguous / transient failures must stay soft: reputation only.
		client := &banScoreRecordingP2PClient{}
		server := &Server{logger: ulogger.TestLogger{}, p2pClient: client}

		server.reportCatchupMalicious(context.Background(), "peer-4", "validation_failure")

		malicious, bans := client.snapshot()
		require.Equal(t, []string{"peer-4"}, malicious)
		require.Empty(t, bans, "transient failures must not accrue ban score")
	})

	t.Run("NilP2PClientIsSafe", func(t *testing.T) {
		server := &Server{logger: ulogger.TestLogger{}}

		require.NotPanics(t, func() {
			server.reportCatchupMaliciousAndBan(context.Background(), "peer-5", "invalid_block", p2p.ReasonInvalidBlock)
		})
	})
}

type banEscalationEnv struct {
	server           *Server
	client           *banScoreRecordingP2PClient
	blockchainClient blockchain.ClientI
	settings         *settings.Settings
}

func newBanEscalationEnv(t *testing.T) *banEscalationEnv {
	t.Helper()

	initPrometheusMetrics()

	tSettings := test.CreateBaseTestSettings(t)

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	subtreeStore := blobmemory.New()

	t.Cleanup(func() {
		_ = subtreeStore.Close(context.Background())
	})

	bv := &BlockValidation{
		logger:                        ulogger.TestLogger{},
		settings:                      tSettings,
		blockchainClient:              blockchainClient,
		subtreeStore:                  subtreeStore,
		lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
		blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
		blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
		blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
		stats:                         gocore.NewStat("blockvalidation"),
	}

	client := &banScoreRecordingP2PClient{}

	server := &Server{
		logger:              ulogger.TestLogger{},
		settings:            tSettings,
		blockValidation:     bv,
		blockchainClient:    blockchainClient,
		p2pClient:           client,
		headerChainCache:    catchup.NewHeaderChainCache(ulogger.TestLogger{}),
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
		blockCatchupAttempts: ttlcache.New[chainhash.Hash, int](
			ttlcache.WithTTL[chainhash.Hash, int](10*time.Minute),
			ttlcache.WithDisableTouchOnHit[chainhash.Hash, int](),
		),
	}

	server.catchupFunc = func(ctx context.Context, blockUpTo *model.Block, peerID, baseURL string) error {
		return server.validateCatchupBlocksForTest(ctx, peerID, baseURL, blockUpTo)
	}

	return &banEscalationEnv{server: server, client: client, blockchainClient: blockchainClient, settings: tSettings}
}

func (u *Server) validateCatchupBlocksForTest(ctx context.Context, peerID, baseURL string, blocks ...*model.Block) error {
	validateBlocksChan := make(chan blockForValidation, len(blocks))

	for _, block := range blocks {
		validateBlocksChan <- blockForValidation{block: block}
	}

	close(validateBlocksChan)

	var size atomic.Int64

	size.Store(int64(len(blocks)))

	catchupCtx := &CatchupContext{
		blockUpTo: blocks[len(blocks)-1],
		baseURL:   baseURL,
		peerID:    peerID,
	}

	return u.validateBlocksOnChannel(validateBlocksChan, ctx, catchupCtx, &size, nil)
}

func (e *banEscalationEnv) newBlock(t *testing.T, prevHash *chainhash.Hash, height uint32, coinbaseScript []byte) *model.Block {
	t.Helper()

	coinbaseTx := bt.NewTx()
	require.NoError(t, coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))

	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(coinbaseScript)

	require.NoError(t, coinbaseTx.AddP2PKHOutputFromAddress("mrjZYVrgH5rXfF1GAsxzGY9bFZYqP1wjnk", 50*100000000))

	nBits, err := model.NewNBitFromString("2000ffff")
	require.NoError(t, err)

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevHash,
		HashMerkleRoot: coinbaseTx.TxIDChainHash(),
		Timestamp:      uint32(time.Now().Unix()), // nolint: gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := header.HasMetTargetDifficulty(); ok {
			break
		}

		header.Nonce++
	}

	block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), height, 0) // nolint: gosec
	require.NoError(t, err)

	return block
}

var (
	validCoinbaseScript   = []byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'}
	invalidCoinbaseScript = []byte{0x01}
)

// TestCatchupInvalidBlockBanEscalation drives the real catchup call sites
// (processCatchupChItem and validateBlocksOnChannel) against a sqlitememory
// blockchain store and asserts the ban score that reaches the P2P client.
func TestCatchupInvalidBlockBanEscalation(t *testing.T) {
	const peerID = "peer-under-test"

	t.Run("ConsensusInvalidBlockAddsBanScoreExactlyOnce", func(t *testing.T) {
		env := newBanEscalationEnv(t)
		ctx := context.Background()

		block := env.newBlock(t, env.settings.ChainCfgParams.GenesisHash, 1, invalidCoinbaseScript)

		env.server.processCatchupChItem(ctx, processBlockCatchup{block: block, peerID: peerID, baseURL: "http://peer"})

		_, meta, err := env.blockchainClient.GetBlockHeader(ctx, block.Hash())
		require.NoError(t, err)
		require.True(t, meta.Invalid, "the consensus-invalid block must be stored as invalid")

		malicious, bans := env.client.snapshot()
		require.NotEmpty(t, malicious)
		require.Equal(t, []string{peerID + ":" + p2p.ReasonInvalidBlock}, bans, "one invalid block must add ban score exactly once")
	})

	t.Run("EachDistinctInvalidBlockAddsBanScoreOnce", func(t *testing.T) {
		env := newBanEscalationEnv(t)
		ctx := context.Background()

		for i := 0; i < 3; i++ {
			script := append([]byte{}, invalidCoinbaseScript...)
			script[0] = byte(i + 1)

			block := env.newBlock(t, env.settings.ChainCfgParams.GenesisHash, 1, script)

			env.server.processCatchupChItem(ctx, processBlockCatchup{block: block, peerID: peerID, baseURL: "http://peer"})
		}

		_, bans := env.client.snapshot()
		require.Len(t, bans, 3, "three distinct invalid blocks must add ban score three times")
	})

	t.Run("ChildOfLocallyInvalidParentDoesNotAddBanScore", func(t *testing.T) {
		env := newBanEscalationEnv(t)
		ctx := context.Background()

		parent := env.newBlock(t, env.settings.ChainCfgParams.GenesisHash, 1, validCoinbaseScript)
		require.NoError(t, env.blockchainClient.AddBlock(ctx, parent, "", blockchainoptions.WithInvalid(true)))

		child := env.newBlock(t, parent.Hash(), 2, validCoinbaseScript)

		err := env.server.validateCatchupBlocksForTest(ctx, peerID, "http://peer", child)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "expected ErrBlockInvalid, got: %v", err)
		require.Contains(t, err.Error(), "parent block is invalid")

		malicious, bans := env.client.snapshot()
		require.Equal(t, []string{peerID}, malicious, "the soft reputation penalty must stay")
		require.Empty(t, bans, "a block that is invalid only through its parent must not add ban score")
	})

	t.Run("ChildOfLocallyInvalidParentDoesNotAddBanScoreThroughCatchupConsumer", func(t *testing.T) {
		env := newBanEscalationEnv(t)
		ctx := context.Background()

		parent := env.newBlock(t, env.settings.ChainCfgParams.GenesisHash, 1, validCoinbaseScript)
		require.NoError(t, env.blockchainClient.AddBlock(ctx, parent, "", blockchainoptions.WithInvalid(true)))

		for i := 0; i < 10; i++ {
			script := append([]byte{}, validCoinbaseScript...)
			script[len(script)-1] = byte('a' + i)

			child := env.newBlock(t, parent.Hash(), 2, script)

			env.server.processCatchupChItem(ctx, processBlockCatchup{block: child, peerID: peerID, baseURL: "http://peer"})
		}

		_, bans := env.client.snapshot()
		require.Empty(t, bans, "repeated announcements of descendants of a locally invalid block must not add ban score")
	})

	t.Run("BlockAlreadyStoredAsInvalidDoesNotAddBanScore", func(t *testing.T) {
		env := newBanEscalationEnv(t)
		ctx := context.Background()

		block := env.newBlock(t, env.settings.ChainCfgParams.GenesisHash, 1, validCoinbaseScript)
		require.NoError(t, env.blockchainClient.AddBlock(ctx, block, "", blockchainoptions.WithInvalid(true)))

		err := env.server.validateCatchupBlocksForTest(ctx, peerID, "http://peer", block)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "expected ErrBlockInvalid, got: %v", err)
		require.Contains(t, err.Error(), "block already exists as invalid")

		_, bans := env.client.snapshot()
		require.Empty(t, bans, "a block that the node already holds as invalid must not add ban score")
	})

	t.Run("InvalidCommonAncestorDoesNotAddBanScore", func(t *testing.T) {
		env := newBanEscalationEnv(t)
		ctx := context.Background()

		block := env.newBlock(t, env.settings.ChainCfgParams.GenesisHash, 1, validCoinbaseScript)

		env.server.catchupFunc = func(_ context.Context, _ *model.Block, _, _ string) error {
			return errors.NewBlockInvalidError("[catchup][%s] common ancestor is marked invalid, not catching up", block.Hash().String())
		}

		env.server.processCatchupChItem(ctx, processBlockCatchup{block: block, peerID: peerID, baseURL: "http://peer"})

		malicious, bans := env.client.snapshot()
		require.Equal(t, []string{peerID}, malicious, "the soft reputation penalty must stay")
		require.Empty(t, bans, "an invalid block error that did not come from block validation must not add ban score")
	})
}

func TestIsInheritedInvalidError(t *testing.T) {
	inherited := markInheritedInvalid(errors.NewBlockInvalidError("parent block is invalid"))

	require.True(t, isInheritedInvalidError(inherited))
	require.True(t, errors.Is(inherited, errors.ErrBlockInvalid), "the mark must not change the error code")
	require.True(t, isInheritedInvalidError(errors.NewProcessingError("wrapped", inherited)), "the mark must survive a wrap")
	require.True(t, isInheritedInvalidError(errors.UnwrapGRPC(errors.WrapGRPC(inherited))), "the mark must survive a gRPC round trip")

	require.False(t, isInheritedInvalidError(nil))
	require.False(t, isInheritedInvalidError(errors.NewBlockInvalidError("bad coinbase length")))
	require.False(t, isInheritedInvalidError(errors.NewProcessingError("wrapped", errors.NewBlockInvalidError("bad coinbase length"))))
	require.False(t, isInheritedInvalidError(context.Canceled))
}
