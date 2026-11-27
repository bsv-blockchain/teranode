package blockvalidation

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
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

type maliciousRecordingP2PClient struct {
	maliciousAbortP2PClient

	mu        sync.Mutex
	malicious []string
}

func (m *maliciousRecordingP2PClient) RecordCatchupMalicious(_ context.Context, peerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.malicious = append(m.malicious, peerID)

	return nil
}

func (m *maliciousRecordingP2PClient) reports() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return append([]string(nil), m.malicious...)
}

type inheritedInvalidEnv struct {
	server           *Server
	validation       *BlockValidation
	client           *maliciousRecordingP2PClient
	blockchainClient blockchain.ClientI
	settings         *settings.Settings
}

func newInheritedInvalidEnv(t *testing.T) *inheritedInvalidEnv {
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

	client := &maliciousRecordingP2PClient{}

	server := &Server{
		logger:              ulogger.TestLogger{},
		settings:            tSettings,
		blockValidation:     bv,
		blockchainClient:    blockchainClient,
		p2pClient:           client,
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
		blockCatchupAttempts: ttlcache.New[chainhash.Hash, int](
			ttlcache.WithTTL[chainhash.Hash, int](10*time.Minute),
			ttlcache.WithDisableTouchOnHit[chainhash.Hash, int](),
		),
	}

	return &inheritedInvalidEnv{server: server, validation: bv, client: client, blockchainClient: blockchainClient, settings: tSettings}
}

func (e *inheritedInvalidEnv) newBlock(t *testing.T, prevHash *chainhash.Hash, height uint32, coinbaseScript []byte) *model.Block {
	t.Helper()

	coinbaseTx := bt.NewTx()
	require.NoError(t, coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))

	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(coinbaseScript)

	require.NoError(t, coinbaseTx.AddP2PKHOutputFromAddress("mrjZYVrgH5rXfF1GAsxzGY9bFZYqP1wjnk", 50*100000000))

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevHash,
		HashMerkleRoot: coinbaseTx.TxIDChainHash(),
		Timestamp:      uint32(time.Now().Unix()), // nolint: gosec
		Bits:           *nBits,
	}

	for {
		if ok, _, _ := header.HasMetTargetDifficulty(); ok {
			break
		}

		header.Nonce++
	}

	block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{}, 0, uint64(coinbaseTx.Size()), height, 0) // nolint: gosec
	require.NoError(t, err)

	return block
}

var (
	validCoinbaseScript   = []byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'}
	invalidCoinbaseScript = []byte{0x01}
)

func (e *inheritedInvalidEnv) storeInvalid(t *testing.T, block *model.Block, peerID string) {
	t.Helper()

	require.NoError(t, e.blockchainClient.AddBlock(context.Background(), block, peerID, blockchainoptions.WithInvalid(true)))
}

// TestValidateBlock_InheritedInvalidVerdicts drives ValidateBlockWithOptions against a
// sqlitememory blockchain store and checks which invalid verdicts carry the inherited mark.
func TestValidateBlock_InheritedInvalidVerdicts(t *testing.T) {
	ctx := context.Background()

	validate := func(env *inheritedInvalidEnv, block *model.Block, opts *ValidateBlockOptions) error {
		return env.validation.ValidateBlockWithOptions(ctx, block, "http://peer", opts)
	}

	t.Run("ChildOfInvalidParentOnTheUncachedHeaderPath", func(t *testing.T) {
		env := newInheritedInvalidEnv(t)

		parent := env.newBlock(t, env.settings.ChainCfgParams.GenesisHash, 1, validCoinbaseScript)
		env.storeInvalid(t, parent, "")

		child := env.newBlock(t, parent.Hash(), 2, validCoinbaseScript)

		err := validate(env, child, &ValidateBlockOptions{PeerID: "peer-a"})
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "got: %v", err)
		require.Contains(t, err.Error(), "parent block is invalid")
		require.True(t, isInheritedInvalidError(err))
	})

	t.Run("ChildOfInvalidParentOnTheCachedHeaderPath", func(t *testing.T) {
		env := newInheritedInvalidEnv(t)

		parent := env.newBlock(t, env.settings.ChainCfgParams.GenesisHash, 1, validCoinbaseScript)
		env.storeInvalid(t, parent, "")

		child := env.newBlock(t, parent.Hash(), 2, validCoinbaseScript)

		opts := &ValidateBlockOptions{PeerID: "peer-a", IsCatchupMode: true, CachedHeaders: []*model.BlockHeader{parent.Header}}

		err := validate(env, child, opts)
		require.Error(t, err)
		require.Contains(t, err.Error(), "parent block is invalid")
		require.True(t, isInheritedInvalidError(err), "production catchup takes this branch")
	})

	t.Run("RepeatedAnnouncementOfInvalidChildStaysInherited", func(t *testing.T) {
		env := newInheritedInvalidEnv(t)

		parent := env.newBlock(t, env.settings.ChainCfgParams.GenesisHash, 1, validCoinbaseScript)
		env.storeInvalid(t, parent, "")

		child := env.newBlock(t, parent.Hash(), 2, validCoinbaseScript)
		env.storeInvalid(t, child, "peer-a")

		err := validate(env, child, &ValidateBlockOptions{PeerID: "peer-a"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "already exists as invalid")
		require.True(t, isInheritedInvalidError(err), "the stored child is invalid through its parent")
	})

	t.Run("PeerReservingItsOwnInvalidBlockIsNotInherited", func(t *testing.T) {
		env := newInheritedInvalidEnv(t)

		bad := env.newBlock(t, env.settings.ChainCfgParams.GenesisHash, 1, invalidCoinbaseScript)
		env.storeInvalid(t, bad, "peer-a")

		err := validate(env, bad, &ValidateBlockOptions{PeerID: "peer-a"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "already exists as invalid")
		require.False(t, isInheritedInvalidError(err), "a valid parent means the block is invalid on its own")
		require.True(t, shouldReportConsensusMalicious(err), "the peer stays chargeable")
	})

	t.Run("OwnConsensusFailureIsNotInherited", func(t *testing.T) {
		env := newInheritedInvalidEnv(t)

		bad := env.newBlock(t, env.settings.ChainCfgParams.GenesisHash, 1, invalidCoinbaseScript)

		err := validate(env, bad, &ValidateBlockOptions{PeerID: "peer-a"})
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "got: %v", err)
		require.Contains(t, err.Error(), "bad coinbase length")
		require.False(t, isInheritedInvalidError(err))
	})
}

// TestProcessCatchupChItem_InheritedInvalidIsNotReportedMalicious drives the real catchup
// consumer with a block whose parent this node holds invalid.
func TestProcessCatchupChItem_InheritedInvalidIsNotReportedMalicious(t *testing.T) {
	const peerID = "honest-peer"

	t.Run("DescendantOfInvalidParentIsNotReported", func(t *testing.T) {
		env := newInheritedInvalidEnv(t)

		parent := env.newBlock(t, env.settings.ChainCfgParams.GenesisHash, 1, validCoinbaseScript)
		env.storeInvalid(t, parent, "")

		child := env.newBlock(t, parent.Hash(), 2, validCoinbaseScript)

		env.server.catchupFunc = func(ctx context.Context, block *model.Block, peer, baseURL string) error {
			return env.validation.ValidateBlockWithOptions(ctx, block, baseURL, &ValidateBlockOptions{PeerID: peer})
		}

		for i := 0; i < 3; i++ {
			env.server.processCatchupChItem(context.Background(), processBlockCatchup{block: child, peerID: peerID, baseURL: "http://peer"})
		}

		require.Empty(t, env.client.reports(), "an honest peer on a chain with a locally invalid block is not malicious")
	})

	t.Run("PeersOwnInvalidBlockIsReported", func(t *testing.T) {
		env := newInheritedInvalidEnv(t)

		bad := env.newBlock(t, env.settings.ChainCfgParams.GenesisHash, 1, invalidCoinbaseScript)

		env.server.catchupFunc = func(ctx context.Context, block *model.Block, peer, baseURL string) error {
			return env.validation.ValidateBlockWithOptions(ctx, block, baseURL, &ValidateBlockOptions{PeerID: peer})
		}

		env.server.processCatchupChItem(context.Background(), processBlockCatchup{block: bad, peerID: peerID, baseURL: "http://peer"})

		require.Equal(t, []string{peerID}, env.client.reports())
	})
}
