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
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestOptimisticCorrupt_InvalidateFailure_EscalatesNotSilentlyAccepted is the C2 guarantee
// (bitcoin-sv/teranode#4692): on the opt-in optimistic-background path a corrupt body was already
// AddBlock'd before block.Valid ran, so if the invalidate route's InvalidateBlock FAILS the block
// must NOT be left quietly on-chain — the guard escalates by re-queuing the block for revalidation
// (u.ReValidateBlock), which retries until the store recovers. Fault injection: InvalidateBlock is
// mocked to fail. Behaviour asserted: (1) InvalidateBlock is attempted, and (2) the block is
// re-queued on revalidateBlockChan (proving it is not silently accepted). A struct-literal
// BlockValidation is used (no background revalidate worker) so the re-queue can be observed on the
// channel directly — the same pattern existing revalidation tests use.
func TestOptimisticCorrupt_InvalidateFailure_EscalatesNotSilentlyAccepted(t *testing.T) {
	initPrometheusMetrics()
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	// Opt in to optimistic mining on peer paths so the add-before-validate path runs.
	tSettings.BlockValidation.OptimisticMining = true
	tSettings.BlockValidation.OptimisticMiningPeerBlocks = true

	// Build a corrupt-body block: a real coinbase + subtree, but a zeroed merkle root, so the
	// background block.Valid fails CheckMerkleRoot -> ERR_BLOCK_CORRUPT.
	privateKey, _ := bec.NewPrivateKey()
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	_ = coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000)

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~^/subtree/[a-z0-9]+\z`, httpmock.NewBytesResponder(200, nodeBytes))

	subtreeStore := blobmemory.New()
	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(context.Background(), subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	nBits, _ := model.NewNBitFromString("2000ffff")
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: &chainhash.Hash{},         // zeroed -> corrupt in the background
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}
	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}
		blockHeader.Nonce++
	}

	block, err := model.NewBlock(blockHeader, coinbaseTx, []*chainhash.Hash{subtree.RootHash()}, uint64(subtree.Length()), uint64(coinbaseTx.Size()), 100, 0) //nolint:gosec
	require.NoError(t, err)

	// Fault injection: InvalidateBlock always fails, and signals that it was attempted.
	invalidateAttempted := make(chan struct{}, 1)
	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	mockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil)
	mockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{1}, nil)
	mockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).Return(nBits, nil).Maybe()
	mockBlockchain.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil).Maybe()
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 99, MinedSet: true}, nil).Maybe()
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 100}, nil).Maybe()
	mockBlockchain.On("AddBlock", mock.Anything, block, mock.Anything, mock.Anything).Return(nil)
	mockBlockchain.On("InvalidateBlock", mock.Anything, block.Header.Hash()).Return([]chainhash.Hash{}, errors.NewError("invalidate store unavailable")).Run(func(mock.Arguments) {
		select {
		case invalidateAttempted <- struct{}{}:
		default:
		}
	})

	// The block carries a subtree, so validateBlockSubtrees runs before the optimistic AddBlock;
	// let it pass so the corrupt verdict comes from the background block.Valid (zeroed merkle).
	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)
	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	// Buffered so the escalation's ReValidateBlock enqueue never blocks and can be observed here.
	revalidateChan := make(chan revalidateBlockData, 2)

	bv := &BlockValidation{
		logger:                        logger,
		settings:                      tSettings,
		blockchainClient:              mockBlockchain,
		subtreeStore:                  subtreeStore,
		txStore:                       blobmemory.New(),
		utxoStore:                     utxoStore,
		subtreeValidationClient:       subtreeValidationClient,
		lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
		blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
		subtreeExistsCache:            expiringmap.New[chainhash.Hash, bool](10 * time.Minute),
		blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
		blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
		setMinedChan:                  make(chan *chainhash.Hash, 1),
		revalidateBlockChan:           revalidateChan,
		stats:                         gocore.NewStat("blockvalidation"),
	}
	defer bv.StopCaches()

	// Optimistic path: ValidateBlock returns nil after the optimistic AddBlock; the corrupt verdict
	// and the escalation happen in the background goroutine.
	err = bv.ValidateBlock(ctx, block, "test")
	require.NoError(t, err)

	select {
	case <-invalidateAttempted:
		// good: the invalidate route was taken (block not left silently accepted)
	case <-time.After(2 * time.Second):
		t.Fatal("InvalidateBlock was not attempted on the optimistic-background corrupt path")
	}

	// The invalidate FAILED, so the escalation must re-queue the block for revalidation rather than
	// leave the optimistically-added corrupt body quietly on-chain.
	select {
	case data := <-revalidateChan:
		require.Equal(t, block.Hash(), data.block.Hash(), "the corrupt block must be re-queued for revalidation after InvalidateBlock fails")
	case <-time.After(2 * time.Second):
		t.Fatal("InvalidateBlock failed but the block was NOT re-queued (C2): a corrupt tip would be left silently accepted")
	}
}

// invalidateFailsFirstNClient wraps a real blockchain client and makes the first N InvalidateBlock
// calls fail, delegating every later call (and every other method) to the real client. This is the
// fault injection the convergence test needs: the optimistic path's InvalidateBlock must fail so the
// block is re-queued, and the retry's InvalidateBlock must then genuinely land in the store so the
// end state can be asserted against real persisted metadata rather than against a mock's own return.
type invalidateFailsFirstNClient struct {
	blockchain.ClientI

	failFirst int

	mu    sync.Mutex
	calls int
}

func (c *invalidateFailsFirstNClient) InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) ([]chainhash.Hash, error) {
	c.mu.Lock()
	c.calls++
	attempt := c.calls
	c.mu.Unlock()

	if attempt <= c.failFirst {
		return nil, errors.NewError("invalidate store unavailable")
	}

	return c.ClientI.InvalidateBlock(ctx, blockHash)
}

func (c *invalidateFailsFirstNClient) attempts() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.calls
}

// coinbaseAtHeight builds a coinbase whose BIP34 scriptSig encodes the given height, using
// production's own canonical minimal encoder so ExtractCoinbaseHeight accepts it. The "/Test" tag is
// not decoration: EncodeCoinbaseHeightPush emits a single opcode for heights <= 16, and a 1-byte
// scriptSig fails the 2-byte consensus minimum in block.Valid.
//
// Tests that drive a REAL blockchain store must use a height matching the height the store will
// derive from the parent, not the height carried on the model block. StoreBlock re-derives the height
// from the parent chain and applies its own BIP34 coinbase-height guard to every non-invalid write,
// so a coinbase encoding some other height couples the test to the network's BIP34 activation height
// for no reason.
func coinbaseAtHeight(t *testing.T, height uint32) *bt.Tx {
	t.Helper()

	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	scriptSig := util.EncodeCoinbaseHeightPush(height)
	scriptSig = append(scriptSig, "/Test"...)

	coinbaseTx := bt.NewTx()
	require.NoError(t, coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(scriptSig)
	require.NoError(t, coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000))
	require.True(t, coinbaseTx.IsCoinbase())

	return coinbaseTx
}

// TestOptimisticCorrupt_ReValidationConvergesOnInvalidation is the end-to-end half of freemans13
// item 4 (bitcoin-sv/teranode#4692). TestOptimisticCorrupt_InvalidateFailure_EscalatesNotSilentlyAccepted
// proves the optimistic path RE-QUEUES when InvalidateBlock fails; this proves the re-queue actually
// CONVERGES, which is what the promise in that path's comment claims and what the old reValidateBlock
// gate could never deliver (a corrupt error matched neither ErrBlockInvalid nor ErrBlockIncomplete,
// so the retry re-failed corrupt and returned without invalidating — forever).
//
// Everything but the first InvalidateBlock is real: a sqlitememory blockchain store, so the closing
// assertion is that the corrupt tip is genuinely recorded Invalid in the store, not that a mock was
// called.
func TestOptimisticCorrupt_ReValidationConvergesOnInvalidation(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	// Opt in to optimistic mining on peer paths so the body is AddBlock'd before block.Valid runs —
	// the only path on which invalidating a corrupt body is correct, because it is already on-chain.
	tSettings.BlockValidation.OptimisticMining = true
	tSettings.BlockValidation.OptimisticMiningPeerBlocks = true

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	realClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	// waitForPreviousBlocksToBeProcessed gates on the parent being mined, and the store writes
	// genesis unmined; without this the optimistic AddBlock is never reached.
	require.NoError(t, realClient.SetBlockMinedSet(ctx, tSettings.ChainCfgParams.GenesisHash))

	blockchainClient := &invalidateFailsFirstNClient{ClientI: realClient, failFirst: 1}

	// The whole test hinges on the FIRST AddBlock succeeding: the corrupt tip has to reach the store
	// before background block.Valid condemns it, otherwise there is no on-chain block for the retry to
	// converge on. The parent is genesis, so StoreBlock derives height 1 and applies its own BIP34
	// coinbase-height guard to that (non-invalid) write. Encode height 1 in the coinbase and carry the
	// same height on the model block, so the model height, the store-derived height and the
	// coinbase-encoded height all agree and the fixture does not depend on where BIP34 activates.
	// The coinbase does NOT need to be bad: the corrupt verdict comes from the zeroed merkle root,
	// which fails at the binding, well before the BIP34 and fee checks.
	const blockHeight = uint32(1)

	coinbaseTx := coinbaseAtHeight(t, blockHeight)

	// A real subtree, stored, but a ZEROED header merkle root: the body loads and binds far enough to
	// reach CheckMerkleRoot, which then fails -> ERR_BLOCK_CORRUPT in the background block.Valid.
	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	subtreeStore := blobmemory.New()
	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	// Mined to the regtest 207fffff target so the real GetNextWorkRequired/difficulty gate accepts it.
	hdr := minedBIP34Header(t, 4, tSettings.ChainCfgParams.GenesisHash, &chainhash.Hash{})

	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{subtree.RootHash()},
		uint64(subtree.Length()), uint64(coinbaseTx.Size()), blockHeight, 0) //nolint:gosec
	require.NoError(t, err)

	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)
	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	// Buffered, and with no background revalidate worker, so the re-queue can be observed and then
	// driven deterministically from the test rather than raced against a worker.
	revalidateChan := make(chan revalidateBlockData, 2)

	bv := &BlockValidation{
		logger:                        logger,
		settings:                      tSettings,
		blockchainClient:              blockchainClient,
		subtreeStore:                  subtreeStore,
		txStore:                       blobmemory.New(),
		utxoStore:                     utxoStore,
		subtreeValidationClient:       subtreeValidationClient,
		lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
		blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
		subtreeExistsCache:            expiringmap.New[chainhash.Hash, bool](10 * time.Minute),
		blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
		blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
		setMinedChan:                  make(chan *chainhash.Hash, 1),
		revalidateBlockChan:           revalidateChan,
		stats:                         gocore.NewStat("blockvalidation"),
	}
	defer bv.StopCaches()

	require.NoError(t, bv.ValidateBlock(ctx, block, "test"),
		"the optimistic path returns once the block is added; the corrupt verdict lands in the background")

	var requeued revalidateBlockData

	select {
	case requeued = <-revalidateChan:
		// good: InvalidateBlock failed, so the block was re-queued instead of left silently accepted
	case <-time.After(10 * time.Second):
		t.Fatal("the corrupt body was not re-queued after InvalidateBlock failed")
	}

	require.Equal(t, 1, blockchainClient.attempts(), "the optimistic path must have attempted the invalidate exactly once")
	require.Equal(t, block.Hash(), requeued.block.Hash())

	// The premise of the whole branch: the body really is on-chain, because it was added before
	// block.Valid ran.
	exists, err := realClient.GetBlockExists(ctx, block.Header.Hash())
	require.NoError(t, err)
	require.True(t, exists, "the optimistic path added the block before validating it")

	_, metaBefore, err := realClient.GetBlockHeader(ctx, block.Header.Hash())
	require.NoError(t, err)
	require.NotNil(t, metaBefore)
	require.False(t, metaBefore.Invalid, "the failed invalidate left the corrupt tip accepted — this is the state the retry must fix")

	// The re-queued revalidation: it re-fails corrupt (nothing about the body changed), and THAT is
	// the case the old gate dropped on the floor. It must now invalidate, because the block is stored.
	revalidateErr := bv.reValidateBlock(requeued)
	require.Error(t, revalidateErr)
	require.True(t, errors.IsBlockCorrupt(revalidateErr), "the body is still corrupt on the retry, got: %v", revalidateErr)

	require.Equal(t, 2, blockchainClient.attempts(), "the retry must attempt the invalidate again rather than return silently")

	_, metaAfter, err := realClient.GetBlockHeader(ctx, block.Header.Hash())
	require.NoError(t, err)
	require.NotNil(t, metaAfter)
	require.True(t, metaAfter.Invalid,
		"the re-queue must CONVERGE: the optimistically-added corrupt tip has to end up invalid in the store")
}

// newCorruptRevalidationHarness builds a block whose body fails CheckMerkleRoot (a real coinbase and
// subtree with a zeroed header merkle root, so block.Valid returns ERR_BLOCK_CORRUPT) together with a
// BlockValidation wired to drive reValidateBlock directly. blockIsStored controls what
// GetBlockExists reports, which is the on-chain guard the corrupt branch keys on. The returned
// channel receives one value per InvalidateBlock call.
func newCorruptRevalidationHarness(ctx context.Context, t *testing.T, blockIsStored bool) (*BlockValidation, *model.Block, chan struct{}) {
	t.Helper()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	coinbaseTx := bt.NewTx()
	require.NoError(t, coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	require.NoError(t, coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000))

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	subtreeStore := blobmemory.New()
	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	nBits, err := model.NewNBitFromString("2000ffff")
	require.NoError(t, err)

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: &chainhash.Hash{},         // zeroed -> CheckMerkleRoot fails -> corrupt
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}

		blockHeader.Nonce++
	}

	block, err := model.NewBlock(blockHeader, coinbaseTx, []*chainhash.Hash{subtree.RootHash()},
		uint64(subtree.Length()), uint64(coinbaseTx.Size()), 100, 0) //nolint:gosec
	require.NoError(t, err)

	invalidateCalled := make(chan struct{}, 2)

	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(blockIsStored, nil).Maybe()
	mockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil).Maybe()
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 99, MinedSet: true}, nil).Maybe()
	mockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).Return(nBits, nil).Maybe()
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 100}, nil).Maybe()
	mockBlockchain.On("InvalidateBlock", mock.Anything, mock.Anything).Return([]chainhash.Hash{}, nil).Run(func(mock.Arguments) {
		select {
		case invalidateCalled <- struct{}{}:
		default:
		}
	}).Maybe()

	subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
	subtreeValidationClient.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)
	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	bv := &BlockValidation{
		logger:                        logger,
		settings:                      tSettings,
		blockchainClient:              mockBlockchain,
		subtreeStore:                  subtreeStore,
		txStore:                       blobmemory.New(),
		utxoStore:                     utxoStore,
		subtreeValidationClient:       subtreeValidationClient,
		lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
		blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
		subtreeExistsCache:            expiringmap.New[chainhash.Hash, bool](10 * time.Minute),
		blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
		blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
		setMinedChan:                  make(chan *chainhash.Hash, 1),
		revalidateBlockChan:           make(chan revalidateBlockData, 2),
		stats:                         gocore.NewStat("blockvalidation"),
	}

	return bv, block, invalidateCalled
}

// TestReValidateBlock_CorruptConvergesOnlyWhenOnChain covers freemans13 item 4
// (bitcoin-sv/teranode#4692). The optimistic path re-queues revalidation when InvalidateBlock fails,
// promising to "converge on invalidation" — but reValidateBlock's gate matched only ErrBlockInvalid
// and ErrBlockIncomplete, and a corrupt body matches neither, so the retry re-ran block.Valid, failed
// corrupt again and returned WITHOUT invalidating: it could never converge.
//
// The fix gates the new corrupt branch on the block actually being in the store, so it converges for
// the optimistically-added tip while the callers that never AddBlock'd — the GetBlockHeaders failure
// paths in ValidateBlockWithOptions — can never poison a hash through it.
func TestReValidateBlock_CorruptConvergesOnlyWhenOnChain(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("corrupt body already on chain converges on invalidation", func(t *testing.T) {
		bv, block, invalidateCalled := newCorruptRevalidationHarness(ctx, t, true)
		defer bv.StopCaches()

		err := bv.reValidateBlock(revalidateBlockData{block: block, baseURL: "test"})
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err), "the retry must still see a corrupt body, got: %v", err)

		select {
		case <-invalidateCalled:
			// good: the re-queued revalidation converged instead of returning silently
		default:
			t.Fatal("an optimistically-added corrupt body must be invalidated on revalidation, otherwise the re-queue can never converge")
		}
	})

	t.Run("corrupt body not in the store is never invalidated", func(t *testing.T) {
		bv, block, invalidateCalled := newCorruptRevalidationHarness(ctx, t, false)
		defer bv.StopCaches()

		err := bv.reValidateBlock(revalidateBlockData{block: block, baseURL: "test"})
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err), "got: %v", err)

		select {
		case <-invalidateCalled:
			t.Fatal("a corrupt body that was never added must NOT be invalidated — that would poison a hash we hold no bound evidence against")
		default:
			// good: the guard held
		}
	})
}

// TestValidateBlock_SubtreeCorrupt_StrikeGatedOnRevalidation covers the fourth corrupt-strike gate
// (bitcoin-sv/teranode#4692): when subtree validation returns a corrupt-body verdict, the serving
// peer is struck on a normal (serving) delivery but NOT on the revalidation path — RevalidateBlock
// carries the original announcing peer's stale ID, which neither served this read nor is
// necessarily connected. Fault injection: CheckBlockSubtrees returns corrupt. Behaviour: a strike
// on the non-revalidation run, none on the revalidation run.
func TestValidateBlock_SubtreeCorrupt_StrikeGatedOnRevalidation(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// A mined block carrying a subtree, so the flow reaches validateBlockSubtrees.
	coinbaseTx := bt.NewTx()
	_ = coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x64, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())

	nBits, _ := model.NewNBitFromString("2000ffff")
	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tSettings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}
	for {
		if ok, _, _ := blockHeader.HasMetTargetDifficulty(); ok {
			break
		}
		blockHeader.Nonce++
	}

	block, err := model.NewBlock(blockHeader, coinbaseTx, []*chainhash.Hash{subtree.RootHash()}, uint64(subtree.Length()), uint64(coinbaseTx.Size()), 100, 0) //nolint:gosec
	require.NoError(t, err)

	// run drives ValidateBlockWithOptions with a corrupt subtree verdict and returns the strikes the
	// fake p2p client recorded.
	run := func(isRevalidation bool) []corruptBanScoreCall {
		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)
		utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
		require.NoError(t, err)

		mockBlockchain := &blockchain.Mock{}
		mockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil).Maybe()
		// Revalidation requires the block to currently be marked invalid.
		mockBlockchain.On("GetBlockHeader", mock.Anything, block.Header.Hash()).Return(block.Header, &model.BlockHeaderMeta{Invalid: true}, nil).Maybe()
		mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 99, MinedSet: true}, nil).Maybe()
		mockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil).Maybe()
		mockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).Return(nBits, nil).Maybe()
		mockBlockchain.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil).Maybe()
		mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 100}, nil).Maybe()

		subtreeValidationClient := &subtreevalidation.MockSubtreeValidation{}
		subtreeValidationClient.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(errors.NewBlockCorruptError("corrupt subtree body"))

		fake := &corruptStrikeP2PClient{}

		bv := &BlockValidation{
			logger:                        logger,
			settings:                      tSettings,
			blockchainClient:              mockBlockchain,
			subtreeStore:                  blobmemory.New(),
			txStore:                       blobmemory.New(),
			utxoStore:                     utxoStore,
			subtreeValidationClient:       subtreeValidationClient,
			p2pClient:                     fake,
			lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
			blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute),
			subtreeExistsCache:            expiringmap.New[chainhash.Hash, bool](10 * time.Minute),
			blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
			blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
			setMinedChan:                  make(chan *chainhash.Hash, 1),
			revalidateBlockChan:           make(chan revalidateBlockData, 2),
			stats:                         gocore.NewStat("blockvalidation"),
		}
		defer bv.StopCaches()

		err = bv.ValidateBlockWithOptions(ctx, block, "test", &ValidateBlockOptions{
			IsRevalidation:          isRevalidation,
			DisableOptimisticMining: true,
			PeerID:                  "announcing-peer",
		})
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err), "a corrupt subtree body must surface as corrupt, got: %v", err)

		return fake.recorded()
	}

	// Normal serving delivery: the serving peer IS struck.
	serving := run(false)
	require.Len(t, serving, 1, "a non-revalidation corrupt subtree must strike the serving peer once")
	require.Equal(t, "announcing-peer", serving[0].peerID)

	// Revalidation: the stale announcing peer must NOT be struck.
	require.Empty(t, run(true), "revalidation must NOT strike the stale announcing peer (bitcoin-sv/teranode#4692)")
}
