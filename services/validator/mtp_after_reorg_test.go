package validator

import (
	"context"
	"net/url"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchainsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// reorgTestBlocks builds n blocks on top of parentHash, the first at height firstHeight, with
// block times 600 seconds apart plus skew.
func reorgTestBlocks(t *testing.T, parentHash *chainhash.Hash, firstHeight uint32, n int, skew uint32) []*model.Block {
	t.Helper()

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, subtree.AddNode(*coinbase.TxIDChainHash(), 0, 0))

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	blocks := make([]*model.Block, 0, n)
	prev := parentHash

	for i := 0; i < n; i++ {
		block := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				Timestamp:      1_600_000_000 + 600*(firstHeight+uint32(i)) + skew, //nolint:gosec // test sizes are small
				Nonce:          2573394689,
				HashPrevBlock:  prev,
				HashMerkleRoot: subtree.RootHash(),
				Bits:           *nBits,
			},
			CoinbaseTx:       coinbase,
			TransactionCount: 1,
			Subtrees:         []*chainhash.Hash{subtree.RootHash()},
		}

		blocks = append(blocks, block)
		prev = block.Hash()
	}

	return blocks
}

// medianBelow is the median of the 11 timestamps below height h, read from timeAt.
func medianBelow(h uint32, timeAt func(uint32) uint32) uint32 {
	window := make([]uint32, 0, 11)
	for x := h - 11; x < h; x++ {
		window = append(window, timeAt(x))
	}

	slices.Sort(window)

	return window[len(window)/2]
}

// reorgFixture is a sqlitememory blockchain store with chain A (heights 1 to 60) and chain B
// (leaving A after height 20, block times two hours ahead, heights 21 to 60), A current, and a
// validator whose MTP store is loaded along A through height 60. makeBCurrent stores B's block
// at 61, making B the current chain.
type reorgFixture struct {
	v              *Validator
	client         blockchain.ClientI
	loads          *loadCounter
	chainA, chainB []*model.Block
	makeBCurrent   func()
}

func newReorgFixture(t *testing.T) *reorgFixture {
	t.Helper()

	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings(t)
	params := *tSettings.ChainCfgParams
	params.CSVHeight = 0
	params.GenesisActivationHeight = 1_000
	tSettings.ChainCfgParams = &params

	storeURL, err := url.Parse("sqlitememory:///" + t.Name())
	require.NoError(t, err)

	store, err := blockchainsql.New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	chainA := reorgTestBlocks(t, params.GenesisHash, 1, 60, 0)
	chainB := reorgTestBlocks(t, chainA[19].Hash(), 21, 41, 7_200)

	for _, block := range append(append([]*model.Block{}, chainA...), chainB[:40]...) {
		_, _, err = store.StoreBlock(ctx, block, "")
		require.NoError(t, err)
	}

	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, store, nil, nil)
	require.NoError(t, err)

	loads := &loadCounter{ClientI: client}

	v := &Validator{
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		txValidator:      NewTxValidator(ulogger.TestLogger{}, tSettings),
		blockchainClient: loads,
		stats:            gocore.NewStat("validator_test"),
	}
	v.txValidator.(*TxValidator).bdk = noopBDKValidator{}

	best, _, err := client.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, chainA[59].Hash().String(), best.Hash().String(), "chain A must be current while the store loads")
	require.NoError(t, v.EnsureMTPLoaded(ctx, 60))

	return &reorgFixture{v: v, client: client, loads: loads, chainA: chainA, chainB: chainB, makeBCurrent: func() {
		_, _, storeErr := store.StoreBlock(ctx, chainB[40], "")
		require.NoError(t, storeErr)

		best, _, bestErr := client.GetBestBlockHeader(ctx)
		require.NoError(t, bestErr)
		require.Equal(t, chainB[40].Hash().String(), best.Hash().String(), "chain B must be current")
	}}
}

func (f *reorgFixture) timeOnA(h uint32) uint32 { return f.chainA[h-1].Header.Timestamp }

func (f *reorgFixture) timeOnB(h uint32) uint32 {
	if h > 20 {
		return f.chainB[h-21].Header.Timestamp
	}

	return f.chainA[h-1].Header.Timestamp
}

func (f *reorgFixture) blockID(t *testing.T, block *model.Block) uint32 {
	t.Helper()

	_, meta, err := f.client.GetBlockHeader(context.Background(), block.Hash())
	require.NoError(t, err)

	return meta.ID
}

// loadCounter counts the main-chain reads of a load's top block, one per store load.
type loadCounter struct {
	blockchain.ClientI

	calls atomic.Int32
}

func (c *loadCounter) GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	c.calls.Add(1)

	return c.ClientI.GetBlockHeadersByHeight(ctx, startHeight, endHeight)
}

// timeLockedFor40Units is a v2 tx whose input carries a 40-unit (20,480 second) time lock.
func timeLockedFor40Units(t *testing.T) *bt.Tx {
	t.Helper()

	tx := bip68Tx(t)
	tx.Version = 2
	tx.Inputs[0].SequenceNumber = SequenceLockTimeTypeFlag | 40

	return tx
}

// TestValidateTransaction_BIP68AfterReorgReadsTheNewChain checks a block at 60 on B, whose parent
// is on the current chain, with a coin at 30: 18,000 seconds old on B, short of a 40-unit lock,
// but past it measured from A's cached median. The store must reload along B first.
func TestValidateTransaction_BIP68AfterReorgReadsTheNewChain(t *testing.T) {
	f := newReorgFixture(t)
	require.NoError(t, f.v.EnsureMTPLoaded(context.Background(), 61)) // one entry past the candidate, on A
	f.makeBCurrent()

	const coinHeight = uint32(30)

	blockMTP := medianBelow(60, f.timeOnB)
	require.Less(t, blockMTP-medianBelow(coinHeight, f.timeOnB), uint32(40*512))
	require.GreaterOrEqual(t, blockMTP-medianBelow(coinHeight, f.timeOnA), uint32(40*512))

	opts := &Options{SkipPolicyChecks: true, CandidateParentMedianTime: blockMTP, CandidateBaseID: f.blockID(t, f.chainB[38]), CandidateBaseSet: true}

	err := f.v.validateTransaction(context.Background(), timeLockedFor40Units(t), 60, []uint32{coinHeight}, opts)
	require.ErrorContains(t, err, "transaction sequence lock time not satisfied")
	require.Equal(t, medianBelow(coinHeight, f.timeOnB), f.v.mtpStore[coinHeight])
	require.Len(t, f.v.mtpStore, 61, "the entry above the reload belonged to A and must be cut")
}

// TestValidateTransaction_BIP68FailsWhenTheStoreCannotHoldTheBase checks that once the store has
// moved to B, a candidate naming a base on A (its block at 59) fails with a ProcessingError rather
// than being judged on B's medians. While the store still held A, it would read A's, correctly.
func TestValidateTransaction_BIP68FailsWhenTheStoreCannotHoldTheBase(t *testing.T) {
	f := newReorgFixture(t)
	f.makeBCurrent()
	require.NoError(t, f.v.EnsureMTPLoaded(context.Background(), 61))

	opts := &Options{SkipPolicyChecks: true, CandidateParentMedianTime: medianBelow(60, f.timeOnA), CandidateBaseID: f.blockID(t, f.chainA[58]), CandidateBaseSet: true}

	before := f.loads.calls.Load()

	for range 5 {
		err := f.v.validateTransaction(context.Background(), timeLockedFor40Units(t), 60, []uint32{30}, opts)
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.ErrorContains(t, err, "does not hold block ID")
	}

	require.Equal(t, int32(1), f.loads.calls.Load()-before, "the block's other transactions must not reload again")

	// Once the memory lapses, a retried block reloads.
	f.v.mtpUnheldAt = time.Now().Add(-2 * mtpUnheldMemory)

	err := f.v.validateTransaction(context.Background(), timeLockedFor40Units(t), 60, []uint32{30}, opts)
	require.ErrorIs(t, err, errors.ErrProcessing)
	require.Equal(t, int32(2), f.loads.calls.Load()-before)
}

// TestValidateTransaction_BIP68ReloadsAShortStore checks that a candidate above what the store
// covers, as after another candidate's reload cut it back, reloads instead of failing.
func TestValidateTransaction_BIP68ReloadsAShortStore(t *testing.T) {
	f := newReorgFixture(t)
	f.makeBCurrent()

	opts := &Options{SkipPolicyChecks: true, CandidateParentMedianTime: medianBelow(62, f.timeOnB), CandidateBaseID: f.blockID(t, f.chainB[40]), CandidateBaseSet: true}

	require.NoError(t, f.v.validateTransaction(context.Background(), bip68Tx(t), 62, []uint32{30}, opts))
	require.Len(t, f.v.mtpStore, 63)
}

// TestEnsureMTPLoaded_ExtensionRepairsADeepReorg checks that extending the store after a reorg
// 40 blocks deep (B leaves A after 20) replaces every entry above the fork, not only the last 12.
func TestEnsureMTPLoaded_ExtensionRepairsADeepReorg(t *testing.T) {
	f := newReorgFixture(t)
	f.makeBCurrent()

	require.NoError(t, f.v.EnsureMTPLoaded(context.Background(), 61))

	for h := uint32(12); h <= 61; h++ {
		require.Equal(t, medianBelow(h, f.timeOnB), f.v.mtpStore[h], "height %d", h)
	}

	require.Equal(t, f.blockID(t, f.chainB[39]), f.v.mtpIDs[61])
}
