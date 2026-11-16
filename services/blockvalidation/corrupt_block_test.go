package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/stores/blob"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// A peer can replay an honest block header (same hash, real proof of work) and serve a body that
// does not belong to it. Such a body must be rejected as corrupt without ever persisting
// invalid=true, so the honest body validates when it arrives.

const corruptTestSubtreeSize = 4

// corruptTestFixture is an honest block over several full subtrees, with every subtree, its meta
// and every transaction stored, plus one spare stored subtree that belongs to no block.
type corruptTestFixture struct {
	ctx              context.Context
	bv               *BlockValidation
	blockchainClient blockchain.ClientI
	subtreeStore     blob.Store
	coinbase         *bt.Tx
	subtrees         []*subtreepkg.Subtree
	spare            *subtreepkg.Subtree
	honest           *model.Block
}

// newCorruptTestFixture builds the honest block over subtrees of the given leaf counts. Every
// subtree has capacity corruptTestSubtreeSize; only the last may hold fewer leaves.
func newCorruptTestFixture(t *testing.T, sizes ...int) *corruptTestFixture {
	t.Helper()

	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	utxoStore, subtreeValidationClient, blockchainClient, txStore, subtreeStore, deferFunc := setup(t)
	t.Cleanup(deferFunc)

	f := &corruptTestFixture{ctx: ctx, blockchainClient: blockchainClient, subtreeStore: subtreeStore}

	txSeed := uint32(0)
	fees := uint64(0)

	newSubtree := func(withCoinbase bool, leaves int) *subtreepkg.Subtree {
		st, err := subtreepkg.NewTreeByLeafCount(corruptTestSubtreeSize)
		require.NoError(t, err)

		meta := subtreepkg.NewSubtreeMeta(st)

		if withCoinbase {
			require.NoError(t, st.AddCoinbaseNode())
		}

		for st.Length() < leaves {
			txSeed++

			parentTx := newTx(txSeed + 100000)
			_, err = utxoStore.Create(ctx, parentTx, 0, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 0, BlockHeight: 0}))
			require.NoError(t, err)

			tx := newTx(txSeed, parentTx.TxIDChainHash())
			require.NoError(t, st.AddNode(*tx.TxIDChainHash(), 100, 0))
			require.NoError(t, meta.SetTxInpointsFromTx(tx))

			_, err = utxoStore.Create(ctx, tx, 0)
			require.NoError(t, err)

			fees += 100
		}

		stBytes, err := st.Serialize()
		require.NoError(t, err)
		require.NoError(t, subtreeStore.Set(ctx, st.RootHash()[:], fileformat.FileTypeSubtree, stBytes))

		metaBytes, err := meta.Serialize()
		require.NoError(t, err)
		require.NoError(t, subtreeStore.Set(ctx, st.RootHash()[:], fileformat.FileTypeSubtreeMeta, metaBytes))

		return st
	}

	nrSubtrees := len(sizes)

	for i, leaves := range sizes {
		f.subtrees = append(f.subtrees, newSubtree(i == 0, leaves))
	}

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	coinbase.Outputs = nil
	require.NoError(t, coinbase.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000000000+fees))

	f.spare = newSubtree(false, corruptTestSubtreeSize)
	f.coinbase = coinbase

	// Top-level merkle root, with the coinbase placeholder replaced and a short final subtree
	// lifted to the height of the first, as model.Block.CheckMerkleRoot computes it.
	top, err := subtreepkg.NewIncompleteTreeByLeafCount(nrSubtrees)
	require.NoError(t, err)

	subtreeHashes := make([]*chainhash.Hash, 0, nrSubtrees)

	for i, st := range f.subtrees {
		root := st.RootHash()
		if i == 0 {
			root, err = st.RootHashWithReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size()))
			require.NoError(t, err)
		} else if st.Length() < corruptTestSubtreeSize {
			root, err = st.RootHashPadded(f.subtrees[0].Height)
			require.NoError(t, err)
		}

		require.NoError(t, top.AddNode(*root, 1, 0))
		subtreeHashes = append(subtreeHashes, st.RootHash())
	}

	merkleRoot, err := chainhash.NewHash(top.RootHash()[:])
	require.NoError(t, err)

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	hashPrevBlock, err := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
	require.NoError(t, err)

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: merkleRoot,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
	}

	for {
		if ok, _, _ := header.HasMetTargetDifficulty(); ok {
			break
		}

		header.Nonce++
	}

	txCount := 0
	for _, leaves := range sizes {
		txCount += leaves
	}

	f.honest, err = model.NewBlock(header, coinbase, subtreeHashes, uint64(txCount), 123123, 1, 0) //nolint:gosec
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false
	tSettings.GlobalBlockHeightRetention = 0

	f.bv = NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

	return f
}

// doctored returns a block with the honest header (same hash) and the given body parts.
func (f *corruptTestFixture) doctored(t *testing.T, coinbase *bt.Tx, subtrees []*chainhash.Hash, txCount uint64) *model.Block {
	t.Helper()

	header := *f.honest.Header

	b, err := model.NewBlock(&header, coinbase, subtrees, txCount, 123123, 1, 0)
	require.NoError(t, err)
	require.Equal(t, *f.honest.Hash(), *b.Hash(), "the doctored body must replay the honest header")

	return b
}

func (f *corruptTestFixture) subtreeHashes(idx ...int) []*chainhash.Hash {
	out := make([]*chainhash.Hash, 0, len(idx))
	for _, i := range idx {
		out = append(out, f.subtrees[i].RootHash())
	}

	return out
}

// validate runs one validation attempt and waits until the per-hash dedupe entry is released, so
// the next attempt for the same hash really validates instead of sharing this result.
func (f *corruptTestFixture) validate(t *testing.T, block *model.Block, peerID string) error {
	t.Helper()

	err := f.bv.ValidateBlockWithOptions(f.ctx, block, "http://localhost:8000", &ValidateBlockOptions{PeerID: peerID})

	require.Eventually(t, func() bool {
		_, inFlight := f.bv.blocksCurrentlyValidating.Get(*block.Hash())
		return !inFlight
	}, 5*time.Second, 10*time.Millisecond)

	return err
}

func (f *corruptTestFixture) requireNotPersistedInvalid(t *testing.T) {
	t.Helper()

	_, meta, err := f.blockchainClient.GetBlockHeader(f.ctx, f.honest.Hash())
	if err != nil {
		require.True(t, errors.Is(err, errors.ErrBlockNotFound) || errors.Is(err, errors.ErrNotFound), "unexpected lookup error: %v", err)
		return
	}

	require.False(t, meta.Invalid, "a corrupt body must never persist invalid=true for the honest hash")
}

func (f *corruptTestFixture) requireHonestAccepted(t *testing.T) {
	t.Helper()

	require.NoError(t, f.validate(t, f.honest, "honest-peer"), "the honest body must validate after the doctored one was rejected")

	_, meta, err := f.blockchainClient.GetBlockHeader(f.ctx, f.honest.Hash())
	require.NoError(t, err)
	require.False(t, meta.Invalid)
}

func TestValidateBlock_CorruptBodyDoesNotPoisonHonestBlock(t *testing.T) {
	t.Run("wrong subtree list: merkle root mismatch", func(t *testing.T) {
		f := newCorruptTestFixture(t, corruptTestSubtreeSize, corruptTestSubtreeSize)

		bad := f.doctored(t, f.coinbase, []*chainhash.Hash{f.subtrees[0].RootHash(), f.spare.RootHash()}, 2*corruptTestSubtreeSize)

		err := f.validate(t, bad, "attacker")
		require.True(t, errors.IsBlockCorrupt(err), "expected corrupt, got: %v", err)
		require.Contains(t, err.Error(), "merkle root does not match")
		f.requireNotPersistedInvalid(t)
		f.requireHonestAccepted(t)
	})

	t.Run("duplicated subtree root", func(t *testing.T) {
		f := newCorruptTestFixture(t, corruptTestSubtreeSize, corruptTestSubtreeSize)

		bad := f.doctored(t, f.coinbase, f.subtreeHashes(0, 1, 1), 3*corruptTestSubtreeSize)

		err := f.validate(t, bad, "attacker")
		require.True(t, errors.IsBlockCorrupt(err), "expected corrupt, got: %v", err)
		f.requireNotPersistedInvalid(t)
		f.requireHonestAccepted(t)
	})

	t.Run("emptied subtree list", func(t *testing.T) {
		f := newCorruptTestFixture(t, corruptTestSubtreeSize, corruptTestSubtreeSize)

		bad := f.doctored(t, f.coinbase, []*chainhash.Hash{}, 1)

		err := f.validate(t, bad, "attacker")
		require.True(t, errors.IsBlockCorrupt(err), "expected corrupt, got: %v", err)
		f.requireNotPersistedInvalid(t)
		f.requireHonestAccepted(t)
	})

	t.Run("tampered coinbase with a bad scriptSig length", func(t *testing.T) {
		f := newCorruptTestFixture(t, corruptTestSubtreeSize, corruptTestSubtreeSize)

		tampered := f.coinbase.Clone()
		tampered.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x01})

		bad := f.doctored(t, tampered, f.subtreeHashes(0, 1), 2*corruptTestSubtreeSize)

		err := f.validate(t, bad, "attacker")
		require.True(t, errors.IsBlockCorrupt(err), "expected corrupt, got: %v", err)
		f.requireNotPersistedInvalid(t)
		f.requireHonestAccepted(t)
	})

	t.Run("declared size above excessiveblocksize", func(t *testing.T) {
		f := newCorruptTestFixture(t, corruptTestSubtreeSize, corruptTestSubtreeSize)
		f.bv.settings.Policy.ExcessiveBlockSize = 1 << 20

		bad := f.doctored(t, f.coinbase, f.subtreeHashes(0, 1), 2*corruptTestSubtreeSize)
		bad.SizeInBytes = 1 << 30

		err := f.validate(t, bad, "attacker")
		require.True(t, errors.IsBlockCorrupt(err), "a peer-declared size must be treated as a corrupt body, got: %v", err)
		require.True(t, isRefetchableBlockError(err))
		f.requireNotPersistedInvalid(t)
		f.requireHonestAccepted(t)
	})

	t.Run("final subtree replaced by its duplicated-half mutation", func(t *testing.T) {
		// CVE-2012-2459: a half-filled final subtree F and M = F||F share a merkle root once F is
		// lifted, so the honest header also commits to the body [S0, M].
		half := corruptTestSubtreeSize / 2
		f := newCorruptTestFixture(t, corruptTestSubtreeSize, half)

		final := f.subtrees[1]

		mutated, err := subtreepkg.NewTreeByLeafCount(corruptTestSubtreeSize)
		require.NoError(t, err)

		for i := 0; i < corruptTestSubtreeSize; i++ {
			node := final.Nodes[i%half]
			require.NoError(t, mutated.AddNode(node.Hash, node.Fee, node.SizeInBytes))
		}

		lifted, err := final.RootHashPadded(f.subtrees[0].Height)
		require.NoError(t, err)
		require.Equal(t, *lifted, *mutated.RootHash(), "the mutation must keep the lifted root")

		mutatedBytes, err := mutated.Serialize()
		require.NoError(t, err)
		require.NoError(t, f.subtreeStore.Set(f.ctx, mutated.RootHash()[:], fileformat.FileTypeSubtree, mutatedBytes))

		bad := f.doctored(t, f.coinbase, []*chainhash.Hash{f.subtrees[0].RootHash(), mutated.RootHash()}, uint64(corruptTestSubtreeSize+half)) //nolint:gosec

		err = f.validate(t, bad, "attacker")
		require.True(t, errors.IsBlockCorrupt(err), "expected corrupt, got: %v", err)
		require.Contains(t, err.Error(), "duplicate transaction")
		f.requireNotPersistedInvalid(t)
		f.requireHonestAccepted(t)
	})
}

// TestOptimisticMiningDisabledForPeerPath pins the gate: optimistic mining runs on the peer-served
// path only when both OptimisticMining and OptimisticMiningPeerBlocks are set, never for legacy.
func TestOptimisticMiningDisabledForPeerPath(t *testing.T) {
	cases := []struct {
		global   bool
		peer     bool
		baseURL  string
		disabled bool
	}{
		{global: false, peer: false, baseURL: "http://peer:8000", disabled: true},
		{global: false, peer: true, baseURL: "http://peer:8000", disabled: true},
		{global: true, peer: false, baseURL: "http://peer:8000", disabled: true},
		{global: true, peer: true, baseURL: "http://peer:8000", disabled: false},
		{global: true, peer: true, baseURL: "legacy", disabled: true},
		{global: false, peer: false, baseURL: "legacy", disabled: true},
	}

	for _, c := range cases {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = c.global
		tSettings.BlockValidation.OptimisticMiningPeerBlocks = c.peer

		require.Equal(t, c.disabled, optimisticMiningDisabledForPeerPath(tSettings, c.baseURL),
			"OptimisticMining=%v OptimisticMiningPeerBlocks=%v baseURL=%s", c.global, c.peer, c.baseURL)
	}

	require.True(t, optimisticMiningDisabledForPeerPath(nil, "http://peer:8000"), "nil settings must report disabled")
}

// TestIsUnvalidatablePeerError_CorruptIsNotUnvalidatable pins the routing: a corrupt body must
// fall through to alternative-peer retry instead of giving up on the block.
func TestIsUnvalidatablePeerError_CorruptIsNotUnvalidatable(t *testing.T) {
	require.False(t, isUnvalidatablePeerError(errors.NewBlockCorruptError("corrupt body")))
	require.False(t, isUnvalidatablePeerError(errors.NewServiceError("outer", errors.NewBlockCorruptError("corrupt body"))))
	require.True(t, isUnvalidatablePeerError(errors.NewBlockInvalidError("invalid")))
	require.True(t, isUnvalidatablePeerError(errors.NewTxInvalidError("tx invalid")))
}

// TestIsRefetchableBlockError pins that a corrupt body, wrapped the way processBlockFound returns
// it, sends the block to the alternative-source retry.
func TestIsRefetchableBlockError(t *testing.T) {
	corrupt := errors.NewBlockCorruptError("merkle root does not match")

	require.True(t, isRefetchableBlockError(corrupt))
	require.True(t, isRefetchableBlockError(errors.NewServiceError("failed block validation BlockFound", corrupt)))
	require.True(t, isRefetchableBlockError(errors.NewNetworkTimeoutError("timeout")))
	require.False(t, isRefetchableBlockError(errors.NewServiceError("failed block validation BlockFound", errors.NewBlockInvalidError("invalid"))))
	require.False(t, isRefetchableBlockError(nil))
}

// failingSubtreeValidation stands in for subtree validation of a body whose subtree list names an
// attacker-built subtree holding an invalid transaction.
func failingSubtreeValidation() *subtreevalidation.MockSubtreeValidation {
	m := &subtreevalidation.MockSubtreeValidation{}
	m.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.NewProcessingError("[CheckBlockSubtrees] failed to process transactions", errors.NewTxInvalidError("bad signature")))

	return m
}

// TestValidateBlock_InvalidTxInUnboundSubtreeListIsCorrupt: subtree validation runs before the
// merkle binding, so an invalid transaction there may come from a subtree the peer substituted
// and cannot condemn the block hash.
func TestValidateBlock_InvalidTxInUnboundSubtreeListIsCorrupt(t *testing.T) {
	f := newCorruptTestFixture(t, corruptTestSubtreeSize, corruptTestSubtreeSize)

	honestClient := f.bv.subtreeValidationClient
	f.bv.subtreeValidationClient = failingSubtreeValidation()

	bad := f.doctored(t, f.coinbase, []*chainhash.Hash{f.subtrees[0].RootHash(), f.spare.RootHash()}, 2*corruptTestSubtreeSize)

	err := f.validate(t, bad, "attacker")
	require.True(t, errors.IsBlockCorrupt(err), "expected corrupt, got: %v", err)
	require.False(t, isUnvalidatablePeerError(err), "catchup must still try other peers for an unbound list")
	f.requireNotPersistedInvalid(t)

	f.bv.subtreeValidationClient = honestClient
	f.requireHonestAccepted(t)
}

// TestValidateBlock_InvalidTxOnLegacyRouteStaysInvalid: the legacy route checks the merkle root
// before it builds the subtree list, so an invalid transaction there is a verdict on the block.
func TestValidateBlock_InvalidTxOnLegacyRouteStaysInvalid(t *testing.T) {
	f := newCorruptTestFixture(t, corruptTestSubtreeSize, corruptTestSubtreeSize)
	f.bv.subtreeValidationClient = failingSubtreeValidation()

	err := f.bv.ValidateBlockWithOptions(f.ctx, f.honest, "legacy", &ValidateBlockOptions{DisableOptimisticMining: true})
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "expected invalid, got: %v", err)
	require.False(t, errors.IsBlockCorrupt(err))
}

// TestRunOncePerBlock_CorruptResultIsNotShared: a caller that waited on another body's validation
// of the same hash must not inherit a corrupt verdict about that other body.
func TestRunOncePerBlock_CorruptResultIsNotShared(t *testing.T) {
	bv := &BlockValidation{
		logger:                    ulogger.TestLogger{},
		blocksCurrentlyValidating: txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
	}

	hash := chainhash.Hash{0x01}
	release := make(chan struct{})
	firstStarted := make(chan struct{})
	firstDone := make(chan error, 1)

	go func() {
		firstDone <- bv.runOncePerBlock(&hash, &ValidateBlockOptions{}, func(*ValidateBlockOptions) error {
			close(firstStarted)
			<-release

			return errors.NewBlockCorruptError("merkle root does not match")
		})
	}()

	<-firstStarted

	secondDone := make(chan error, 1)

	go func() {
		secondDone <- bv.runOncePerBlock(&hash, &ValidateBlockOptions{}, func(*ValidateBlockOptions) error {
			return nil
		})
	}()

	// Give the second caller time to start waiting on the first validation.
	time.Sleep(50 * time.Millisecond)
	close(release)

	require.True(t, errors.IsBlockCorrupt(<-firstDone))
	require.NoError(t, <-secondDone, "the waiting caller must validate its own body")
}
