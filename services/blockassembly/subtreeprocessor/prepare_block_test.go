package subtreeprocessor

import (
	"context"
	"encoding/hex"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const prepareTestSubtree = "fd61a79793c4fb02ba14b85df98f5f60f727be359089d8fa125c4ce37945106b"

// newPrepareTestStp returns a subtree processor on the genesis block, its subtree store holding
// the testdata subtree, and a block on top of genesis made of that subtree.
func newPrepareTestStp(t *testing.T) (*SubtreeProcessor, blob.Store, *model.Block) {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///prepare")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, test.CreateBaseTestSettings(t), utxoStoreURL)
	require.NoError(t, err)

	subtreeStore := blob_memory.New()
	putPrepareTestSubtree(t, subtreeStore)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 32

	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	newSubtreeChan := make(chan NewSubtreeRequest, 16)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	stp, err := NewSubtreeProcessor(ctx, logger, tSettings, subtreeStore, blockchainClient, utxoStore, newSubtreeChan)
	require.NoError(t, err)

	stp.currentBlockHeader.Store(model.GenesisBlockHeader)

	blockBytes, err := hex.DecodeString("000000206a21d13c3d2656557493b4652f67a763f835b86bf90107a60f412c290000000083ba48026c405d5a4b4d5aa3f10cee9de605a012e9a25f72a19aa9fe123380c689505c67c874461cc6dda18002fde501016b104579e34c5c12fad8899035be27f7605f8ff95db814ba02fbc49397a761fd01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1903af32190000000000205f7c477c327c437c5f200001000000ffffffff01e50b5402000000001976a9147a112f6a373b80b4ebb2b02acef97f35aef7494488ac00000000feaf321900")
	require.NoError(t, err)

	block, err := model.NewBlockFromBytes(blockBytes)
	require.NoError(t, err)

	block.Header.HashPrevBlock = model.GenesisBlockHeader.Hash()
	block.GetHash() // refresh the cached hash after the header change

	return stp, subtreeStore, block
}

func putPrepareTestSubtree(t *testing.T, store blob.Store) {
	t.Helper()

	hash, err := chainhash.NewHashFromStr(prepareTestSubtree)
	require.NoError(t, err)

	b, err := os.ReadFile("./testdata/" + prepareTestSubtree + ".subtree")
	require.NoError(t, err)
	require.NoError(t, store.Set(context.Background(), hash.CloneBytes(), fileformat.FileTypeSubtree, b))
}

func delPrepareTestSubtree(t *testing.T, store blob.Store) {
	t.Helper()

	hash, err := chainhash.NewHashFromStr(prepareTestSubtree)
	require.NoError(t, err)
	require.NoError(t, store.Del(context.Background(), hash.CloneBytes(), fileformat.FileTypeSubtree))
}

// waitPrepared waits until the map prepared for hash is built and returns whether one exists.
func waitPrepared(t *testing.T, stp *SubtreeProcessor, hash *chainhash.Hash) bool {
	t.Helper()

	stp.preparedMu.Lock()
	p := stp.prepared
	stp.preparedMu.Unlock()

	if p == nil || !p.hash.IsEqual(hash) {
		return false
	}

	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		t.Fatal("prepared transaction map not built in time")
	}

	return true
}

func otherBlockOnSameParent(t *testing.T, block *model.Block) *model.Block {
	t.Helper()

	b, err := block.Bytes()
	require.NoError(t, err)

	other, err := model.NewBlockFromBytes(b)
	require.NoError(t, err)

	other.Header.HashPrevBlock = block.Header.HashPrevBlock
	other.Header.Nonce++
	other.GetHash()
	require.False(t, other.Hash().IsEqual(block.Hash()))

	return other
}

// A map prepared for a block is the one moveForwardBlock uses for it: once prepared, the block's
// subtrees are not read again.
func TestPrepareBlock_MoveForwardUsesPreparedMap(t *testing.T) {
	ctx := context.Background()
	stp, store, block := newPrepareTestStp(t)

	blockSubtreesMap, _ := stp.processBlockSubtrees(block)
	want, _, err := stp.createTransactionMapIfNeeded(ctx, block, blockSubtreesMap)
	require.NoError(t, err)
	require.Positive(t, want.Length())

	stp.PrepareBlock(block)
	require.True(t, waitPrepared(t, stp, block.Hash()))

	delPrepareTestSubtree(t, store)

	got, _, err := stp.createTransactionMapIfNeeded(ctx, block, blockSubtreesMap)
	require.NoError(t, err)
	require.Equal(t, want.Length(), got.Length())

	stp.preparedMu.Lock()
	defer stp.preparedMu.Unlock()
	require.Nil(t, stp.prepared, "a used map is handed over, not kept")
}

// Only a block on the current tip is prepared.
func TestPrepareBlock_IgnoresBlockNotOnTip(t *testing.T) {
	stp, _, block := newPrepareTestStp(t)

	block.Header.HashPrevBlock = &chainhash.Hash{1}
	block.GetHash()
	stp.PrepareBlock(block)

	stp.preparedMu.Lock()
	defer stp.preparedMu.Unlock()
	require.Nil(t, stp.prepared)
}

// A map prepared for one block is never used for another, and is dropped when another block
// moves the tip.
func TestPrepareBlock_OtherBlockDoesNotUsePreparedMap(t *testing.T) {
	ctx := context.Background()
	stp, store, block := newPrepareTestStp(t)

	stp.PrepareBlock(block)
	require.True(t, waitPrepared(t, stp, block.Hash()))

	delPrepareTestSubtree(t, store)

	other := otherBlockOnSameParent(t, block)
	blockSubtreesMap, _ := stp.processBlockSubtrees(other)

	_, _, err := stp.createTransactionMapIfNeeded(ctx, other, blockSubtreesMap)
	require.Error(t, err, "the other block's subtrees must be read, and they are gone")

	stp.preparedMu.Lock()
	defer stp.preparedMu.Unlock()
	require.Nil(t, stp.prepared)
}

// A prepare that failed leaves moveForwardBlock to build the map itself.
func TestPrepareBlock_FailedPrepareFallsBack(t *testing.T) {
	ctx := context.Background()
	stp, store, block := newPrepareTestStp(t)

	delPrepareTestSubtree(t, store)

	stp.PrepareBlock(block)
	require.True(t, waitPrepared(t, stp, block.Hash()))

	putPrepareTestSubtree(t, store)

	blockSubtreesMap, _ := stp.processBlockSubtrees(block)
	got, _, err := stp.createTransactionMapIfNeeded(ctx, block, blockSubtreesMap)
	require.NoError(t, err)
	require.Positive(t, got.Length())
}

// A map prepared over all of a block's subtrees is not used when some of them are this node's
// own, which moveForwardBlock leaves out of the map.
func TestPrepareBlock_NotUsedWhenOwnSubtreesInBlock(t *testing.T) {
	ctx := context.Background()
	stp, store, block := newPrepareTestStp(t)

	// a second subtree in the block, also in the store
	second, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, second.AddNode(chainhash.HashH([]byte("prepare-second")), 1, 1))

	secondBytes, err := second.Serialize()
	require.NoError(t, err)
	require.NoError(t, store.Set(ctx, second.RootHash().CloneBytes(), fileformat.FileTypeSubtree, secondBytes))

	block.Subtrees = append(block.Subtrees, second.RootHash())

	stp.PrepareBlock(block)
	require.True(t, waitPrepared(t, stp, block.Hash()))

	delPrepareTestSubtree(t, store)

	// as if the second subtree were this node's own: only the first is mapped
	blockSubtreesMap := map[chainhash.Hash]int{*block.Subtrees[0]: 0}

	_, _, err = stp.createTransactionMapIfNeeded(ctx, block, blockSubtreesMap)
	require.Error(t, err, "the first subtree must be read, and it is gone")
}
