package blockvalidation

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/stretchr/testify/require"
)

// observingNBitsClient records which parent hashes the expected-nBits lookup was asked about,
// delegating everything else to the real client. It is what makes a test assert that validation
// PERFORMED the difficulty check, rather than only that a block it independently believes to be
// correct was accepted — an assertion that survives deleting the check entirely.
type observingNBitsClient struct {
	blockchain.ClientI

	mu    sync.Mutex
	asked []chainhash.Hash
}

func (c *observingNBitsClient) GetNextWorkRequired(ctx context.Context, blockHash *chainhash.Hash, currentBlockTime int64) (*model.NBit, error) {
	c.mu.Lock()
	c.asked = append(c.asked, *blockHash)
	c.mu.Unlock()

	return c.ClientI.GetNextWorkRequired(ctx, blockHash, currentBlockTime)
}

// timesAskedAbout returns how many times the expected difficulty was looked up for a given parent.
func (c *observingNBitsClient) timesAskedAbout(hash *chainhash.Hash) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	count := 0

	for _, seen := range c.asked {
		if seen.IsEqual(hash) {
			count++
		}
	}

	return count
}

// TestReconsider_BelowCheckpoint_RunsExpectedNBitsAndSucceeds is the executable guard for the caller
// whose behaviour changed when the checkpoint-prefix shortcut was removed
// (bitcoin-sv/teranode#4844).
//
// An operator reconsidering a below-checkpoint block used to skip the expected-nBits rule. It no
// longer does, so that path now calls GetNextWorkRequired against a chain whose ancestors were just
// invalidated. The argument that this is safe is that invalidation takes descendants off the main
// chain but leaves their rows, their parent_id ancestry, their timestamps and their chainwork
// queryable, and that the difficulty walk does not filter invalid rows — so the calculator still has
// the history it needs and a genuinely mined block agrees with it.
//
// That argument is only worth what an execution of it is worth, so this drives the whole thing: two
// genuinely mined post-DAA blocks below the checkpoint, the ancestor subtree invalidated, the
// ancestor reconsidered, and then the descendant put back through the real reconsider pipeline.
//
// The blockchain client is wrapped for that last step, because "a correct block is accepted" is not
// evidence that the check ran — it holds just as well if the check is deleted. The assertion is that
// validation ASKED for the expected difficulty.
func TestReconsider_BelowCheckpoint_RunsExpectedNBitsAndSucceeds(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := checkpointPrefixSettings(t)
	bv, client := newNoPersistHarness(ctx, t, tSettings)

	// Height 1, genuinely mined at the expected difficulty.
	parent := storeHonestPrefixParent(ctx, t, client, tSettings)

	// Height 2, also genuinely mined at the expected difficulty — the block that will be
	// reconsidered, and the one whose nBits the reconsider path now recomputes.
	//
	// One target spacing after its parent: this chain does not allow blocks to be generated
	// quickly, so a timestamp merely equal to the parent's fails the median-time-past rule inside
	// block.Valid and the reconsider would never reach the assertion this test is about.
	childTimestamp := parent.Header.Timestamp + uint32(tSettings.ChainCfgParams.TargetTimePerBlock.Seconds()) //nolint:gosec

	expected, err := client.GetNextWorkRequired(ctx, parent.Hash(), int64(childTimestamp))
	require.NoError(t, err)
	require.NotNil(t, expected)

	childCoinbase := coinbaseAtHeight(t, 2)
	childHdr := minedHeaderWithBits(t, parent.Hash(), childCoinbase.TxIDChainHash(), *expected, childTimestamp)

	child, err := model.NewBlock(childHdr, childCoinbase, []*chainhash.Hash{}, 1, uint64(childCoinbase.Size()), 2, 0)
	require.NoError(t, err)

	require.NoError(t, client.AddBlock(ctx, child, "test",
		blockchainoptions.WithMinedSet(true), blockchainoptions.WithSubtreesSet(true)))

	require.True(t, model.BelowCheckpoint(tSettings.ChainCfgParams.Checkpoints, child.Height),
		"fixture precondition: the reconsidered block sits inside the checkpoint prefix")

	// Invalidate the ANCESTOR, which takes the whole subtree below it off the main chain.
	_, err = client.InvalidateBlock(ctx, parent.Hash())
	require.NoError(t, err)

	_, childMeta, err := client.GetBlockHeader(ctx, child.Hash())
	require.NoError(t, err)
	require.True(t, childMeta.Invalid, "fixture precondition: invalidating the ancestor condemns the descendant")

	// Reconsider the ancestor. RevalidateBlock clears mined_set, so re-stamp it the way the
	// setMined worker would; the descendant's validation waits on the parent's mined state.
	require.NoError(t, client.RevalidateBlock(ctx, parent.Hash()))
	require.NoError(t, client.SetBlockMinedSet(ctx, parent.Hash()))

	// The difficulty inputs must still be reachable after the invalidation, which is the whole
	// claim: the calculator walks parent_id and does not filter invalid rows.
	recomputed, err := client.GetNextWorkRequired(ctx, parent.Hash(), int64(childTimestamp))
	require.NoError(t, err, "the difficulty walk must survive the ancestor invalidation")
	require.Equal(t, expected.String(), recomputed.String(),
		"a genuinely mined block's expected bits must not change because its ancestor was invalidated")

	// The reconsideration must be a fresh validation, not a result replayed from the once-per-block
	// grace window.
	time.Sleep(2 * validationResultGrace)

	// Watch the lookup from here on. Without this the test asserts only that a block it had already
	// convinced itself was correct is accepted — which stays true if the expected-nBits block is
	// deleted outright, and is exactly the mutation this test exists to catch.
	observer := &observingNBitsClient{ClientI: client}
	bv.blockchainClient = observer

	require.Zero(t, observer.timesAskedAbout(parent.Hash()), "nothing has asked yet")

	// Now the real reconsider pipeline, which since this change evaluates expected nBits for this
	// block instead of skipping it.
	err = bv.ValidateBlockWithOptions(ctx, child, "http://localhost", &ValidateBlockOptions{
		IsRevalidation:          true,
		DisableOptimisticMining: true,
	})
	require.NoError(t, err, "a genuinely mined below-checkpoint block must still reconsider cleanly")

	require.GreaterOrEqual(t, observer.timesAskedAbout(parent.Hash()), 1,
		"the reconsider path must actually look up the expected difficulty for this block's parent")

	_, childMeta, err = client.GetBlockHeader(ctx, child.Hash())
	require.NoError(t, err)
	require.False(t, childMeta.Invalid, "the reconsidered block must no longer be marked invalid")
}
