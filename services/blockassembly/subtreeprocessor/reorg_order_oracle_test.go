package subtreeprocessor

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// The order oracle is what every reorg test of issue 2269 trusts, so it is tested on hand-built
// sequences first: it must accept a valid parent-first sequence and reject each kind of breach.

func oracleHash(b byte) chainhash.Hash { return chainhash.Hash{b, 0xa5, b} }

func oracleInpoints(parents ...chainhash.Hash) *subtreepkg.TxInpoints {
	p := voutZeroInpoints(parents...)
	return &p
}

type oracleCase struct {
	seq      []chainhash.Hash
	inpoints map[chainhash.Hash]*subtreepkg.TxInpoints
	onChain  map[chainhash.Hash]struct{}
}

func (c oracleCase) check() error {
	nodes := make([]subtreepkg.Node, len(c.seq))
	for i, h := range c.seq {
		nodes[i] = subtreepkg.Node{Hash: h}
	}

	return checkAssemblyOrder(nodes, func(h chainhash.Hash) (*subtreepkg.TxInpoints, bool) {
		p, ok := c.inpoints[h]
		return p, ok
	}, func(h chainhash.Hash) bool { _, ok := c.onChain[h]; return ok })
}

func TestOrderOracle_AcceptsParentFirst(t *testing.T) {
	chainParent, x, c := oracleHash(1), oracleHash(2), oracleHash(3)

	require.NoError(t, oracleCase{
		seq:      []chainhash.Hash{x, c},
		inpoints: map[chainhash.Hash]*subtreepkg.TxInpoints{x: oracleInpoints(chainParent), c: oracleInpoints(x)},
		onChain:  map[chainhash.Hash]struct{}{chainParent: {}},
	}.check())
}

func TestOrderOracle_RejectsChildBeforeParent(t *testing.T) {
	chainParent, x, c := oracleHash(1), oracleHash(2), oracleHash(3)

	err := oracleCase{
		seq:      []chainhash.Hash{c, x},
		inpoints: map[chainhash.Hash]*subtreepkg.TxInpoints{x: oracleInpoints(chainParent), c: oracleInpoints(x)},
		onChain:  map[chainhash.Hash]struct{}{chainParent: {}},
	}.check()
	require.ErrorContains(t, err, "before its parent")
}

func TestOrderOracle_RejectsUnresolvedParent(t *testing.T) {
	x := oracleHash(2)

	err := oracleCase{
		seq:      []chainhash.Hash{x},
		inpoints: map[chainhash.Hash]*subtreepkg.TxInpoints{x: oracleInpoints(oracleHash(9))},
	}.check()
	require.ErrorContains(t, err, "neither on chain nor in assembly")
}

func TestOrderOracle_RejectsDuplicateHash(t *testing.T) {
	chainParent, x := oracleHash(1), oracleHash(2)

	err := oracleCase{
		seq:      []chainhash.Hash{x, x},
		inpoints: map[chainhash.Hash]*subtreepkg.TxInpoints{x: oracleInpoints(chainParent)},
		onChain:  map[chainhash.Hash]struct{}{chainParent: {}},
	}.check()
	require.ErrorContains(t, err, "twice")
}

func TestOrderOracle_RejectsTwoSpendersOfOneOutpoint(t *testing.T) {
	chainParent, x, y := oracleHash(1), oracleHash(2), oracleHash(3)

	err := oracleCase{
		seq:      []chainhash.Hash{x, y},
		inpoints: map[chainhash.Hash]*subtreepkg.TxInpoints{x: oracleInpoints(chainParent), y: oracleInpoints(chainParent)},
		onChain:  map[chainhash.Hash]struct{}{chainParent: {}},
	}.check()
	require.ErrorContains(t, err, "spent twice")
}

func TestOrderOracle_RejectsMissingInpoints(t *testing.T) {
	err := oracleCase{seq: []chainhash.Hash{oracleHash(2)}}.check()
	require.ErrorContains(t, err, "no inpoints")
}

func TestOrderOracle_SkipsCoinbasePlaceholder(t *testing.T) {
	chainParent, x := oracleHash(1), oracleHash(2)

	require.NoError(t, oracleCase{
		seq:      []chainhash.Hash{subtreepkg.CoinbasePlaceholderHashValue, x},
		inpoints: map[chainhash.Hash]*subtreepkg.TxInpoints{x: oracleInpoints(chainParent)},
		onChain:  map[chainhash.Hash]struct{}{chainParent: {}},
	}.check())
}

func TestSubtreeShapeOracle(t *testing.T) {
	build := func(n int) *subtreepkg.Subtree {
		st, err := subtreepkg.NewTreeByLeafCount(4)
		require.NoError(t, err)

		for i := range n {
			require.NoError(t, st.AddNode(oracleHash(byte(10+i)), 1, 1))
		}

		return st
	}

	require.NoError(t, checkSubtreeShape([]*subtreepkg.Subtree{build(4), build(4), build(2)}))
	require.ErrorContains(t, checkSubtreeShape([]*subtreepkg.Subtree{build(4), build(3), build(4)}), "size")
}
