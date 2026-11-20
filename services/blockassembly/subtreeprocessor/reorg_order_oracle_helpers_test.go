package subtreeprocessor

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// The order oracle checks the order rule of the issue 2269 design (section 5 item 9) on what block
// assembly holds: every input of a transaction resolves on the chain or to an earlier transaction,
// no outpoint has two spenders, no hash appears twice. The rule is prefix-closed, so a sequence that
// passes it passes it on every prefix a miner can be given; the oracle checks the full sequence once
// instead of every prefix.
//
// A differential test between two reorg paths does not replace this check: both paths can share the
// same inversion (design section 5 item 2 c).

type inpointsLookup func(chainhash.Hash) (*subtreepkg.TxInpoints, bool)

// voutZeroInpoints is the inpoints of a transaction that spends output 0 of each parent. The packed
// vout layout is a count word, then that many vouts, per parent.
func voutZeroInpoints(parents ...chainhash.Hash) subtreepkg.TxInpoints {
	vouts := make([]uint32, 0, 2*len(parents))
	for range parents {
		vouts = append(vouts, 1, 0)
	}

	return subtreepkg.NewTxInpointsFromPacked(parents, vouts)
}

// checkAssemblyOrder checks the order rule on seq. inpoints gives each transaction's parents, and
// onChain reports whether a hash's outputs are on the chain the assembly builds on.
func checkAssemblyOrder(seq []subtreepkg.Node, inpoints inpointsLookup, onChain func(chainhash.Hash) bool) error {
	type outpoint struct {
		parent chainhash.Hash
		vout   uint32
	}

	position := make(map[chainhash.Hash]int, len(seq))
	spent := make(map[outpoint]chainhash.Hash, len(seq))

	for i, node := range seq {
		if node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			continue
		}

		if _, dup := position[node.Hash]; dup {
			return errors.NewProcessingError("tx %s appears twice, at %d and %d", node.Hash, position[node.Hash], i)
		}

		position[node.Hash] = i
	}

	for i, node := range seq {
		if node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			continue
		}

		p, ok := inpoints(node.Hash)
		if !ok || p == nil {
			return errors.NewProcessingError("tx %s at %d has no inpoints in the tx map", node.Hash, i)
		}

		for _, in := range p.GetTxInpoints() {
			if !onChain(in.Hash) {
				parentAt, inAssembly := position[in.Hash]

				switch {
				case !inAssembly:
					return errors.NewProcessingError("tx %s at %d spends %s:%d, which is neither on chain nor in assembly", node.Hash, i, in.Hash, in.Index)
				case parentAt >= i:
					return errors.NewProcessingError("tx %s at %d comes before its parent %s at %d", node.Hash, i, in.Hash, parentAt)
				}
			}

			op := outpoint{in.Hash, in.Index}
			if other, taken := spent[op]; taken {
				return errors.NewProcessingError("outpoint %s:%d spent twice, by %s and %s", in.Hash, in.Index, other, node.Hash)
			}

			spent[op] = node.Hash
		}
	}

	return nil
}

// checkSubtreeShape checks the block shape rule (model/Block.go): every subtree but the last has
// the same size and is full. It also recomputes each root from the nodes.
func checkSubtreeShape(subtrees []*subtreepkg.Subtree) error {
	for i, st := range subtrees {
		if i < len(subtrees)-1 {
			if st.Size() != subtrees[0].Size() || st.Length() != st.Size() {
				return errors.NewProcessingError("subtree %d has size %d and length %d; every subtree but the last must be full at size %d", i, st.Size(), st.Length(), subtrees[0].Size())
			}
		}

		rebuilt, err := subtreepkg.NewTreeByLeafCount(st.Size())
		if err != nil {
			return err
		}

		for j, node := range st.Nodes {
			if j == 0 && node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
				err = rebuilt.AddCoinbaseNode()
			} else {
				err = rebuilt.AddNode(node.Hash, node.Fee, node.SizeInBytes)
			}

			if err != nil {
				return errors.NewProcessingError("subtree %d node %d", i, j, err)
			}
		}

		if st.Length() > 0 && !rebuilt.RootHash().Equal(*st.RootHash()) {
			return errors.NewProcessingError("subtree %d root %s does not match its nodes (%s)", i, st.RootHash(), rebuilt.RootHash())
		}
	}

	return nil
}

// assemblyNodes returns what block assembly holds, in order: the chained subtrees, then the
// current subtree.
func assemblyNodes(stp *SubtreeProcessor) []subtreepkg.Node {
	chained := stp.GetChainedSubtrees()

	total := 0
	for _, st := range chained {
		total += st.Length()
	}

	current := stp.GetCurrentSubtree()
	if current != nil {
		total += current.Length()
	}

	nodes := make([]subtreepkg.Node, 0, total)
	for _, st := range chained {
		nodes = append(nodes, st.Nodes...)
	}

	if current != nil {
		nodes = append(nodes, current.Nodes...)
	}

	return nodes
}

// requireAssemblyValid runs both oracles on the processor: the order rule over the whole assembly,
// with inpoints from its tx map, and the shape rule over its chained subtrees.
func requireAssemblyValid(t testing.TB, stp *SubtreeProcessor, onChain func(chainhash.Hash) bool) {
	t.Helper()

	txMap := stp.GetCurrentTxMap()
	require.NoError(t, checkAssemblyOrder(assemblyNodes(stp), txMap.Get, onChain), "assembly order")
	require.NoError(t, checkSubtreeShape(stp.GetChainedSubtrees()), "chained subtree shape")
}
