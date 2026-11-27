package subtreeprocessor

import (
	"context"
	"runtime"
	"sort"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"golang.org/x/sync/errgroup"
)

// nodeRuns reads several node slices as one list, in place.
type nodeRuns struct {
	runs   [][]subtreepkg.Node
	starts []int // starts[i] is the list position of runs[i][0]
	total  int
}

func newNodeRuns(runs [][]subtreepkg.Node) *nodeRuns {
	r := &nodeRuns{}

	for _, run := range runs {
		if len(run) == 0 {
			continue
		}

		r.runs = append(r.runs, run)
		r.starts = append(r.starts, r.total)
		r.total += len(run)
	}

	return r
}

// each calls fn with the nodes at list positions [lo, hi), in order, and stops at the first error.
func (r *nodeRuns) each(lo, hi int, fn func(subtreepkg.Node) error) error {
	// the last run that starts at or before lo
	i := sort.Search(len(r.starts), func(i int) bool { return r.starts[i] > lo }) - 1

	for pos := lo; pos < hi; i++ {
		run := r.runs[i]

		for _, node := range run[pos-r.starts[i] : min(len(run), hi-r.starts[i])] {
			if err := fn(node); err != nil {
				return err
			}
		}

		pos = r.starts[i] + len(run)
	}

	return nil
}

// streamBuildSubtrees is bulkBuildSubtrees for nodes that sit in several slices: it reads them in place
// and copies each node once, into the subtree that takes it, on up to GOMAXPROCS goroutines at a time,
// where bulkBuildSubtrees needs them copied into one slice first (issue 2269, PR 9). Every subtree gets
// the same contiguous run of the list as there, so the leaf order, the subtrees and the bookkeeping it
// leaves are the same. The subtrees are installed once all are built, so a failure installs none of them
// and retires the ones it built.
func (stp *SubtreeProcessor) streamBuildSubtrees(ctx context.Context, runs [][]subtreepkg.Node, subtreeSize int) error {
	list := newNodeRuns(runs)
	if list.total == 0 {
		return nil
	}

	currentSt := stp.currentSubtree.Load()
	if currentSt == nil {
		return errors.NewProcessingError("[streamBuildSubtrees] currentSubtree is nil")
	}

	type chunk struct {
		st     *subtreepkg.Subtree
		lo, hi int
		full   bool // the subtree is complete once it holds the chunk
	}

	// the open subtree takes what it has room for (none, if it is full already, and it is chained as
	// it is), then whole subtrees, then a partial one
	fill := min(max(subtreeSize-len(currentSt.Nodes), 0), list.total)
	chunks := []chunk{{st: currentSt, lo: 0, hi: fill, full: len(currentSt.Nodes)+fill == currentSt.Size()}}
	pos := fill

	for ; pos < list.total; pos += subtreeSize {
		hi := min(pos+subtreeSize, list.total)
		chunks = append(chunks, chunk{lo: pos, hi: hi, full: hi-pos == subtreeSize})
	}

	// built is what this call allocated, to retire if it fails
	var built []*subtreepkg.Subtree

	fail := func(err error) error {
		stp.retireSubtrees(built...)

		return err
	}

	for i := range chunks {
		if chunks[i].st == nil {
			st, err := stp.newSubtree(subtreeSize)
			if err != nil {
				return fail(err)
			}

			chunks[i].st = st
			built = append(built, st)
		}
	}

	// Bound the fan-out, as bulkBuildSubtrees does: with small subtrees a large reorg yields thousands.
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(runtime.GOMAXPROCS(0))

	for i := range chunks {
		c := chunks[i]

		g.Go(func() error {
			if err := gCtx.Err(); err != nil {
				return err
			}

			if err := list.each(c.lo, c.hi, func(node subtreepkg.Node) error {
				return c.st.AddNode(node.Hash, node.Fee, node.SizeInBytes)
			}); err != nil {
				return err
			}

			if c.full {
				// Hash it here: RootHash() caches without a lock, and once the chain is handed to
				// storage the storage pipeline would hash it concurrently with the processor.
				_ = c.st.RootHash()
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fail(err)
	}

	var open *subtreepkg.Subtree

	for _, c := range chunks {
		if !c.full {
			open = c.st

			continue
		}

		stp.chainedSubtrees = append(stp.chainedSubtrees, c.st)

		if stp.diskTxMap != nil {
			stp.diskTxMap.recordErr(stp.diskTxMap.UpdateSubtreeIndexBatch(c.st.Nodes, int16(len(stp.chainedSubtrees)))) //nolint:gosec // as bulkBuildSubtrees
		}
	}

	if open == nil {
		// every node went into whole subtrees: there must be somewhere to add the next one
		st, err := stp.newSubtree(subtreeSize)
		if err != nil {
			// the whole subtrees are chained already; the caller restores its own and retires them
			return err
		}

		open = st
	}

	stp.currentSubtree.Store(open)
	stp.updateChainedSubtreeCounts()

	return nil
}

// withoutCoinbasePlaceholder returns nodes without the coinbase placeholder, which only the first node
// of a subtree can be.
func withoutCoinbasePlaceholder(nodes []subtreepkg.Node) []subtreepkg.Node {
	if len(nodes) > 0 && nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
		return nodes[1:]
	}

	return nodes
}
